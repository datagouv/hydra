import csv as stdcsv
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Iterator

import sentry_sdk
from asyncpg import Record
from csv_detective.detection import engine_to_file
from csv_detective.explore_csv import routine as csv_detective_routine
from progressist import ProgressBar
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.schema import CreateTable
from str2bool import str2bool
from str2float import str2float

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.analysis.errors import ParseException
from udata_hydra.db import compute_insert_query
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import Reader, Timer, download_resource, queue, send
from udata_hydra.utils.minio import MinIOClient
from udata_hydra.utils.parquet import save_as_parquet

log = logging.getLogger("udata-hydra")

# Increase CSV field size limit to maximum possible
# https://stackoverflow.com/a/15063941
field_size_limit = sys.maxsize
while True:
    try:
        stdcsv.field_size_limit(field_size_limit)
        break
    except OverflowError:
        field_size_limit = int(field_size_limit / 10)

PYTHON_TYPE_TO_PG = {
    "string": String,
    "float": Float,
    "int": BigInteger,
    "bool": Boolean,
    "json": JSON,
    "date": Date,
    "datetime": DateTime,
}

PYTHON_TYPE_TO_PY = {
    "string": str,
    "float": float,
    "int": int,
    "bool": bool,
    "json": helpers.to_json,
    "date": helpers.to_date,
    "datetime": helpers.to_datetime,
}

RESERVED_COLS = ("__id", "tableoid", "xmin", "cmin", "xmax", "cmax", "ctid")
minio_client = MinIOClient()


async def notify_udata(check_id: int, table_name: str) -> None:
    """Notify udata of the result of a parsing"""
    check: Record | None = await Check.get_by_id(check_id, with_deleted=True)
    resource_id = check["resource_id"]
    db = await context.pool()
    record = await db.fetchrow("SELECT dataset_id FROM catalog WHERE resource_id = $1", resource_id)
    if record:
        payload = {
            "resource_id": resource_id,
            "dataset_id": record["dataset_id"],
            "document": {
                "analysis:parsing:error": check["parsing_error"],
                "analysis:parsing:started_at": check["parsing_started_at"].isoformat()
                if check["parsing_started_at"]
                else None,
                "analysis:parsing:finished_at": check["parsing_finished_at"].isoformat()
                if check["parsing_finished_at"]
                else None,
            },
        }
        if config.CSV_TO_PARQUET:
            payload["parquet_id"] = table_name
        queue.enqueue(send, _priority="high", **payload)


async def analyse_csv(
    check_id: int | None = None,
    url: str | None = None,
    file_path: str | None = None,
    debug_insert: bool = False,
) -> None:
    """Launch csv analysis from a check or an URL (debug), using previously downloaded file at file_path if any"""
    if not config.CSV_ANALYSIS:
        log.debug("CSV_ANALYSIS turned off, skipping.")
        return

    # Get check and resource_id
    check: Record | None = (
        await Check.get_by_id(check_id, with_deleted=True) if check_id is not None else {}
    )
    resource_id: str = check.get("resource_id")

    # Update resource status to ANALYSING_CSV
    await Resource.update(resource_id, {"status": "ANALYSING_CSV"})

    exceptions = config.LARGE_RESOURCES_EXCEPTIONS

    timer = Timer("analyse-csv")
    assert any(_ is not None for _ in (check_id, url))
    url: str = check.get("url") or url
    exception_file = str(check.get("resource_id", "")) in exceptions

    headers = json.loads(check.get("headers") or "{}")
    tmp_file = (
        open(file_path, "rb")
        if file_path
        else await download_resource(
            url=url,
            headers=headers,
            max_size_allowed=None if exception_file else int(config.MAX_FILESIZE_ALLOWED["csv"]),
        )
    )
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    timer.mark("download-file")

    try:
        if check_id:
            await Check.update(check_id, {"parsing_started_at": datetime.now(timezone.utc)})
        csv_inspection = await perform_csv_inspection(tmp_file.name)
        timer.mark("csv-inspection")

        await csv_to_db(
            file_path=tmp_file.name,
            inspection=csv_inspection,
            table_name=table_name,
            resource_id=resource_id,
            debug_insert=debug_insert,
        )
        timer.mark("csv-to-db")

        await csv_to_parquet(
            file_path=tmp_file.name,
            inspection=csv_inspection,
            table_name=table_name,
            resource_id=resource_id,
        )
        timer.mark("csv-to-parquet")

        if check_id:
            await Check.update(
                check_id,
                {
                    "parsing_table": table_name,
                    "parsing_finished_at": datetime.now(timezone.utc),
                },
            )
        await csv_to_db_index(table_name, csv_inspection, check)

    except ParseException as e:
        await handle_parse_exception(e, check_id, table_name)
    finally:
        if check_id:
            await notify_udata(check_id, table_name)
        timer.stop()
        tmp_file.close()
        os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})


def smart_cast(_type: str, value, failsafe: bool = False) -> Any:
    try:
        if value is None or value == "":
            return None
        if _type == "bool":
            return str2bool(value)
        return PYTHON_TYPE_TO_PY[_type](value)
    except ValueError as e:
        if _type == "int":
            _value = str2float(value, default=None)
            if _value:
                return int(_value)
        elif _type == "float":
            return str2float(value, default=None)
        if not failsafe:
            raise e
        log.warning(f'Could not convert "{value}" to {_type}, defaulting to null')
        return None


def compute_create_table_query(table_name: str, columns: list) -> str:
    """Use sqlalchemy to build a CREATE TABLE statement that should not be vulnerable to injections"""
    metadata = MetaData()
    table = Table(table_name, metadata, Column("__id", Integer, primary_key=True))
    for col_name, col_type in columns.items():
        table.append_column(Column(col_name, PYTHON_TYPE_TO_PG.get(col_type, String)))
    compiled = CreateTable(table).compile(dialect=asyncpg.dialect())
    # compiled query will want to write "%% mon pourcent" VARCHAR but will fail when querying "% mon pourcent"
    # also, "% mon pourcent" works well in pg as a column
    # TODO: dirty hack, maybe find an alternative
    return compiled.string.replace("%%", "%")


def generate_records(file_path: str, inspection: dict, columns: dict) -> Iterator[list]:
    # because we need the iterator twice, not possible to
    # handle parquet and db through the same iteration
    with Reader(file_path, inspection) as reader:
        for line in reader:
            if line:
                yield [smart_cast(t, v, failsafe=True) for t, v in zip(columns.values(), line)]


async def csv_to_parquet(
    file_path: str,
    inspection: dict,
    table_name: str,
    resource_id: str | None = None,
) -> None:
    """
    Convert a csv file to parquet using inspection data.

    :file_path: CSV file path to convert
    :inspection: CSV detective report
    :table_name: used to name the parquet file
    """
    if not config.CSV_TO_PARQUET:
        log.debug("CSV_TO_PARQUET turned off, skipping parquet export.")
        return

    log.debug(
        f"Converting from {engine_to_file.get(inspection.get('engine', ''), 'CSV')} "
        f"to parquet for {table_name} and sending to Minio."
    )

    if resource_id:
        # Update resource status to CONVERTING_TO_PARQUET
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PARQUET"})

    columns = {c: v["python_type"] for c, v in inspection["columns"].items()}
    # save the file as parquet and store it on Minio instance
    parquet_file, _ = save_as_parquet(
        records=generate_records(file_path, inspection, columns),
        columns=columns,
        output_name=table_name,
    )
    minio_client.send_file(parquet_file)


async def csv_to_db(
    file_path: str,
    inspection: dict,
    table_name: str,
    resource_id: str | None = None,
    debug_insert: bool = False,
) -> None:
    """
    Convert a csv file to database table using inspection data. It should (re)create one table:
    - `table_name` with data from `file_path`

    :file_path: CSV file path to convert
    :inspection: CSV detective report
    :table_name: used to create tables
    :debug_insert: insert record one by one instead of using postgresql COPY
    """
    if not config.CSV_TO_DB:
        log.debug("CSV_TO_DB turned off, skipping.")
        return

    log.debug(
        f"Converting from {engine_to_file.get(inspection.get('engine', ''), 'CSV')} "
        f"to db for {table_name}"
    )

    if resource_id:
        # Update resource status to INSERTING_IN_DB
        await Resource.update(resource_id, {"status": "INSERTING_IN_DB"})

    # build a `column_name: type` mapping and explicitely rename reserved column names
    columns = {
        f"{c}__hydra_renamed" if c.lower() in RESERVED_COLS else c: v["python_type"]
        for c, v in inspection["columns"].items()
    }
    q = f'DROP TABLE IF EXISTS "{table_name}"'
    db = await context.pool("csv")
    await db.execute(q)
    q = compute_create_table_query(table_name, columns)
    await db.execute(q)
    # this use postgresql COPY from an iterator, it's fast but might be difficult to debug
    if not debug_insert:
        # NB: also see copy_to_table for a file source
        try:
            await db.copy_records_to_table(
                table_name,
                records=generate_records(file_path, inspection, columns),
                columns=columns.keys(),
            )
        except Exception as e:  # I know what I'm doing, pinky swear
            raise ParseException("copy_records_to_table") from e
    # this inserts rows from iterator one by one, slow but useful for debugging
    else:
        bar = ProgressBar(total=inspection["total_lines"])
        for r in bar.iter(generate_records(file_path, inspection, columns)):
            data = {k: v for k, v in zip(columns.keys(), r)}
            # NB: possible sql injection here, but should not be used in prod
            q = compute_insert_query(table_name=table_name, data=data, returning="__id")
            await db.execute(q, *data.values())


async def csv_to_db_index(table_name: str, inspection: dict, check: dict) -> None:
    """Store meta info about a converted CSV table in `DATABASE_URL_CSV.tables_index`"""
    db = await context.pool("csv")
    q = "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, url) VALUES($1, $2, $3, $4)"
    await db.execute(
        q,
        table_name,
        json.dumps(inspection),
        check.get("resource_id"),
        check.get("url"),
    )


async def perform_csv_inspection(file_path: str) -> dict | None:
    """Launch csv-detective against given file"""
    try:
        return csv_detective_routine(
            file_path, output_profile=True, num_rows=-1, save_results=False
        )
    except Exception as e:
        raise ParseException("csv_detective") from e


async def delete_table(table_name: str) -> None:
    db = await context.pool("csv")
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    await db.execute("DELETE FROM tables_index WHERE parsing_table = $1", table_name)


async def handle_parse_exception(e: Exception, check_id: int, table_name: str) -> None:
    """Specific ParsingError handling. Enriches sentry w/ context if available,
    and store error if in a check context. Also cleanup :table_name: if needed."""
    db = await context.pool("csv")
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    if check_id:
        if config.SENTRY_DSN:
            check: Record | None = await Check.get_by_id(check_id, with_deleted=True)
            url = check["url"]
            with sentry_sdk.push_scope() as scope:
                scope.set_extra("check_id", check_id)
                scope.set_extra("csv_url", url)
                scope.set_extra("resource_id", check["resource_id"])
                event_id = sentry_sdk.capture_exception(e)
        # e.__cause__ let us access the "inherited" error of ParseException (raise e from cause)
        # it's called explicit exception chaining and it's very cool, look it up (PEP 3134)!
        err = f"{e.step}:sentry:{event_id}" if config.SENTRY_DSN else f"{e.step}:{str(e.__cause__)}"
        await Check.update(
            check_id,
            {"parsing_error": err, "parsing_finished_at": datetime.now(timezone.utc)},
        )
        log.error("Parsing error", exc_info=e)
    else:
        raise e
