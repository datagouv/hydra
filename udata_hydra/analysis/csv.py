import csv as stdcsv
import hashlib
import json
import logging
import os
import pytz
import sys

from datetime import datetime
from typing import Any

import sentry_sdk

from csv_detective.explore_csv import routine as csv_detective_routine
from progressist import ProgressBar
from sqlalchemy import (
    MetaData, Table, Column,
    BigInteger, String, Float, Boolean, Integer, JSON, Date, DateTime,
)
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.schema import CreateTable
from str2bool import str2bool
from str2float import str2float

from udata_hydra import context, config
from udata_hydra.analysis import helpers
from udata_hydra.analysis.errors import ParseException
from udata_hydra.utils.db import get_check, compute_insert_query, update_check
from udata_hydra.utils.file import download_resource
from udata_hydra.utils.timer import Timer


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


async def analyse_csv(check_id: int = None, url: str = None, file_path: str = None, debug_insert: bool = False) -> None:
    """Launch csv analysis from a check or an URL (debug), using previsously downloaded file at file_path if any"""
    if not config.CSV_ANALYSIS_ENABLED:
        log.debug("CSV_ANALYSIS_ENABLED turned off, skipping.")
        return

    timer = Timer("analyse-csv")
    assert any(_ is not None for _ in (check_id, url))
    check = await get_check(check_id) if check_id is not None else {}
    url = check.get("url") or url

    headers = json.loads(check.get("headers") or "{}")
    tmp_file = open(file_path, "rb") if file_path else await download_resource(url, headers)
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    timer.mark("download-file")

    try:
        if check_id:
            await update_check(check_id, {"parsing_started_at": datetime.now(pytz.UTC)})
        csv_inspection = await perform_csv_inspection(tmp_file.name)
        timer.mark("csv-inspection")
        await csv_to_db(tmp_file.name, csv_inspection, table_name, debug_insert=debug_insert)
        timer.mark("csv-to-db")
        if check_id:
            await update_check(check_id, {
                "parsing_table": table_name,
                "parsing_finished_at": datetime.now(pytz.UTC),
            })
        await csv_to_db_index(table_name, csv_inspection, check)
    except ParseException as e:
        await handle_parse_exception(e, check_id, table_name)
    finally:
        timer.stop()
        tmp_file.close()
        os.remove(tmp_file.name)


def generate_dialect(inspection: dict) -> stdcsv.Dialect:
    class CustomDialect(stdcsv.unix_dialect):
        # TODO: it would be nice to have more info from csvdetective to feed the dialect
        # in the meantime we might want to sniff the file a bit
        delimiter = inspection["separator"]
    return CustomDialect()


def smart_cast(_type, value, failsafe=False) -> Any:
    try:
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


async def csv_to_db(file_path: str, inspection: dict, table_name: str, debug_insert: bool = False):
    """
    Convert a csv file to database table using inspection data. It should (re)create one table:
    - `table_name` with data from `file_path`

    :file_path: CSV file path to convert
    :inspection: CSV detective report
    :table_name: used to create tables
    :debug_insert: insert record one by one instead of using postgresql COPY
    """
    log.debug(f"Converting from CSV to db for {table_name}")
    dialect = generate_dialect(inspection)
    columns = inspection["columns"]
    # build a `column_name: type` mapping and explicitely rename reserved column names
    columns = {
        f"{c}__hydra_renamed" if c.lower() in RESERVED_COLS else c: v["python_type"]
        for c, v in columns.items()
    }
    q = f'DROP TABLE IF EXISTS "{table_name}"'
    db = await context.pool("csv")
    await db.execute(q)
    q = compute_create_table_query(table_name, columns)
    await db.execute(q)
    with open(file_path, encoding=inspection["encoding"]) as f:
        reader = stdcsv.reader(f, dialect=dialect)
        # pop header row(s)
        for _ in range(inspection["header_row_idx"] + 1):
            f.readline()
        # this is an iterator! noice.
        records = (
            [
                smart_cast(t, v, failsafe=True)
                for t, v in zip(columns.values(), line)
            ]
            for line in reader if line
        )
        # this use postgresql COPY from an iterator, it's fast but might be difficult to debug
        if not debug_insert:
            # NB: also see copy_to_table for a file source
            try:
                await db.copy_records_to_table(table_name, records=records, columns=columns.keys())
            except Exception as e:  # I know what I'm doing, pinky swear
                raise ParseException("copy_records_to_table") from e
        # this inserts rows from iterator one by one, slow but useful for debugging
        else:
            bar = ProgressBar(total=inspection["total_lines"])
            for r in bar.iter(records):
                data = {k: v for k, v in zip(columns.keys(), r)}
                # NB: possible sql injection here, but should not be used in prod
                q = compute_insert_query(data, table_name, returning="__id")
                await db.execute(q, *data.values())


async def csv_to_db_index(table_name: str, inspection: dict, check: dict):
    """Store meta info about a converted CSV table in `DATABASE_URL_CSV.tables_index`"""
    db = await context.pool("csv")
    q = "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, url) VALUES($1, $2, $3, $4)"
    await db.execute(q, table_name, json.dumps(inspection), check.get("resource_id"), check.get("url"))


async def perform_csv_inspection(file_path):
    """Launch csv-detective against given file"""
    try:
        return csv_detective_routine(file_path, output_profile=True, num_rows=-1)
    except Exception as e:
        raise ParseException("csv_detective") from e


async def delete_table(table_name: str):
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
            check = await get_check(check_id)
            url = check["url"]
            with sentry_sdk.push_scope() as scope:
                scope.set_extra("check_id", check_id)
                scope.set_extra("csv_url", url)
                scope.set_extra("resource_id", check["resource_id"])
                event_id = sentry_sdk.capture_exception(e)
        # e.__cause__ let us access the "inherited" error of ParseException (raise e from cause)
        # it's called explicit exception chaining and it's very cool, look it up (PEP 3134)!
        err = f"{e.step}:sentry:{event_id}" if config.SENTRY_DSN else f"{e.step}:{str(e.__cause__)}"
        await update_check(check_id, {"parsing_error": err, "parsing_finished_at": datetime.now(pytz.UTC)})
        log.error("Parsing error", exc_info=e)
    else:
        raise e
