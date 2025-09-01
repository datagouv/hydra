import csv as stdcsv
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd
from asyncpg import Record
from csv_detective import routine as csv_detective_routine
from csv_detective import validate_then_detect
from csv_detective.detection.engine import engine_to_file
from progressist import ProgressBar
from slugify import slugify
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
from sqlalchemy.schema import CreateIndex, CreateTable, Index

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.analysis.geojson import csv_to_geojson_and_pmtiles
from udata_hydra.db import compute_insert_query
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    detect_tabular_from_headers,
    handle_parse_exception,
    remove_remainders,
)
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
    "datetime_aware": DateTime(timezone=True),
}

RESERVED_COLS = ("__id", "cmin", "cmax", "collation", "ctid", "tableoid", "xmin", "xmax")
minio_client = MinIOClient(bucket=config.MINIO_PARQUET_BUCKET, folder=config.MINIO_PARQUET_FOLDER)


async def analyse_csv(
    check: dict,
    file_path: str | None = None,
    debug_insert: bool = False,
) -> None:
    """Launch csv analysis from a check or an URL (debug), using previously downloaded file at file_path if any"""
    if not config.CSV_ANALYSIS:
        log.debug("CSV_ANALYSIS turned off, skipping.")
        return

    resource_id: str = str(check["resource_id"])
    url = check["url"]

    # Update resource status to ANALYSING_CSV
    resource: Record | None = await Resource.update(resource_id, {"status": "ANALYSING_CSV"})

    # Check if the resource is in the exceptions table
    # If it is, get the table_indexes to use them later
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)
    table_indexes: dict | None = None
    if exception and exception.get("table_indexes"):
        table_indexes = json.loads(exception["table_indexes"])

    timer = Timer("analyse-csv", resource_id)
    assert any(_ is not None for _ in (check["id"], url))

    table_name, tmp_file = None, None
    try:
        _, file_format = detect_tabular_from_headers(check)
        tmp_file = await helpers.read_or_download_file(
            check=check,
            file_path=file_path,
            file_format=file_format,
            exception=exception,
        )
        table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
        timer.mark("download-file")

        check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})

        # Launch csv-detective against given file
        try:
            previous_analysis: dict | None = await get_previous_analysis(resource_id=resource_id)
            if previous_analysis:
                await Resource.update(resource_id, {"status": "VALIDATING_CSV"})
                csv_inspection, df = validate_then_detect(
                    file_path=tmp_file.name,
                    previous_analysis=previous_analysis,
                    output_profile=True,
                    output_df=True,
                    cast_json=False,
                    num_rows=-1,
                    save_results=False,
                )
            else:
                csv_inspection, df = csv_detective_routine(
                    file_path=tmp_file.name,
                    output_profile=True,
                    output_df=True,
                    cast_json=False,
                    num_rows=-1,
                    save_results=False,
                )
        except Exception as e:
            raise ParseException(
                message=str(e),
                step="csv_detective",
                resource_id=resource_id,
                url=url,
                check_id=check["id"],
            ) from e
        timer.mark("csv-inspection")

        await csv_to_db(
            df=df,
            inspection=csv_inspection,
            table_name=table_name,
            table_indexes=table_indexes,
            resource_id=resource_id,
            debug_insert=debug_insert,
        )
        check = await Check.update(check["id"], {"parsing_table": table_name})
        timer.mark("csv-to-db")

        try:
            await csv_to_parquet(
                df=df,
                inspection=csv_inspection,
                resource_id=resource_id,
                check_id=check["id"],
            )
            timer.mark("csv-to-parquet")
        except Exception as e:
            remove_remainders(resource_id, ["parquet"])
            raise ParseException(
                message=str(e),
                step="parquet_export",
                resource_id=resource_id,
                url=url,
                check_id=check["id"],
            ) from e

        try:
            await csv_to_geojson_and_pmtiles(
                df=df,
                inspection=csv_inspection,
                resource_id=resource_id,
                check_id=check["id"],
            )
            timer.mark("csv-to-geojson-pmtiles")
        except Exception as e:
            remove_remainders(resource_id, ["geojson", "pmtiles", "pmtiles-journal"])
            raise ParseException(
                message=str(e),
                step="geojson_export",
                resource_id=resource_id,
                url=url,
                check_id=check["id"],
            ) from e

        check = await Check.update(
            check["id"],
            {
                "parsing_finished_at": datetime.now(timezone.utc),
            },
        )
        await csv_to_db_index(table_name, csv_inspection, check)

    except (ParseException, IOException) as e:
        await handle_parse_exception(e, table_name, check)
    finally:
        await helpers.notify_udata(resource, check)
        timer.stop()
        if tmp_file is not None:
            tmp_file.close()
            os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})


async def get_previous_analysis(resource_id: str) -> dict | None:
    def match_columns(table_columns: list, analysis_columns: list) -> dict | None:
        """If a column name is too long for postgres (>60 characters) it gets truncated in the table"""
        # retrieving the columns that match exactly between table and analysis
        matching = {col: col for col in table_columns if col in analysis_columns}
        # early stop if all columns match perfectly
        if len(matching) == len(table_columns):
            return matching
        # matching truncated columns in table with actual label
        for col in table_columns:
            if col in matching:
                continue
            for label in analysis_columns:
                if label.startswith(col):
                    matching[col] = label
                    break
        return matching if len(matching) == len(table_columns) else None

    db = await context.pool("csv")
    q = (
        "SELECT parsing_table, csv_detective FROM tables_index "
        f"WHERE resource_id='{resource_id}' ORDER BY created_at DESC LIMIT 1"
    )
    res = await db.fetch(q)
    if not res:
        return None
    analysis = json.loads(res[0]["csv_detective"])
    # the csv_detective column is JSONB, so keys are reordered compared to the actual table
    # so we get the right order from the table
    # landing here we can safely assume that the table exists
    rows: list[Record] = await db.fetch(f'SELECT * FROM "{res[0]["parsing_table"]}" LIMIT 1')
    # the __id column is generated by hydra, not natively in the data
    table_columns = [col for col in rows[0].keys() if col != "__id"]
    matching = match_columns(table_columns, list(analysis["columns"].keys()))
    if matching is None:
        return None
    analysis["columns"] = {
        matching[col]: analysis["columns"][matching[col]] for col in table_columns
    }
    return analysis


def compute_create_table_query(
    table_name: str, columns: dict, indexes: dict[str, str] | None = None
) -> str:
    """Use sqlalchemy to build a CREATE TABLE statement that should not be vulnerable to injections"""

    metadata = MetaData()
    table = Table(table_name, metadata, Column("__id", Integer, primary_key=True))

    for col_name, col_type in columns.items():
        table.append_column(Column(col_name, PYTHON_TYPE_TO_PG.get(col_type, String)))

    indexes_labels = {}
    if indexes:
        for col_name, index_type in indexes.items():
            if index_type not in config.SQL_INDEXES_TYPES_SUPPORTED:
                log.error(
                    f'Index type "{index_type}" is unknown or not supported yet! '
                    f"Index for column {col_name} was not created."
                )
                continue

            else:
                if index_type == "index":
                    index_name = f"{table_name}_{slugify(col_name)}_idx"
                    indexes_labels[index_name] = {"column": col_name, "type": index_type}
                    try:
                        table.append_constraint(Index(index_name, col_name))
                    except KeyError:
                        raise KeyError(
                            f'Error creating index "{index_name}" on column "{col_name}". '
                            f'Does the column "{col_name}" exist in the table?'
                        )
                # TODO: other index types. Not easy with sqlalchemy, maybe use raw sql?

    compiled_query = CreateTable(table).compile(dialect=asyncpg.dialect())
    query: str = compiled_query.string

    # Add the index creation queries to the main query
    for index in table.indexes:
        log.debug(
            f'Creating {indexes_labels[index.name]["type"]} on column "{indexes_labels[index.name]["column"]}"'
        )
        query_idx = CreateIndex(index).compile(dialect=asyncpg.dialect())
        query: str = query + ";" + query_idx.string

    # compiled query will want to write "%% mon pourcent" VARCHAR but will fail when querying "% mon pourcent"
    # also, "% mon pourcent" works well in pg as a column
    # TODO: dirty hack, maybe find an alternative
    query = query.replace("%%", "%")
    return query


def generate_records(df: pd.DataFrame) -> Iterator[list]:
    # pandas cannot have None in columns typed as int so we have to cast
    # NaN-int values to None for db insertion, and we also change NaN to None
    for row in df.values:
        yield tuple(cell if not pd.isna(cell) else None for cell in row)


async def csv_to_parquet(
    df: pd.DataFrame,
    inspection: dict,
    resource_id: str | None = None,
    check_id: int | None = None,
) -> tuple[str, int] | None:
    """
    Convert a csv file to parquet using inspection data.

    Args:
        file_path: CSV file path to convert.
        inspection: CSV detective report.
        table_name: used to name the parquet file.

    Returns:
        parquet_url: URL of the parquet file.
        parquet_size: size of the parquet file.
    """
    if not config.CSV_TO_PARQUET:
        log.debug("CSV_TO_PARQUET turned off, skipping parquet export.")
        return

    if int(inspection.get("total_lines", 0)) < config.MIN_LINES_FOR_PARQUET:
        log.debug(
            f"Skipping parquet export for {resource_id} because it has less than {config.MIN_LINES_FOR_PARQUET} lines."
        )
        return

    log.debug(
        f"Converting from {engine_to_file.get(inspection.get('engine', ''), 'CSV')} "
        f"to parquet for {resource_id} and sending to Minio."
    )

    if resource_id:
        # Update resource status to CONVERTING_TO_PARQUET
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PARQUET"})

    # save the file as parquet and store it on Minio instance
    parquet_file, _ = save_as_parquet(
        df=df,
        output_filename=resource_id,
    )
    parquet_size: int = os.path.getsize(parquet_file)
    parquet_url: str = minio_client.send_file(parquet_file)
    await Check.update(
        check_id,
        {
            "parquet_url": parquet_url,
            "parquet_size": parquet_size,
        },
    )
    # returning only for tests purposes
    return parquet_url, parquet_size


async def csv_to_db(
    df: pd.DataFrame,
    inspection: dict,
    table_name: str,
    table_indexes: dict[str, str] | None = None,
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
        f"{c}__hydra_renamed" if c.lower() in RESERVED_COLS else c: helpers.get_python_type(v)
        for c, v in inspection["columns"].items()
    }

    q = f'DROP TABLE IF EXISTS "{table_name}"'
    db = await context.pool("csv")
    await db.execute(q)

    # Create table
    q = compute_create_table_query(table_name=table_name, columns=columns, indexes=table_indexes)
    try:
        await db.execute(q)
    except Exception as e:
        raise ParseException(
            message=str(e),
            step="create_table_query",
            resource_id=resource_id,
            table_name=table_name,
        ) from e

    # this use postgresql COPY from an iterator, it's fast but might be difficult to debug
    if not debug_insert:
        # NB: also see copy_to_table for a file source
        try:
            await db.copy_records_to_table(
                table_name,
                records=generate_records(df),
                columns=list(columns.keys()),
            )
        except Exception as e:  # I know what I'm doing, pinky swear
            raise ParseException(
                message=str(e),
                step="copy_records_to_table",
                resource_id=resource_id,
                table_name=table_name,
            ) from e
    # this inserts rows from iterator one by one, slow but useful for debugging
    else:
        bar = ProgressBar(total=inspection["total_lines"])
        for r in bar.iter(generate_records(df)):
            data = {k: v for k, v in zip(df.columns, r)}
            # NB: possible sql injection here, but should not be used in prod
            q = compute_insert_query(table_name=table_name, data=data, returning="__id")
            await db.execute(q, *data.values())


async def csv_to_db_index(table_name: str, inspection: dict, check: Record) -> None:
    """Store meta info about a converted CSV table in `DATABASE_URL_CSV.tables_index`"""
    db = await context.pool("csv")
    q = "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, url) VALUES($1, $2, $3, $4)"
    await db.execute(
        q,
        table_name,
        json.dumps(inspection, default=str),
        check.get("resource_id"),
        check.get("url"),
    )


async def delete_table(table_name: str) -> None:
    pool = await context.pool("csv")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            await conn.execute("DELETE FROM tables_index WHERE parsing_table = $1", table_name)
