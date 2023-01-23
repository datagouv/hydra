import csv
import hashlib
import json
import logging
import os
import sys

from datetime import datetime
from typing import Any

from csv_detective.explore_csv import routine as csv_detective_routine
from progressist import ProgressBar
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, BigInteger, String, Float, Boolean, Integer
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.schema import CreateTable
from str2bool import str2bool
from str2float import str2float

from udata_hydra import context
from udata_hydra.utils.db import get_check, insert_csv_analysis, compute_insert_query, update_csv_analysis
from udata_hydra.utils.file import download_resource


log = logging.getLogger("udata-hydra")

# Increase CSV field size limit to maximum possible
# https://stackoverflow.com/a/15063941
field_size_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(field_size_limit)
        break
    except OverflowError:
        field_size_limit = int(field_size_limit / 10)

PYTHON_TYPE_TO_PG = {
    "string": String,
    "float": Float,
    "int": BigInteger,
    "bool": Boolean,
}

PYTHON_TYPE_TO_PY = {
    "string": str,
    "float": float,
    "int": int,
    "bool": bool,
}


async def analyse_csv(check_id: int = None, url: str = None, file_path: str = None, optimized: bool = True) -> None:
    """Launch csv analysis from a check or an URL (debug), using previsously downloaded file at file_path if any"""
    assert any([_ is not None for _ in (check_id, url)])
    check = await get_check(check_id) if check_id is not None else {}
    url = check.get("url") or url

    headers = json.loads(check.get("headers") or "{}")
    tmp_file = open(file_path, "rb") if file_path else await download_resource(url, headers)

    try:
        csv_inspection = await perform_csv_inspection(tmp_file.name)
        ca_id = await insert_csv_analysis({
            "resource_id": check.get("resource_id"),
            "url": url,
            "check_id": check_id,
            "csv_detective": csv_inspection
        })
        table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
        await csv_to_db(tmp_file.name, csv_inspection, table_name, optimized=optimized)
        await update_csv_analysis(ca_id, {
            "parsing_table": table_name,
            "parsing_date": datetime.utcnow(),
        })
    finally:
        tmp_file.close()
        os.remove(tmp_file.name)


def generate_dialect(inspection: dict) -> csv.Dialect:
    class CustomDialect(csv.unix_dialect):
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
        if not failsafe:
            raise e
        if _type == "float":
            return str2float(value, default=None)
        log.warning(f'Could not convert "{value}" to {_type}, defaulting to null')
        return None


def compute_create_table_query(table_name, columns):
    """Use sqlalchemy to build a CREATE TABLE statement that should not be vulnerable to injections"""
    metadata = MetaData()
    table = Table(table_name, metadata, Column("__id", Integer, primary_key=True))
    for col_name, col_info in columns.items():
        table.append_column(Column(col_name, PYTHON_TYPE_TO_PG.get(col_info["python_type"], String)))
    compiled = CreateTable(table).compile(dialect=asyncpg.dialect())
    return compiled.string


async def csv_to_db(file_path: str, inspection: dict, table_name: str, optimized=True):
    """
    Convert a csv file to database table using inspection data

    :optimized: use postgres COPY if True, else insert record one by one
    """
    log.debug(f"Converting from CSV to db for {table_name}")
    dialect = generate_dialect(inspection)
    columns = inspection["columns"]
    q = f'DROP TABLE IF EXISTS "{table_name}"'
    db = await context.pool("csv")
    await db.execute(q)
    q = compute_create_table_query(table_name, columns)
    await db.execute(q)
    with open(file_path, encoding=inspection["encoding"]) as f:
        reader = csv.reader(f, dialect=dialect)
        # pop header row(s)
        for _ in range(inspection["header_row_idx"] + 1):
            f.readline()
        # this is an iterator! noice.
        records = (
            [
                smart_cast(t, v, failsafe=True)
                for t, v in zip([c["python_type"] for c in columns.values()], line)
            ]
            for line in reader if line
        )
        # this use postgresql COPY from an iterator, it's fast but might be difficult to debug
        if optimized:
            # NB: also see copy_to_table for a file source
            await db.copy_records_to_table(table_name, records=records, columns=columns.keys())
        # this inserts rows from iterator one by one, slow but useful for debugging
        else:
            bar = ProgressBar(total=inspection["total_lines"])
            for r in bar.iter(records):
                data = {k: v for k, v in zip(columns.keys(), r)}
                # NB: possible sql injection here, but should not be used in prod
                q = compute_insert_query(data, table_name, returning="__id")
                await db.execute(q, *data.values())


async def perform_csv_inspection(file_path):
    """Launch csv-detective against given file"""
    return csv_detective_routine(file_path)


async def detect_csv_from_headers(check) -> bool:
    """Determine if content-type header looks like a csv's one"""
    headers = json.loads(check["headers"] or {})
    return any([
        headers.get("content-type", "").lower().startswith(ct) for ct in [
            "application/csv", "text/plain", "text/csv"
        ]
    ])


async def delete_table(table_name: str):
    db = await context.pool("csv")
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
