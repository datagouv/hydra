import csv
import hashlib
import json
import logging
import os

from csv_detective.explore_csv import routine as csv_detective_routine

from udata_hydra import context
from udata_hydra.utils.db import get_check, insert_csv_analysis
from udata_hydra.utils.file import download_resource

log = logging.getLogger("udata-hydra")


async def analyse_csv(check_id: int):
    """Launch csv analysis from a check"""
    check = await get_check(check_id)

    # ATM we (might) re-download the file, to avoid spaghetti code
    # TODO: find a way to mutualize with main analysis
    url = check["url"]
    headers = json.loads(check["headers"] or "{}")
    tmp_file = await download_resource(url, headers)

    try:
        csv_inspection = await perform_csv_inspection(tmp_file.name)
        await insert_csv_analysis({
            "resource_id": check["resource_id"],
            "url": check["url"],
            "check_id": check_id,
            "csv_detective": csv_inspection
        })
        table_name = hashlib.md5(check["url"].encode("utf-8")).hexdigest()
        await csv_to_db(tmp_file.name, csv_inspection, table_name)
    finally:
        os.remove(tmp_file.name)


PYTHON_TYPE_TO_PG = {
    "string": "text",
    "float": "float",
    "int": "bigint",
}

PYTHON_TYPE_TO_PY = {
    "string": str,
    "float": float,
    "int": int,
}


def generate_dialect(inspection: dict) -> csv.Dialect:
    class CustomDialect(csv.unix_dialect):
        # TODO: it would be nice to have more info from csvdetective to feed the dialect
        # in the meantime we might want to sniff the file a bit
        delimiter = inspection["separator"]
    return CustomDialect()


def smart_cast(_type, value, failsafe=False):
    try:
        return PYTHON_TYPE_TO_PY[_type](value)
    except ValueError as e:
        if not failsafe:
            raise e
        log.warning(f'Could not convert "{value}" to {_type}, defaulting to null')
        return None


async def csv_to_db(file_path: str, inspection: dict, table_name: str):
    """Convert a csv file to database table using inspection data"""
    dialect = generate_dialect(inspection)
    columns = inspection["columns"]
    # ["col_name float", "col_2_name bigint", "col_3_name text", ...]
    col_sql = [f'"{k}" {PYTHON_TYPE_TO_PG.get(c["python_type"], "text")}' for k, c in columns.items()]
    q = f'DROP TABLE IF EXISTS "{table_name}"'
    db = await context.pool("csv")
    await db.execute(q)
    q = f"""
    CREATE TABLE "{table_name}" (
        __id serial PRIMARY KEY, {", ".join(col_sql)}
    )
    """
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
            for line in reader
        )
        # NB: also see copy_to_table for a file source
        await db.copy_records_to_table(table_name, records=records, columns=columns.keys())


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
