import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from asyncpg import Record
from progressist import ProgressBar

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.analysis.csv import compute_create_table_query, csv_to_db_index
from udata_hydra.db import compute_insert_query
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    IOException,
    ParseException,
    Timer,
    handle_parse_exception,
)

log = logging.getLogger("udata-hydra")

PYARROW_TYPE_TO_PYTHON = {
    # using regex because of bits-differing types (e.g. int32 and int64)
    # the "^" makes sure we don't consider the types of elements within structured objects (lists, dicts)
    "string$": "string",  # large_string also exists
    "^double": "float",
    "^float": "float",
    "^decimal": "float",
    "^int": "int",
    "^bool": "bool",
    "^date": "date",
    "^struct": "json",  # dictionary
    "^list": "json",
    "^binary": "binary",
    r"^timestamp\[\ws\]": "datetime",
    r"^timestamp\[\ws,": "datetime_aware",  # the rest of the field depends on the timezone
}

RESERVED_COLS = ("__id", "cmin", "cmax", "collation", "ctid", "tableoid", "xmin", "xmax")


async def analyse_parquet(
    check: dict,
    file_path: str | None = None,
    debug_insert: bool = False,
) -> None:
    """Insert parquet file and metadata in db"""
    if not config.PARQUET_TO_DB:
        log.debug("PARQUET_TO_DB turned off, skipping.")
        return

    resource_id: str = str(check["resource_id"])
    url = check["url"]
    # Preserve dataset_id from original check record
    dataset_id = check.get("dataset_id")

    resource: Record | None = await Resource.update(resource_id, {"status": "ANALYSING_PARQUET"})

    # Check if the resource is in the exceptions table
    # If it is, get the table_indexes to use them later
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)
    table_indexes: dict | None = None
    if exception and exception.get("table_indexes"):
        table_indexes = json.loads(exception["table_indexes"])

    timer = Timer("analyse-parquet", resource_id)
    assert any(_ is not None for _ in (check["id"], url))

    table_name, tmp_file = None, None
    try:
        tmp_file = await helpers.read_or_download_file(
            check=check,
            file_path=file_path,
            file_format="parquet",
            exception=exception,
        )
        table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
        timer.mark("download-file")

        check = await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})

        # open the file and read the metadata
        try:
            parquet_file = pq.ParquetFile(tmp_file.name)
            columns = {}
            for col in parquet_file.schema_arrow:
                col_type = str(col.type)
                if col_type.startswith("dictionary"):
                    # dictionaries are for columns with repeated values
                    # we need to dig deeper to get the type
                    col_type = str(col.type.value_type)
                try:
                    columns[col.name] = next(
                        pytype
                        for pyartype, pytype in PYARROW_TYPE_TO_PYTHON.items()
                        if re.match(pyartype, col_type)
                    )
                except StopIteration:
                    raise ValueError(f"Unknown pyarrow type: {col.type}")
            inspection = {
                "columns": {
                    col_name: {
                        "format": pytype,
                        "python_type": pytype,
                    }
                    for col_name, pytype in columns.items()
                }
            }
            inspection["total_lines"] = parquet_file.metadata.num_rows
        except Exception as e:
            raise ParseException(
                message=str(e),
                step="parquet_analysis",
                resource_id=resource_id,
                url=url,
                check_id=check["id"],
            ) from e
        timer.mark("parquet-analysis")

        await parquet_to_db(
            parquet_file=parquet_file,
            inspection=inspection,
            table_name=table_name,
            table_indexes=table_indexes,
            resource_id=resource_id,
            debug_insert=debug_insert,
        )
        check = await Check.update(check["id"], {"parsing_table": table_name})
        timer.mark("parquet-to-db")

        check = await Check.update(
            check["id"],
            {
                "parsing_finished_at": datetime.now(timezone.utc),
            },
        )
        await parquet_to_db_index(table_name, inspection, check, dataset_id)

    except (ParseException, IOException) as e:
        check = await handle_parse_exception(e, table_name, check)
    finally:
        await helpers.notify_udata(resource, check)
        timer.stop()
        if tmp_file is not None:
            tmp_file.close()
            os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})


def generate_records_from_parquet(parquet_file: pq.ParquetFile) -> Iterator[list]:
    # pandas cannot have None in columns typed as int so we have to cast
    # NaN-int values to None for db insertion, and we also change NaN to None
    for batch in parquet_file.iter_batches():
        df = batch.to_pandas()
        for row in df.values:
            yield tuple(
                (cell if not pd.isna(cell) else None)
                # dumping the cell if it's a JSON object
                if not isinstance(cell, (list, dict, np.ndarray))
                else json.dumps(
                    # np.ndarray is the cast type for lists in pandas
                    # but it's not JSON-serializable, we need a pure list
                    cell if isinstance(cell, (list, dict)) else cell.tolist()
                )
                for cell in row
            )


async def parquet_to_db(
    parquet_file: pq.ParquetFile,
    inspection: dict,
    table_name: str,
    table_indexes: dict[str, str] | None = None,
    resource_id: str | None = None,
    debug_insert: bool = False,
) -> None:
    """
    Convert a parquet file to database table using inspection data. It should (re)create one table:
    - `table_name` with data from `file_path`

    :parquet_file: parquet file to convert
    :inspection: CSV detective-like report
    :table_name: used to create tables
    :debug_insert: insert record one by one instead of using postgresql COPY
    """

    log.debug(f"Converting from parquet to db for {table_name}")

    if any(
        sum(len(char.encode("utf-8")) for char in col) > config.NAMEDATALEN - 1
        for col in inspection["columns"]
    ):
        raise ParseException(
            step="scan_column_names",
            resource_id=resource_id,
            table_name=table_name,
        ) from ValueError(
            f"Column names cannot exceed {config.NAMEDATALEN - 1} characters in Postgres"
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
                records=generate_records_from_parquet(parquet_file),
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
        for r in bar.iter(generate_records_from_parquet(parquet_file)):
            data = {k: v for k, v in zip(parquet_file.schema.names, r)}
            print(data)
            # NB: possible sql injection here, but should not be used in prod
            q = compute_insert_query(table_name=table_name, data=data, returning="__id")
            await db.execute(q, *data.values())


async def parquet_to_db_index(
    table_name: str,
    inspection: dict,
    check: Record,
    dataset_id: str,
) -> None:
    # convenience method just to make it clearer
    await csv_to_db_index(table_name, inspection, check, dataset_id)
