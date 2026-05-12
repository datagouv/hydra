import json
import logging
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from progressist import ProgressBar

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.conversion.schema import compute_create_table_query
from udata_hydra.db import compute_insert_query, db_col_name
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException

log = logging.getLogger("udata-hydra")


def iter_parquet_rows(parquet_file: pq.ParquetFile) -> Iterator[tuple]:
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
    - `table_name` with data from `parquet_file`

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
    columns = {db_col_name(c): helpers.get_python_type(v) for c, v in inspection["columns"].items()}

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
                records=iter_parquet_rows(parquet_file),
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
        for r in bar.iter(iter_parquet_rows(parquet_file)):
            data = {k: v for k, v in zip(parquet_file.schema.names, r)}
            print(data)
            # NB: possible sql injection here, but should not be used in prod
            q = compute_insert_query(table_name=table_name, data=data, returning="__id")
            await db.execute(q, *data.values())
