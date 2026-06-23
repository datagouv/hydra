import hashlib
import json
import logging
from typing import TYPE_CHECKING, Iterator

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from progressist import ProgressBar

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.analysis.tables_index import insert_tables_index_entry
from udata_hydra.conversion.schema import compute_create_table_query
from udata_hydra.data_formats import Parquet
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import ParseException
from udata_hydra.utils.db import compute_insert_query, db_col_name

if TYPE_CHECKING:
    from udata_hydra.data_formats.table import Table

log = logging.getLogger("udata-hydra")


def _iter_parquet_rows(parquet_file: pq.ParquetFile) -> Iterator[tuple]:
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
    file: Parquet,
    check: dict,
    table_indexes: dict[str, str] | None = None,
    debug_insert: bool = False,
) -> "Table":
    """
    Convert a parquet file to database table using inspection data. It should (re)create one table:
    - `table_name` with data from `parquet_file`

    :parquet_file: parquet file to convert
    :inspection: CSV detective-like report
    :table_name: used to create tables
    :debug_insert: insert record one by one instead of using postgresql COPY
    """
    from udata_hydra.data_formats import Table

    table_name = hashlib.md5(check["url"].encode("utf-8")).hexdigest()

    log.debug(f"Converting from parquet to db for {table_name}")

    if any(
        sum(len(char.encode("utf-8")) for char in col) > config.NAMEDATALEN - 1
        for col in file.inspection["columns"]
    ):
        raise ParseException(
            step="scan_column_names",
            resource_id=file.resource_id,
            table_name=table_name,
        ) from ValueError(
            f"Column names cannot exceed {config.NAMEDATALEN - 1} characters in Postgres"
        )

    if file.resource_id:
        # Update resource status to INSERTING_IN_DB
        await Resource.update(file.resource_id, {"status": "INSERTING_IN_DB"})

    # build a `column_name: type` mapping and explicitely rename reserved column names
    columns = {
        db_col_name(c): helpers.get_python_type(v) for c, v in file.inspection["columns"].items()
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
            resource_id=file.resource_id,
            table_name=table_name,
        ) from e

    # this use postgresql COPY from an iterator, it's fast but might be difficult to debug
    if not debug_insert:
        # NB: also see copy_to_table for a file source
        try:
            await db.copy_records_to_table(
                table_name,
                records=_iter_parquet_rows(pq.ParquetFile(file.path)),
                columns=list(columns.keys()),
            )
        except Exception as e:  # I know what I'm doing, pinky swear
            raise ParseException(
                message=str(e),
                step="copy_records_to_table",
                resource_id=file.resource_id,
                table_name=table_name,
            ) from e
    # this inserts rows from iterator one by one, slow but useful for debugging
    else:
        bar = ProgressBar(total=file.inspection["total_lines"])
        pqf = pq.ParquetFile(file.path)
        for r in bar.iter(_iter_parquet_rows(pqf)):
            data = {k: v for k, v in zip(pqf.schema.names, r)}
            print(data)
            # NB: possible sql injection here, but should not be used in prod
            q = compute_insert_query(table_name=table_name, data=data, returning="__id")
            await db.execute(q, *data.values())

    check = await Check.update(check["id"], {"parsing_table": table_name})  # type: ignore[assignment]
    await insert_tables_index_entry(table_name, file.inspection, check, file.dataset_id)
    return Table(
        table_name=table_name,
        inspection=file.inspection,
        resource_id=file.resource_id,
        dataset_id=file.dataset_id,
    )
