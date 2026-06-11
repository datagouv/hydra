import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from udata_hydra.conversion.schema import PYTHON_TYPE_TO_PA
from udata_hydra.data_formats import Table
from udata_hydra.db import db_col_name

if TYPE_CHECKING:
    from udata_hydra.data_formats import Parquet

DEFAULT_PARQUET_FILENAME = "converted_from_db.parquet"
BATCH_SIZE = 50_000
log = logging.getLogger("udata-hydra")


async def db_to_parquet(table: Table) -> "Parquet":
    """Build a Parquet file by reading directly from the PostgreSQL CSV table,
    avoiding a second read + cast pass over the original file."""
    from udata_hydra import context
    from udata_hydra.data_formats import Parquet

    log.debug(f"Converting db table to parquet for {table.resource_id}")

    pool = await context.pool("csv")

    original_cols = list(table.inspection["columns"].keys())
    db_cols = [db_col_name(c) for c in original_cols]
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)

    schema = pa.schema(
        [
            pa.field(c, PYTHON_TYPE_TO_PA[table.inspection["columns"][c]["python_type"]])
            for c in original_cols
        ]
    )

    parquet_path = Path(
        f"{table.resource_id}.parquet"
        if table.resource_id is not None
        else DEFAULT_PARQUET_FILENAME
    )
    writer = pq.ParquetWriter(parquet_path, schema, compression="zstd")

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                stmt = await conn.prepare(f'SELECT {cols_sql} FROM "{table.table_name}"')
                cursor = await stmt.cursor()
                while batch := await cursor.fetch(BATCH_SIZE):
                    table = pa.table(
                        {
                            orig: [row[db_col] for row in batch]
                            for orig, db_col in zip(original_cols, db_cols)
                        },
                        schema=schema,
                    )
                    if writer:
                        writer.write_table(table)
    finally:
        if writer:
            writer.close()

    return Parquet(
        path=parquet_path, inspection=table.inspection, resource_id=table.resource_id, dataset_id=table.dataset_id
    )
