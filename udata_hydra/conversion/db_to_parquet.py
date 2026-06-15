import pyarrow as pa
import pyarrow.parquet as pq

from udata_hydra.conversion.schema import PYTHON_TYPE_TO_PA
from udata_hydra.db import db_col_name
from udata_hydra.utils.file import temporary_folder

BATCH_SIZE = 50_000


async def db_to_parquet(
    table_name: str,
    inspection: dict,
    output_filename: str | None = None,
) -> tuple[str, pa.Table]:
    """Build a Parquet file by reading directly from the PostgreSQL CSV table,
    avoiding a second read + cast pass over the original file."""
    from udata_hydra import context

    pool = await context.pool("csv")

    original_cols = list(inspection["columns"].keys())
    db_cols = [db_col_name(c) for c in original_cols]
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)

    schema = pa.schema(
        [
            pa.field(c, PYTHON_TYPE_TO_PA[inspection["columns"][c]["python_type"]])
            for c in original_cols
        ]
    )

    parquet_path = temporary_folder() / f"{output_filename}.parquet" if output_filename else None
    writer = pq.ParquetWriter(parquet_path, schema, compression="zstd") if parquet_path else None

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                stmt = await conn.prepare(f'SELECT {cols_sql} FROM "{table_name}"')
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

    if parquet_path:
        return str(parquet_path), pq.read_table(parquet_path)
    return "", table
