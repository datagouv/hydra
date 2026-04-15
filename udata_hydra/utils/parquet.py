import json
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

PYTHON_TYPE_TO_PA = {
    "string": pa.string(),
    "float": pa.float64(),
    "int": pa.int64(),
    "bool": pa.bool_(),
    "json": pa.string(),
    "date": pa.date32(),
    "datetime": pa.date64(),
    "binary": pa.binary(),
}


def save_as_parquet(
    records: Iterator[list],
    columns: dict[str, dict],
    output_filename: str | None = None,
) -> tuple[str, pa.Table]:
    # the "output_filename = None" case is only used in tests
    table = pa.Table.from_pylist(
        [{c: v for c, v in zip(columns, values)} for values in records],
        schema=pa.schema(
            [pa.field(c, PYTHON_TYPE_TO_PA[columns[c]["python_type"]]) for c in columns]
        ),
    )
    if output_filename:
        pq.write_table(table, f"{output_filename}.parquet", compression="zstd")
    return f"{output_filename}.parquet", table


RESERVED_COLS = ("__id", "cmin", "cmax", "collation", "ctid", "tableoid", "xmin", "xmax")


BATCH_SIZE = 50_000


async def save_as_parquet_from_db(
    table_name: str,
    inspection: dict,
    output_filename: str | None = None,
) -> tuple[str, pa.Table]:
    """Build a Parquet file by reading directly from the PostgreSQL CSV table,
    avoiding a second read + cast pass over the original file."""
    from udata_hydra import context

    pool = await context.pool("csv")

    original_cols = list(inspection["columns"].keys())
    db_cols = [f"{c}__hydra_renamed" if c.lower() in RESERVED_COLS else c for c in original_cols]
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)

    schema = pa.schema(
        [
            pa.field(c, PYTHON_TYPE_TO_PA[inspection["columns"][c]["python_type"]])
            for c in original_cols
        ]
    )

    parquet_path = f"{output_filename}.parquet"
    writer = pq.ParquetWriter(parquet_path, schema, compression="zstd") if output_filename else None

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

    if output_filename:
        return parquet_path, pq.read_table(parquet_path)
    return parquet_path, table


async def detect_parquet_from_headers(check: dict) -> bool:
    headers: dict = json.loads(check["headers"] or "{}")
    # most parquet files are exposed with "application/octet-stream"
    # which combined with "parquet" in the url is a good hint
    # the ideal case is "application/vnd.apache.parquet"
    return any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/vnd.apache.parquet"]
    ) or "parquet" in check.get("url", "")
