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
    records: Iterator[tuple],
    columns: dict[str, dict],
    output_filename: str | None = None,
) -> tuple[str, pa.Table]:
    # the "output_name = None" case is only used in tests
    table = pa.Table.from_pylist(
        [{c: v for c, v in zip(columns, values)} for values in records],
        schema=pa.schema(
            [pa.field(c, PYTHON_TYPE_TO_PA[columns[c]["python_type"]]) for c in columns]
        ),
    )
    if output_filename:
        pq.write_table(table, f"{output_filename}.parquet")
    return f"{output_filename}.parquet", table


async def detect_parquet_from_headers(check: dict) -> bool:
    headers: dict = json.loads(check["headers"] or "{}")
    # most parquet files are exposed with "application/octet-stream"
    # which combined with "parquet" in the url is a good hint
    # the ideal case is "application/vnd.apache.parquet"
    return any(
        headers.get("content-type", "").lower().startswith(ct)
        for ct in ["application/vnd.apache.parquet"]
    ) or "parquet" in check.get("url", "")
