from typing import Generator

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
}


def save_as_parquet(
    records: Generator,
    columns: dict,
    output_filename: str | None = None,
) -> tuple[str, pa.Table]:
    # the "output_name = None" case is only used in tests
    table = pa.Table.from_pylist(
        [{c: v for c, v in zip(columns, values)} for values in records],
        schema=pa.schema([pa.field(c, PYTHON_TYPE_TO_PA[columns[c]]) for c in columns]),
    )
    if output_filename:
        pq.write_table(table, f"{output_filename}.parquet")
    return f"{output_filename}.parquet", table
