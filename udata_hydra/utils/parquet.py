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


def save_as_parquet(records, columns, output_name, save_output=True):
    # the "save_output" argument is only used in tests
    table = pa.Table.from_pylist(
        [{
            c: v
            for c, v in zip(columns, values)
        } for values in records],
        schema=pa.schema([
            pa.field(
                c,
                PYTHON_TYPE_TO_PA[columns[c]]
            )
            for c in columns
        ])
    )
    if save_output:
        pq.write_table(table, f"{output_name}.parquet")
    return f"{output_name}.parquet", table
