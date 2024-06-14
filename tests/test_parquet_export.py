import pytest
from io import BytesIO
import pyarrow.parquet as pq

from udata_hydra import config
from udata_hydra.utils.parquet import save_as_parquet
from udata_hydra.analysis.csv import (
    RESERVED_COLS,
    perform_csv_inspection,
    generate_records,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("file_and_count", (
    ("catalog.csv", 2),
    ("catalog.xls", 2),
    ("catalog.xlsx", 2),
))
async def test_parquet_conversion(setup_catalog, rmock, db, fake_check, produce_mock, file_and_count):
    filename, expected_count = file_and_count
    file_path = f"tests/data/{filename}"
    inspection = await perform_csv_inspection(file_path)
    columns = inspection["columns"]
    columns = {
        f"{c}__hydra_renamed" if c.lower() in RESERVED_COLS else c: v["python_type"]
        for c, v in columns.items()
    }
    _, table = save_as_parquet(
        records=generate_records(file_path, inspection, columns),
        columns=columns,
        output_name=None,
        save_output=False,
    )
    assert len(table) == expected_count
    fake_file = BytesIO()
    pq.write_table(table, fake_file)
