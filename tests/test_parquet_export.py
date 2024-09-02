from io import BytesIO
from typing import Optional

import pyarrow.parquet as pq
import pytest

from udata_hydra.analysis.csv import (
    RESERVED_COLS,
    generate_records,
    perform_csv_inspection,
)
from udata_hydra.utils.parquet import save_as_parquet

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "file_and_count",
    (
        ("catalog.csv", 2),
        ("catalog.xls", 2),
        ("catalog.xlsx", 2),
    ),
)
async def test_parquet_conversion(
    setup_catalog, rmock, db, fake_check, produce_mock, file_and_count
):
    filename, expected_count = file_and_count
    file_path = f"tests/data/{filename}"
    inspection: Optional[dict] = await perform_csv_inspection(file_path)
    assert inspection
    columns = inspection["columns"]
    columns = {
        f"{c}__hydra_renamed" if c.lower() in RESERVED_COLS else c: v["python_type"]
        for c, v in columns.items()
    }
    _, table = save_as_parquet(
        records=generate_records(file_path, inspection, columns),
        columns=columns,
        output_name=None,
    )
    assert len(table) == expected_count
    fake_file = BytesIO()
    pq.write_table(table, fake_file)
