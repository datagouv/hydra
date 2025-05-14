import os
from io import BytesIO
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from udata_hydra.analysis.csv import (
    RESERVED_COLS,
    csv_detective_routine,
    csv_to_parquet,
    generate_records,
)
from udata_hydra.utils.minio import MinIOClient
from udata_hydra.utils.parquet import save_as_parquet

from .conftest import RESOURCE_ID

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "file_and_count",
    (
        ("catalog.csv", 2),
        ("catalog.xls", 2),
        ("catalog.xlsx", 2),
    ),
)
async def test_save_as_parquet(file_and_count):
    filename, expected_count = file_and_count
    file_path = f"tests/data/{filename}"
    inspection: dict | None = csv_detective_routine(
        file_path=file_path, output_profile=True, num_rows=-1, save_results=False
    )
    assert inspection
    columns = inspection["columns"]
    columns = {
        f"{c}__hydra_renamed" if c.lower() in RESERVED_COLS else c: v["python_type"]
        for c, v in columns.items()
    }
    _, table = save_as_parquet(
        records=generate_records(file_path, inspection, columns),
        columns=columns,
        output_filename=None,
    )
    assert len(table) == expected_count
    fake_file = BytesIO()
    pq.write_table(table, fake_file)


@pytest.mark.parametrize(
    "parquet_config",
    (
        (False, 1, False),  # CSV_TO_PARQUET = False, MIN_LINES_FOR_PARQUET = 1
        (True, 1, True),  # CSV_TO_PARQUET = True, MIN_LINES_FOR_PARQUET = 1
        (True, 3, False),  # CSV_TO_PARQUET = True, MIN_LINES_FOR_PARQUET = 3
    ),
)
async def test_csv_to_parquet(mocker, parquet_config):
    async def execute_csv_to_parquet() -> tuple[str, int] | None:
        file_path = "tests/data/catalog.csv"
        inspection: dict | None = csv_detective_routine(
            file_path=file_path, output_profile=True, num_rows=-1, save_results=False
        )
        assert inspection
        return await csv_to_parquet(
            file_path=file_path, inspection=inspection, resource_id=RESOURCE_ID
        )

    csv_to_parquet_config, min_lines_for_parquet_config, expected_conversion = parquet_config
    mocker.patch("udata_hydra.config.CSV_TO_PARQUET", csv_to_parquet_config)
    mocker.patch("udata_hydra.config.MIN_LINES_FOR_PARQUET", min_lines_for_parquet_config)

    if not expected_conversion:
        assert not await execute_csv_to_parquet()

    else:
        minio_url = "my.minio.fr"
        bucket = "bucket"
        folder = "folder"
        mocker.patch("udata_hydra.config.MINIO_URL", minio_url)
        mocked_minio = MagicMock()
        mocked_minio.fput_object.return_value = None
        mocked_minio.bucket_exists.return_value = True
        with patch("udata_hydra.utils.minio.Minio", return_value=mocked_minio):
            mocked_minio_client = MinIOClient(bucket=bucket, folder=folder)
        with patch("udata_hydra.analysis.csv.minio_client", new=mocked_minio_client):
            parquet_url, parquet_size = await execute_csv_to_parquet()
        assert parquet_url == f"https://{minio_url}/{bucket}/{folder}/{RESOURCE_ID}.parquet"
        assert isinstance(parquet_size, int)
