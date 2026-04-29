from io import BytesIO
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest
from csv_detective import routine as csv_detective_routine

from tests.conftest import RESOURCE_ID
from udata_hydra.analysis.csv import (
    csv_to_db,
    csv_to_parquet,
    generate_records,
)
from udata_hydra.db import db_col_name
from udata_hydra.utils.minio import MinIOClient
from udata_hydra.utils.parquet import db_to_parquet, save_as_parquet

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
    inspection = csv_detective_routine(
        file_path=file_path,
        output_profile=True,
        num_rows=-1,
        save_results=False,
    )
    assert inspection
    columns = inspection["columns"]
    columns = {db_col_name(c): v["python_type"] for c, v in columns.items()}
    _, table = save_as_parquet(
        records=generate_records(file_path, inspection),
        columns=inspection["columns"],
        output_filename=None,
    )
    assert len(table) == expected_count
    fake_file = BytesIO()
    pq.write_table(table, fake_file)


async def test_db_to_parquet(clean_db):
    file_path = "tests/data/catalog.csv"
    table_name = "test_parquet_from_db"
    inspection = csv_detective_routine(
        file_path=file_path,
        output_profile=True,
        num_rows=-1,
        save_results=False,
    )
    assert inspection

    await csv_to_db(file_path, inspection=inspection, table_name=table_name)

    _, table_from_db = await db_to_parquet(
        table_name=table_name,
        inspection=inspection,
        output_filename=None,
    )

    _, table_from_csv = save_as_parquet(
        records=generate_records(file_path, inspection),
        columns=inspection["columns"],
        output_filename=None,
    )

    assert table_from_db.num_rows == table_from_csv.num_rows
    assert table_from_db.schema == table_from_csv.schema
    assert table_from_db.to_pydict() == table_from_csv.to_pydict()


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
        inspection = csv_detective_routine(
            file_path=file_path,
            output_profile=True,
            num_rows=-1,
            save_results=False,
        )
        assert inspection
        return await csv_to_parquet(file_path, inspection=inspection, resource_id=RESOURCE_ID)

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
            result = await execute_csv_to_parquet()
            assert result is not None
            parquet_url, parquet_size = result
        assert parquet_url == f"https://{minio_url}/{bucket}/{folder}/{RESOURCE_ID}.parquet"
        assert isinstance(parquet_size, int)
