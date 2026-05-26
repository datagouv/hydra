from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from csv_detective import routine as csv_detective_routine

from tests.conftest import RESOURCE_ID
from udata_hydra.analysis.csv import export_db_to_parquet
from udata_hydra.conversion.csv_to_db import csv_to_db
from udata_hydra.conversion.db_to_parquet import db_to_parquet
from udata_hydra.conversion.schema import PYTHON_TYPE_TO_PA
from udata_hydra.utils.casting import iter_tabular_rows
from udata_hydra.utils.s3 import S3Client

pytestmark = pytest.mark.asyncio


def _reference_table_from_csv(file_path: str, inspection: dict) -> pa.Table:
    """Build an Arrow table from a tabular file for assertions against ``db_to_parquet`` output.

    Uses ``iter_tabular_rows`` (``cast_json=False``) and ``PYTHON_TYPE_TO_PA`` for schema alignment.
    """
    rows = list(iter_tabular_rows(file_path, inspection, cast_json=False))
    columns = inspection["columns"]
    return pa.Table.from_pylist(
        [{c: v for c, v in zip(columns, values)} for values in rows],
        schema=pa.schema(
            [pa.field(c, PYTHON_TYPE_TO_PA[columns[c]["python_type"]]) for c in columns]
        ),
    )


@pytest.mark.parametrize(
    "file_and_count",
    (
        ("catalog.csv", 2),
        ("catalog.xls", 2),
        ("catalog.xlsx", 2),
    ),
)
async def test_reference_table_from_csv(file_and_count):
    """Catalog fixtures: typed Arrow table row count and Parquet serialization for CSV / XLS / XLSX."""
    filename, expected_count = file_and_count
    file_path = f"tests/data/{filename}"
    inspection = csv_detective_routine(
        file_path=file_path,
        output_profile=True,
        num_rows=-1,
        save_results=False,
    )
    assert inspection

    table = _reference_table_from_csv(file_path, inspection)
    assert table.num_rows == expected_count

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

    table_from_csv = _reference_table_from_csv(file_path, inspection)

    assert table_from_db.num_rows == table_from_csv.num_rows
    assert table_from_db.schema == table_from_csv.schema
    assert table_from_db.to_pydict() == table_from_csv.to_pydict()


@pytest.mark.parametrize(
    "parquet_config",
    (
        (False, 1, False),  # DB_TO_PARQUET off → no export
        (True, 10, False),  # MIN_LINES_FOR_PARQUET = 10 (> total_lines) → skip
        (True, 1, True),  # DB_TO_PARQUET on, MIN_LINES = 1 → convert
        (True, 3, False),  # MIN_LINES_FOR_PARQUET = 3 (> fixture row count) → skip
    ),
)
async def test_export_db_to_parquet(mocker, parquet_config, clean_db):
    file_path = "tests/data/catalog.csv"
    inspection = csv_detective_routine(
        file_path=file_path,
        output_profile=True,
        num_rows=-1,
        save_results=False,
    )
    assert inspection

    db_to_parquet_flag, min_lines_for_parquet_config, expected_conversion = parquet_config
    mocker.patch("udata_hydra.config.DB_TO_PARQUET", db_to_parquet_flag)
    mocker.patch("udata_hydra.config.MIN_LINES_FOR_PARQUET", min_lines_for_parquet_config)

    async def run_export() -> tuple[str, int] | None:
        table_name = "test_export_parquet_tbl"
        if expected_conversion:
            await csv_to_db(file_path, inspection=inspection, table_name=table_name)
        return await export_db_to_parquet(
            table_name=table_name,
            inspection=inspection,
            resource_id=RESOURCE_ID,
            check_id=1,
        )

    mocker.patch("udata_hydra.analysis.csv.Resource.update", new_callable=AsyncMock)
    check_update = mocker.patch("udata_hydra.analysis.csv.Check.update", new_callable=AsyncMock)

    if not expected_conversion:
        assert await run_export() is None
        check_update.assert_not_called()
    else:
        s3_endpoint = "s3-example.com"
        bucket = "bucket"
        mocker.patch("udata_hydra.config.S3_ENDPOINT", s3_endpoint)
        mocked_resource = MagicMock()
        mocked_resource.meta.client.head_bucket.return_value = {}
        mocked_resource.Bucket.return_value = MagicMock()
        with patch("udata_hydra.utils.s3.boto3.resource", return_value=mocked_resource):
            mocked_s3_client = S3Client(bucket=bucket)
        with patch(
            "udata_hydra.analysis.csv._parquet_s3_client",
            new=mocked_s3_client,
        ):
            result = await run_export()
            assert result is not None
            parquet_url, parquet_size = result
        assert parquet_url == f"https://{s3_endpoint}/{bucket}/parquet/{RESOURCE_ID}.parquet"
        assert isinstance(parquet_size, int)
        check_update.assert_called()
