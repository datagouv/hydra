import json
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.analysis.csv import csv_detective_routine
from udata_hydra.analysis.geojson import (
    DEFAULT_GEOJSON_FILEPATH,
    analyse_geojson,
    csv_to_geojson,
    geojson_to_pmtiles,
)
from udata_hydra.utils.minio import MinIOClient

BIG_CSV_GEO_TEST_FILE = Path("tests/data/.MN_36_latest-2024-2025.csv")
BIG_GEOJSON_TEST_FILE = DEFAULT_GEOJSON_FILEPATH

log = logging.getLogger("udata-hydra")


@pytest.mark.asyncio
async def test_analyse_geojson_disabled(fake_check):
    """Test that the function returns None when GEOJSON_TO_PMTILES is False"""
    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", False),
        patch("udata_hydra.analysis.helpers.read_or_download_file") as mock_func,
    ):
        check = await fake_check()
        result = await analyse_geojson(check, "test.geojson")
        assert result is None
        mock_func.assert_not_called()


@pytest.mark.asyncio
async def test_geojson_to_pmtiles_invalid_geometry():
    """Test handling of invalid geometry"""
    # Mock data with invalid geometry
    with pytest.raises(Exception):
        await geojson_to_pmtiles(Path("tests/data/invalid.geojson"), RESOURCE_ID)
    os.remove(f"{RESOURCE_ID}.pmtiles")


@pytest.mark.asyncio
async def test_geojson_to_pmtiles_valid_geometry(mocker):
    """Test handling of valid geometry"""
    minio_url = "my.minio.fr"
    bucket = "bucket"
    folder = "folder"
    mocker.patch("udata_hydra.config.MINIO_URL", minio_url)
    mocked_minio = MagicMock()
    mocked_minio.fput_object.return_value = None
    mocked_minio.bucket_exists.return_value = True
    with patch("udata_hydra.utils.minio.Minio", return_value=mocked_minio):
        mocked_minio_client = MinIOClient(bucket=bucket, folder=folder)
    with patch("udata_hydra.analysis.geojson.minio_client_pmtiles", new=mocked_minio_client):
        mock_os = mocker.patch("udata_hydra.utils.minio.os")
        mock_os.path = os.path
        mock_os.remove.return_value = None
        filepath, size, url = await geojson_to_pmtiles(
            Path("tests/data/valid.geojson"), RESOURCE_ID
        )
    # very (too?) simple test, we could install a specific library to read the file
    with open(f"{RESOURCE_ID}.pmtiles", "rb") as f:
        header = f.read(7)
    assert header == b"PMTiles"
    assert url == f"https://{minio_url}/{bucket}/{folder}/{RESOURCE_ID}.pmtiles"
    # size slightly differs depending on the env
    assert 850 <= size <= 900
    os.remove(f"{RESOURCE_ID}.pmtiles")


@pytest.mark.asyncio
async def test_geojson_analysis(setup_catalog, db, fake_check, rmock, produce_mock):
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=b"{pretend this is a geojson}")
    pmtiles_filepath = Path("tests/data/valid.pmtiles")
    pmtiles_size = 100
    pmtiles_url = "http://minio/test.pmtiles"
    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", True),
        patch(
            "udata_hydra.analysis.geojson.geojson_to_pmtiles",
            return_value=(pmtiles_filepath, pmtiles_size, pmtiles_url),
        ),
    ):
        await analyse_geojson(check=check)
    res = await db.fetchrow(f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID}'")
    assert res["parsing_finished_at"] is not None
    assert res["pmtiles_size"] == pmtiles_size
    assert res["pmtiles_url"] == pmtiles_url


@pytest.mark.asyncio
@pytest.mark.slow
async def test_csv_to_geojson_big_file(mocker, keep_result_file=False):
    """Test performance with a big real CSV file containing geographical data"""

    # Use the big CSV file for performance testing
    if not BIG_CSV_GEO_TEST_FILE.exists():
        pytest.skip("BIG_CSV_GEO_TEST_FILE not found, skipping performance test")

    # Mock MinIO for the test
    minio_url = "my.minio.fr"
    bucket = "bucket"
    folder = "folder"
    mocker.patch("udata_hydra.config.MINIO_URL", minio_url)
    mocked_minio = MagicMock()
    mocked_minio.fput_object.return_value = None
    mocked_minio.bucket_exists.return_value = True

    with patch("udata_hydra.utils.minio.Minio", return_value=mocked_minio):
        mocked_minio_client = MinIOClient(bucket=bucket, folder=folder)

    with patch("udata_hydra.analysis.geojson.minio_client_geojson", new=mocked_minio_client):
        mock_os = mocker.patch("udata_hydra.utils.minio.os")
        mock_os.path = os.path
        mock_os.remove.return_value = None

        # Analyze the CSV with csv_detective first
        inspection, df = csv_detective_routine(
            file_path=str(BIG_CSV_GEO_TEST_FILE),
            output_profile=True,
            output_df=True,
            cast_json=False,
            num_rows=-1,
            save_results=False,
        )

        # Test the performance of csv_to_geojson with the real file
        result = await csv_to_geojson(
            df=df, inspection=inspection, resource_id=None, upload_to_minio=False
        )

        if result:
            geojson_filepath, geojson_size, geojson_url = result

            # Verify the GeoJSON file was created correctly
            with geojson_filepath.open("r") as f:
                geojson_data = json.load(f)
                assert geojson_data["type"] == "FeatureCollection"
                assert "features" in geojson_data
                assert len(geojson_data["features"]) > 0

            # The size should be significant
            assert geojson_size > 1000  # Should be much larger than small files

            # Get file sizes before cleanup
            csv_size = BIG_CSV_GEO_TEST_FILE.stat().st_size

            # Clean up using pathlib (only if keep_result_file is False)
            if not keep_result_file:
                geojson_filepath.unlink(missing_ok=True)
                log.info(f"GeoJSON file removed: {geojson_filepath}")
            else:
                log.info(f"GeoJSON file kept: {geojson_filepath}")

            log.info(f"Real CSV to GeoJSON test completed. Input CSV size: {csv_size} bytes")
            log.info(f"Output GeoJSON size: {geojson_size} bytes")
            log.info(f"Features converted: {len(geojson_data['features'])}")
        else:
            pytest.skip("CSV file does not contain geographical data, skipping test")


@pytest.mark.asyncio
@pytest.mark.slow
async def test_geojson_to_pmtiles_big_file(mocker):
    """Test performance with a big real GeoJSON file"""

    # Use the big GeoJSON file for performance testing
    if not BIG_GEOJSON_TEST_FILE.exists():
        pytest.skip("BIG_GEOJSON_TEST_FILE not found, skipping performance test")

    # Mock MinIO for the test
    minio_url = "my.minio.fr"
    bucket = "bucket"
    folder = "folder"
    mocker.patch("udata_hydra.config.MINIO_URL", minio_url)
    mocked_minio = MagicMock()
    mocked_minio.fput_object.return_value = None
    mocked_minio.bucket_exists.return_value = True

    with patch("udata_hydra.utils.minio.Minio", return_value=mocked_minio):
        mocked_minio_client = MinIOClient(bucket=bucket, folder=folder)

    with patch("udata_hydra.analysis.geojson.minio_client_pmtiles", new=mocked_minio_client):
        mock_os = mocker.patch("udata_hydra.utils.minio.os")
        mock_os.path = os.path
        mock_os.remove.return_value = None

        # Test the performance of geojson_to_pmtiles with the real file
        result = await geojson_to_pmtiles(BIG_GEOJSON_TEST_FILE, RESOURCE_ID)
        pmtiles_filepath, pmtiles_size, pmtiles_url = result

    # Verify the PMTiles file was created correctly
    with pmtiles_filepath.open("rb") as f:
        header = f.read(7)
    assert header == b"PMTiles"
    assert pmtiles_url == f"https://{minio_url}/{bucket}/{folder}/{RESOURCE_ID}.pmtiles"

    # The size should be significantly larger than the small test file
    assert pmtiles_size > 1000  # Should be much larger than the 850-900 range of small file
    # Clean up using pathlib
    pmtiles_filepath.unlink(missing_ok=True)
