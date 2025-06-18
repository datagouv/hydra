import os
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.analysis.geojson import analyse_geojson, geojson_to_pmtiles
from udata_hydra.utils.minio import MinIOClient


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
        await geojson_to_pmtiles("tests/data/invalid.geojson", RESOURCE_ID)
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
        url, size = await geojson_to_pmtiles("tests/data/valid.geojson", RESOURCE_ID)
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
    pmtiles_url = "http://minio/test.pmtiles"
    pmtiles_size = 100
    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", True),
        patch(
            "udata_hydra.analysis.geojson.geojson_to_pmtiles",
            return_value=(pmtiles_url, pmtiles_size),
        ),
    ):
        await analyse_geojson(check=check)
    res = await db.fetchrow(f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID}'")
    assert res["parsing_finished_at"] is not None
    assert res["pmtiles_url"] == pmtiles_url
    assert res["pmtiles_size"] == pmtiles_size
