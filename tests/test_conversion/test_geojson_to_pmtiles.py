import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.conversion.geojson_to_pmtiles import geojson_to_pmtiles
from udata_hydra.utils.minio import MinIOClient
from udata_hydra.utils.timer import Timer

log = logging.getLogger("udata-hydra")

pytestmark = pytest.mark.asyncio


async def test_geojson_to_pmtiles_invalid_geometry():
    """Test handling of invalid geometry"""
    test_pmtiles_path = Path(f"{RESOURCE_ID}.pmtiles")
    with pytest.raises(Exception):
        await geojson_to_pmtiles(Path("tests/data/invalid.geojson"), test_pmtiles_path)


async def test_geojson_to_pmtiles_valid_geometry(mocker):
    """Test handling of valid geometry"""
    minio_url = "my.minio.fr"
    bucket = "bucket"
    folder = "folder"
    mocker.patch("udata_hydra.config.MINIO_URL", minio_url)
    mocked_minio = MagicMock()
    mocked_minio.fput_object.return_value = None
    mocked_minio.bucket_exists.return_value = True
    # Make sure that we don't crash even if output pmtiles already exists
    Path(f"{RESOURCE_ID}.pmtiles").touch()
    with patch("udata_hydra.utils.minio.Minio", return_value=mocked_minio):
        mocked_minio_client = MinIOClient(bucket=bucket, folder=folder)
    with (
        patch(
            "udata_hydra.conversion.geojson_to_pmtiles.minio_client_pmtiles",
            new=mocked_minio_client,
        ),
        patch("udata_hydra.config.REMOVE_GENERATED_FILES", False),
    ):
        mock_os = mocker.patch("udata_hydra.utils.minio.os")
        mock_os.path = os.path
        mock_os.remove.return_value = None
        size, url = await geojson_to_pmtiles(
            Path("tests/data/valid.geojson"), Path(f"{RESOURCE_ID}.pmtiles")
        )
    # very (too?) simple test, we could install a specific library to read the file
    with open(f"{RESOURCE_ID}.pmtiles", "rb") as f:
        header = f.read(7)
    assert header == b"PMTiles"
    assert url == f"https://{minio_url}/{bucket}/{folder}/{RESOURCE_ID}.pmtiles"
    # size slightly differs depending on the env
    assert 850 <= size <= 900
    os.remove(f"{RESOURCE_ID}.pmtiles")


@pytest.mark.slow
async def test_geojson_to_pmtiles_big_file(mocker, input_file: str | None):
    """Test performance with a GeoJSON file

    :input_file: Path to the GeoJSON file to test (mandatory)
    """
    if not input_file:
        pytest.skip(reason="No input_file provided, skipping performance test")

    geojson_path = "tests/data" / Path(input_file)
    test_pmtiles_path = geojson_path.parent / f"{geojson_path.stem}.pmtiles"

    # Create timer for performance measurement
    timer = Timer("geojson-to-pmtiles-performance-test")

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

    with (
        patch(
            "udata_hydra.conversion.geojson_to_pmtiles.minio_client_pmtiles",
            new=mocked_minio_client,
        ),
        patch("udata_hydra.config.REMOVE_GENERATED_FILES", False),
    ):
        mock_os = mocker.patch("udata_hydra.utils.minio.os")
        mock_os.path = os.path
        mock_os.remove.return_value = None

        # Test the performance of geojson_to_pmtiles with the real file
        result = await geojson_to_pmtiles(geojson_path, test_pmtiles_path)
        timer.mark("pmtiles-conversion")
        pmtiles_size, pmtiles_url = result

    # Verify the PMTiles file was created correctly
    with test_pmtiles_path.open("rb") as f:
        header = f.read(7)
    assert header == b"PMTiles"
    assert pmtiles_url == f"https://{minio_url}/{bucket}/{folder}/{geojson_path.stem}.pmtiles"

    # The size should be significantly larger than the small test file
    assert pmtiles_size > 5000  # Should be much larger than the 850-900 range of small file

    # Stop timer and log performance results
    timer.stop()

    # Get input file size for logging
    input_geojson_size = geojson_path.stat().st_size

    # Log performance results
    log.info(f"PMTiles conversion completed. Input GeoJSON size: {input_geojson_size} bytes")
    log.info(f"Output PMTiles size: {pmtiles_size} bytes")
    log.info(
        f"Performance: PMTiles conversion took {timer.steps[1] - timer.steps[0]:.4f}s, total time: {timer.steps[-1] - timer.steps[0]:.4f}s"
    )

    # Clean up using pathlib
    test_pmtiles_path.unlink(missing_ok=True)
