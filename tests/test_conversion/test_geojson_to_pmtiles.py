import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.data_formats import Geojson, PMTiles
from udata_hydra.utils.s3 import S3Client
from udata_hydra.utils.timer import Timer

log = logging.getLogger("udata-hydra")

pytestmark = pytest.mark.asyncio


async def test_geojson_to_pmtiles_invalid_geometry():
    """Test handling of invalid geometry"""
    with pytest.raises(Exception):
        await Geojson(path="tests/data/invalid.geojson").to_pmtiles()


async def test_geojson_to_pmtiles_valid_geometry(mocker):
    """Test handling of valid geometry"""
    # Make sure that we don't crash even if output pmtiles already exists
    Path(f"{RESOURCE_ID}.pmtiles").touch()
    with patch("udata_hydra.config.REMOVE_GENERATED_FILES", False):
        pmtiles_file = await Geojson(path="tests/data/valid.geojson").to_pmtiles()
    # very (too?) simple test, we could install a specific library to read the file
    with open(pmtiles_file.path, "rb") as f:
        header = f.read(7)
    assert header == b"PMTiles"
    # size slightly differs depending on the env
    assert 850 <= pmtiles_file.filesize <= 900
    pmtiles_file.path.unlink()


@pytest.mark.slow
async def test_geojson_to_pmtiles_big_file(mocker, input_file: str | None):
    """Test performance with a GeoJSON file

    :input_file: Path to the GeoJSON file to test (mandatory)
    """
    if input_file is None:
        pytest.skip(reason="No input_file provided, skipping performance test")
        return

    geojson_file = Geojson(path="tests/data" / Path(input_file))

    # Create timer for performance measurement
    timer = Timer("geojson-to-pmtiles-performance-test")

    with patch("udata_hydra.config.REMOVE_GENERATED_FILES", False):

        # Test the performance of geojson_to_pmtiles with the real file
        pmtiles_file = await geojson_file.to_pmtiles()
        timer.mark("pmtiles-conversion")

    # Verify the PMTiles file was created correctly
    with pmtiles_file.path.open("rb") as f:
        header = f.read(7)
    assert header == b"PMTiles"

    # The size should be significantly larger than the small test file
    assert pmtiles_file.filesize > 5000  # Should be much larger than the 850-900 range of small file

    # Stop timer and log performance results
    timer.stop()

    # Log performance results
    log.info(f"PMTiles conversion completed. Input GeoJSON size: {geojson_file.filesize} bytes")
    log.info(f"Output PMTiles size: {pmtiles_file.filesize} bytes")
    log.info(
        f"Performance: PMTiles conversion took {timer.steps[1] - timer.steps[0]:.4f}s, total time: {timer.steps[-1] - timer.steps[0]:.4f}s"
    )

    # Clean up using pathlib
    pmtiles_file.path.unlink(missing_ok=True)
