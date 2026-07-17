from unittest.mock import patch

import pytest

from udata_hydra.data_formats import Geojson
from udata_hydra.data_formats.geojson.to_pmtiles import DEFAULT_PMTILES_FILENAME
from udata_hydra.utils import storage_path

pytestmark = pytest.mark.asyncio


async def test_geojson_to_pmtiles_invalid_geometry():
    """Test handling of invalid geometry"""
    with pytest.raises(Exception):
        await Geojson(file_name="tests/data/invalid.geojson").to_pmtiles()


async def test_geojson_to_pmtiles_valid_geometry():
    """Test handling of valid geometry"""
    # Make sure that we don't crash even if output pmtiles already exists
    storage_path(DEFAULT_PMTILES_FILENAME).touch()
    with patch("udata_hydra.config.REMOVE_GENERATED_FILES", False):
        pmtiles_file = await Geojson(file_name="tests/data/valid.geojson").to_pmtiles()
    # very (too?) simple test, we could install a specific library to read the file
    with pmtiles_file.path.open("rb") as f:
        header = f.read(7)
    assert header == b"PMTiles"
    # size slightly differs depending on the env
    assert 820 <= pmtiles_file.filesize <= 900
    pmtiles_file.path.unlink()
