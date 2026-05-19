from unittest.mock import patch

import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.analysis.geojson import analyse_geojson

pytestmark = pytest.mark.asyncio


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


async def test_analyse_geojson(setup_catalog, db, fake_check, rmock, produce_mock):
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=b"{pretend this is a geojson}")
    pmtiles_size = 100
    pmtiles_url = "http://minio/test.pmtiles"
    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", True),
        patch(
            "udata_hydra.analysis.geojson.geojson_to_pmtiles",
            return_value=(pmtiles_size, pmtiles_url),
        ),
    ):
        await analyse_geojson(check=check)
    res = await db.fetchrow(f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID}'")
    assert res["parsing_finished_at"] is not None
    assert res["pmtiles_size"] == pmtiles_size
    assert res["pmtiles_url"] == pmtiles_url
