from unittest.mock import patch

import pytest

from tests.conftest import RESOURCE_ID, job_states
from udata_hydra.analysis import helpers
from udata_hydra.data_formats import Geojson
from udata_hydra.db.resource_job_status import ResourceJobStatus

pytestmark = pytest.mark.asyncio


async def test_analyse_geojson_disabled(fake_check):
    """Test that the function returns None when GEOJSON_TO_PMTILES is False"""
    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", False),
        patch("udata_hydra.analysis.exports.export_pmtiles") as mock_func,
    ):
        check = await fake_check()
        await Geojson(file_name="tests/data/valid.geojson").analyse(check)
        mock_func.assert_not_called()


async def test_analyse_geojson(
    setup_catalog, db, fake_check, rmock, produce_mock, job_status_snapshots
):
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=b"{pretend this is a geojson}")
    with (
        patch("udata_hydra.config.GEOJSON_TO_PMTILES", True),
        patch(
            "udata_hydra.analysis.exports.export_pmtiles",
            return_value=None,
        ),
    ):
        file = await helpers.download_from_check(check, Geojson)
        await file.analyse(check=check)
    assert job_states(job_status_snapshots, "geojson") == ["ANALYSING_GEOJSON"]
    assert await ResourceJobStatus.for_resource(RESOURCE_ID) == {}
    res = await db.fetchrow(f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID}'")
    assert res["parsing_finished_at"] is not None
