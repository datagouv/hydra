import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_job_status import ResourceJobStatus

pytestmark = pytest.mark.asyncio


async def test_clear_csv_does_not_clear_parquet(setup_catalog):
    await ResourceJobStatus.set(RESOURCE_ID, "csv", "ANALYSING_CSV")
    await ResourceJobStatus.set(RESOURCE_ID, "parquet", "CONVERTING_TO_PARQUET")

    await ResourceJobStatus.clear(RESOURCE_ID, "csv")

    status = await ResourceJobStatus.for_resource(RESOURCE_ID)
    assert "csv" not in status
    assert status["parquet"]["state"] == "CONVERTING_TO_PARQUET"


async def test_clear_last_job_yields_idle(setup_catalog):
    await ResourceJobStatus.set(RESOURCE_ID, "parquet", "CONVERTING_TO_PARQUET")
    await ResourceJobStatus.clear(RESOURCE_ID, "parquet")

    assert await ResourceJobStatus.for_resource(RESOURCE_ID) == {}


async def test_update_job_status_is_atomic(setup_catalog):
    await ResourceJobStatus.set(RESOURCE_ID, "crawler", "ANALYSING_DOWNLOADED_RESOURCE")

    await ResourceJobStatus.update(RESOURCE_ID, "crawler", "csv", "TO_ANALYSE_CSV")

    status = await ResourceJobStatus.for_resource(RESOURCE_ID)
    assert "crawler" not in status
    assert status["csv"]["state"] == "TO_ANALYSE_CSV"


async def test_transition_geojson_to_pmtiles(setup_catalog):
    await ResourceJobStatus.set(RESOURCE_ID, "geojson", "CONVERTING_TO_GEOJSON")

    await ResourceJobStatus.update(RESOURCE_ID, "geojson", "pmtiles", "CONVERTING_TO_PMTILES")

    status = await ResourceJobStatus.for_resource(RESOURCE_ID)
    assert "geojson" not in status
    assert status["pmtiles"]["state"] == "CONVERTING_TO_PMTILES"


async def test_soft_delete_clears_job_status(setup_catalog):
    await ResourceJobStatus.set(RESOURCE_ID, "csv", "ANALYSING_CSV")
    await Resource.delete(RESOURCE_ID)
    assert await ResourceJobStatus.for_resource(RESOURCE_ID) == {}
