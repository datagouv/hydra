import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio


async def test_clear_csv_does_not_clear_parquet(setup_catalog):
    await Resource.set_job_status(RESOURCE_ID, "csv", "ANALYSING_CSV")
    await Resource.set_job_status(RESOURCE_ID, "parquet", "CONVERTING_TO_PARQUET")

    await Resource.clear_job_status(RESOURCE_ID, "csv")

    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert "csv" not in resource["status"]
    assert resource["status"]["parquet"]["state"] == "CONVERTING_TO_PARQUET"


async def test_clear_last_job_yields_idle(setup_catalog):
    await Resource.set_job_status(RESOURCE_ID, "parquet", "CONVERTING_TO_PARQUET")
    await Resource.clear_job_status(RESOURCE_ID, "parquet")

    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["status"] == {}


async def test_update_job_status_is_atomic(setup_catalog):
    await Resource.set_job_status(RESOURCE_ID, "crawler", "ANALYSING_DOWNLOADED_RESOURCE")

    await Resource.update_job_status(RESOURCE_ID, "crawler", "csv", "TO_ANALYSE_CSV")

    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert "crawler" not in resource["status"]
    assert resource["status"]["csv"]["state"] == "TO_ANALYSE_CSV"


async def test_transition_geojson_to_pmtiles(setup_catalog):
    await Resource.set_job_status(RESOURCE_ID, "geojson", "CONVERTING_TO_GEOJSON")

    await Resource.update_job_status(RESOURCE_ID, "geojson", "pmtiles", "CONVERTING_TO_PMTILES")

    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert "geojson" not in resource["status"]
    assert resource["status"]["pmtiles"]["state"] == "CONVERTING_TO_PMTILES"
