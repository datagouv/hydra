import nest_asyncio
import pytest

from tests.conftest import RESOURCE_ID
from udata_hydra.cli import insert_resource_into_catalog, load_catalog
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_load_catalog_url_has_changed(setup_catalog, rmock, db, catalog_content):
    """Test the load_catalog CLI command when resource URL has changed"""
    # the resource url has changed in comparison to load_catalog
    catalog_content = catalog_content.replace(
        b"https://example.com/resource-1", b"https://example.com/resource-0"
    )
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    # Call the async function directly - CliRunner.invoke() doesn't properly await in async tests
    await load_catalog(url=catalog, drop_meta=False, drop_all=False, quiet=False)

    # check that we still only have one entry for this resource in the catalog
    res = await db.fetch("SELECT * FROM catalog WHERE resource_id = $1", f"{RESOURCE_ID}")
    assert len(res) == 1
    assert res[0]["url"] == "https://example.com/resource-0"
    assert res[0]["deleted"] is False


async def test_load_catalog_harvest_modified_at_has_changed(
    setup_catalog, rmock, db, catalog_content
):
    """Test the load_catalog CLI command when harvest_modified_at has changed"""
    # the harvest_modified_at has changed in comparison to load_catalog
    catalog_content = catalog_content.replace(b'""\n', b'"2025-03-14 15:49:16.876+02"\n')
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    # Call the async function directly - CliRunner.invoke() doesn't properly await in async tests
    await load_catalog(url=catalog, drop_meta=False, drop_all=False, quiet=False)

    # check that harvest metadata has been updated
    res = await db.fetch("SELECT * FROM catalog WHERE resource_id = $1", f"{RESOURCE_ID}")
    assert len(res) == 1
    from datetime import datetime, timezone

    expected_date = datetime(2025, 3, 14, 15, 49, 16, 876000, tzinfo=timezone.utc)
    assert res[0]["harvest_modified_at"] == expected_date


async def test_insert_resource_in_catalog(rmock):
    """Test the insert_resource_into_catalog CLI command"""
    new_dataset_id = "a" * 24
    new_resource_url = "https://new-url.xyz"
    rmock.get(
        f"https://www.data.gouv.fr/api/2/datasets/resources/{RESOURCE_ID}/",
        status=200,
        payload={
            "dataset_id": new_dataset_id,
            "resource": {
                "id": RESOURCE_ID,
                "url": new_resource_url,
            },
        },
    )
    # Call the async function directly - CliRunner.invoke() doesn't properly await in async tests
    await insert_resource_into_catalog(resource_id=RESOURCE_ID)
    resource = await Resource.get(RESOURCE_ID)
    assert resource is not None
    assert resource["dataset_id"] == new_dataset_id
    assert resource["url"] == new_resource_url
