import nest_asyncio
import pytest
from minicli import run

from tests.conftest import DATASET_ID, RESOURCE_ID, RESOURCE_URL
from udata_hydra.crawl import start_checks

pytestmark = pytest.mark.asyncio
# allows nested async to test async with async :mindblown:
nest_asyncio.apply()


async def test_catalog(setup_catalog, db):
    res = await db.fetch(
        "SELECT * FROM catalog WHERE resource_id = $1",
        RESOURCE_ID,
    )
    # Only one resource because the other belonged to an archived dataset
    assert len(res) == 1
    resource = res[0]
    assert resource["url"] == RESOURCE_URL
    assert resource["dataset_id"] == DATASET_ID
    assert resource["status"] is None


async def test_catalog_deleted(setup_catalog, db, rmock):
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = FALSE")
    assert len(res) == 1
    with open("tests/data/catalog.csv", "rb") as cfile:
        catalog_content = cfile.readlines()
    catalog = "https://example.com/catalog"
    # feed empty catalog, should delete the previously loaded resource
    rmock.get(catalog, status=200, body=catalog_content[0])
    run("load_catalog", url=catalog)
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = TRUE")
    assert len(res) == 1
    res = await db.fetch("SELECT id FROM catalog")
    assert len(res) == 1


async def test_catalog_deleted_with_checked_resource(
    setup_catalog, db, rmock, event_loop, mocker, analysis_mock
):
    mocker.patch("udata_hydra.config.WEBHOOK_ENABLED", False)

    rurl = RESOURCE_URL
    rmock.get(rurl)
    event_loop.run_until_complete(start_checks(iterations=1))

    res = await db.fetch("SELECT id FROM catalog WHERE deleted = FALSE and last_check IS NOT NULL")
    assert len(res) == 1

    with open("tests/data/catalog.csv", "rb") as cfile:
        catalog_content = cfile.readlines()
    catalog = "https://example.com/catalog"
    # feed empty catalog, should delete the previously loaded resource
    rmock.get(catalog, status=200, body=catalog_content[0])
    run("load_catalog", url=catalog)
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = TRUE")
    assert len(res) == 1
    res = await db.fetch("SELECT id FROM catalog")
    assert len(res) == 1


async def test_catalog_deleted_with_new_url(
    setup_catalog, db, rmock, event_loop, mocker, analysis_mock
):
    # load a new catalog with a new URL for this resource
    with open("tests/data/catalog.csv", "r") as cfile:
        catalog_content = cfile.readlines()
    catalog_content[1] = catalog_content[1].replace("resource-1", "resource-2")
    catalog_content = "\n".join(catalog_content)
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content.encode("utf-8"))
    run("load_catalog", url=catalog)

    # check catalog coherence, replacing the URL in the existing entry
    res = await db.fetch("SELECT * FROM catalog WHERE resource_id = $1", RESOURCE_ID)
    assert len(res) == 1
    assert res[0]["deleted"] is False
    assert "resource-2" in res[0]["url"]
