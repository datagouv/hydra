import asyncpg
import nest_asyncio2 as nest_asyncio
import pytest

from tests.conftest import DATABASE_URL, DATASET_ID, RESOURCE_ID, RESOURCE_URL
from udata_hydra.cli import load_catalog
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
    await load_catalog(url=catalog, drop_meta=False, drop_all=False, quiet=False)
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = TRUE")
    assert len(res) == 1
    res = await db.fetch("SELECT id FROM catalog")
    assert len(res) == 1


async def test_catalog_deleted_with_checked_resource(
    setup_catalog, db, rmock, mocker, analysis_mock
):
    mocker.patch("udata_hydra.config.WEBHOOK_ENABLED", False)

    rurl = RESOURCE_URL
    rmock.get(rurl)
    await start_checks(iterations=1)

    res = await db.fetch("SELECT id FROM catalog WHERE deleted = FALSE and last_check IS NOT NULL")
    assert len(res) == 1

    with open("tests/data/catalog.csv", "rb") as cfile:
        catalog_content = cfile.readlines()
    catalog = "https://example.com/catalog"
    # feed empty catalog, should delete the previously loaded resource
    rmock.get(catalog, status=200, body=catalog_content[0])
    # we have to mock the pool again, as it's closed in the finally clause of start_checks
    m = mocker.patch("udata_hydra.context.pool")
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, max_size=50)
    m.return_value = pool
    await load_catalog(url=catalog, drop_meta=False, drop_all=False, quiet=False)
    res = await db.fetch("SELECT id FROM catalog WHERE deleted = TRUE")
    assert len(res) == 1
    res = await db.fetch("SELECT id FROM catalog")
    assert len(res) == 1


async def test_catalog_deleted_with_new_url(setup_catalog, db, rmock, mocker, analysis_mock):
    # load a new catalog with a new URL for this resource
    with open("tests/data/catalog.csv", "r") as cfile:
        catalog_content = cfile.readlines()
    catalog_content[1] = catalog_content[1].replace("resource-1", "resource-2")
    catalog_content = "\n".join(catalog_content)
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content.encode("utf-8"))
    await load_catalog(url=catalog, drop_meta=False, drop_all=False, quiet=False)

    # check catalog coherence, replacing the URL in the existing entry
    res = await db.fetch("SELECT * FROM catalog WHERE resource_id = $1", RESOURCE_ID)
    assert len(res) == 1
    assert res[0]["deleted"] is False
    assert "resource-2" in res[0]["url"]
