import nest_asyncio
import pytest

from minicli import run

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_purge_checks(setup_catalog, db, fake_check):
    await fake_check()
    await fake_check()
    check = await fake_check()
    run("purge_checks", limit=2)
    res = await db.fetch("SELECT id FROM checks WHERE resource_id = $1", check["resource_id"])
    assert len(res) == 2


async def test_purge_csv_tables(setup_catalog, db, fake_check):
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
    # pretend the resource is deleted
    await db.execute("UPDATE catalog SET deleted = TRUE")
    # purge
    run("purge_csv_tables")
    # check table is gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    assert res is None


async def test_purge_csv_tables_url_used_by_other_resource(setup_catalog, db, fake_check):
    """We should not delete csv table if the url is used by a resource still active"""
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
    # pretend the resource is deleted
    await db.execute("UPDATE catalog SET deleted = TRUE")
    # insert another resource with same url
    await db.execute(
        "INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority) VALUES($1, $2, $3, $4, $5)",
        "6115eed4acb337ce13b83db3", "7a0c10a0-8e6f-403f-a987-2e223b22ee33", check["url"], False, False
    )
    # purge
    run("purge_csv_tables")
    # check table is _not_ gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None


async def test_purge_csv_tables_url_used_by_deleted_resource_only(setup_catalog, db, fake_check):
    """We should delete csv table if all resource with this url are marked as deleted"""
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
    # pretend the resource is deleted
    await db.execute("UPDATE catalog SET deleted = TRUE")
    # insert another deleted resource with same url
    await db.execute(
        "INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority) VALUES($1, $2, $3, $4, $5)",
        "6115eed4acb337ce13b83db3", "7a0c10a0-8e6f-403f-a987-2e223b22ee33", check["url"], True, False
    )
    # purge
    run("purge_csv_tables")
    # check table is gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    assert res is None


async def test_load_catalog_url_has_changed(setup_catalog, rmock, db, catalog_content):
    # the resource url has changed in comparison to load_catalog
    catalog_content = catalog_content.replace(b"https://example.com/resource-1", b"https://example.com/resource-0")
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    run("load_catalog", url=catalog)

    # check that we still only have one entry for this resource in the catalog
    res = await db.fetch("SELECT * FROM catalog WHERE resource_id = $1",
                         "c4e3a9fb-4415-488e-ba57-d05269b27adf")
    assert len(res) == 1
    assert res[0]["url"] == "https://example.com/resource-0"
    assert res[0]["deleted"] is False
