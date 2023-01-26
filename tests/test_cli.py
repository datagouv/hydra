import hashlib

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


async def test_purge_csv_tables(setup_catalog, db):
    rurl = "https://example.com/resource-1"
    md5 = hashlib.md5(rurl.encode("utf-8")).hexdigest()
    # pretend we have a csv_analysis with a converted table for this url
    await db.execute("INSERT INTO csv_analysis(url, parsing_table) VALUES ($1, $2)", rurl, md5)
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


async def test_purge_csv_tables_url_used_by_other_resource(setup_catalog, db):
    """We should not delete csv table if the url is used by a resource still active"""
    rurl = "https://example.com/resource-1"
    md5 = hashlib.md5(rurl.encode("utf-8")).hexdigest()
    # pretend we have a csv_analysis with a converted table for this url
    await db.execute("INSERT INTO csv_analysis(url, parsing_table) VALUES ($1, $2)", rurl, md5)
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
    # pretend the resource is deleted
    await db.execute("UPDATE catalog SET deleted = TRUE")
    # insert another resource with same url
    await db.execute(
        "INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority) VALUES($1, $2, $3, $4, $5)",
        "6115eed4acb337ce13b83db3", "7a0c10a0-8e6f-403f-a987-2e223b22ee33", rurl, False, False
    )
    # purge
    run("purge_csv_tables")
    # check table is _not_ gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
