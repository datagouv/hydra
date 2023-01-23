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
