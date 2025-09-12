from datetime import datetime, timedelta, timezone

import nest_asyncio
import pytest
from asyncpg.exceptions import UndefinedTableError
from minicli import run

from tests.conftest import RESOURCE_ID

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_purge_checks(setup_catalog, db, fake_check):
    """Test the purge_checks CLI command"""
    await fake_check(created_at=datetime.now() - timedelta(days=50))
    await fake_check(created_at=datetime.now() - timedelta(days=30))
    await fake_check(created_at=datetime.now() - timedelta(days=10))

    run("purge_checks", retention_days=40)
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 2

    run("purge_checks", retention_days=20)
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1


@pytest.mark.parametrize(
    "hard_delete,expected_deleted",
    [
        (False, True),  # hard_delete=False: table_index entry should be marked as deleted
        (True, None),  # hard_delete=True: table_index entry should be completely deleted
    ],
)
async def test_purge_csv_tables(setup_catalog, db, fake_check, hard_delete, expected_deleted):
    """Test the purge_csv_tables CLI command with different hard_delete values"""
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')

    # Create the tables_index entry
    await db.execute(
        "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, url) VALUES($1, $2, $3, $4)",
        md5,
        "{}",
        check.get("resource_id"),
        check.get("url"),
    )

    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None

    # pretend the resource is deleted
    await db.execute("UPDATE catalog SET deleted = TRUE")

    # purge with the specified hard_delete parameter
    run("purge_csv_tables", hard_delete=hard_delete)

    # check table is gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None

    # Check tables_index entry based on hard_delete parameter
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    if expected_deleted is None:
        # Entry should be completely deleted
        assert res is None
    else:
        # Entry should exist and be marked as deleted
        assert res is not None
        assert res["deleted"] == expected_deleted


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
        "6115eed4acb337ce13b83db3",
        "7a0c10a0-8e6f-403f-a987-2e223b22ee33",
        check["url"],
        False,
        False,
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
        "6115eed4acb337ce13b83db3",
        "7a0c10a0-8e6f-403f-a987-2e223b22ee33",
        check["url"],
        True,
        False,
    )

    # purge
    run("purge_csv_tables")

    # check table is gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    assert res is None


async def test_purge_csv_tables_url_not_in_catalog(setup_catalog, db, fake_check):
    """We should delete csv table if the url is not the catalog anymore"""
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')

    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None

    # pretend the resource URL have been updated
    await db.execute("UPDATE catalog SET url = 'https://example.com/resource-0'")

    # purge
    run("purge_csv_tables")

    # check table is gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    assert res is None


@pytest.mark.parametrize(
    "_kwargs",
    (
        # in both cases by construction the number of
        # remaining tables should be the value of the kwarg
        {"retention_days": 6},
        {"retention_tables": 4},
    ),
)
async def test_purge_selected_csv_tables(setup_catalog, db, fake_check, _kwargs):
    """Test the purge_selected_csv_tables CLI command"""
    # pretend we have a bunch of tables
    nb = 10
    tables = []
    for k in range(1, nb + 1):
        check = await fake_check(
            resource=k,
            parsing_table=True,
            resource_id=RESOURCE_ID[: -len(str(k))] + str(k),
        )
        md5 = check["parsing_table"]
        tables.append(md5)
        await db.execute(f'CREATE TABLE "{md5}"(id serial)')
        await db.execute(
            "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, url) VALUES($1, $2, $3, $4)",
            md5,
            "{}",
            check.get("resource_id"),
            check.get("url"),
        )
        # setting that each resource was created on a specific day from today
        await db.execute(
            f"UPDATE tables_index SET created_at = $1 WHERE parsing_table = '{md5}'",
            datetime.now(timezone.utc) - timedelta(days=k - 1),
        )

    tb_idx = await db.fetch("SELECT * FROM tables_index")
    assert len(tb_idx) == nb
    checks = await db.fetch("SELECT * FROM checks")
    assert len(checks) == nb
    assert all(check["parsing_table"] is not None for check in checks)

    run("purge_selected_csv_tables", **_kwargs)
    tb_idx = await db.fetch("SELECT * FROM tables_index")
    expected_count = list(_kwargs.values())[0]
    assert len(tb_idx) == expected_count

    # by construction, the tables we keep are the X first ones we created
    assert all(tb["parsing_table"] in tables[:expected_count] for tb in tb_idx)

    for idx, table_name in enumerate(tables):
        if idx + 1 <= expected_count:
            await db.fetch(f'SELECT * FROM "{table_name}"')
            check = await db.fetch(
                f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID[: -len(str(idx + 1))] + str(idx + 1)}'"
            )
            assert len(check) == 1
            assert check[0]["parsing_table"] == table_name
        else:
            with pytest.raises(UndefinedTableError):
                await db.execute(f'SELECT * FROM "{table_name}"')
            check = await db.fetch(
                f"SELECT * FROM checks WHERE resource_id='{RESOURCE_ID[: -len(str(idx + 1))] + str(idx + 1)}'"
            )
            assert len(check) == 1
            assert check[0]["parsing_table"] is None
