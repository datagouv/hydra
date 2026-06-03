from datetime import datetime, timedelta, timezone

import nest_asyncio2 as nest_asyncio
import pytest
import pytest_asyncio
from asyncpg.exceptions import UndefinedTableError

from tests.conftest import RESOURCE_ID, RESOURCE_URL
from udata_hydra.cli import (
    purge_checks,
    purge_csv_tables,
    purge_selected_csv_tables,
)

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def _create_parsed_csv_table(db, fake_check, *, with_tables_index: bool = False) -> dict:
    """Fake CSV analysis table (+ optional tables_index row) for purge CLI tests."""
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    if with_tables_index:
        await db.execute(
            "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, url) VALUES($1, $2, $3, $4)",
            md5,
            "{}",
            check.get("resource_id"),
            check.get("url"),
        )
    return {"check": check, "md5": md5, "url": check["url"]}


async def _assert_pg_table_exists(db, md5: str) -> None:
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None


async def _assert_pg_table_missing(db, md5: str) -> None:
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None


@pytest_asyncio.fixture
async def csv_table_for_purge(db, fake_check):
    async def setup(*, with_tables_index: bool = False) -> dict:
        return await _create_parsed_csv_table(db, fake_check, with_tables_index=with_tables_index)

    return setup


async def test_purge_checks(setup_catalog, db, fake_check):
    """Test the purge_checks CLI command"""
    await fake_check(created_at=datetime.now() - timedelta(days=50))
    await fake_check(created_at=datetime.now() - timedelta(days=30))
    await fake_check(created_at=datetime.now() - timedelta(days=10))

    await purge_checks(retention_days=40, quiet=False)
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 2

    await purge_checks(retention_days=20, quiet=False)
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1


@pytest.mark.parametrize(
    "hard_delete,expected_deleted_at",
    [
        (
            False,
            "timestamp",
        ),  # hard_delete=False: table_index entry should be marked as deleted with timestamp
        (True, None),  # hard_delete=True: table_index entry should be completely deleted
    ],
)
async def test_purge_csv_tables(
    setup_catalog, db, csv_table_for_purge, hard_delete, expected_deleted_at
):
    """Test the purge_csv_tables CLI command with different hard_delete values"""
    table = await csv_table_for_purge(with_tables_index=True)
    md5 = table["md5"]
    await _assert_pg_table_exists(db, md5)

    await db.execute(
        f"UPDATE catalog SET url = '{RESOURCE_URL + '-new'}' WHERE resource_id = '{RESOURCE_ID}'"
    )

    await purge_csv_tables(quiet=False, hard_delete=hard_delete)

    await _assert_pg_table_missing(db, md5)

    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    if expected_deleted_at is None:
        assert res is None
    else:
        assert res is not None
        assert res["deleted_at"] is not None
        assert isinstance(res["deleted_at"], datetime)


async def test_purge_csv_tables_url_used_by_other_resource(setup_catalog, db, csv_table_for_purge):
    """We should not delete csv table if the url is used by a resource still active"""
    table = await csv_table_for_purge()
    md5 = table["md5"]
    await _assert_pg_table_exists(db, md5)

    await db.execute("UPDATE catalog SET deleted = TRUE")

    await db.execute(
        "INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority) VALUES($1, $2, $3, $4, $5)",
        "6115eed4acb337ce13b83db3",
        "7a0c10a0-8e6f-403f-a987-2e223b22ee33",
        table["url"],
        False,
        False,
    )

    await purge_csv_tables(quiet=False, hard_delete=False)

    await _assert_pg_table_exists(db, md5)


async def test_purge_csv_tables_url_used_by_deleted_resource_only(
    setup_catalog, db, csv_table_for_purge
):
    """We should delete csv table if all resource with this url are marked as deleted"""
    table = await csv_table_for_purge()
    md5 = table["md5"]
    await _assert_pg_table_exists(db, md5)

    await db.execute("UPDATE catalog SET deleted = TRUE")

    await db.execute(
        "INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority) VALUES($1, $2, $3, $4, $5)",
        "6115eed4acb337ce13b83db3",
        "7a0c10a0-8e6f-403f-a987-2e223b22ee33",
        table["url"],
        True,
        False,
    )

    await purge_csv_tables(quiet=False, hard_delete=False)

    await _assert_pg_table_missing(db, md5)
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    assert res is None


async def test_purge_csv_tables_url_not_in_catalog(setup_catalog, db, csv_table_for_purge):
    """We should delete csv table if the url is not the catalog anymore"""
    table = await csv_table_for_purge()
    md5 = table["md5"]
    await _assert_pg_table_exists(db, md5)

    await db.execute("UPDATE catalog SET url = 'https://example.com/resource-0'")

    await purge_csv_tables(quiet=False, hard_delete=False)

    await _assert_pg_table_missing(db, md5)
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

    await purge_selected_csv_tables(**_kwargs)

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
