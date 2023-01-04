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
