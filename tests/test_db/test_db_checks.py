import pytest
from asyncpg import Record

from tests.conftest import RESOURCE_ID
from udata_hydra.db.check import Check

pytestmark = pytest.mark.asyncio


async def test_delete_check(setup_catalog, fake_check):
    # Delete a non existing check to see if it fails
    await Check.delete(check_id=123)

    # Create a check
    await fake_check()
    checks: list[Record] = await Check.get_all(resource_id=RESOURCE_ID)
    assert len(checks) == 1
    check_id: int = checks[0]["id"]

    # Delete the check
    await Check.delete(check_id)
    checks: list[Record] = await Check.get_all(resource_id=RESOURCE_ID)
    assert len(checks) == 0
