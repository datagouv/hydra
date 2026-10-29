import pytest

from udata_hydra import config

pytestmark = pytest.mark.asyncio


async def test_get_max_sizes(client):
    resp = await client.get(path="/api/config")
    assert resp.status == 200
    data: dict = await resp.json()
    assert data == {
        "max_filesize": config.MAX_FILESIZE_ALLOWED
        | {"default": config.DEFAULT_MAX_FILESIZE_ALLOWED}
    }
