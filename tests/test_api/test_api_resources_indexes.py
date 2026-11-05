import pytest

from tests.conftest import NOT_EXISTING_RESOURCE_ID, RESOURCE_ID, RESOURCE_TABLE_INDEXES
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.db.resource_index import ResourceIndex

pytestmark = pytest.mark.asyncio


async def test_get_resource_indexes(setup_catalog, client):
    resp = await client.get(path=f"/api/resources-indexes/{RESOURCE_ID}")
    assert resp.status == 200
    assert await resp.json() == {"resource_id": RESOURCE_ID, "indexes": {}}

    await ResourceIndex.replace(RESOURCE_ID, RESOURCE_TABLE_INDEXES)
    resp = await client.get(path=f"/api/resources-indexes/{RESOURCE_ID}")
    assert resp.status == 200
    assert await resp.json() == {"resource_id": RESOURCE_ID, "indexes": RESOURCE_TABLE_INDEXES}

    resp = await client.get(path=f"/api/resources-indexes/{NOT_EXISTING_RESOURCE_ID}")
    assert resp.status == 404


async def test_replace_resource_indexes(
    setup_catalog, client, api_headers, api_headers_wrong_token
):
    path = f"/api/resources-indexes/{RESOURCE_ID}"
    body = {"indexes": RESOURCE_TABLE_INDEXES}

    resp = await client.put(path=path, headers=None, json=body)
    assert resp.status == 401
    resp = await client.put(path=path, headers=api_headers_wrong_token, json=body)
    assert resp.status == 403

    resp = await client.put(path=path, headers=api_headers, json={"indexes": "stupid"})
    assert resp.status == 400
    resp = await client.put(path=path, headers=api_headers, json={"indexes": {"Nom": "stupid"}})
    assert resp.status == 400
    assert "index type must be one of" in await resp.text()

    resp = await client.put(
        path=f"/api/resources-indexes/{NOT_EXISTING_RESOURCE_ID}", headers=api_headers, json=body
    )
    assert resp.status == 404

    resp = await client.put(path=path, headers=api_headers, json=body)
    assert resp.status == 200
    assert await resp.json() == {"resource_id": RESOURCE_ID, "indexes": RESOURCE_TABLE_INDEXES}
    assert await ResourceIndex.get_by_resource_id(RESOURCE_ID) == RESOURCE_TABLE_INDEXES
    # declaring indexes does not make the resource an exception
    assert await ResourceException.get_by_resource_id(RESOURCE_ID) is None

    # the mapping replaces the previous one: dropped columns lose their index
    resp = await client.put(path=path, headers=api_headers, json={"indexes": {"Nom": "index"}})
    assert resp.status == 200
    assert await ResourceIndex.get_by_resource_id(RESOURCE_ID) == {"Nom": "index"}

    resp = await client.put(path=path, headers=api_headers, json={"indexes": {}})
    assert resp.status == 200
    assert await ResourceIndex.get_by_resource_id(RESOURCE_ID) == {}
