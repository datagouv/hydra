import pytest

from tests.conftest import (
    NOT_EXISTING_RESOURCE_ID,
    RESOURCE_EXCEPTION_ID,
    RESOURCE_ID,
    RESOURCE_TABLE_INDEXES,
)
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.db.resource_index import ResourceIndex

pytestmark = pytest.mark.asyncio


async def test_get_resources_exceptions(setup_catalog_with_resource_exception, client):
    resp = await client.get(path="/api/resources-exceptions")
    assert resp.status == 200
    data: dict = await resp.json()
    assert len(data) == 1
    assert data[0]["resource_id"] == RESOURCE_EXCEPTION_ID


async def test_create_resource_exception(
    setup_catalog, client, api_headers, api_headers_wrong_token
):
    # Test API call with no token
    resp = await client.post(
        path="/api/resources-exceptions", headers=None, json={"resource_id": RESOURCE_ID}
    )
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.post(
        path="/api/resources-exceptions",
        headers=api_headers_wrong_token,
        json={"resource_id": RESOURCE_ID},
    )
    assert resp.status == 403

    # Test API call with invalid POST data
    resp = await client.post(
        path="/api/resources-exceptions", headers=api_headers, json={"stupid": "stupid"}
    )
    assert resp.status == 400

    # Test API post success
    resp = await client.post(
        path="/api/resources-exceptions",
        headers=api_headers,
        json={"resource_id": RESOURCE_ID, "comment": "This is a test comment."},
    )
    assert resp.status == 201
    data: dict = await resp.json()
    assert data["resource_id"] == RESOURCE_ID
    assert data["comment"] == "This is a test comment."

    # Test posting the same resource exception
    resp = await client.post(
        path="/api/resources-exceptions", headers=api_headers, json={"resource_id": RESOURCE_ID}
    )
    assert resp.status == 400
    assert "Resource exception already exists" in await resp.text()


async def test_resource_exception_rejects_table_indexes(
    setup_catalog_with_resource_exception, client, api_headers
):
    """Indexes are declared on /api/resources-indexes: a client still sending them to the
    exceptions gets an error, instead of seeing them silently ignored"""
    resp = await client.post(
        path="/api/resources-exceptions",
        headers=api_headers,
        json={"resource_id": RESOURCE_ID, "table_indexes": RESOURCE_TABLE_INDEXES},
    )
    assert resp.status == 400
    assert "table_indexes" in await resp.text()
    assert await ResourceException.get_by_resource_id(RESOURCE_ID) is None

    resp = await client.put(
        path=f"/api/resources-exceptions/{RESOURCE_EXCEPTION_ID}",
        headers=api_headers,
        json={"table_indexes": RESOURCE_TABLE_INDEXES},
    )
    assert resp.status == 400
    assert "table_indexes" in await resp.text()
    assert await ResourceIndex.get_by_resource_id(RESOURCE_EXCEPTION_ID) == {}


async def test_update_resource_exception(
    setup_catalog_with_resource_exception, client, api_headers, api_headers_wrong_token
):
    # Test API call with invalid token
    resp = await client.put(
        path=f"/api/resources-exceptions/{RESOURCE_EXCEPTION_ID}",
        headers=api_headers_wrong_token,
        json={"comment": "Updated test comment."},
    )
    assert resp.status == 403

    # Test API call with invalid PUT data
    resp = await client.put(
        path=f"/api/resources-exceptions/{RESOURCE_EXCEPTION_ID}",
        headers=api_headers,
        json={"comment": ["stupid"]},
    )
    assert resp.status == 400

    # Test API call with non existing resource id data
    resp = await client.put(
        path=f"/api/resources-exceptions/{NOT_EXISTING_RESOURCE_ID}",
        headers=api_headers,
        json={"comment": "Updated test comment."},
    )
    assert resp.status == 404

    # Test API call with a resource which is in the catalog but has no exception
    resp = await client.put(
        path=f"/api/resources-exceptions/{RESOURCE_ID}",
        headers=api_headers,
        json={"comment": "Updated test comment."},
    )
    assert resp.status == 404

    # Test API call success
    resp = await client.put(
        path=f"/api/resources-exceptions/{RESOURCE_EXCEPTION_ID}",
        headers=api_headers,
        json={"comment": "Updated test comment."},
    )
    assert resp.status == 200
    data: dict = await resp.json()
    assert data["resource_id"] == RESOURCE_EXCEPTION_ID
    assert data["comment"] == "Updated test comment."


async def test_delete_resource_exception(
    setup_catalog_with_resource_exception, client, api_headers, api_headers_wrong_token
):
    # Test API call with wrong token
    resp = await client.delete(
        path=f"/api/resources-exceptions/{RESOURCE_EXCEPTION_ID}",
        headers=api_headers_wrong_token,
    )
    assert resp.status == 403

    # Test API call with non existing resource id data
    resp = await client.delete(
        path=f"/api/resources-exceptions/{NOT_EXISTING_RESOURCE_ID}",
        headers=api_headers,
    )
    assert resp.status == 404

    # Test API call success
    resp = await client.delete(
        path=f"/api/resources-exceptions/{RESOURCE_EXCEPTION_ID}",
        headers=api_headers,
    )
    assert resp.status == 204
