"""
NB: we can't use pytest-aiohttp helpers because
it will interfere with the rest of our async code
"""

import pytest

from tests.conftest import (
    NOT_EXISTING_RESOURCE_ID,
    RESOURCE_EXCEPTION_ID,
    RESOURCE_EXCEPTION_TABLE_INDEXES,
    RESOURCE_ID,
)
from udata_hydra.db.resource import Resource
from udata_hydra.utils import is_valid_uri

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
        path="/api/resources-exceptions",
        headers=None,
        json={
            "resource_id": RESOURCE_ID,
            "table_indexes": RESOURCE_EXCEPTION_TABLE_INDEXES,
        },
    )
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.post(
        path="/api/resources-exceptions",
        headers=api_headers_wrong_token,
        json={
            "resource_id": RESOURCE_ID,
            "table_indexes": RESOURCE_EXCEPTION_TABLE_INDEXES,
        },
    )
    assert resp.status == 403

    # Test API call with invalid POST data
    stupid_post_data: dict = {"stupid": "stupid"}
    resp = await client.post(
        path="/api/resources-exceptions", headers=api_headers, json=stupid_post_data
    )
    assert resp.status == 400

    # Test API call success
    resp = await client.post(
        path="/api/resources-exceptions",
        headers=api_headers,
        json={
            "resource_id": RESOURCE_ID,
            "table_indexes": RESOURCE_EXCEPTION_TABLE_INDEXES,
        },
    )
    assert resp.status == 201
    data: dict = await resp.json()
    assert data["resource_id"] == RESOURCE_ID


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
