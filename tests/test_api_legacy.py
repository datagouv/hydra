"""
NB: we can't use pytest-aiohttp helpers because
it will interfere with the rest of our async code
"""

import hashlib
import json
from datetime import datetime
from typing import Callable

import pytest
from aiohttp import RequestInfo
from aiohttp.client_exceptions import ClientError, ClientResponseError
from yarl import URL

from tests.conftest import DATASET_ID, RESOURCE_ID
from udata_hydra.db.resource import Resource
from udata_hydra.utils import is_valid_uri

pytestmark = pytest.mark.asyncio


async def test_api_get_resource_legacy(setup_catalog, client):
    query: str = f"dataset_id={DATASET_ID}&resource_id={RESOURCE_ID}"
    resp = await client.get(f"/api/resources/?{query}")
    assert resp.status == 200
    data: dict = await resp.json()
    assert data["dataset_id"] == DATASET_ID
    assert data["resource_id"] == RESOURCE_ID
    assert data["status"] is None


async def test_api_create_resource_legacy(
    client, api_headers, api_headers_wrong_token, udata_resource_payload
):
    # Test API call with no token
    resp = await client.post(
        path="/api/resource/created/", headers=None, json=udata_resource_payload
    )
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.post(
        path="/api/resource/created/", headers=api_headers_wrong_token, json=udata_resource_payload
    )
    assert resp.status == 403

    # Test API call with invalid POST data
    stupid_post_data: dict = {"stupid": "stupid"}
    resp = await client.post(
        path="/api/resource/created/", headers=api_headers, json=stupid_post_data
    )
    assert resp.status == 400

    # Test API call success
    resp = await client.post(
        path="/api/resource/created/", headers=api_headers, json=udata_resource_payload
    )
    assert resp.status == 200
    data: dict = await resp.json()
    assert data == {"message": "created"}

    # Test API call with missing document body
    udata_resource_payload["document"] = None
    resp = await client.post(
        path="/api/resource/created/", headers=api_headers, json=udata_resource_payload
    )
    assert resp.status == 400
    text = await resp.text()
    assert text == "Missing document body"


async def test_api_update_resource_legacy(client, api_headers, api_headers_wrong_token):
    # Test invalid PUT data
    stupid_post_data: dict = {"stupid": "stupid"}
    resp = await client.post(
        path="/api/resource/updated/", headers=api_headers, json=stupid_post_data
    )
    assert resp.status == 400

    payload = {
        "resource_id": RESOURCE_ID,
        "dataset_id": DATASET_ID,
        "document": {
            "id": RESOURCE_ID,
            "url": "http://dev.local/",
            "title": "random title",
            "description": "random description",
            "filetype": "file",
            "type": "documentation",
            "mime": "text/plain",
            "filesize": 1024,
            "checksum_type": "sha1",
            "checksum_value": "b7b1cd8230881b18b6b487d550039949867ec7c5",
            "created_at": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat(),
        },
    }

    # Test API call with no token
    resp = await client.post(path="/api/resource/updated/", headers=None, json=payload)
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.post(
        path="/api/resource/updated/", headers=api_headers_wrong_token, json=payload
    )
    assert resp.status == 403

    # Test API call success
    resp = await client.post(path="/api/resource/updated/", headers=api_headers, json=payload)
    assert resp.status == 200
    data: dict = await resp.json()
    assert data == {"message": "updated"}

    # Test API call with missing document body
    payload["document"] = None
    resp = await client.post(path="/api/resource/updated/", headers=api_headers, json=payload)
    assert resp.status == 400
    text = await resp.text()
    assert text == "Missing document body"


async def test_api_update_resource_url_since_load_catalog_legacy(
    setup_catalog, db, client, api_headers
):
    # We modify the url for this resource
    await db.execute(
        "UPDATE catalog SET url = 'https://example.com/resource-0' "
        "WHERE resource_id = 'c4e3a9fb-4415-488e-ba57-d05269b27adf'"
    )

    # We're sending an update signal on the (dataset_id,resource_id) with the previous url.
    payload = {
        "resource_id": RESOURCE_ID,
        "dataset_id": DATASET_ID,
        "document": {
            "id": RESOURCE_ID,
            "url": "https://example.com/resource-1",
            "title": "random title",
            "description": "random description",
            "filetype": "file",
            "type": "documentation",
            "mime": "text/plain",
            "filesize": 1024,
            "checksum_type": "sha1",
            "checksum_value": "b7b1cd8230881b18b6b487d550039949867ec7c5",
            "created_at": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat(),
        },
    }
    # It does not create any duplicated resource.
    # The existing entry get updated accordingly.

    # Test API call success
    resp = await client.post(path="/api/resource/updated/", headers=api_headers, json=payload)
    assert resp.status == 200

    res = await db.fetch(f"SELECT * FROM catalog WHERE resource_id = '{RESOURCE_ID}'")
    assert len(res) == 1
    res[0]["url"] == "https://example.com/resource-1"


async def test_api_delete_resource_legacy(client, api_headers, api_headers_wrong_token):
    # Test invalid DELETE data
    stupid_delete_data: dict = {"stupid": "stupid"}
    resp = await client.post(
        path="/api/resource/deleted/", headers=api_headers, json=stupid_delete_data
    )
    assert resp.status == 400

    payload = {
        "resource_id": RESOURCE_ID,
        "dataset_id": DATASET_ID,
        "document": None,
    }

    # Test API call with no token
    resp = await client.post(path="/api/resource/deleted/", headers=None, json=payload)
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.post(
        path="/api/resource/deleted/", headers=api_headers_wrong_token, json=payload
    )
    assert resp.status == 403

    # Test API call success
    resp = await client.post(path="/api/resource/deleted/", headers=api_headers, json=payload)
    assert resp.status == 200
    data: dict = await resp.json()
    assert data == {"message": "deleted"}
