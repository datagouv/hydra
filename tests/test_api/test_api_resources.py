"""
NB: we can't use pytest-aiohttp helpers because
it will interfere with the rest of our async code
"""

from datetime import datetime

import pytest

from tests.conftest import DATASET_ID, NOT_EXISTING_RESOURCE_ID, RESOURCE_ID, RESOURCE_URL
from udata_hydra.db.resource import Resource
from udata_hydra.utils import is_valid_uri

pytestmark = pytest.mark.asyncio


async def test_get_resource(setup_catalog, client):
    # Test invalid resource_id
    resp = await client.get(path="/api/resources/STUPID-ID")
    assert resp.status == 400

    # Test non existing resource_id
    resp = await client.get(path=f"/api/resources/{NOT_EXISTING_RESOURCE_ID}")
    assert resp.status == 404

    # Test existing resource
    resp = await client.get(f"/api/resources/{RESOURCE_ID}")
    assert resp.status == 200
    data: dict = await resp.json()
    assert data["dataset_id"] == DATASET_ID
    assert data["resource_id"] == RESOURCE_ID
    assert data["status"] is None


@pytest.mark.parametrize("resource_status,resource_status_verbose", list(Resource.STATUSES.items()))
async def test_get_resource_status(
    db, client, insert_fake_resource, resource_status, resource_status_verbose
):
    # Create resource with specific status
    await insert_fake_resource(db, status=resource_status)

    # Test invalid resource_id
    resp = await client.get(path="/api/resources/STUPID-ID/status")
    assert resp.status == 400

    # Test non existing resource_id
    resp = await client.get(path=f"/api/resources/{NOT_EXISTING_RESOURCE_ID}")
    assert resp.status == 404

    # Test existing resource
    resp = await client.get(f"/api/resources/{RESOURCE_ID}/status")
    assert resp.status == 200
    data = await resp.json()
    assert data["resource_id"] == RESOURCE_ID
    assert data["status"] == resource_status
    assert data["status_verbose"] == resource_status_verbose
    assert is_valid_uri(data["latest_check_url"])
    assert data["latest_check_url"].endswith(f"/api/checks/latest?resource_id={RESOURCE_ID}")


async def test_create_resource(
    client, api_headers, api_headers_wrong_token, udata_resource_payload
):
    # Test API call with no token
    resp = await client.post(path="/api/resources/", headers=None, json=udata_resource_payload)
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.post(
        path="/api/resources/", headers=api_headers_wrong_token, json=udata_resource_payload
    )
    assert resp.status == 403

    # Test API call with invalid POST data
    stupid_post_data: dict = {"stupid": "stupid"}
    resp = await client.post(path="/api/resources/", headers=api_headers, json=stupid_post_data)
    assert resp.status == 400

    # Test API call success
    resp = await client.post(
        path="/api/resources/", headers=api_headers, json=udata_resource_payload
    )
    # assert resp.status == 201
    data: dict = await resp.json()
    assert data["id"] == "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec"

    # Test API call with missing document body
    udata_resource_payload["document"] = None
    resp = await client.post(
        path="/api/resources/", headers=api_headers, json=udata_resource_payload
    )
    assert resp.status == 400
    text = await resp.text()
    assert text == "Missing document body"


async def test_update_resource(client, api_headers, api_headers_wrong_token):
    # Test invalid PUT data
    stupid_post_data: dict = {"stupid": "stupid"}
    resp = await client.put(
        path=f"/api/resources/{RESOURCE_ID}/", headers=api_headers, json=stupid_post_data
    )
    assert resp.status == 400

    payload = {
        "resource_id": RESOURCE_ID,
        "dataset_id": DATASET_ID,
        "document": {
            "id": RESOURCE_ID,
            "url": RESOURCE_URL,
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
    resp = await client.put(path=f"/api/resources/{RESOURCE_ID}", headers=None, json=payload)
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.put(
        path=f"/api/resources/{RESOURCE_ID}", headers=api_headers_wrong_token, json=payload
    )
    assert resp.status == 403

    # Test API call success
    resp = await client.put(path=f"/api/resources/{RESOURCE_ID}", headers=api_headers, json=payload)
    assert resp.status == 200
    data: dict = await resp.json()
    assert data["id"] == RESOURCE_ID

    # Test API call with missing document body
    payload["document"] = None
    resp = await client.put(path=f"/api/resources/{RESOURCE_ID}", headers=api_headers, json=payload)
    assert resp.status == 400
    text: str = await resp.text()
    assert text == "Missing document body"


async def test_update_resource_url_since_load_catalog(setup_catalog, db, client, api_headers):
    # We modify the url for this resource
    await db.execute(
        "UPDATE catalog SET url = 'https://example.com/resource-0' "
        f"WHERE resource_id = '{RESOURCE_ID}'"
    )

    # We're sending an update signal on the (dataset_id,resource_id) with the previous url.
    payload = {
        "resource_id": RESOURCE_ID,
        "dataset_id": DATASET_ID,
        "document": {
            "id": RESOURCE_ID,
            "url": RESOURCE_URL,
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
    resp = await client.put(path=f"/api/resources/{RESOURCE_ID}", headers=api_headers, json=payload)
    assert resp.status == 200

    res = await db.fetch(f"SELECT * FROM catalog WHERE resource_id = '{RESOURCE_ID}'")
    assert len(res) == 1
    res[0]["url"] == RESOURCE_URL


async def test_delete_resource(client, api_headers, api_headers_wrong_token):
    # Test invalid resource_id
    resp = await client.delete(path="/api/resources/STUPID-ID", headers=api_headers)
    assert resp.status == 400

    # Test non existing resource_id
    resp = await client.delete(
        path=f"/api/resources/{NOT_EXISTING_RESOURCE_ID}",
        headers=api_headers,
    )
    assert resp.status == 404

    # Test API call with no token
    resp = await client.delete(path=f"/api/resources/{RESOURCE_ID}", headers=None)
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.delete(
        path=f"/api/resources/{RESOURCE_ID}", headers=api_headers_wrong_token
    )
    assert resp.status == 403

    # Test API call success
    resp = await client.delete(path=f"/api/resources/{RESOURCE_ID}", headers=api_headers)
    assert resp.status == 200
