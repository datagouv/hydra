"""
NB: we can't use pytest-aiohttp helpers beause
it will interfere with the rest of our async code
"""

import hashlib
from datetime import datetime

import pytest

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "query",
    [
        "url=https://example.com/resource-1",
        "resource_id=c4e3a9fb-4415-488e-ba57-d05269b27adf",
    ],
)
async def test_api_checks_latest(setup_catalog, query, client, fake_check):
    await fake_check(parsing_table=True)
    resp = await client.get(f"/api/checks/latest/?{query}")
    assert resp.status == 200
    data = await resp.json()
    assert data.pop("created_at")
    assert data.pop("id")
    url = "https://example.com/resource-1"
    assert data == {
        "response_time": 0.1,
        "deleted": False,
        "resource_id": "c4e3a9fb-4415-488e-ba57-d05269b27adf",
        "catalog_id": 1,
        "domain": "example.com",
        "error": None,
        "url": url,
        "headers": {"x-do": "you"},
        "timeout": False,
        "dataset_id": "601ddcfc85a59c3a45c2435a",
        "status": 200,
        "parsing_error": None,
        "parsing_finished_at": None,
        "parsing_started_at": None,
        "parsing_table": hashlib.md5(url.encode("utf-8")).hexdigest(),
    }


@pytest.mark.parametrize(
    "query",
    [
        "url=https://example.com/resource-1",
        "resource_id=c4e3a9fb-4415-488e-ba57-d05269b27adf",
    ],
)
async def test_api_checks_all(setup_catalog, query, client, fake_check):
    await fake_check(status=500, error="no-can-do")
    await fake_check()
    resp = await client.get(f"/api/checks/all/?{query}")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 2
    first, second = data
    assert first["status"] == 200
    assert second["status"] == 500
    assert second["error"] == "no-can-do"


async def test_api_status_crawler(setup_catalog, client, fake_check):
    resp = await client.get("/api/status/crawler/")
    assert resp.status == 200
    data = await resp.json()
    assert data == {
        "total": 1,
        "pending_checks": 1,
        "fresh_checks": 0,
        "checks_percentage": 0.0,
        "fresh_checks_percentage": 0.0,
    }

    await fake_check()
    resp = await client.get("/api/status/crawler/")
    assert resp.status == 200
    data = await resp.json()
    assert data == {
        "total": 1,
        "pending_checks": 0,
        "fresh_checks": 1,
        "checks_percentage": 100.0,
        "fresh_checks_percentage": 100.0,
    }


async def test_api_stats(setup_catalog, client, fake_check):
    resp = await client.get("/api/stats/")
    assert resp.status == 200
    data = await resp.json()
    assert data == {
        "status": [
            {"label": "error", "count": 0, "percentage": 0},
            {"label": "timeout", "count": 0, "percentage": 0},
            {"label": "ok", "count": 0, "percentage": 0},
        ],
        "status_codes": [],
    }

    # only the last one should count
    await fake_check()
    await fake_check(timeout=True, status=None)
    await fake_check(status=500, error="error")
    resp = await client.get("/api/stats/")
    assert resp.status == 200
    data = await resp.json()
    assert data == {
        "status": [
            {"label": "error", "count": 1, "percentage": 100.0},
            {"label": "timeout", "count": 0, "percentage": 0},
            {"label": "ok", "count": 0, "percentage": 0},
        ],
        "status_codes": [{"code": 500, "count": 1, "percentage": 100.0}],
    }


async def test_api_resource_created(client, udata_resource_payload):
    resp = await client.post("/api/resource/created/", json=udata_resource_payload)
    assert resp.status == 200
    data = await resp.json()
    assert data == {"message": "created"}

    udata_resource_payload["document"] = None
    resp = await client.post("/api/resource/created/", json=udata_resource_payload)
    assert resp.status == 400
    text = await resp.text()
    assert text == "Missing document body"


async def test_api_resource_updated(client):
    payload = {
        "resource_id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
        "dataset_id": "61fd30cb29ea95c7bc0e1211",
        "document": {
            "id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
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
    resp = await client.post("/api/resource/updated/", json=payload)
    assert resp.status == 200
    data = await resp.json()
    assert data == {"message": "updated"}

    payload["document"] = None
    resp = await client.post("/api/resource/updated/", json=payload)
    assert resp.status == 400
    text = await resp.text()
    assert text == "Missing document body"


async def test_api_resource_updated_url_since_load_catalog(setup_catalog, db, client):
    # We modify the url for this resource
    await db.execute(
        "UPDATE catalog SET url = 'https://example.com/resource-0' "
        "WHERE resource_id = 'c4e3a9fb-4415-488e-ba57-d05269b27adf'"
    )

    # We're sending an update signal on the (dataset_id,resource_id) with the previous url.
    payload = {
        "resource_id": "c4e3a9fb-4415-488e-ba57-d05269b27adf",
        "dataset_id": "601ddcfc85a59c3a45c2435a",
        "document": {
            "id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
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
    resp = await client.post("/api/resource/updated/", json=payload)
    assert resp.status == 200

    res = await db.fetch(
        "SELECT * FROM catalog WHERE resource_id = 'c4e3a9fb-4415-488e-ba57-d05269b27adf'"
    )
    assert len(res) == 1
    res[0]["url"] == "https://example.com/resource-1"


async def test_api_resource_deleted(client):
    payload = {
        "resource_id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
        "dataset_id": "61fd30cb29ea95c7bc0e1211",
        "document": None,
    }
    resp = await client.post("/api/resource/deleted/", json=payload)
    assert resp.status == 200
    data = await resp.json()
    assert data == {"message": "deleted"}
