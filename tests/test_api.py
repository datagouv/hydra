"""
NB: we can't use pytest-aiohttp helpers beause
it will interfere with the rest of our async code
"""
from datetime import datetime, timedelta
import pytest
import pytest_asyncio

from aiohttp.test_utils import TestClient, TestServer

from udata_hydra.app import app_factory

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def client():
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.mark.parametrize(
    "query",
    [
        "url=https://example.com/resource-1",
        "resource_id=c4e3a9fb-4415-488e-ba57-d05269b27adf",
    ],
)
async def test_api_latest(setup_catalog, query, client, fake_check):
    await fake_check()
    resp = await client.get(f"/api/checks/latest/?{query}")
    assert resp.status == 200
    data = await resp.json()
    assert data.pop("created_at")
    assert data.pop("id")
    assert data == {
        "response_time": 0.1,
        "deleted": False,
        "resource_id": "c4e3a9fb-4415-488e-ba57-d05269b27adf",
        "catalog_id": 1,
        "domain": "example.com",
        "error": None,
        "url": "https://example.com/resource-1",
        "headers": {"x-do": "you"},
        "timeout": False,
        "dataset_id": "601ddcfc85a59c3a45c2435a",
        "status": 200,
    }


@pytest.mark.parametrize(
    "query",
    [
        "url=https://example.com/resource-1",
        "resource_id=c4e3a9fb-4415-488e-ba57-d05269b27adf",
    ],
)
async def test_api_all(setup_catalog, query, client, fake_check):
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


async def test_api_status(setup_catalog, client, fake_check):
    resp = await client.get("/api/status/")
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
    resp = await client.get("/api/status/")
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


@pytest.mark.skip(reason="Unimplemented endpoint")
async def test_changed_last_modified(setup_catalog, client, fake_check):
    check = await fake_check(
        headers={
            "last-modified": "Wed, 21 Oct 2015 07:28:00 GMT",
            "content-length": 1,
        }
    )
    resp = await client.get(f"/api/changed/?url={check['url']}")
    assert resp.status == 200
    assert await resp.json() == {
        "changed_at": "2015-10-21T07:28:00",
        "detection": "last-modified",
    }

    # last-modified takes precendence over content-length comparison
    check = await fake_check(
        headers={
            "last-modified": "Wed, 21 Oct 2015 07:28:00 GMT",
            "content-length": 2,
        }
    )
    resp = await client.get(f"/api/changed/?url={check['url']}")
    assert resp.status == 200
    assert await resp.json() == {
        "changed_at": "2015-10-21T07:28:00",
        "detection": "last-modified",
    }


@pytest.mark.skip(reason="Unimplemented endpoint")
async def test_changed_no_header(setup_catalog, client, fake_check):
    check = await fake_check(headers={})
    resp = await client.get(f"/api/changed/?url={check['url']}")
    assert resp.status == 204


@pytest.mark.skip(reason="Unimplemented endpoint")
async def test_changed_content_length(setup_catalog, client, fake_check):
    c1 = datetime.now() - timedelta(days=2)
    check = await fake_check(headers={"content-length": 1}, created_at=c1)
    resp = await client.get(f"/api/changed/?url={check['url']}")
    assert resp.status == 204

    c2 = datetime.now() - timedelta(days=1)
    check = await fake_check(headers={"content-length": 2}, created_at=c2)
    resp = await client.get(f"/api/changed/?url={check['url']}")
    assert resp.status == 200
    assert await resp.json() == {
        "changed_at": c2.isoformat(),
        "detection": "content-length",
    }

    c3 = datetime.now()
    check = await fake_check(headers={"content-length": 3}, created_at=c3)
    resp = await client.get(f"/api/changed/?url={check['url']}")
    assert resp.status == 200
    assert await resp.json() == {
        "changed_at": c3.isoformat(),
        "detection": "content-length",
    }

    c4 = datetime.now()
    check = await fake_check(headers={"content-length": 3}, created_at=c4)
    resp = await client.get(f"/api/changed/?url={check['url']}")
    assert resp.status == 200
    assert await resp.json() == {
        "changed_at": c3.isoformat(),
        "detection": "content-length",
    }


@pytest.mark.skip(reason="Unimplemented endpoint")
async def test_changed_content_length_unchanged(
    setup_catalog, client, fake_check
):
    c1 = datetime.now() - timedelta(days=2)
    check = await fake_check(headers={"content-length": 1}, created_at=c1)
    c2 = datetime.now() - timedelta(days=1)
    check = await fake_check(headers={"content-length": 1}, created_at=c2)
    resp = await client.get(f"/api/changed/?url={check['url']}")
    assert resp.status == 204


async def test_api_resource_created(client):
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
            "modified": datetime.now().isoformat(),
            "published": datetime.now().isoformat(),
        }
    }
    resp = await client.post("/api/resource/created/", json=payload)
    assert resp.status == 200
    data = await resp.json()
    assert data == {"message": "created"}

    payload["document"] = None
    resp = await client.post("/api/resource/created/", json=payload)
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
            "modified": datetime.now().isoformat(),
            "published": datetime.now().isoformat(),
        }
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


async def test_api_resource_deleted(client):
    payload = {
        "resource_id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
        "dataset_id": "61fd30cb29ea95c7bc0e1211",
        "document": None
    }
    resp = await client.post("/api/resource/deleted/", json=payload)
    assert resp.status == 200
    data = await resp.json()
    assert data == {"message": "deleted"}
