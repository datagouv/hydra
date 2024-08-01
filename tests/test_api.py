"""
NB: we can't use pytest-aiohttp helpers beause
it will interfere with the rest of our async code
"""

import hashlib
from datetime import datetime
from typing import Callable

import pytest

from tests.conftest import DATASET_ID, RESOURCE_ID
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "query",
    [
        "url=https://example.com/resource-1",
        f"resource_id={RESOURCE_ID}",
    ],
)
async def test_api_get_latest_check(setup_catalog, client, query, fake_check, fake_resource_id):
    await fake_check(parsing_table=True)

    # Test invalid query
    stupid_query: str = "stupid=stupid"
    resp = await client.get(f"/api/checks/latest/?{stupid_query}")
    assert resp.status == 400

    # Test not existing resource url
    not_existing_url_query: str = "url=https://example.com/not-existing-resource"
    resp = await client.get(f"/api/checks/latest/?{not_existing_url_query}")
    assert resp.status == 404

    # Test not existing resource_id
    not_existing_resource_id_query: str = f"resource_id={fake_resource_id()}"
    resp = await client.get(f"/api/checks/latest/?{not_existing_resource_id_query}")
    assert resp.status == 404

    # Test existing resource
    resp = await client.get(f"/api/checks/latest/?{query}")
    assert resp.status == 200
    data = await resp.json()
    assert data.pop("created_at")
    assert data.pop("id")
    url = "https://example.com/resource-1"
    assert data == {
        "response_time": 0.1,
        "deleted": False,
        "resource_id": RESOURCE_ID,
        "catalog_id": 1,
        "domain": "example.com",
        "error": None,
        "url": url,
        "headers": {"x-do": "you"},
        "timeout": False,
        "dataset_id": DATASET_ID,
        "status": 200,
        "parsing_error": None,
        "parsing_finished_at": None,
        "parsing_started_at": None,
        "parsing_table": hashlib.md5(url.encode("utf-8")).hexdigest(),
    }

    # Test deleted resource
    await Resource.update(resource_id=RESOURCE_ID, data={"deleted": True})
    resp = await client.get(f"/api/checks/latest/?{query}")
    assert resp.status == 410


@pytest.mark.parametrize(
    "query",
    [
        "url=https://example.com/resource-1",
        f"resource_id={RESOURCE_ID}",
    ],
)
async def test_api_get_all_checks(setup_catalog, client, query, fake_check):
    resp = await client.get(f"/api/checks/all/?{query}")
    assert resp.status == 404

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


async def test_api_get_resource(db, client, insert_fake_resource):
    await insert_fake_resource(db)
    query: str = f"dataset_id={DATASET_ID}&resource_id={RESOURCE_ID}"
    resp = await client.get(f"/api/resources/?{query}")
    assert resp.status == 200
    data = await resp.json()
    assert data["dataset_id"] == DATASET_ID
    assert data["resource_id"] == RESOURCE_ID


@pytest.mark.parametrize(
    "route",
    [
        {"url": "/api/resources/", "method": "post"},
        {"url": "/api/resource/created/", "method": "post"},  # legacy route
    ],
)  # TODO: can be removed once we don't use legacy route anymore
async def test_api_create_resource(client, route, udata_resource_payload):
    client_http_method: Callable = getattr(
        client, route["method"]
    )  # TODO: can be removed once we don't use legacy route anymore

    # Test invalid POST data
    stupid_post_data: dict = {"stupid": "stupid"}
    resp = await client_http_method(route["url"], json=stupid_post_data)
    assert resp.status == 400

    resp = await client_http_method(route["url"], json=udata_resource_payload)
    assert resp.status == 200
    data = await resp.json()
    assert data == {"message": "created"}

    udata_resource_payload["document"] = None
    resp = await client_http_method(route["url"], json=udata_resource_payload)
    assert resp.status == 400
    text = await resp.text()
    assert text == "Missing document body"


@pytest.mark.parametrize(
    "route",
    [
        {"url": "/api/resources/", "method": "put"},
        {"url": "/api/resource/updated/", "method": "post"},  # legacy route
    ],
)  # TODO: can be removed once we don't use legacy route anymore
async def test_api_update_resource(client, route):
    client_http_method: Callable = getattr(
        client, route["method"]
    )  # TODO: can be removed once we don't use legacy route anymore

    # Test invalid PUT data
    stupid_post_data: dict = {"stupid": "stupid"}
    resp = await client_http_method(route["url"], json=stupid_post_data)
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

    resp = await client_http_method(route["url"], json=payload)
    assert resp.status == 200
    data = await resp.json()
    assert data == {"message": "updated"}

    payload["document"] = None
    resp = await client_http_method(route["url"], json=payload)
    assert resp.status == 400
    text = await resp.text()
    assert text == "Missing document body"


@pytest.mark.parametrize(
    "route",
    [
        {"url": "/api/resources/", "method": "put"},
        {"url": "/api/resource/updated/", "method": "post"},  # legacy route
    ],
)  # TODO: can be removed once we don't use legacy route anymore
async def test_api_update_resource_url_since_load_catalog(setup_catalog, db, client, route):
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
    client_http_method: Callable = getattr(
        client, route["method"]
    )  # TODO: can be removed once we don't use legacy route anymore
    resp = await client_http_method(route["url"], json=payload)
    assert resp.status == 200

    res = await db.fetch(f"SELECT * FROM catalog WHERE resource_id = '{RESOURCE_ID}'")
    assert len(res) == 1
    res[0]["url"] == "https://example.com/resource-1"


@pytest.mark.parametrize(
    "route",
    [
        {"url": "/api/resources/", "method": "delete"},
        {"url": "/api/resource/deleted/", "method": "post"},  # legacy route
    ],
)  # TODO: can be removed once we don't use legacy route anymore
async def test_api_delete_resource(client, route):
    client_http_method: Callable = getattr(
        client, route["method"]
    )  # TODO: can be removed once we don't use legacy route anymore

    # Test invalid DELETE data
    stupid_delete_data: dict = {"stupid": "stupid"}
    resp = await client_http_method(route["url"], json=stupid_delete_data)
    assert resp.status == 400

    payload = {
        "resource_id": RESOURCE_ID,
        "dataset_id": DATASET_ID,
        "document": None,
    }
    resp = await client_http_method(route["url"], json=payload)
    assert resp.status == 200
    data = await resp.json()
    assert data == {"message": "deleted"}


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
