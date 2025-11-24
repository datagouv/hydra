"""
NB: we can't use pytest-aiohttp helpers because
it will interfere with the rest of our async code
"""

import pytest

from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio


async def test_get_crawler_status(setup_catalog, client, fake_check):
    expected_resources_statuses_count = {s: 0 for s in Resource.STATUSES if s}
    expected_resources_statuses_count["null"] = 1
    expected_data = {
        "checks": {
            "pending_count": 1,
            "fresh_count": 0,
            "checked_percentage": 0.0,
            "fresh_percentage": 0.0,
        },
        "resources": {
            "total_count": 1,
            "total_filtered_count": 1,
            "deleted_count": 0,
            "statuses_count": expected_resources_statuses_count,
        },
    }

    resp = await client.get("/api/status/crawler")
    assert resp.status == 200
    data: dict = await resp.json()
    assert data == expected_data

    expected_resources_statuses_count = {s: 0 for s in Resource.STATUSES if s}
    expected_resources_statuses_count["null"] = 1
    expected_data = {
        "checks": {
            "pending_count": 0,
            "fresh_count": 1,
            "checked_percentage": 100.0,
            "fresh_percentage": 100.0,
        },
        "resources": {
            "total_count": 1,
            "total_filtered_count": 1,
            "deleted_count": 0,
            "statuses_count": expected_resources_statuses_count,
        },
    }

    await fake_check()
    resp = await client.get("/api/status/crawler")
    assert resp.status == 200
    data: dict = await resp.json()
    assert data == expected_data


async def test_get_stats(setup_catalog, client, fake_check):
    resp = await client.get("/api/stats")
    assert resp.status == 200
    data: dict = await resp.json()
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
    resp = await client.get("/api/stats")
    assert resp.status == 200
    data: dict = await resp.json()
    assert data == {
        "status": [
            {"label": "error", "count": 1, "percentage": 100.0},
            {"label": "timeout", "count": 0, "percentage": 0},
            {"label": "ok", "count": 0, "percentage": 0},
        ],
        "status_codes": [{"code": 500, "count": 1, "percentage": 100.0}],
    }


async def test_get_health(client) -> None:
    resp = await client.get("/api/health")
    assert resp.status == 200
