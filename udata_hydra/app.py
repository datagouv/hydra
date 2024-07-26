import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Union

from aiohttp import web
from humanfriendly import parse_timespan
from marshmallow import ValidationError

from udata_hydra import config, context
from udata_hydra.crawl import get_excluded_clause
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.logger import setup_logging
from udata_hydra.schemas import CheckSchema, ResourceQuerySchema
from udata_hydra.utils.minio import delete_resource_from_minio
from udata_hydra.worker import QUEUES

log = setup_logging()
routes = web.RouteTableDef()


def _get_args(request, params=("url", "resource_id")) -> list:
    """Get GET parameters from request"""
    data = [request.query.get(param) for param in params]
    if not any(data):
        raise web.HTTPBadRequest()
    return data


@routes.get("/api/resource/status/")
async def get_resource_status(request: web.Request) -> web.Response:
    """Get the current status of a resource"""
    try:
        payload = await request.json()
        valid_payload: Union[dict, list, None] = ResourceQuerySchema().load(payload)
        if not isinstance(valid_payload, dict):
            raise ValidationError("Invalid payload")
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource_id = valid_payload["resource_id"]

    res = await Resource.get(resource_id)
    if not res:
        raise web.HTTPNotFound()
    status: str = res[0]["status"]
    verbose: str = Resource.STATUSES[status]

    return web.json_response({"resource_id": resource_id, "status": status, "verbose": verbose})


@routes.post("/api/resource/created/")
async def resource_created(request: web.Request) -> web.Response:
    """Endpoint to receive a resource creation event from a source
    Will create a new resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "created"
    If error, respond with a 400 status code
    """
    try:
        payload = await request.json()
        valid_payload: dict = ResourceQuerySchema().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource = valid_payload["document"]
    if not resource:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id = valid_payload["dataset_id"]
    resource_id = valid_payload["resource_id"]

    await Resource.insert(
        dataset_id=dataset_id,
        resource_id=resource_id,
        url=resource["url"],
        priority=True,
    )

    return web.json_response({"message": "created"})


@routes.post("/api/resource/updated/")
async def resource_updated(request: web.Request) -> web.Response:
    """Endpoint to receive a resource update event from a source
    Will update an existing resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "updated"
    If error, respond with a 400 status code
    """
    try:
        payload = await request.json()
        valid_payload: dict = ResourceQuerySchema().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource = valid_payload["document"]
    if not resource:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id = valid_payload["dataset_id"]
    resource_id = valid_payload["resource_id"]

    await Resource.update_or_insert(
        dataset_id=dataset_id, resource_id=resource_id, url=resource["url"], status="TO_CHECK"
    )

    return web.json_response({"message": "updated"})


@routes.post("/api/resource/deleted/")
async def resource_deleted(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
        valid_payload: dict = ResourceQuerySchema().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    dataset_id = valid_payload["dataset_id"]
    resource_id = valid_payload["resource_id"]

    pool = request.app["pool"]
    async with pool.acquire() as connection:
        if config.SAVE_TO_MINIO:
            delete_resource_from_minio(dataset_id, resource_id)
        # Mark resource as deleted in catalog table
        q = f"""UPDATE catalog SET deleted = TRUE WHERE resource_id = '{resource_id}';"""
        await connection.execute(q)

    return web.json_response({"message": "deleted"})


@routes.get("/api/checks/latest/")
async def get_check(request: web.Request) -> web.Response:
    """Get the latest check for a given URL or resource_id"""
    url, resource_id = _get_args(request)
    data = await Check.get_latest(url, resource_id)
    if not data:
        raise web.HTTPNotFound()
    if data["deleted"]:
        raise web.HTTPGone()
    return web.json_response(CheckSchema().dump(dict(data)))


@routes.get("/api/checks/all/")
async def get_checks(request: web.Request) -> web.Response:
    url, resource_id = _get_args(request)
    data = await Check.get_all(url, resource_id)
    if not data:
        raise web.HTTPNotFound()
    return web.json_response([CheckSchema().dump(dict(r)) for r in data])


@routes.get("/api/status/crawler/")
async def status_crawler(request: web.Request) -> web.Response:
    q = f"""
        SELECT
            SUM(CASE WHEN last_check IS NULL THEN 1 ELSE 0 END) AS count_left,
            SUM(CASE WHEN last_check IS NOT NULL THEN 1 ELSE 0 END) AS count_checked
        FROM catalog
        WHERE {get_excluded_clause()}
        AND catalog.deleted = False
    """
    stats_catalog = await request.app["pool"].fetchrow(q)

    since = parse_timespan(config.SINCE)
    since = datetime.now(timezone.utc) - timedelta(seconds=since)
    q = f"""
        SELECT
            SUM(CASE WHEN checks.created_at <= $1 THEN 1 ELSE 0 END) AS count_outdated
            --, SUM(CASE WHEN checks.created_at > $1 THEN 1 ELSE 0 END) AS count_fresh
        FROM catalog, checks
        WHERE {get_excluded_clause()}
        AND catalog.last_check = checks.id
        AND catalog.deleted = False
    """
    stats_checks = await request.app["pool"].fetchrow(q, since)

    count_left = stats_catalog["count_left"] + (stats_checks["count_outdated"] or 0)
    # all w/ a check, minus those with an outdated checked
    count_checked = stats_catalog["count_checked"] - (stats_checks["count_outdated"] or 0)
    total = stats_catalog["count_left"] + stats_catalog["count_checked"]
    rate_checked = round(stats_catalog["count_checked"] / total * 100, 1)
    rate_checked_fresh = round(count_checked / total * 100, 1)

    return web.json_response(
        {
            "total": total,
            "pending_checks": count_left,
            "fresh_checks": count_checked,
            "checks_percentage": rate_checked,
            "fresh_checks_percentage": rate_checked_fresh,
        }
    )


@routes.get("/api/status/worker/")
async def status_worker(request: web.Request) -> web.Response:
    res = {"queued": {q: len(context.queue(q)) for q in QUEUES}}
    return web.json_response(res)


@routes.get("/api/stats/")
async def stats(request: web.Request) -> web.Response:
    q = f"""
        SELECT count(*) AS count_checked
        FROM catalog
        WHERE {get_excluded_clause()}
        AND last_check IS NOT NULL
        AND catalog.deleted = False
    """
    stats_catalog = await request.app["pool"].fetchrow(q)

    q = f"""
        SELECT
            SUM(CASE WHEN error IS NULL AND timeout = False THEN 1 ELSE 0 END) AS count_ok,
            SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) AS count_error,
            SUM(CASE WHEN timeout = True THEN 1 ELSE 0 END) AS count_timeout
        FROM catalog, checks
        WHERE {get_excluded_clause()}
        AND catalog.last_check = checks.id
        AND catalog.deleted = False
    """
    stats_status = await request.app["pool"].fetchrow(q)

    def cmp_rate(key):
        if stats_catalog["count_checked"] == 0:
            return 0
        return round(stats_status[key] / stats_catalog["count_checked"] * 100, 1)

    q = f"""
        SELECT checks.status, count(*) as count FROM checks, catalog
        WHERE catalog.last_check = checks.id
        AND checks.status IS NOT NULL
        AND {get_excluded_clause()}
        AND last_check IS NOT NULL
        AND catalog.deleted = False
        GROUP BY checks.status
        ORDER BY count DESC;
    """
    res = await request.app["pool"].fetch(q)
    return web.json_response(
        {
            "status": sorted(
                [
                    {
                        "label": s,
                        "count": stats_status[f"count_{s}"] or 0,
                        "percentage": cmp_rate(f"count_{s}"),
                    }
                    for s in ["error", "timeout", "ok"]
                ],
                key=lambda x: x["count"],
                reverse=True,
            ),
            "status_codes": [
                {
                    "code": r["status"],
                    "count": r["count"],
                    "percentage": round(r["count"] / sum(r["count"] for r in res) * 100, 1),
                }
                for r in res
            ],
        }
    )


@routes.get("/api/health/")
async def health(request: web.Request) -> web.Response:
    test_connection = await request.app["pool"].fetchrow("SELECT 1")
    assert next(test_connection.values()) == 1
    return web.HTTPOk()


async def app_factory() -> web.Application:
    async def app_startup(app):
        app["pool"] = await context.pool()

    async def app_cleanup(app):
        if "pool" in app:
            await app["pool"].close()

    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(app_startup)
    app.on_cleanup.append(app_cleanup)
    return app


def run():
    web.run_app(app_factory(), path=os.environ.get("HYDRA_APP_SOCKET_PATH"))


if __name__ == "__main__":
    run()
