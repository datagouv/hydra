import json
import os
import pytz

from datetime import datetime, timedelta

from aiohttp import web
from humanfriendly import parse_timespan
from marshmallow import Schema, fields, ValidationError

from udata_hydra import context, config
from udata_hydra.crawl import get_excluded_clause
from udata_hydra.logger import setup_logging
from udata_hydra.utils.minio import delete_resource_from_minio
from udata_hydra.worker import QUEUES


log = setup_logging()
routes = web.RouteTableDef()


class CheckSchema(Schema):
    check_id = fields.Integer(data_key="id")
    catalog_id = fields.Integer()
    url = fields.Str()
    domain = fields.Str()
    created_at = fields.DateTime()
    check_status = fields.Integer(data_key="status")
    headers = fields.Function(
        lambda obj: json.loads(obj["headers"]) if obj["headers"] else {}
    )
    timeout = fields.Boolean()
    response_time = fields.Float()
    error = fields.Str()
    dataset_id = fields.Str()
    resource_id = fields.UUID()
    deleted = fields.Boolean()
    parsing_started_at = fields.DateTime()
    parsing_finished_at = fields.DateTime()
    parsing_error = fields.Str()
    parsing_table = fields.Str()


class ResourceDocument(Schema):
    id = fields.Str(required=True)
    url = fields.Str(required=True)
    format = fields.Str(allow_none=True)
    title = fields.Str(required=True)
    schema = fields.Dict()
    description = fields.Str(allow_none=True)
    filetype = fields.Str(required=True)
    type = fields.Str(required=True)
    mime = fields.Str(allow_none=True)
    filesize = fields.Int(allow_none=True)
    checksum_type = fields.Str(allow_none=True)
    checksum_value = fields.Str(allow_none=True)
    created_at = fields.DateTime(required=True)
    last_modified = fields.DateTime(required=True)
    extras = fields.Dict()
    harvest = fields.Dict()


class ResourceQuery(Schema):
    dataset_id = fields.Str(required=True)
    resource_id = fields.Str(required=True)
    document = fields.Nested(ResourceDocument(), allow_none=True)


def _get_args(request, params=("url", "resource_id")):
    data = [request.query.get(param) for param in params]
    if not any(data):
        raise web.HTTPBadRequest()
    return data


@routes.post("/api/resource/created/")
async def resource_created(request):
    try:
        payload = await request.json()
        valid_payload = ResourceQuery().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource = valid_payload["document"]
    if not resource:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id = valid_payload["dataset_id"]
    resource_id = valid_payload["resource_id"]

    pool = request.app["pool"]
    async with pool.acquire() as connection:
        # Insert new resource in catalog table and mark as high priority for crawling
        q = f"""
                INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority)
                VALUES ('{dataset_id}', '{resource_id}', '{resource["url"]}', FALSE, TRUE)
                ON CONFLICT (resource_id) DO UPDATE SET
                    priority = TRUE,
                    url = '{resource["url"]}',
                    dataset_id = '{dataset_id}';"""
        await connection.execute(q)

    return web.json_response({"message": "created"})


@routes.post("/api/resource/updated/")
async def resource_updated(request):
    try:
        payload = await request.json()
        valid_payload = ResourceQuery().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource = valid_payload["document"]
    if not resource:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id = valid_payload["dataset_id"]
    resource_id = valid_payload["resource_id"]

    pool = request.app["pool"]
    async with pool.acquire() as connection:
        # Make resource high priority for crawling
        # Check if resource is in catalog then insert or update into table
        q = f"""SELECT * FROM catalog WHERE resource_id = '{resource_id}';"""
        res = await connection.fetch(q)
        if len(res):
            q = f"""UPDATE catalog SET priority = TRUE, url = '{resource["url"]}'
            WHERE resource_id = '{resource_id}';"""
        else:
            q = f"""
                    INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority)
                    VALUES ('{dataset_id}', '{resource_id}', '{resource["url"]}', FALSE, TRUE)
                    ON CONFLICT (resource_id) DO UPDATE SET
                        priority = TRUE,
                        url = '{resource["url"]}',
                        dataset_id = '{dataset_id}';"""
        await connection.execute(q)

    return web.json_response({"message": "updated"})


@routes.post("/api/resource/deleted/")
async def resource_deleted(request):
    try:
        payload = await request.json()
        valid_payload = ResourceQuery().load(payload)
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
async def get_check(request):
    url, resource_id = _get_args(request)
    column = "url" if url else "resource_id"
    q = f"""
    SELECT catalog.id as catalog_id, checks.id as check_id,
           catalog.status as catalog_status, checks.status as check_status, *
    FROM checks, catalog
    WHERE checks.id = catalog.last_check
    AND catalog.{column} = $1
    """
    data = await request.app["pool"].fetchrow(q, url or resource_id)
    if not data:
        raise web.HTTPNotFound()
    if data["deleted"]:
        raise web.HTTPGone()
    return web.json_response(CheckSchema().dump(dict(data)))


@routes.get("/api/checks/all/")
async def get_checks(request):
    url, resource_id = _get_args(request)
    column = "url" if url else "resource_id"
    q = f"""
    SELECT catalog.id as catalog_id, checks.id as check_id,
           catalog.status as catalog_status, checks.status as check_status, *
    FROM checks, catalog
    WHERE catalog.{column} = $1
    AND catalog.url = checks.url
    ORDER BY created_at DESC
    """
    data = await request.app["pool"].fetch(q, url or resource_id)
    if not data:
        raise web.HTTPNotFound()
    return web.json_response([CheckSchema().dump(dict(r)) for r in data])


@routes.get("/api/status/crawler/")
async def status_crawler(request):
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
    since = datetime.now(pytz.UTC) - timedelta(seconds=since)
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

    count_left = stats_catalog["count_left"] + (
        stats_checks["count_outdated"] or 0
    )
    # all w/ a check, minus those with an outdated checked
    count_checked = stats_catalog["count_checked"] - (
        stats_checks["count_outdated"] or 0
    )
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
async def status_worker(request):
    res = {
        "queued": {q: len(context.queue(q)) for q in QUEUES}
    }
    return web.json_response(res)


@routes.get("/api/stats/")
async def stats(request):
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
        return round(
            stats_status[key] / stats_catalog["count_checked"] * 100, 1
        )

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
                    "percentage": round(
                        r["count"] / sum(r["count"] for r in res) * 100, 1
                    ),
                }
                for r in res
            ],
        }
    )


@routes.get("/api/health/")
async def health(request):
    test_connection = await request.app["pool"].fetchrow("SELECT 1")
    assert next(test_connection.values()) == 1
    return web.HTTPOk()


async def app_factory():
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
