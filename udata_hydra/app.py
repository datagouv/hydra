import json
import logging

from datetime import datetime, timedelta

from aiohttp import web
from dateutil.parser import parse as date_parser, ParserError
from humanfriendly import parse_timespan
from marshmallow import Schema, fields

from udata_hydra import context, config
from udata_hydra.crawl import get_excluded_clause


log = logging.getLogger("aiohttp.access")
routes = web.RouteTableDef()


class CheckSchema(Schema):
    check_id = fields.Integer(data_key="id")
    catalog_id = fields.Integer()
    url = fields.Str()
    domain = fields.Str()
    created_at = fields.DateTime()
    status = fields.Integer()
    headers = fields.Function(
        lambda obj: json.loads(obj["headers"]) if obj["headers"] else {}
    )
    timeout = fields.Boolean()
    response_time = fields.Float()
    error = fields.Str()
    dataset_id = fields.Str()
    resource_id = fields.UUID()
    deleted = fields.Boolean()


def _get_args(request):
    url = request.query.get("url")
    resource_id = request.query.get("resource_id")
    if not url and not resource_id:
        raise web.HTTPBadRequest()
    return url, resource_id


@routes.get("/api/checks/latest/")
async def get_check(request):
    url, resource_id = _get_args(request)
    column = "url" if url else "resource_id"
    q = f"""
    SELECT catalog.id as catalog_id, checks.id as check_id, *
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
    SELECT catalog.id as catalog_id, checks.id as check_id, *
    FROM checks, catalog
    WHERE catalog.{column} = $1
    AND catalog.url = checks.url
    ORDER BY created_at DESC
    """
    data = await request.app["pool"].fetch(q, url or resource_id)
    if not data:
        raise web.HTTPNotFound()
    return web.json_response([CheckSchema().dump(dict(r)) for r in data])


@routes.get("/api/changed/")
async def get_changed(request):
    """Detect if a resource has changed

    Returns 204 if no hint available
    """
    url, resource_id = _get_args(request)
    column = "url" if url else "resource_id"

    # do we have a last-modified on the latest check?
    q = f"""
    SELECT
        checks.headers->>'last-modified' as last_modified,
        checks.headers->>'content-length' as content_length,
        catalog.url
    FROM checks, catalog
    WHERE checks.id = catalog.last_check
    AND catalog.{column} = $1
    """
    data = await request.app["pool"].fetchrow(q, url or resource_id)
    if not data:
        raise web.HTTPNotFound()
    if data["last_modified"]:
        try:
            return web.json_response(
                {
                    # this is GMT so we should be able to safely ignore tz info
                    "changed_at": date_parser(
                        data["last_modified"], ignoretz=True
                    ).isoformat(),
                    "detection": "last-modified",
                }
            )
        except ParserError:
            pass

    # switch to content-length comparison
    if not data["content_length"]:
        raise web.HTTPNoContent(text="")
    q = """
    SELECT
        created_at,
        checks.headers->>'content-length' as content_length
    FROM checks
    WHERE url = $1
    ORDER BY created_at DESC
    """
    data = await request.app["pool"].fetch(q, data["url"])
    # not enough checks to make a comparison
    if len(data) <= 1:
        raise web.HTTPNoContent(text="")
    changed_at = None
    last_length = None
    previous_date = None
    for check in data:
        if not check["content_length"]:
            continue
        if not last_length:
            last_length = check["content_length"]
        else:
            if check["content_length"] != last_length:
                changed_at = previous_date
                break
        previous_date = check["created_at"]
    if changed_at:
        return web.json_response(
            {
                "changed_at": changed_at.isoformat(),
                "detection": "content-length",
            }
        )
    else:
        raise web.HTTPNoContent(text="")


@routes.get("/api/status/")
async def status(request):
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
    since = datetime.utcnow() - timedelta(seconds=since)
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
        SELECT status, count(*) as count FROM checks, catalog
        WHERE catalog.last_check = checks.id
        AND status IS NOT NULL
        AND {get_excluded_clause()}
        AND last_check IS NOT NULL
        AND catalog.deleted = False
        GROUP BY status
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


if __name__ == "__main__":
    web.run_app(app_factory())
