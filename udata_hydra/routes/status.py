from datetime import datetime, timedelta, timezone

from aiohttp import web
from humanfriendly import parse_timespan

from udata_hydra import config, context
from udata_hydra.db.resource import Resource
from udata_hydra.worker import QUEUES


async def get_crawler_status(request: web.Request) -> web.Response:
    q = f"""
        SELECT
            SUM(CASE WHEN last_check IS NULL THEN 1 ELSE 0 END) AS count_left,
            SUM(CASE WHEN last_check IS NOT NULL THEN 1 ELSE 0 END) AS count_checked
        FROM catalog
        WHERE {Resource.get_excluded_clause()}
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
        WHERE {Resource.get_excluded_clause()}
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


async def get_worker_status(request: web.Request) -> web.Response:
    res = {"queued": {q: len(context.queue(q)) for q in QUEUES}}
    return web.json_response(res)


async def get_stats(request: web.Request) -> web.Response:
    q = f"""
        SELECT count(*) AS count_checked
        FROM catalog
        WHERE {Resource.get_excluded_clause()}
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
        WHERE {Resource.get_excluded_clause()}
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
        AND {Resource.get_excluded_clause()}
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


async def get_health(request: web.Request) -> web.Response:
    test_connection = await request.app["pool"].fetchrow("SELECT 1")
    assert next(test_connection.values()) == 1
    return web.HTTPOk()
