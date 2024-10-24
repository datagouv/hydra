from aiohttp import web

from udata_hydra import config, context
from udata_hydra.db.resource import Resource
from udata_hydra.worker import QUEUES


async def get_crawler_status(request: web.Request) -> web.Response:
    # Count resources with no check and resources with a check
    q = f"""
        SELECT
            SUM(CASE WHEN last_check IS NULL THEN 1 ELSE 0 END) AS count_not_checked,
            SUM(CASE WHEN last_check IS NOT NULL THEN 1 ELSE 0 END) AS count_checked
        FROM catalog
        WHERE {Resource.get_excluded_clause()}
        AND catalog.deleted = False
    """
    stats_catalog = await request.app["pool"].fetchrow(q)

    # Count resources with an outdated check.
    # To get resources with outdated checks, they need to meet one of the following conditions:
    # - Don't have at least two checks yet, and the last check is older than CHECK_DELAYS[0]
    # - At least one the their two most recent last checks have no detected_last_modified_at, and the last check is older than CHECK_DELAYS[0]
    # - Their two most recent last checks have changed, and the last check is older than CHECK_DELAYS[0]
    # - Their last two checks have not changed, the two checks have done between two delays in CHECK_DELAYS, and the last check is older than the same delay in CHECK_DELAYS (this is in order to avoid checking too often the same resource which doesn't change)
    q_start = f"""
        WITH recent_checks AS (
            SELECT resource_id, detected_last_modified_at, created_at,
                ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY created_at DESC) AS rn
            FROM checks
        )
        SELECT COUNT(*) AS count_outdated
        FROM (
            SELECT catalog.url, dataset_id, catalog.resource_id
            FROM catalog
                LEFT JOIN recent_checks ON catalog.resource_id = recent_checks.resource_id
            WHERE catalog.priority = False
                AND {Resource.get_excluded_clause()}
            GROUP BY catalog.url, dataset_id, catalog.resource_id
            HAVING (
                (
                    COUNT(recent_checks.resource_id) < 2
                    AND MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[0]}'
                )
                OR (
                    COUNT(CASE WHEN recent_checks.detected_last_modified_at IS NULL THEN 1 END) >= 1
                    AND MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[0]}'
                )
                OR (
                    COUNT(DISTINCT recent_checks.detected_last_modified_at) > 1
                    AND MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[0]}'
                )
    """
    q_dynamic = ""
    for i in range(len(config.CHECK_DELAYS) - 1):
        q_dynamic += f"""
                OR (
                    COUNT(recent_checks.resource_id) = 2
                    AND COUNT(DISTINCT recent_checks.detected_last_modified_at) < 2
                    AND MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[i]}'
                    AND MAX(recent_checks.created_at) >= NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[i + 1]}'
                    AND MIN(recent_checks.created_at) < MAX(recent_checks.created_at) - INTERVAL '{config.CHECK_DELAYS[i]}'
                    AND MIN(recent_checks.created_at) >= MAX(recent_checks.created_at) - INTERVAL '{config.CHECK_DELAYS[i + 1]}'
                )
        """

    q_end = f"""
                OR MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[-1]}'
            )
        ) AS subquery;
    """
    q = f"{q_start} {q_dynamic} {q_end}"
    stats_checks = await request.app["pool"].fetchrow(q)

    count_to_check = stats_catalog["count_not_checked"] + (stats_checks["count_outdated"] or 0)
    # all w/ a check, minus those with an outdated checked
    count_recently_checked = stats_catalog["count_checked"] - (stats_checks["count_outdated"] or 0)
    total = stats_catalog["count_not_checked"] + stats_catalog["count_checked"]
    rate_checked = round(stats_catalog["count_checked"] / total * 100, 1)
    rate_checked_fresh = round(count_recently_checked / total * 100, 1)

    return web.json_response(
        {
            "total": total,
            "pending_checks": count_to_check,
            "fresh_checks": count_recently_checked,
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
    return web.json_response(
        {
            "version": config.APP_VERSION,
            "environment": config.ENVIRONMENT or "unknown",
        }
    )
