from datetime import datetime, timezone

from aiohttp import web

from udata_hydra import config, context
from udata_hydra.db.resource import Resource
from udata_hydra.worker import QUEUES


async def get_crawler_status(request: web.Request) -> web.Response:
    # Count outdated checks (checks that need to be refreshed, filtered by excluded patterns)
    now = datetime.now(timezone.utc)
    q = f"""
        SELECT
            COALESCE(SUM(CASE WHEN checks.next_check_at <= $1 THEN 1 ELSE 0 END), 0) AS count_outdated
        FROM catalog, checks
        WHERE {Resource.get_excluded_clause()}
        AND catalog.last_check = checks.id
    """
    stats_checks: dict = await request.app["pool"].fetchrow(q, now)

    # Count total resources and deleted resources (all resources in catalog, no filters)
    q_total = """
        SELECT
            COALESCE(COUNT(*), 0) AS total_resources,
            COALESCE(SUM(CASE WHEN catalog.deleted = True THEN 1 ELSE 0 END), 0) AS deleted_resources
        FROM catalog
    """
    stats_resources: dict = await request.app["pool"].fetchrow(q_total)

    # Count resources with no check and resources with a check (filtered by excluded patterns)
    q = f"""
        SELECT
            COALESCE(SUM(CASE WHEN catalog.deleted = False AND last_check IS NULL THEN 1 ELSE 0 END), 0) AS count_never_checked,
            COALESCE(SUM(CASE WHEN catalog.deleted = False AND last_check IS NOT NULL THEN 1 ELSE 0 END), 0) AS count_checked
        FROM catalog
        WHERE {Resource.get_excluded_clause()}
    """
    stats_check_status: dict = await request.app["pool"].fetchrow(q)
    count_pending_checks: int = (
        stats_check_status["count_never_checked"] + stats_checks["count_outdated"]
    )
    # all w/ a check, minus those with an outdated checked
    count_fresh_checks: int = stats_check_status["count_checked"] - stats_checks["count_outdated"]
    # Total resources eligible for checks (filtered by excluded patterns, non-deleted)
    total_resources_filtered: int = (
        stats_check_status["count_never_checked"] + stats_check_status["count_checked"]
    )
    if total_resources_filtered > 0:
        rate_checked: float = round(
            stats_check_status["count_checked"] / total_resources_filtered * 100, 1
        )
        rate_checked_fresh: float = round(count_fresh_checks / total_resources_filtered * 100, 1)
    else:
        rate_checked, rate_checked_fresh = None, None

    async def get_resources_status_counts(request: web.Request) -> dict[str | None, int]:
        status_counts: dict = {status: 0 for status in Resource.STATUSES}
        status_counts[None] = 0

        q = """
            SELECT COALESCE(status, 'NULL') AS status, COUNT(*) AS count
            FROM catalog
            GROUP BY COALESCE(status, 'NULL');
        """
        rows = await request.app["pool"].fetch(q)

        for row in rows:
            status = row["status"] if row["status"] != "NULL" else None
            status_counts[status] = row["count"]

        return status_counts

    async def get_resources_cors_stats(_request: web.Request) -> dict:
        """Stats on CORS headers: resources (not on data.gouv.fr) that have at least one check with a CORS probe (OPTIONS)."""
        q = """
            -- External resources only; count how many have ≥1 check with CORS data and coverage %.
            SELECT
                COUNT(DISTINCT resource_id) FILTER (WHERE has_cors_check) AS resources_with_cors_data,
                COUNT(DISTINCT resource_id) FILTER (WHERE NOT has_cors_check) AS resources_without_cors_data,
                ROUND(
                    COUNT(DISTINCT resource_id) FILTER (WHERE has_cors_check) * 100.0 / NULLIF(COUNT(DISTINCT resource_id), 0),
                    2
                ) AS coverage_percentage
            FROM (
                SELECT
                    c.resource_id,
                    BOOL_OR(ch.cors_headers IS NOT NULL) AS has_cors_check
                FROM catalog c
                LEFT JOIN checks ch ON c.resource_id = ch.resource_id
                WHERE c.url NOT LIKE '%data.gouv.fr%'
                AND c.deleted = False
                GROUP BY c.resource_id
            ) resources_summary
        """
        row = await _request.app["pool"].fetchrow(q)
        q_dist = """
            -- Among external resources with ≥1 CORS check, classify by allow-origin and aggregate counts/percentages.
            SELECT
                access_status,
                COUNT(*) AS unique_resources_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
            FROM (
                SELECT
                    c.resource_id,
                    CASE
                        WHEN BOOL_OR(ch.cors_headers->>'allow-origin' = '*') THEN 'Accessible (Wildcard *)'
                        WHEN BOOL_OR(ch.cors_headers->>'allow-origin' ILIKE '%data.gouv.fr%') THEN 'Accessible (Specific Whitelist)'
                        WHEN BOOL_AND(ch.cors_headers->>'allow-origin' IS NULL OR ch.cors_headers->>'allow-origin' = '') THEN 'Blocked (Missing Header)'
                        ELSE 'Blocked (Other Domain Only)'
                    END AS access_status
                FROM catalog c
                INNER JOIN checks ch ON c.resource_id = ch.resource_id
                WHERE c.url NOT LIKE '%data.gouv.fr%'
                AND c.deleted = False
                AND ch.cors_headers IS NOT NULL
                GROUP BY c.resource_id
            ) unique_resources_summary
            GROUP BY access_status
            ORDER BY access_status
        """
        rows_dist = await _request.app["pool"].fetch(q_dist)
        allow_origin_distribution = [
            {
                "access_status": r["access_status"],
                "unique_resources_count": r["unique_resources_count"],
                "percentage": float(r["percentage"]) if r["percentage"] is not None else None,
            }
            for r in rows_dist
        ]
        return {
            "external_resources_with_cors_data": row["resources_with_cors_data"] or 0,
            "external_resources_without_cors_data": row["resources_without_cors_data"] or 0,
            "external_resources_cors_coverage_percentage": float(row["coverage_percentage"])
            if row["coverage_percentage"] is not None
            else None,
            "external_resources_allow_origin_distribution": allow_origin_distribution,
        }

    return web.json_response(
        {
            "checks": {
                "pending_count": count_pending_checks,
                "fresh_count": count_fresh_checks,
                "checked_percentage": rate_checked,
                "fresh_percentage": rate_checked_fresh,
            },
            "resources": {
                "total_count": stats_resources["total_resources"],
                "total_filtered_count": total_resources_filtered,
                "deleted_count": stats_resources["deleted_resources"],
                "statuses_count": await get_resources_status_counts(request),
                "cors": await get_resources_cors_stats(request),
            },
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
    """
    stats_status = await request.app["pool"].fetchrow(q)

    def cmp_rate(key: str) -> float | int:
        if stats_catalog["count_checked"] == 0:
            return 0
        return round(stats_status[key] / stats_catalog["count_checked"] * 100, 1)

    q = f"""
        SELECT checks.status, count(*) as count FROM checks, catalog
        WHERE catalog.last_check = checks.id
        AND checks.status IS NOT NULL
        AND {Resource.get_excluded_clause()}
        AND last_check IS NOT NULL
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
            "csv_analysis": config.CSV_ANALYSIS,
            "csv_to_db": config.CSV_TO_DB,
            "csv_to_parquet": config.CSV_TO_PARQUET,
            "geojson_to_pmtiles": config.GEOJSON_TO_PMTILES,
            "csv_to_geojson": config.CSV_TO_GEOJSON,
            "parquet_to_db": config.PARQUET_TO_DB,
        }
    )
