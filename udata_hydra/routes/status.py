from datetime import datetime, timezone

from aiohttp import web

from udata_hydra import config, context
from udata_hydra.db.resource import Resource
from udata_hydra.worker import QUEUES


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


async def get_crawler_status(request: web.Request) -> web.Response:
    # Combined query: count resources with no check, with a check, and outdated checks
    # (filtered by excluded patterns) - all in a single query for better performance
    now = datetime.now(timezone.utc)
    q = f"""
        SELECT
            COALESCE(SUM(CASE WHEN catalog.last_check IS NULL THEN 1 ELSE 0 END), 0) AS count_never_checked,
            COALESCE(SUM(CASE WHEN catalog.last_check IS NOT NULL THEN 1 ELSE 0 END), 0) AS count_checked,
            COALESCE(SUM(CASE WHEN checks.next_check_at <= $1 THEN 1 ELSE 0 END), 0) AS count_outdated
        FROM catalog
        LEFT JOIN checks ON catalog.last_check = checks.id
        WHERE {Resource.get_excluded_clause()}
    """
    stats_combined: dict = await request.app["pool"].fetchrow(q, now)

    # Count resources in progress (have a status that is not NULL and not 'BACKOFF')
    # Build excluded patterns clause similar to get_excluded_clause() but without status filter
    excluded_patterns_clause = " AND ".join(
        [f"catalog.url NOT LIKE '{p}'" for p in (config.EXCLUDED_PATTERNS or [])]
        + [
            "catalog.deleted = False",
            "catalog.status IS NOT NULL",
            "catalog.status != 'BACKOFF'",
        ]
    )
    q_in_progress = f"""
        SELECT
            COALESCE(COUNT(*), 0) AS count_in_progress
        FROM catalog
        WHERE {excluded_patterns_clause}
    """
    stats_in_progress: dict = await request.app["pool"].fetchrow(q_in_progress)

    # Resources that need a check (never checked + outdated checks)
    needs_check_count: int = (
        stats_combined["count_never_checked"] + stats_combined["count_outdated"]
    )
    # Resources with up-to-date checks (all with a check, minus those with an outdated check)
    up_to_date_check_count: int = stats_combined["count_checked"] - stats_combined["count_outdated"]
    # Total resources eligible for checks (filtered by excluded patterns, non-deleted, not in progress)
    total_eligible_resources: int = (
        stats_combined["count_never_checked"] + stats_combined["count_checked"]
    )
    # Total resources including in-progress ones
    total_resources_with_in_progress: int = (
        total_eligible_resources + stats_in_progress["count_in_progress"]
    )

    # Calculate percentages
    needs_check_percentage: float | None
    up_to_date_check_percentage: float | None
    if total_eligible_resources > 0:
        needs_check_percentage = round(needs_check_count / total_eligible_resources * 100, 1)
        up_to_date_check_percentage = round(
            up_to_date_check_count / total_eligible_resources * 100, 1
        )
    else:
        needs_check_percentage, up_to_date_check_percentage = None, None

    in_progress_percentage: float | None
    if total_resources_with_in_progress > 0:
        in_progress_percentage = round(
            stats_in_progress["count_in_progress"] / total_resources_with_in_progress * 100, 1
        )
    else:
        in_progress_percentage = None

    return web.json_response(
        {
            "checks": {
                "in_progress_count": stats_in_progress["count_in_progress"],
                "in_progress_percentage": in_progress_percentage,
                "needs_check_count": needs_check_count,
                "needs_check_percentage": needs_check_percentage,
                "up_to_date_check_count": up_to_date_check_count,
                "up_to_date_check_percentage": up_to_date_check_percentage,
            },
            "resources": {
                "total_eligible_count": total_eligible_resources,
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
