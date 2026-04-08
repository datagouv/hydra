import asyncio
import json
import logging
import uuid

import aiohttp
from aiohttp import web
from asyncpg import Record

from udata_hydra import config
from udata_hydra.crawl.check_resources import check_resource
from udata_hydra.db.resource import Resource
from udata_hydra.schemas import ResourceDocumentSchema, ResourceSchema

log = logging.getLogger(__name__)


async def _immediate_check_resource(resource_id: str) -> None:
    """Run check_resource in the background (same RQ tier as POST /api/checks)."""
    try:
        resource = await Resource.get(resource_id)
        if resource is None or resource["status"] is not None:
            return
        async with aiohttp.ClientSession(
            timeout=None,
            headers={"user-agent": config.USER_AGENT_FULL},
        ) as session:
            await check_resource(
                url=resource["url"],
                resource=resource,
                session=session,
                worker_priority="high",
            )
    except Exception:
        log.exception("Background check failed for resource %s", resource_id)


async def get_resource(request: web.Request) -> web.Response:
    """Endpoint to get a resource from the DB
    Respond with a 200 status code and a JSON body with the resource data
    If resource is not found, respond with a 404 status code
    """

    try:
        resource_id = str(uuid.UUID(request.match_info["resource_id"]))
    except Exception as e:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(e)}))

    resource: Record | None = await Resource.get(resource_id)
    if not resource:
        raise web.HTTPNotFound()

    return web.json_response(ResourceSchema().dump(dict(resource)))


async def create_resource(request: web.Request) -> web.Response:
    """Endpoint to receive a resource creation event from a source
    Will create a new resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "created"
    If error, respond with a 400 status code
    """
    try:
        payload = await request.json()
        valid_payload: dict = ResourceSchema().load(payload)
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    document: dict | None = valid_payload["document"]
    if not document:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id = valid_payload["dataset_id"]
    resource_id = valid_payload["resource_id"]
    instant_analysis: bool = bool(valid_payload.get("instant_analysis", False))

    await Resource.insert(
        dataset_id=dataset_id,
        resource_id=resource_id,
        url=document["url"],
        type=document["type"],
        format=document["format"],
        priority=True,
        title=document["title"],
        instant_analysis=instant_analysis,
    )

    if instant_analysis:
        background_tasks = request.app["background_tasks"]
        task = asyncio.create_task(_immediate_check_resource(resource_id))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    return web.json_response(ResourceDocumentSchema().dump(dict(document)), status=201)


async def update_resource(request: web.Request) -> web.Response:
    """Endpoint to receive a resource update event from a source
    Will update an existing resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "updated"
    If error, respond with a 400 status code
    """

    try:
        payload = await request.json()
        valid_payload: dict = ResourceSchema().load(payload)
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    document: dict | None = valid_payload["document"]
    if not document:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id: str = valid_payload["dataset_id"]
    resource_id: str = valid_payload["resource_id"]
    instant_analysis: bool | None = (
        valid_payload["instant_analysis"] if "instant_analysis" in valid_payload else None
    )

    await Resource.update_or_insert(
        dataset_id=dataset_id,
        resource_id=resource_id,
        url=document["url"],
        type=document["type"],
        format=document["format"],
        title=document["title"],
        instant_analysis=instant_analysis,
    )

    return web.json_response(ResourceDocumentSchema().dump(document), status=200)


async def delete_resource(request: web.Request) -> web.Response:
    try:
        resource_id = str(uuid.UUID(request.match_info["resource_id"]))
    except Exception as e:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(e)}))

    resource: Record | None = await Resource.get(resource_id=resource_id)
    if not resource:
        raise web.HTTPNotFound()

    # Mark resource as deleted in catalog table
    await Resource.delete(resource_id=resource_id)

    return web.HTTPNoContent()


async def get_resources_stats(request: web.Request) -> web.Response:
    """Endpoint to get statistics about resources (CORS headers, etc.)."""
    # CORS stats: external resources (not on data.gouv.fr) that have at least one check with CORS probe (OPTIONS).
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
    row = await request.app["pool"].fetchrow(q)
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
    rows_dist = await request.app["pool"].fetch(q_dist)
    allow_origin_distribution = [
        {
            "access_status": r["access_status"],
            "unique_resources_count": r["unique_resources_count"],
            "percentage": float(r["percentage"]) if r["percentage"] is not None else None,
        }
        for r in rows_dist
    ]
    return web.json_response(
        {
            "cors": {
                "external_resources_with_cors_data": row["resources_with_cors_data"] or 0,
                "external_resources_without_cors_data": row["resources_without_cors_data"] or 0,
                "external_resources_cors_coverage_percentage": float(row["coverage_percentage"])
                if row["coverage_percentage"] is not None
                else None,
                "external_resources_allow_origin_distribution": allow_origin_distribution,
            }
        }
    )
