import json
import logging
import uuid

from aiohttp import web
from asyncpg import Record

from udata_hydra.db.resource import Resource
from udata_hydra.routes.status import get_resources_status_counts
from udata_hydra.schemas import CreateResourceRequest, ResourceSchema

log = logging.getLogger("udata-hydra")


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

    return web.json_response(ResourceSchema.model_validate(dict(resource)).model_dump(mode="json"))


async def create_resource(request: web.Request) -> web.Response:
    """Endpoint to receive a resource creation event from a source
    Will create a new resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "created"
    If error, respond with a 400 status code
    """

    try:
        request_data: dict = await request.json()
        payload: CreateResourceRequest = CreateResourceRequest.model_validate(request_data)
    except Exception as e:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(e)}))

    document = payload.document

    await Resource.insert(
        dataset_id=payload.dataset_id,
        resource_id=str(payload.resource_id),
        url=document.url,
        type=document.type,
        format=document.format or None,
        priority=True,
        title=document.title,
    )

    return web.json_response(document.model_dump(mode="json", by_alias=True), status=201)


async def update_resource(request: web.Request) -> web.Response:
    """Endpoint to receive a resource update event from a source
    Will update an existing resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "updated"
    If error, respond with a 400 status code
    """

    try:
        request_data: dict = await request.json()
        payload: CreateResourceRequest = CreateResourceRequest.model_validate(request_data)
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    document = payload.document

    # Temporary debug for https://github.com/datagouv/data.gouv.fr/issues/2048
    # (hydra#466). Remove once the check-notify gap is understood.
    resource_id: str = str(payload.resource_id)
    new_url: str = document.url

    existing: Record | None = await Resource.get(resource_id)
    if existing:
        old_url: str = existing["url"]
        url_changed: bool = old_url != new_url
        log.info(
            f"[resource_id={resource_id}] update_resource webhook: "
            f"url_changed={url_changed} old={old_url} new={new_url} priority=True"
        )
    else:
        log.info(
            f"[resource_id={resource_id}] update_resource webhook: created url={new_url} priority=True"
        )

    await Resource.update_or_insert(
        dataset_id=payload.dataset_id,
        resource_id=resource_id,
        url=document.url,
        type=document.type,
        format=document.format or None,
        title=document.title,
    )

    return web.json_response(document.model_dump(mode="json", by_alias=True), status=200)


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
    """Aggregate statistics about resources in catalog (counts and crawler status breakdown)."""
    q_total = """
        SELECT
            COALESCE(COUNT(*), 0) AS total_resources,
            COALESCE(SUM(CASE WHEN catalog.deleted = True THEN 1 ELSE 0 END), 0) AS deleted_resources
        FROM catalog
    """
    stats_resources: dict = await request.app["pool"].fetchrow(q_total)
    return web.json_response(
        {
            "total_count": stats_resources["total_resources"],
            "deleted_count": stats_resources["deleted_resources"],
            "statuses_count": await get_resources_status_counts(request),
        }
    )
