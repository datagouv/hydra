import json

from aiohttp import web
from asyncpg import Record
from marshmallow import ValidationError

from udata_hydra.db.resource import Resource
from udata_hydra.schemas import ResourceSchema
from udata_hydra.utils import get_request_params


async def get_resource(request: web.Request) -> web.Response:
    """Endpoint to get a resource from the DB
    Respond with a 200 status code and a JSON body with the resource data
    If resource is not found, respond with a 404 status code
    """
    [resource_id] = get_request_params(request, params_names=["resource_id"])
    resource: Record | None = await Resource.get(resource_id)
    if not resource:
        raise web.HTTPNotFound()

    return web.json_response(ResourceSchema().dump(dict(resource)))


async def get_resource_status(request: web.Request) -> web.Response:
    """Endpoint to get the current status of a resource from the DB.
    It is the same as get_resource but only returns the status of the resource, saving bandwith and processing time.
    Respond with a 200 status code and a JSON body with the resource status
    If resource is not found, respond with a 404 status code
    """
    try:
        resource_id: str = request.match_info["resource_id"]
    except Exception as e:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(e)}))

    resource: Record | None = await Resource.get(resource_id=resource_id, column_name="status")
    if not resource:
        raise web.HTTPNotFound()

    status: str | None = resource["status"]
    status_verbose: str = Resource.STATUSES[status]

    latest_check_endpoint = str(request.app.router["get-latest-check"].url_for())

    return web.json_response(
        {
            "resource_id": resource_id,
            "status": status,
            "status_verbose": status_verbose,
            "latest_check_url": f"{request.scheme}://{request.host}{latest_check_endpoint}?resource_id={resource_id}",
        }
    )


async def create_resource(request: web.Request) -> web.Response:
    """Endpoint to receive a resource creation event from a source
    Will create a new resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "created"
    If error, respond with a 400 status code
    """
    try:
        payload = await request.json()
        valid_payload: dict = ResourceSchema().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource: dict = valid_payload["document"]
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


async def update_resource(request: web.Request) -> web.Response:
    """Endpoint to receive a resource update event from a source
    Will update an existing resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "updated"
    If error, respond with a 400 status code
    """
    try:
        payload = await request.json()
        valid_payload: dict = ResourceSchema().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource: dict = valid_payload["document"]
    if not resource:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id: str = valid_payload["dataset_id"]
    resource_id: str = valid_payload["resource_id"]

    await Resource.update_or_insert(dataset_id, resource_id, resource["url"])

    return web.json_response({"message": "updated"})


async def delete_resource(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
        valid_payload: dict = ResourceSchema().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource_id: str = valid_payload["resource_id"]

    pool = request.app["pool"]
    async with pool.acquire() as connection:
        # Mark resource as deleted in catalog table
        q = f"""UPDATE catalog SET deleted = TRUE WHERE resource_id = '{resource_id}';"""
        await connection.execute(q)

    return web.json_response({"message": "deleted"})
