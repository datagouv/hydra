import json

from aiohttp import web
from asyncpg import Record

from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import get_request_params


async def get_all_resources_exceptions(request: web.Request) -> web.Response:
    """Endpoint to get all resource exceptions from the DB
    Respond with a 200 status code with a list of resource exceptions
    """
    resource_exceptions: list[Record] = await ResourceException.get_all()
    return web.json_response(resource_exceptions)


async def create_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to receive a resource exception creation event from a source
    Will create a new exception in the DB "resources_exceptions" table.
    Respond with a 201 status code with the created resource exception
    If error, respond with a 400 status code
    """
    try:
        payload = await request.json()
        resource_id: str = payload["resource_id"]
        table_indexes: dict[str, str] = payload["table_indexes"]
        # format should be like this:
        #    {
        #       "column_name": "index_type",
        #       "column_name": "index_type"
        #       ...
        #    },
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps(err))

    try:
        resource_exception: Record = await ResourceException.insert(
            resource_id=resource_id,
            table_indexes=table_indexes,
        )
    except ValueError as err:
        raise web.HTTPBadRequest(text=f"Resource exception could not be created: {err}")

    return web.json_response(resource_exception, status=201)


async def delete_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to delete a resource exception from the DB
    Respond with a 204 status code
    If error, respond with a 400 status code
    """
    [resource_id] = get_request_params(request, params_names=["resource_id"])
    resource: Record | None = await Resource.get(resource_id)
    if not resource:
        raise web.HTTPNotFound()

    await ResourceException.delete(resource_id)

    return web.HTTPNoContent()
