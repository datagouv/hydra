import json

from aiohttp import web
from asyncpg import Record

from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException


async def create_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to receive a resource exception creation event from a source
    Will create a new exception in the DB "resources_exceptions" table.
    Respond with a 201 status code with
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
