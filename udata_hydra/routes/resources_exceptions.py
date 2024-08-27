import json

from aiohttp import web

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
        indexes: list[str] = payload["indexes"]
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps(err))

    try:
        resource_exception: dict = await ResourceException.insert(
            resource_id=resource_id,
            indexes=indexes,
        )
    except ValueError:
        raise web.HTTPBadRequest(text="Resource not found")

    return web.json_response(resource_exception, status=201)
