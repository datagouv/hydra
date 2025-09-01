import json
import uuid

from aiohttp import web
from asyncpg import Record
from asyncpg.exceptions import UniqueViolationError

from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.schemas import ResourceExceptionSchema


async def get_all_resources_exceptions(request: web.Request) -> web.Response:
    """Endpoint to get all resource exceptions from the DB
    Respond with a 200 status code with a list of resource exceptions
    """
    resources_exceptions: list[Record] = await ResourceException.get_all()

    return web.json_response(
        [ResourceExceptionSchema().dump(dict(r)) for r in resources_exceptions]
    )


async def create_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to receive a resource exception creation event from a source
    Will create a new exception in the DB "resources_exceptions" table.
    Respond with a 201 status code with the created resource exception
    """
    try:
        payload = await request.json()
        resource_id: str = payload["resource_id"]
        table_indexes: dict[str, str] | None = payload.get("table_indexes")
        # format should be like this:
        #    {
        #       "column_name": "index_type",
        #       "column_name": "index_type"
        #       ...
        #    },
        comment: str | None = payload.get("comment")
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    try:
        resource_exception: Record = await ResourceException.insert(
            resource_id=resource_id,
            table_indexes=table_indexes,
            comment=comment,
        )
    except ValueError as err:
        raise web.HTTPBadRequest(text=f"Resource exception could not be created: {str(err)}")
    except UniqueViolationError:
        raise web.HTTPBadRequest(text="Resource exception already exists")

    return web.json_response(ResourceExceptionSchema().dump(dict(resource_exception)), status=201)


async def update_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to update a resource exception in the DB
    Respond with a 200 status code with the updated resource exception
    """
    try:
        resource_id = str(uuid.UUID(request.match_info["resource_exception_id"]))
    except Exception as e:
        raise web.HTTPBadRequest(text=f"error: {str(e)}")

    resource: Record | None = await Resource.get(resource_id)
    if not resource:
        raise web.HTTPNotFound()

    try:
        payload = await request.json()
        table_indexes: dict[str, str] | None = payload.get("table_indexes")
        comment: str | None = payload.get("comment")
        if table_indexes:
            valid, error = ResourceExceptionSchema.are_table_indexes_valid(table_indexes)
            if not valid:
                raise web.HTTPBadRequest(text=error)
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    resource_exception: Record = await ResourceException.update(
        resource_id=resource_id,
        table_indexes=table_indexes,
        comment=comment,
    )

    return web.json_response(ResourceExceptionSchema().dump(dict(resource_exception)))


async def delete_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to delete a resource exception from the DB
    Respond with a 204 status code
    """
    try:
        resource_id = str(uuid.UUID(request.match_info["resource_exception_id"]))
    except Exception as e:
        raise web.HTTPBadRequest(text=f"error: {str(e)}")

    resource: Record | None = await Resource.get(resource_id)
    if not resource:
        raise web.HTTPNotFound()

    await ResourceException.delete(resource_id)

    return web.HTTPNoContent()
