import json
import uuid

from aiohttp import web
from asyncpg import Record
from asyncpg.exceptions import UniqueViolationError

from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.schemas import (
    CreateResourceExceptionRequest,
    ResourceExceptionSchema,
    UpdateResourceExceptionRequest,
)


async def get_all_resources_exceptions(request: web.Request) -> web.Response:
    """Endpoint to get all resource exceptions from the DB
    Respond with a 200 status code with a list of resource exceptions
    """

    resources_exceptions: list[Record] = await ResourceException.get_all()

    return web.json_response(
        [
            ResourceExceptionSchema.model_validate(dict(r)).model_dump(mode="json")
            for r in resources_exceptions
        ]
    )


async def create_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to receive a resource exception creation event from a source
    Will create a new exception in the DB "resources_exceptions" table.
    Respond with a 201 status code with the created resource exception
    """

    try:
        request_data: dict = await request.json()
        payload: CreateResourceExceptionRequest = CreateResourceExceptionRequest.model_validate(
            request_data
        )
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    try:
        resource_exception: Record | None = await ResourceException.insert(
            resource_id=str(payload.resource_id),
            table_indexes=payload.table_indexes,
            comment=payload.comment,
        )
        if not resource_exception:
            raise web.HTTPBadRequest(text="Resource exception could not be created")
    except ValueError as err:
        raise web.HTTPBadRequest(text=f"Resource exception could not be created: {str(err)}")
    except UniqueViolationError:
        raise web.HTTPBadRequest(text="Resource exception already exists")

    return web.json_response(
        ResourceExceptionSchema.model_validate(dict(resource_exception)).model_dump(mode="json"),
        status=201,
    )


async def update_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to update a resource exception in the DB
    Respond with a 200 status code with the updated resource exception
    """

    # Extract and validate resource_id from URL path parameter
    try:
        resource_id = str(uuid.UUID(request.match_info["resource_exception_id"]))
    except Exception as e:
        raise web.HTTPBadRequest(text=f"error: {str(e)}")

    # Get resource from the DB
    resource: Record | None = await Resource.get(resource_id)
    if not resource:
        raise web.HTTPNotFound()

    # Parse and validate the request payload
    try:
        request_data: dict = await request.json()
        payload: UpdateResourceExceptionRequest = UpdateResourceExceptionRequest.model_validate(
            request_data
        )
        if payload.table_indexes:
            valid, error = ResourceExceptionSchema.are_table_indexes_valid(payload.table_indexes)
            if not valid:
                raise web.HTTPBadRequest(text=error)
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    resource_exception: Record | None = await ResourceException.update(
        resource_id=resource_id,
        table_indexes=payload.table_indexes,
        comment=payload.comment,
    )

    if not resource_exception:
        raise web.HTTPNotFound(text="Resource exception not found")

    return web.json_response(
        ResourceExceptionSchema.model_validate(dict(resource_exception)).model_dump(mode="json")
    )


async def delete_resource_exception(request: web.Request) -> web.Response:
    """Endpoint to delete a resource exception from the DB
    Respond with a 204 status code
    """

    # Extract and validate resource_id from URL path parameter
    try:
        resource_id = str(uuid.UUID(request.match_info["resource_exception_id"]))
    except Exception as e:
        raise web.HTTPBadRequest(text=f"error: {str(e)}")

    resource: Record | None = await Resource.get(resource_id)
    if not resource:
        raise web.HTTPNotFound()

    await ResourceException.delete(resource_id)

    return web.HTTPNoContent()
