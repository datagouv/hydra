import json
import uuid

from aiohttp import web

from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_index import ResourceIndex
from udata_hydra.schemas import ReplaceResourceIndexesRequest, ResourceIndexesSchema


def _resource_id(request: web.Request) -> str:
    try:
        return str(uuid.UUID(request.match_info["resource_id"]))
    except Exception as e:
        raise web.HTTPBadRequest(text=f"error: {str(e)}")


def _response(resource_id: str, indexes: dict[str, str]) -> web.Response:
    return web.json_response(
        ResourceIndexesSchema(resource_id=uuid.UUID(resource_id), indexes=indexes).model_dump(
            mode="json"
        )
    )


async def get_resource_indexes(request: web.Request) -> web.Response:
    """Endpoint to get the indexes declared on the table of a resource
    Respond with a 200 status code, and an empty mapping when the resource has none
    """
    resource_id = _resource_id(request)
    if not await Resource.get(resource_id):
        raise web.HTTPNotFound()
    return _response(resource_id, await ResourceIndex.get_by_resource_id(resource_id))


async def replace_resource_indexes(request: web.Request) -> web.Response:
    """Endpoint to replace the indexes declared on the table of a resource
    They are created at the next conversion of the resource. An empty mapping removes them.
    Respond with a 200 status code with the indexes now declared
    """
    resource_id = _resource_id(request)
    if not await Resource.get(resource_id):
        raise web.HTTPNotFound()
    try:
        payload = ReplaceResourceIndexesRequest.model_validate(await request.json())
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    await ResourceIndex.replace(resource_id, payload.indexes)
    return _response(resource_id, payload.indexes)
