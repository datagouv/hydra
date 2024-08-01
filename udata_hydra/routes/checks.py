from aiohttp import web

from udata_hydra.db.check import Check
from udata_hydra.schemas import CheckSchema
from udata_hydra.utils import get_request_params


async def get_latest_check(request: web.Request) -> web.Response:
    """Get the latest check for a given URL or resource_id"""
    url, resource_id = get_request_params(request, params_names=["url", "resource_id"])
    data = await Check.get_latest(url, resource_id)
    if not data:
        raise web.HTTPNotFound()
    if data["deleted"]:
        raise web.HTTPGone()
    return web.json_response(CheckSchema().dump(dict(data)))


async def get_all_checks(request: web.Request) -> web.Response:
    url, resource_id = get_request_params(request, params_names=["url", "resource_id"])
    data = await Check.get_all(url, resource_id)
    if not data:
        raise web.HTTPNotFound()
    return web.json_response([CheckSchema().dump(dict(r)) for r in data])
