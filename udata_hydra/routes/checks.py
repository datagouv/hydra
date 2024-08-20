import json
from typing import Union

import aiohttp
from aiohttp import web
from marshmallow import ValidationError

from udata_hydra import config, context
from udata_hydra.crawl import RESOURCE_RESPONSE_STATUSES, check_url
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.schemas import CheckSchema
from udata_hydra.utils import get_request_params


async def get_latest_check(request: web.Request) -> web.Response:
    """Get the latest check for a given URL or resource_id"""
    url, resource_id = get_request_params(request, params_names=["url", "resource_id"])
    data: Union[dict, None] = await Check.get_latest(url, resource_id)
    if not data:
        raise web.HTTPNotFound()
    if data["deleted"]:
        raise web.HTTPGone()
    return web.json_response(CheckSchema().dump(dict(data)))


async def get_all_checks(request: web.Request) -> web.Response:
    url, resource_id = get_request_params(request, params_names=["url", "resource_id"])
    data: Union[list, None] = await Check.get_all(url, resource_id)
    if not data:
        raise web.HTTPNotFound()
    return web.json_response([CheckSchema().dump(dict(r)) for r in data])


async def create_check(request: web.Request) -> web.Response:
    """Create a new check"""

    # Get resource_id from request
    try:
        payload: dict = await request.json()
        resource_id: str = payload["resource_id"]
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))
    except KeyError as e:
        raise web.HTTPBadRequest(text=f"Missing key: {e}")

    # Get URL from resource_id
    try:
        resource: dict = await Resource.get(resource_id, "url")
        url: str = resource["url"]
    except Exception:
        raise web.HTTPNotFound(text=f"Couldn't find URL for resource {resource_id}")

    context.monitor().set_status(f'Crawling url "{url}"...')

    async with aiohttp.ClientSession(
        timeout=None, headers={"user-agent": config.USER_AGENT}
    ) as session:
        status: str = await check_url(
            url=url, resource_id=resource_id, session=session, worker_priority="high"
        )
        context.monitor().refresh(status)

    if status == RESOURCE_RESPONSE_STATUSES["OK"]:
        return web.HTTPOk()
    elif status == RESOURCE_RESPONSE_STATUSES["TIMEOUT"]:
        return web.HTTPGatewayTimeout()
    return web.HTTPBadGateway(text=f"Error while checking the resource: {status}")
