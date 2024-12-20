import json
from datetime import date

import aiohttp
from aiohttp import web
from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.crawl.check_resources import check_resource
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.schemas import CheckGroupBy, CheckSchema
from udata_hydra.utils import get_request_params


async def get_latest_check(request: web.Request) -> web.Response:
    """Get the latest check for a given URL or resource_id"""
    url, resource_id = get_request_params(request, params_names=["url", "resource_id"])
    data: Record | None = await Check.get_latest(url, resource_id)
    if not data:
        raise web.HTTPNotFound()
    if data["deleted"]:
        raise web.HTTPGone()

    return web.json_response(CheckSchema().dump(dict(data)))


async def get_all_checks(request: web.Request) -> web.Response:
    url, resource_id = get_request_params(request, params_names=["url", "resource_id"])
    data: list | None = await Check.get_all(url, resource_id)
    if not data:
        raise web.HTTPNotFound()

    return web.json_response([CheckSchema().dump(dict(r)) for r in data])


async def get_checks_aggregate(request: web.Request) -> web.Response:
    created_at: str = request.query.get("created_at")
    if not created_at:
        raise web.HTTPBadRequest(
            text="Missing mandatory 'created_at' param. You can use created_at=today to filter on today checks."
        )

    if created_at == "today":
        created_at_date: date = date.today()
    else:
        created_at_date: date = date.fromisoformat(created_at)

    column: str = request.query.get("group_by")
    if not column:
        raise web.HTTPBadRequest(text="Missing mandatory 'group_by' param.")
    data: list | None = await Check.get_group_by_for_date(column, created_at_date)
    if not data:
        raise web.HTTPNotFound()

    return web.json_response([CheckGroupBy().dump(dict(r)) for r in data])


async def create_check(request: web.Request) -> web.Response:
    """Create a new check"""

    # Get resource_id from request
    try:
        payload: dict = await request.json()
        resource_id: str = payload["resource_id"]
        force_analysis: bool = payload.get("force_analysis", True)
    except Exception as err:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(err)}))

    # Get URL from resource_id
    try:
        resource: Record | None = await Resource.get(resource_id)
        url: str = resource["url"]
    except Exception:
        raise web.HTTPNotFound(text=f"Couldn't find URL for resource {resource_id}")

    context.monitor().set_status(f'Crawling url "{url}"...')

    async with aiohttp.ClientSession(
        timeout=None, headers={"user-agent": config.USER_AGENT}
    ) as session:
        status: str = await check_resource(
            url=url,
            resource=resource,
            force_analysis=force_analysis,
            session=session,
            worker_priority="high",
        )
        context.monitor().refresh(status)

    check: Record | None = await Check.get_latest(url, resource_id)
    if not check:
        raise web.HTTPBadRequest(text=f"Check not created, status: {status}")

    return web.json_response(CheckSchema().dump(dict(check)), status=201)
