from aiohttp import web

from udata_hydra.routes.checks import get_all_checks, get_latest_check
from udata_hydra.routes.resources import (
    create_resource,
    delete_resource,
    get_resource,
    update_resource,
)
from udata_hydra.routes.status import get_crawler_status, get_stats, get_worker_status

# routes = web.RouteTableDef()
routes: list = [
    # Routes for checks
    web.get("/api/checks/latest/", get_latest_check),
    web.get("/api/checks/all/", get_all_checks),
    # Routes for resources
    web.get("/api/resources/", get_resource),
    web.post("/api/resources/", create_resource),
    web.put("/api/resources/", update_resource),
    web.delete("/api/resources/", delete_resource),
    web.post("/api/resource/created/", create_resource),  # TODO: legacy, to remove
    web.post("/api/resource/updated/", update_resource),  # TODO: legacy, to remove
    web.post("/api/resource/deleted/", delete_resource),  # TODO: legacy, to remove
    # Routes for statuses
    web.get("/api/status/crawler/", get_crawler_status),
    web.get("/api/status/worker/", get_worker_status),
    web.get("/api/stats/", get_stats),
]
