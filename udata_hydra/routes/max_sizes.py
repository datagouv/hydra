from aiohttp import web
from udata_hydra import config


async def get_all_format_size_thresholds(request: web.Request) -> web.Response:
    """Endpoint to get all the threshold up to which a file of a specific format will be analyzed
    Respond with a 200 status code with a dict format: max_size
    """

    return web.json_response(config.MAX_FILESIZE_ALLOWED)
