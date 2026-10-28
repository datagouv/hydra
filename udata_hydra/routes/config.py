from aiohttp import web

from udata_hydra import config


async def get_exposed_config(request: web.Request) -> web.Response:
    """Endpoint to get all the threshold up to which a file of a specific format will be analyzed
    Respond with a 200 status code with a dict format: max_size
    """

    return web.json_response(
        {
            "max_filesize": config.MAX_FILESIZE_ALLOWED
            | {"default": config.DEFAULT_MAX_FILESIZE_ALLOWED}
        }
    )
