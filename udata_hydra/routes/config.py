from aiohttp import web

from udata_hydra import config


async def get_exposed_config(request: web.Request) -> web.Response:
    """Endpoint to expose parts of the config that is of interest to producers.
    Respond with a 200 status code with a dict containing the exposed config attributes
    """

    return web.json_response(
        {
            "max_filesize": config.MAX_FILESIZE_ALLOWED
            | {"default": config.DEFAULT_MAX_FILESIZE_ALLOWED}
        }
    )
