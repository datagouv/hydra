import os
from typing import Union

from aiohttp import web
from aiohttp_tokenauth import token_auth_middleware

from udata_hydra import config, context
from udata_hydra.routes import routes


async def app_factory() -> web.Application:
    async def app_startup(app):
        app["pool"] = await context.pool()

    async def app_cleanup(app):
        if "pool" in app:
            await app["pool"].close()

    async def user_loader(token: str) -> Union[dict, None]:
        """Checks that app token is valid
        Callback that will get the token from "Authorization" header.
        Args:
            token (str): A token from "Authorization" http header.

        Returns:
            Dict but could be something else. If the callback returns None then the aiohttp.web.HTTPForbidden will be raised.
        """
        if token == config.API_TOKEN:
            return {"username": "admin"}
        return None

    app = web.Application(
        middlewares=[token_auth_middleware(user_loader=user_loader, exclude_methods=("GET"))]
    )
    app.add_routes(routes)
    app.on_startup.append(app_startup)
    app.on_cleanup.append(app_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(app_factory(), path=os.environ.get("HYDRA_APP_SOCKET_PATH"))
