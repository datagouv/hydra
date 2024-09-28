import os

from aiohttp import web

from udata_hydra import context
from udata_hydra.routes import routes
from udata_hydra.utils import token_auth_middleware


async def app_factory() -> web.Application:
    async def app_startup(app):
        app["pool"] = await context.pool()

    async def app_cleanup(app):
        if "pool" in app:
            await app["pool"].close()

    app = web.Application(middlewares=[token_auth_middleware(exclude_methods=("GET",))])
    app.add_routes(routes)
    app.on_startup.append(app_startup)
    app.on_cleanup.append(app_cleanup)
    return app


def run():
    web.run_app(app_factory(), path=os.environ.get("HYDRA_APP_SOCKET_PATH"))


if __name__ == "__main__":
    run()
