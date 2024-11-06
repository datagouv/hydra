import re
from typing import Coroutine

from aiohttp import web

from udata_hydra import config


def _is_exclude(request, exclude: tuple[str]) -> bool:
    for pattern in exclude:
        if re.fullmatch(pattern, request.path):
            return True
    return False


def token_auth_middleware(
    request_property: str = "user",
    auth_scheme: str = "Bearer",
    exclude_routes: tuple[str] = tuple(),
    exclude_methods: tuple[str] = tuple(),
) -> Coroutine:
    """Checks a auth token and adds a user in request.

    Token auth middleware that checks the "Authorization" http header for token and, if the token in the requet headers is valid, then middleware adds the user to request with key that contain the "request_property" variable, else it will raise an HTTPForbiddenexception.

    Args:
        request_property: Key for save in request object.
            Defaults to 'user'.
        auth_scheme: Prefix for value in "Authorization" header.
            Defaults to 'Bearer'.
        exclude_routes: Tuple of pathes that will be excluded.
            Defaults to empty tuple.
        exclude_methods: Tuple of http methods that will be
            excluded. Defaults to empty tuple.

    Raises:
        web.HTTPUnauthorized: If "Authorization" token is missing.
        web.HTTPForbidden: Wrong token, schema or header.

    Returns:
        Aiohttp middleware.
    """

    @web.middleware
    async def middleware(request, handler):
        if _is_exclude(request, exclude_routes) or request.method in exclude_methods:
            return await handler(request)

        try:
            scheme, token = request.headers["Authorization"].strip().split(" ")
        except KeyError:
            raise web.HTTPUnauthorized(
                reason="Missing authorization header",
            )
        except ValueError:
            raise web.HTTPForbidden(
                reason="Invalid authorization header",
            )

        if auth_scheme.lower() != scheme.lower():
            raise web.HTTPForbidden(
                reason="Invalid token scheme",
            )

        if token == config.API_KEY:
            request[request_property] = {"username": "udata"}
        else:
            raise web.HTTPForbidden(reason="Invalid authentication token")

        return await handler(request)

    return middleware
