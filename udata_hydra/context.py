import logging

from unittest.mock import MagicMock

import asyncpg

from udata_hydra import config

log = logging.getLogger("udata-hydra")
context = {}


def monitor():
    if "monitor" in context:
        return context["monitor"]
    monitor = MagicMock()
    monitor.set_status = lambda x: log.debug(x)
    monitor.init = lambda **kwargs: log.debug(
        f"Starting udata-hydra... {kwargs}"
    )
    context["monitor"] = monitor
    return context["monitor"]


async def pool():
    if "pool" not in context:
        dsn = config.DATABASE_URL
        context["pool"] = await asyncpg.create_pool(dsn=dsn, max_size=50)
    return context["pool"]
