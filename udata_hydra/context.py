import os
import logging

from unittest.mock import MagicMock

import asyncpg

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
        dsn = os.getenv(
            "DATABASE_URL",
            "postgres://postgres:postgres@localhost:5432/postgres",
        )
        context["pool"] = await asyncpg.create_pool(dsn=dsn, max_size=50)
    return context["pool"]
