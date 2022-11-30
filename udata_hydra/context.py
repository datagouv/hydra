import logging

from unittest.mock import MagicMock

import asyncpg

from rq import Queue

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


def queue():
    if "queue" not in context:
        if config.TESTING:
            from fakeredis import FakeStrictRedis
            connection = FakeStrictRedis()
        else:
            import redis
            connection = redis.from_url(config.REDIS_URL)
        context["queue"] = Queue(connection=connection, is_async=(not config.TESTING))
    return context["queue"]
