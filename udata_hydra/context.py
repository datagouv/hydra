import logging
from unittest.mock import MagicMock

import asyncpg
import redis
from rq import Queue

from udata_hydra import config
from udata_hydra.db.codec import init_connection

log = logging.getLogger("udata-hydra")

# Shared lazy state for reusable infrastructure components.
# - "databases": asyncpg pools, keyed by db name (e.g. "main") — see pool()
# - "queues": RQ queues, keyed by queue name (e.g. "default") — see queue()
# - "monitor": healthcheck mock singleton — see monitor(), added on first use
# - "s3": S3 upload client singleton — see s3_client(), added on first use
context: dict = {
    "databases": {},
    "queues": {},
}


def monitor() -> MagicMock:
    if "monitor" in context:
        return context["monitor"]
    monitor = MagicMock()
    monitor.set_status = lambda x: log.debug(x)
    monitor.init = lambda **kwargs: log.debug(f"Starting udata-hydra... {kwargs}")
    context["monitor"] = monitor
    return context["monitor"]


async def pool(db: str = "main") -> asyncpg.pool.Pool:
    if db not in context["databases"]:
        dsn = config.DATABASE_URL if db == "main" else getattr(config, f"DATABASE_URL_{db.upper()}")
        context["databases"][db] = await asyncpg.create_pool(
            dsn=dsn,
            max_size=config.MAX_POOL_SIZE,
            server_settings={"search_path": config.DATABASE_SCHEMA},
            init=init_connection,
        )
    return context["databases"][db]


def queue(name: str = "default", exception: bool = False) -> Queue | None:
    if not context["queues"].get(name):
        # we dont need a queue while testing, make sure we're not using a real Redis connection
        if config.TESTING:
            return None
        connection = redis.from_url(config.REDIS_URL)
        context["queues"][name] = Queue(
            name,
            connection=connection,
            default_timeout=(
                config.RQ_DEFAULT_TIMEOUT * 5 if exception else config.RQ_DEFAULT_TIMEOUT
            ),
        )
    return context["queues"][name]


def s3_client():
    if "s3" in context:
        return context["s3"]
    # Import here, not at module level: utils.s3 pulls in boto3/botocore on load.
    # Defer until first upload so CLI commands that never touch S3 stay quiet/fast.
    from udata_hydra.utils.s3 import S3Client

    context["s3"] = S3Client(bucket=config.S3_BUCKET)
    return context["s3"]
