import logging
from unittest.mock import MagicMock

import asyncpg
import redis
from rq import Queue

from udata_hydra import config

log = logging.getLogger("udata-hydra")
context = {
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


async def pool(db: str = "main", component: str = "worker") -> asyncpg.pool.Pool:
    """Get or create a connection pool for a specific database and component.

    Args:
        db: Database name ("main" or "csv")
        component: Component name ("crawler", "api", "worker")

    Returns:
        Connection pool for the specified database/component

    Examples:
        crawler_pool = await pool("main", "crawler")
        api_pool = await pool("main", "api")
        worker_pool = await pool("main", "worker")
        csv_pool = await pool("csv", "worker")
    """
    # Create a unique key for the pool
    pool_key = f"{db}:{component}"

    if pool_key not in context["databases"]:
        dsn = config.DATABASE_URL if db == "main" else getattr(config, f"DATABASE_URL_{db.upper()}")

        # Get component-specific pool size
        component_size_key = f"MAX_POOL_SIZE_{component.upper()}"
        max_size = getattr(config, component_size_key)

        log.info(f"Creating {component} pool for {db} database with max_size={max_size}")

        context["databases"][pool_key] = await asyncpg.create_pool(
            dsn=dsn,
            max_size=max_size,
            server_settings={"search_path": config.DATABASE_SCHEMA},
        )

    return context["databases"][pool_key]


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
