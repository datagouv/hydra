import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import aiohttp
from humanfriendly import parse_timespan

from udata_hydra import config, context
from udata_hydra.crawl.check_resource import check_resource
from udata_hydra.crawl.utils_db import get_excluded_clause, select_rows_based_on_query
from udata_hydra.logger import setup_logging
from udata_hydra.utils import queue  # noqa

results: defaultdict = defaultdict(int)


log = setup_logging()


async def check_resources(to_parse: list[str]) -> None:
    context.monitor().set_status("Checking urls...")
    tasks: list = []
    async with aiohttp.ClientSession(
        timeout=None, headers={"user-agent": config.USER_AGENT}
    ) as session:
        for row in to_parse:
            tasks.append(
                check_resource(
                    url=row["url"],
                    resource_id=row["resource_id"],
                    session=session,
                    worker_priority="low",
                )
            )
        for task in asyncio.as_completed(tasks):
            result = await task
            results[result] += 1
            context.monitor().refresh(results)


async def check_batch() -> None:
    """Check a batch of resources from the catalog"""
    context.monitor().set_status("Getting a batch from catalog...")
    pool = await context.pool()
    async with pool.acquire() as connection:
        excluded = get_excluded_clause()
        # first urls that are prioritised
        q = f"""
            SELECT * FROM (
                SELECT catalog.url, dataset_id, resource_id
                FROM catalog
                WHERE {excluded}
                AND priority = True
            ) s
            ORDER BY random() LIMIT {config.BATCH_SIZE}
        """
        to_check = await select_rows_based_on_query(connection, q)
        # then urls without checks
        if len(to_check) < config.BATCH_SIZE:
            q = f"""
                SELECT * FROM (
                    SELECT catalog.url, dataset_id, resource_id
                    FROM catalog
                    WHERE catalog.last_check IS NULL
                    AND {excluded}
                    AND priority = False
                ) s
                ORDER BY random() LIMIT {config.BATCH_SIZE}
            """
            to_check += await select_rows_based_on_query(connection, q)
        # if not enough for our batch size, handle outdated checks
        if len(to_check) < config.BATCH_SIZE:
            since = parse_timespan(config.SINCE)  # in seconds
            since = datetime.now(timezone.utc) - timedelta(seconds=since)
            limit = config.BATCH_SIZE - len(to_check)
            q = f"""
            SELECT * FROM (
                SELECT catalog.url, dataset_id, catalog.resource_id
                FROM catalog, checks
                WHERE catalog.last_check IS NOT NULL
                AND {excluded}
                AND catalog.last_check = checks.id
                AND checks.created_at <= $1
                AND catalog.priority = False
            ) s
            ORDER BY random() LIMIT {limit}
            """
            to_check += await select_rows_based_on_query(connection, q, since)

    if len(to_check):
        await check_resources(to_check)
    else:
        context.monitor().set_status("Nothing to crawl for now.")
    await asyncio.sleep(config.SLEEP_BETWEEN_BATCHES)


async def check_catalog(iterations: int = -1) -> None:
    """Launch check batches

    :iterations: for testing purposes (break infinite loop)
    """
    try:
        context.monitor().init(
            SINCE=config.SINCE,
            BATCH_SIZE=config.BATCH_SIZE,
            BACKOFF_NB_REQ=config.BACKOFF_NB_REQ,
            BACKOFF_PERIOD=config.BACKOFF_PERIOD,
        )
        while iterations != 0:
            await check_batch()
            iterations -= 1
    finally:
        pool = await context.pool()
        await pool.close()


def run() -> None:
    """Main function

    :iterations: for testing purposes (break infinite loop)
    """
    try:
        asyncio.get_event_loop().run_until_complete(check_catalog())
    except KeyboardInterrupt:
        pass
    finally:
        context.monitor().teardown()


if __name__ == "__main__":
    run()
