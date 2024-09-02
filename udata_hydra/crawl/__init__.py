import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import aiohttp
import asyncpg
from humanfriendly import parse_timespan

from udata_hydra import config, context
from udata_hydra.crawl.check_resource import check_resource
from udata_hydra.crawl.select_resources_to_check import select_resources_to_check
from udata_hydra.logger import setup_logging
from udata_hydra.utils import queue  # noqa

results: defaultdict = defaultdict(int)


log = setup_logging()


async def check_resources(to_parse: list[str]) -> None:
    """Check a batch of resources"""
    context.monitor().set_status("Checking resources...")
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
            to_check = await select_resources_to_check()

            if to_check and len(to_check):
                await check_resources(to_check)

            else:
                context.monitor().set_status("No resources to check for now.")

            await asyncio.sleep(config.SLEEP_BETWEEN_BATCHES)
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
