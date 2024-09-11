from datetime import datetime, timedelta, timezone

from asyncpg import Record
from humanfriendly import parse_timespan

from udata_hydra import config, context
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource


async def select_batch_resources_to_check() -> list[Record]:
    """Select a batch of resources to check from the catalog
    - It first selects resources with priority=True
    - ...then resources without checks
    - and if the total number of selected resources is still less than the batch size, it will also add resources with outdated checks in the batch.
    """
    context.monitor().set_status("Getting a batch from catalog...")

    pool = await context.pool()
    async with pool.acquire() as connection:
        excluded = Resource.get_excluded_clause()

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
        to_check: list[Record] = await connection.fetch(q)

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
            to_check += await connection.fetch(q)

        # if not enough for our batch size, handle outdated checks
        if len(to_check) < config.BATCH_SIZE:
            limit = config.BATCH_SIZE - len(to_check)

            # Base query parts
            query_start = f"""
                SELECT * FROM (
                    SELECT catalog.url, dataset_id, catalog.resource_id
                    FROM catalog
                    JOIN checks ON catalog.last_check = checks.id
                    WHERE (
                        (checks.detected_last_modified_at IS NULL AND checks.created_at < CURRENT_DATE - INTERVAL '{config.CHECK_DELAY_DEFAULT}')
                        OR
                        (checks.detected_last_modified_at IS NOT NULL AND (
            """

            # Construct the dynamic part of the query
            dynamic_conditions = " OR ".join(
                f"(checks.created_at >= checks.detected_last_modified_at + INTERVAL '{delay}' AND checks.created_at < CURRENT_DATE - INTERVAL '{delay}')"
                for delay in config.CHECK_DELAYS
            )

            query_end = f"""
                        ))
                    )
                    AND catalog.priority = False
                ) s
                ORDER BY random() LIMIT {limit};
            """

            # Combine all parts to form the final query
            final_query = f"{query_start} {dynamic_conditions} {query_end}"

            to_check += await connection.fetch(final_query)

    return to_check
