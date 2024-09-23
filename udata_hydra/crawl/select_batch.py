from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.db.resource import Resource


async def select_batch_resources_to_check() -> list[Record]:
    """Select a batch of resources to check from the catalog
    1) It first selects resources with priority=True
    2) ...then resources without checks
    3) and if the total number of selected resources is still less than the batch size, it will also add resources with outdated checks in the batch
    """
    context.monitor().set_status("Getting a batch from catalog...")

    pool = await context.pool()
    async with pool.acquire() as connection:
        excluded = Resource.get_excluded_clause()

        # 1) Resources that are prioritised
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

        # 2) Resources without checks
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

        # 3) if not enough for our batch size, add resources with outdated checks, with an increasing delay if resources are not changing:
        # To get resources with outdated checks, they need to meet one of the following conditions:
        # - Don't have at least two checks yet, and the last check is older than CHECK_DELAYS[0]
        # - At least one the their two most recent last checks have no detected_last_modified_at, and the last check is older than CHECK_DELAYS[0]
        # - Their two most recent last checks have changed, and the last check is older than CHECK_DELAYS[0]
        # - Their last two checks have not changed, the two checks have done between two delays in CHECK_DELAYS, and the last check is older than the same delay in CHECK_DELAYS (this is in order to avoid checking too often the same resource which doesn't change)
        if len(to_check) < config.BATCH_SIZE:
            limit = config.BATCH_SIZE - len(to_check)

            query_start = f"""
                WITH recent_checks AS (
                    SELECT resource_id, detected_last_modified_at, created_at,
                        ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY created_at DESC) AS rn
                    FROM checks
                )
                SELECT catalog.url, dataset_id, catalog.resource_id
                FROM catalog
                    LEFT JOIN recent_checks ON catalog.resource_id = recent_checks.resource_id
                WHERE catalog.priority = False
                GROUP BY catalog.url, dataset_id, catalog.resource_id
                HAVING (
                    (
                        COUNT(recent_checks.resource_id) < 2
                        AND MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[0]}'
                    )
                    OR (
                        COUNT(CASE WHEN recent_checks.detected_last_modified_at IS NULL THEN 1 END) >= 1
                        AND MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[0]}'
                    )
                    OR (
                        COUNT(DISTINCT recent_checks.detected_last_modified_at) > 1
                        AND MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[0]}'
                    )
            """

            query_dynamic = ""
            for i in range(len(config.CHECK_DELAYS) - 1):
                query_dynamic += f"""
                    OR (
                        COUNT(recent_checks.resource_id) = 2
                        AND COUNT(DISTINCT recent_checks.detected_last_modified_at) < 2
                        AND MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[i]}'
                        AND MAX(recent_checks.created_at) >= NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[i + 1]}'
                        AND MIN(recent_checks.created_at) < MAX(recent_checks.created_at) - INTERVAL '{config.CHECK_DELAYS[i]}'
                        AND MIN(recent_checks.created_at) >= MAX(recent_checks.created_at) - INTERVAL '{config.CHECK_DELAYS[i + 1]}'
                    )
                """

            query_end = f"""
                    OR MAX(recent_checks.created_at) < NOW() AT TIME ZONE 'UTC' - INTERVAL '{config.CHECK_DELAYS[-1]}'
                )
                ORDER BY random() LIMIT {limit};
            """

            # Combine all parts to form the final query
            q = f"{query_start} {query_end}"

            to_check += await connection.fetch(q)

    return to_check
