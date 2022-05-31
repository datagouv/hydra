import logging

from hydra import context
from hydra.background_tasks import manage_resource

log = logging.getLogger("hydra-kafka")


async def process_message(key: str, message: dict, topic: str) -> None:
    log.info("Received message")
    dataset_id = message["meta"]["dataset_id"]
    resource = message["value"]["resource"]
    if topic in ["resource.created", "resource.modified"]:
        manage_resource.delay(dataset_id, resource)

    pool = await context.pool()
    async with pool.acquire() as connection:
        if topic == "resource.created":
            # Insert new resource in catalog table and mark as high priority for crawling
            q = f"""
                    INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority, initialization)
                    VALUES ('{dataset_id}', '{resource["id"]}', '{resource["url"]}', FALSE, TRUE, FALSE)
                    ON CONFLICT (dataset_id, resource_id, url) DO UPDATE SET priority = TRUE;"""
            await connection.execute(q)
        elif topic == "resource.deleted":
            # Mark resource as deleted in catalog table
            q = f"""UPDATE catalog SET deleted = TRUE WHERE resource_id = '{resource["id"]}';"""
            await connection.execute(q)
        elif topic == "resource.modified":
            # Make resource high priority for crawling
            q = f"""UPDATE catalog SET priority = TRUE WHERE resource_id = '{resource["id"]}';"""
            await connection.execute(q)
        else:
            log.error(f"Unknown topic {topic}")
