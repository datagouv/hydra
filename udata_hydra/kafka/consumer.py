import logging

from udata_hydra import context
from udata_hydra.utils.kafka import get_topic
from udata_hydra.utils.minio import delete_resource_from_minio

log = logging.getLogger("udata-hydra-kafka")


async def process_message(key: str, message: dict, topic: str) -> None:
    log.debug(f"Received message in topic {topic} with key {key}")
    dataset_id = message["meta"]["dataset_id"]

    pool = await context.pool()
    async with pool.acquire() as connection:
        if topic == get_topic("resource.created"):
            resource = message["value"]
            # Insert new resource in catalog table and mark as high priority for crawling
            q = f"""
                    INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority, initialization)
                    VALUES ('{dataset_id}', '{key}', '{resource["url"]}', FALSE, TRUE, FALSE)
                    ON CONFLICT (dataset_id, resource_id, url) DO UPDATE SET priority = TRUE;"""
            await connection.execute(q)
        elif topic == get_topic("resource.deleted"):
            delete_resource_from_minio(dataset_id, key)
            # Mark resource as deleted in catalog table
            q = f"""UPDATE catalog SET deleted = TRUE WHERE resource_id = '{key}';"""
            await connection.execute(q)
        elif topic == get_topic("resource.modified"):
            # Make resource high priority for crawling
            # Check if resource is in catalog then insert or update into table
            q = f"""SELECT * FROM catalog WHERE resource_id = '{key}';"""
            res = await connection.fetch(q)
            if(len(res) != 0):
                q = f"""UPDATE catalog SET priority = TRUE WHERE resource_id = '{key}';"""
            else:
                resource = message["value"]
                q = f"""
                        INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority, initialization)
                        VALUES ('{dataset_id}', '{key}', '{resource["url"]}', FALSE, TRUE, FALSE)
                        ON CONFLICT (dataset_id, resource_id, url) DO UPDATE SET priority = TRUE;"""
            await connection.execute(q)
        else:
            log.error(f"Unknown topic {topic}")

