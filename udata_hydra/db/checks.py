from udata_hydra import context
from udata_hydra.db import (
    compute_insert_query, update_table_record, convert_dict_values_to_json
)


async def insert(data: dict) -> int:
    data = convert_dict_values_to_json(data)
    q = compute_insert_query(data, "checks")
    pool = await context.pool()
    async with pool.acquire() as connection:
        last_check = await connection.fetchrow(q, *data.values())
        q = """UPDATE catalog SET last_check = $1 WHERE resource_id = $2"""
        await connection.execute(q, last_check["id"], data["resource_id"])
    return last_check["id"]


async def update(check_id: int, data: dict) -> int:
    return await update_table_record("checks", check_id, data)


async def get(check_id):
    pool = await context.pool()
    async with pool.acquire() as connection:
        q = """
            SELECT * FROM catalog JOIN checks
            ON catalog.last_check = checks.id
            WHERE checks.id = $1
            AND catalog.deleted = FALSE;
        """
        check = await connection.fetchrow(q, check_id)
    return check
