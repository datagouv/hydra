import json

from udata_hydra import context


def convert_dict_values_to_json(data: dict):
    """
    Convert values in dict that are dict to json for DB serialization
    TODO: this is suboptimal from asyncpg, dig into this
    """
    for k, v in data.items():
        if type(v) is dict:
            data[k] = json.dumps(v)
    return data


async def insert_check(data: dict):
    data = convert_dict_values_to_json(data)
    columns = ",".join(data.keys())
    # $1, $2...
    placeholders = ",".join([f"${x + 1}" for x in range(len(data.values()))])
    q = f"""
        INSERT INTO checks ({columns})
        VALUES ({placeholders})
        RETURNING id
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        last_check = await connection.fetchrow(q, *data.values())
        q = """UPDATE catalog SET last_check = $1 WHERE resource_id = $2"""
        await connection.execute(q, last_check["id"], data["resource_id"])
    return last_check["id"]


async def update_check(check_id: int, data: dict) -> int:
    data = convert_dict_values_to_json(data)
    columns = data.keys()
    # $1, $2...
    placeholders = [f"${x + 1}" for x in range(len(data.values()))]
    set_clause = ",".join([f"{c} = {v}" for c, v in zip(columns, placeholders)])
    q = f"""
        UPDATE checks
        SET {set_clause}
        WHERE id = ${len(placeholders) + 1}
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        await connection.execute(q, *data.values(), check_id)
    return check_id


async def get_check(check_id):
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
