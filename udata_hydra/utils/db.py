import json

from udata_hydra import context


def convert_dict_values_to_json(data: dict) -> dict:
    """
    Convert values in dict that are dict to json for DB serialization
    TODO: this is suboptimal from asyncpg, dig into this
    https://magicstack.github.io/asyncpg/current/usage.html#example-automatic-json-conversion
    """
    for k, v in data.items():
        if type(v) is dict:
            data[k] = json.dumps(v)
    return data


def compute_insert_query(data: dict, table: str, returning: str = "id") -> str:
    columns = ",".join([f'"{k}"' for k in data.keys()])
    # $1, $2...
    placeholders = ",".join([f"${x + 1}" for x in range(len(data.values()))])
    return f"""
        INSERT INTO "{table}" ({columns})
        VALUES ({placeholders})
        RETURNING {returning}
    """


async def insert_check(data: dict) -> int:
    data = convert_dict_values_to_json(data)
    q = compute_insert_query(data, "checks")
    pool = await context.pool()
    async with pool.acquire() as connection:
        last_check = await connection.fetchrow(q, *data.values())
        q = """UPDATE catalog SET last_check = $1 WHERE resource_id = $2"""
        await connection.execute(q, last_check["id"], data["resource_id"])
    return last_check["id"]


def compute_update_query(table: str, data: dict):
    columns = data.keys()
    # $1, $2...
    placeholders = [f"${x + 1}" for x in range(len(data.values()))]
    set_clause = ",".join([f"{c} = {v}" for c, v in zip(columns, placeholders)])
    return f"""
        UPDATE "{table}"
        SET {set_clause}
        WHERE id = ${len(placeholders) + 1}
    """


async def update_table_record(table: str, record_id: int, data: dict) -> int:
    data = convert_dict_values_to_json(data)
    q = compute_update_query(table, data)
    pool = await context.pool()
    await pool.execute(q, *data.values(), record_id)
    return record_id


async def update_check(check_id: int, data: dict) -> int:
    return await update_table_record("checks", check_id, data)


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
