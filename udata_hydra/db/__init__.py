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


def compute_insert_query(table_name: str, data: dict, returning: str = "id") -> str:
    columns = ",".join([f'"{k}"' for k in data.keys()])
    # $1, $2...
    placeholders = ",".join([f"${x + 1}" for x in range(len(data.values()))])
    return f"""
        INSERT INTO "{table_name}" ({columns})
        VALUES ({placeholders})
        RETURNING {returning}
    """


def compute_update_query(table_name: str, data: dict) -> str:
    columns = data.keys()
    # $1, $2...
    placeholders = [f"${x + 1}" for x in range(len(data.values()))]
    set_clause = ",".join([f"{c} = {v}" for c, v in zip(columns, placeholders)])
    return f"""
        UPDATE "{table_name}"
        SET {set_clause}
        WHERE id = ${len(placeholders) + 1}
    """


async def update_table_record(table_name: str, record_id: int, data: dict) -> int:
    data = convert_dict_values_to_json(data)
    q = compute_update_query(table_name, data)
    pool = await context.pool()
    await pool.execute(q, *data.values(), record_id)
    return record_id
