import json

from udata_hydra import context


def get_placeholders(values):
    # $1, $2...
    return [f"${x + 1}" for x in range(len(values))]


def compute_update_query(table: str, data: dict):
    columns = data.keys()
    placeholders = get_placeholders(data.values())
    set_clause = ",".join([f"{c} = {v}" for c, v in zip(columns, placeholders)])
    return f"""
        UPDATE "{table}"
        SET {set_clause}
        WHERE id = ${len(placeholders) + 1}
    """


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
    placeholders = ",".join(get_placeholders(data.values()))
    return f"""
        INSERT INTO "{table}" ({columns})
        VALUES ({placeholders})
        RETURNING {returning}
    """


async def update_table_record(table: str, record_id: int, data: dict) -> int:
    data = convert_dict_values_to_json(data)
    q = compute_update_query(table, data)
    pool = await context.pool()
    await pool.execute(q, *data.values(), record_id)
    return record_id
