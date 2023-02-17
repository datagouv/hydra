from udata_hydra import context
from udata_hydra.db import get_placeholders


async def insert(columns, values, on_conflict=None, pool=None):
    columns = ",".join(columns)
    placeholders = ",".join(get_placeholders(values))
    pool = pool or await context.pool()
    q = f"INSERT INTO catalog ({columns}) VALUES ({placeholders})"
    if on_conflict:
        q += f" ON CONFLICT (dataset_id, resource_id, url) DO {on_conflict}"
    print(q, values)
    await pool.execute(q, *values)
