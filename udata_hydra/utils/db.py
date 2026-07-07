from asyncpg import Record

from udata_hydra import context

# PostgreSQL system columns and hydra's own __id that must be renamed when
# a user CSV happens to use them as headers.  Shared across csv, parquet and
# geojson modules.
RESERVED_COLS = ("__id", "cmin", "cmax", "collation", "ctid", "tableoid", "xmin", "xmax")


def db_col_name(col: str) -> str:
    """Map a CSV column name to its actual PostgreSQL column name."""
    return f"{col}__hydra_renamed" if col.lower() in RESERVED_COLS else col


def compute_insert_query(table_name: str, data: dict, returning: str = "id") -> str:
    columns = ",".join([f'"{k}"' for k in data.keys()])
    # $1, $2...
    placeholders = ",".join([f"${x + 1}" for x in range(len(data.values()))])
    return f"""
        INSERT INTO "{table_name}" ({columns})
        VALUES ({placeholders})
        RETURNING {returning};
    """


def compute_update_query(table_name: str, data: dict, returning: str = "*") -> str:
    columns = data.keys()
    # $1, $2...
    placeholders = [f"${x + 1}" for x in range(len(data.values()))]
    set_clause = ",".join([f"{c} = {v}" for c, v in zip(columns, placeholders)])
    return f"""
        UPDATE "{table_name}"
        SET {set_clause}
        WHERE id = ${len(placeholders) + 1}
        RETURNING {returning};
    """


async def update_table_record(table_name: str, record_id: int, data: dict) -> Record | None:
    q = compute_update_query(table_name, data)
    pool = await context.pool()
    return await pool.fetchrow(q, *data.values(), record_id)
