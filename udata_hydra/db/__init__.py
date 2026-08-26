import json
import re
from typing import Iterable

from asyncpg import Record

from udata_hydra import context

# PostgreSQL system columns and hydra's own __id that must be renamed when
# a user CSV happens to use them as headers.  Shared across csv, parquet and
# geojson modules.
RESERVED_COLS = ("__id", "cmin", "cmax", "collation", "ctid", "tableoid", "xmin", "xmax")

RENAMED_SUFFIX = "__hydra_renamed"

# Postgres' limit for identifiers (columns, indexes). It is fixed at compile time and
# 63 everywhere but on a custom build, so it is not configurable.
PG_MAX_IDENTIFIER_BYTES = 63

# the suffixes hydra appends to build a PostgreSQL name: a source column already ending
# with one of them is renamed as well, which is what makes collisions impossible below
_GENERATED_SUFFIX = re.compile(rf"({RENAMED_SUFFIX}|__col\d+)$")


def truncate_utf8(value: str, max_bytes: int) -> str:
    """Cut a string to `max_bytes` UTF-8 bytes, on a character boundary."""
    # errors="ignore" drops the incomplete multi-byte sequence left by the cut
    return value.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")


def build_db_columns_mapping(columns: Iterable[str]) -> dict[str, str]:
    """Map every source column name to its actual PostgreSQL column name.

    The order of `columns` is significant: it gives each column the positional index used
    to disambiguate truncated names, so it must be the order of the columns in the table.

    A source name is kept as-is only if it fits in Postgres' identifier limit and doesn't
    already look like a name hydra generates; otherwise it is truncated and suffixed with
    `__col{position}`. Kept names therefore never end with a generated suffix, and
    generated ones differ by their position: two columns can never end up with the same
    PostgreSQL name.
    """
    sources: list[str] = list(columns)
    names: list[str] = []
    for position, col in enumerate(sources):
        # rename reserved columns first: the limit applies to the renamed name, which is longer
        base = f"{col}{RENAMED_SUFFIX}" if col.lower() in RESERVED_COLS else col
        fits = len(base.encode("utf-8")) <= PG_MAX_IDENTIFIER_BYTES
        if fits and not _GENERATED_SUFFIX.search(col):
            names.append(base)
            continue
        suffix = f"__col{position}"
        names.append(truncate_utf8(base, PG_MAX_IDENTIFIER_BYTES - len(suffix)) + suffix)
    return dict(zip(sources, names))


def db_col_name(col: str, mapping: dict[str, str]) -> str:
    """Resolve a source column name through a published (partial) mapping.

    An inspection stored before `columns_mapping` existed has no mapping at all, yet its
    reserved columns were already renamed: falling back to the source name would make
    `SELECT "xmin"` return the system column instead of failing.
    """
    if col in mapping:
        return mapping[col]
    return f"{col}{RENAMED_SUFFIX}" if col.lower() in RESERVED_COLS else col


def convert_dict_values_to_json(data: dict) -> dict:
    """
    Convert values in dict that are dict to json for DB serialization
    TODO: this is suboptimal from asyncpg, dig into this
    https://magicstack.github.io/asyncpg/current/usage.html#example-automatic-json-conversion
    """
    return {k: json.dumps(v) if type(v) is dict else v for k, v in data.items()}


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
    data = convert_dict_values_to_json(data)
    q = compute_update_query(table_name, data)
    pool = await context.pool()
    return await pool.fetchrow(q, *data.values(), record_id)
