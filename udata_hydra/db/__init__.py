import json
from collections import Counter
from typing import Iterable

from asyncpg import Record

from udata_hydra import config, context

# PostgreSQL system columns and hydra's own __id that must be renamed when
# a user CSV happens to use them as headers.  Shared across csv, parquet and
# geojson modules.
RESERVED_COLS = ("__id", "cmin", "cmax", "collation", "ctid", "tableoid", "xmin", "xmax")

# Postgres' hard limit for index names. Kept separate from config.NAMEDATALEN, which tests
# lower to 10 to build long column names cheaply: applied to index names, which already
# embed a 32-character md5, such a value would make every index collide.
PG_MAX_IDENTIFIER_BYTES = 63


def truncate_utf8(value: str, max_bytes: int) -> str:
    """Cut a string to `max_bytes` UTF-8 bytes, on a character boundary."""
    # errors="ignore" drops the incomplete multi-byte sequence left by the cut
    return value.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")


def build_db_columns_mapping(columns: Iterable[str]) -> dict[str, str]:
    """Map every source column name to its actual PostgreSQL column name.

    The order of `columns` is significant: it gives each column the positional index used
    to disambiguate truncated names, so it must be the order of the columns in the table.

    Names that don't fit in Postgres' identifier limit are truncated and suffixed with
    `__col{position}`. That suffix also guarantees a truncated name can never collide with
    a reserved column name.
    """
    limit: int = config.NAMEDATALEN - 1
    sources: list[str] = list(columns)
    # rename reserved columns first: the limit applies to the renamed name, which is longer
    bases: list[str] = [
        f"{col}__hydra_renamed" if col.lower() in RESERVED_COLS else col for col in sources
    ]

    def fit(index: int, force_suffix: bool) -> str:
        if not force_suffix and len(bases[index].encode("utf-8")) <= limit:
            return bases[index]
        suffix = f"__col{index}"
        budget = limit - len(suffix)
        if budget < 1:
            raise ValueError(f"NAMEDATALEN={config.NAMEDATALEN} is too small to name columns")
        return truncate_utf8(bases[index], budget) + suffix

    names: list[str] = [fit(i, force_suffix=False) for i in range(len(sources))]

    # a short column may be named exactly like the truncated form of a longer one: forcing
    # the suffixed form on every member of a colliding group makes them unique, since the
    # positional suffixes differ. Each round forces at least one more index, so this ends.
    forced: set[int] = set()
    while True:
        counts = Counter(names)
        colliding = {i for i, name in enumerate(names) if counts[name] > 1 and i not in forced}
        if not colliding:
            break
        forced |= colliding
        for i in colliding:
            names[i] = fit(i, force_suffix=True)

    if len(set(names)) != len(names):
        raise ValueError("Could not build unique PostgreSQL column names")

    return dict(zip(sources, names))


def db_col_name(col: str, mapping: dict[str, str]) -> str:
    """Resolve a source column name through a published (partial) mapping."""
    return mapping.get(col, col)


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
