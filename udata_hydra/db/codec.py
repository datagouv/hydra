import json
from typing import Any

import asyncpg


async def init_connection(connection: asyncpg.Connection) -> None:
    """Decode JSONB columns as Python dicts/lists in asyncpg records."""
    await connection.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


def parse_json_value(value: Any, default: Any = None) -> Any:
    """Parse a JSON/JSONB column value that may already be decoded by asyncpg."""
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return value
