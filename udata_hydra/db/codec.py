import json

import asyncpg


async def init_connection(connection: asyncpg.Connection) -> None:
    """Decode JSONB columns as Python dicts/lists in asyncpg records."""
    await connection.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
