import asyncpg

from udata_hydra import config


def get_excluded_clause() -> str:
    """Return the WHERE clause to get only resources from the check which:
    - have a URL in the excluded URLs patterns
    - are not deleted
    - are not currently being crawled or analysed (i.e. resources with no status, or status 'BACKOFF')
    """
    return " AND ".join(
        [f"catalog.url NOT LIKE '{p}'" for p in config.EXCLUDED_PATTERNS]
        + [
            "catalog.deleted = False",
            "(catalog.status IS NULL OR catalog.status = 'BACKOFF')",
        ]
    )


async def select_rows_based_on_query(connection, q: str, *args) -> list[asyncpg.Record]:
    """
    A transaction wrapper around a select query q pass as param with *args.
    It first creates a temporary table based on this query,
    then fetches the selected rows and drops the temporary table.
    It finally returns the selected rows.
    """
    temporary_table = "check_urls"
    create_temp_select_table_query = (
        f"""CREATE TEMPORARY TABLE {temporary_table} AS {q} FOR UPDATE;"""
    )
    async with connection.transaction():
        await connection.execute("BEGIN;")
        await connection.execute(create_temp_select_table_query, *args)
        to_check = await connection.fetch(f"SELECT * FROM {temporary_table};")
        await connection.execute("COMMIT;")
    await connection.execute(f"DROP TABLE {temporary_table};")
    return to_check
