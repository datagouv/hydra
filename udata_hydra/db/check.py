from typing import Union

from udata_hydra import context
from udata_hydra.db import (
    compute_insert_query,
    convert_dict_values_to_json,
    update_table_record,
)


class Check:
    """Represents a check in the "checks" DB table"""

    @classmethod
    async def get(cls, check_id: int) -> dict:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                SELECT * FROM catalog JOIN checks
                ON catalog.last_check = checks.id
                WHERE checks.id = $1;
            """
            check = await connection.fetchrow(q, check_id)
        return check

    @classmethod
    async def get_latest(cls, url: Union[str, None], resource_id: Union[str, None]) -> dict | None:
        column: str = "url" if url else "resource_id"
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
            SELECT catalog.id as catalog_id, checks.id as check_id,
                catalog.status as catalog_status, checks.status as check_status, *
            FROM checks, catalog
            WHERE checks.id = catalog.last_check
            AND catalog.{column} = $1
            """
            return await connection.fetchrow(q, url or resource_id)

    @classmethod
    async def get_all(cls, url: Union[str, None], resource_id: Union[str, None]) -> dict | None:
        column: str = "url" if url else "resource_id"
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
            SELECT catalog.id as catalog_id, checks.id as check_id,
                catalog.status as catalog_status, checks.status as check_status, *
            FROM checks, catalog
            WHERE catalog.{column} = $1
            AND catalog.url = checks.url
            ORDER BY created_at DESC
            """
            return await connection.fetch(q, url or resource_id)

    @classmethod
    async def insert(cls, data: dict) -> int:
        """
        Insert a new check in DB and return the check id in DB
        This use the info from the last check of the same resource
        """
        data = convert_dict_values_to_json(data)
        q = compute_insert_query(table_name="checks", data=data)
        pool = await context.pool()
        async with pool.acquire() as connection:
            last_check = await connection.fetchrow(q, *data.values())
            q = """UPDATE catalog SET last_check = $1 WHERE resource_id = $2"""
            await connection.execute(q, last_check["id"], data["resource_id"])
        return last_check["id"]

    @classmethod
    async def update(cls, check_id: int, data: dict) -> int:
        """Update a check in DB with new data and return the check id in DB"""
        return await update_table_record(table_name="checks", record_id=check_id, data=data)
