from asyncpg import Record

from udata_hydra import context
from udata_hydra.db import (
    compute_insert_query,
    convert_dict_values_to_json,
    update_table_record,
)


class Check:
    """Represents a check in the "checks" DB table"""

    @classmethod
    async def get_by_id(cls, check_id: int, with_deleted: bool = False) -> Record | None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                SELECT * FROM catalog JOIN checks
                ON catalog.last_check = checks.id
                WHERE checks.id = $1
            """
            if not with_deleted:
                q += " AND catalog.deleted = FALSE"
            return await connection.fetchrow(q, check_id)

    @classmethod
    async def get_by_resource_id(
        cls, resource_id: str, with_deleted: bool = False
    ) -> Record | None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                SELECT * FROM catalog JOIN checks
                ON catalog.last_check = checks.id
                WHERE catalog.resource_id = $1
            """
            if not with_deleted:
                q += " AND catalog.deleted = FALSE"
            return await connection.fetchrow(q, resource_id)

    @classmethod
    async def get_latest(
        cls, url: str | None = None, resource_id: str | None = None
    ) -> Record | None:
        column: str = "url" if url else "resource_id"
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
            SELECT catalog.id as catalog_id, checks.id as check_id,
                catalog.status as catalog_status, checks.status as check_status, catalog.deleted as deleted, *
            FROM checks, catalog
            WHERE checks.id = catalog.last_check
            AND catalog.{column} = $1
            """
            return await connection.fetchrow(q, url or resource_id)

    @classmethod
    async def get_all(cls, url: str | None = None, resource_id: str | None = None) -> list | None:
        column: str = "url" if url else "resource_id"
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
            SELECT catalog.id as catalog_id, checks.id as check_id,
                catalog.status as catalog_status, checks.status as check_status, catalog.deleted as deleted, *
            FROM checks, catalog
            WHERE catalog.{column} = $1
            AND catalog.url = checks.url
            ORDER BY created_at DESC
            """
            return await connection.fetch(q, url or resource_id)

    @classmethod
    async def insert(cls, data: dict) -> Record:
        """
        Insert a new check in DB and return the check record in DB
        This use the info from the last check of the same resource
        """
        data = convert_dict_values_to_json(data)
        q1: str = compute_insert_query(table_name="checks", data=data)
        pool = await context.pool()
        async with pool.acquire() as connection:
            last_check = await connection.fetchrow(q1, *data.values())
            q2 = """UPDATE catalog SET last_check = $1 WHERE resource_id = $2"""
            await connection.execute(q2, last_check["id"], data["resource_id"])
            return last_check

    @classmethod
    async def update(cls, check_id: int, data: dict) -> int:
        """Update a check in DB with new data and return the check id in DB"""
        return await update_table_record(table_name="checks", record_id=check_id, data=data)

    @classmethod
    async def delete(cls, check_id: int) -> int:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """DELETE FROM checks WHERE id = $1"""
            return await connection.fetch(q, check_id)
