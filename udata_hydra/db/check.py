from datetime import date

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
    async def get_by_url(cls, url: str) -> list[Record]:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                SELECT * FROM checks
                WHERE url = $1
                ORDER BY created_at DESC
            """
            return await connection.fetch(q, url)

    @classmethod
    async def get_latest(
        cls, url: str | None = None, resource_id: str | None = None
    ) -> Record | None:
        column: str = "url" if url else "resource_id"
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
            SELECT catalog.id as catalog_id, checks.id as check_id,
                catalog.status as catalog_status, checks.status as check_status, checks.next_check_at as next_check_at, catalog.deleted as deleted, *
            FROM checks, catalog
            WHERE catalog.{column} = $1
            AND checks.id = catalog.last_check
            """
            return await connection.fetchrow(q, url or resource_id)

    @classmethod
    async def get_all(cls, url: str | None = None, resource_id: str | None = None) -> list[Record]:
        column: str = "url" if url else "resource_id"
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
            SELECT catalog.id as catalog_id, checks.id as check_id,
                catalog.status as catalog_status, checks.status as check_status, checks.next_check_at as next_check_at, catalog.deleted as deleted, *
            FROM checks, catalog
            WHERE catalog.{column} = $1
            AND catalog.{column} = checks.{column}
            ORDER BY created_at DESC
            """
            return await connection.fetch(q, url or resource_id)

    @classmethod
    async def get_group_by_for_date(
        cls, column: str, date: date, page_size: int = 20
    ) -> list[Record]:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
            SELECT {column} as value, count(*) as count
            FROM checks
            WHERE created_at::date = $1
            GROUP BY {column}
            ORDER BY count desc
            LIMIT $2
            """
            return await connection.fetch(q, date, page_size)

    @classmethod
    async def insert(cls, data: dict, returning: str = "id") -> dict:
        """
        Insert a new check in DB, associate it with the resource and return the check dict, optionally associated with the resource dataset_id.
        This uses the info from the last check of the same resource.

        Note: Returns dict instead of Record because this method performs additional operations beyond simple insertion (joins with catalog table, adds dataset_id).
        """
        json_data = convert_dict_values_to_json(data)
        q1: str = compute_insert_query(table_name="checks", data=json_data, returning=returning)
        pool = await context.pool()
        async with pool.acquire() as connection:
            last_check: Record = await connection.fetchrow(q1, *json_data.values())
            last_check_dict = dict(last_check)
            q2 = (
                """UPDATE catalog SET last_check = $1 WHERE resource_id = $2 RETURNING dataset_id"""
            )
            updated_resource: Record | None = await connection.fetchrow(
                q2, last_check["id"], json_data["resource_id"]
            )
            # Add the dataset_id arg to the check response, if we can, and if it's asked
            if returning in ["*", "dataset_id"] and updated_resource:
                last_check_dict["dataset_id"] = updated_resource["dataset_id"]
            return last_check_dict

    @classmethod
    async def update(cls, check_id: int, data: dict) -> Record | None:
        """Update a check in DB with new data and return the check id in DB"""
        return await update_table_record(table_name="checks", record_id=check_id, data=data)

    @classmethod
    async def delete(cls, check_id: int) -> int:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """DELETE FROM checks WHERE id = $1"""
            return await connection.fetch(q, check_id)
