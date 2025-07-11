import json

from asyncpg import Record

from udata_hydra import context
from udata_hydra.db.resource import Resource
from udata_hydra.schemas import ResourceExceptionSchema


class ResourceException:
    """Represents a row in the "resources_exceptions" DB table
    Resources that are too large to be processed normally but that we want to have anyway"""

    @classmethod
    async def get_all(cls) -> list[Record]:
        """
        Get all resource_exceptions
        """
        pool = await context.pool("csv")
        async with pool.acquire() as connection:
            q = "SELECT * FROM resources_exceptions;"
            return await connection.fetch(q)

    @classmethod
    async def get_by_resource_id(cls, resource_id: str) -> Record | None:
        """
        Get a resource_exception by its resource_id
        """
        pool = await context.pool("csv")
        async with pool.acquire() as connection:
            q = "SELECT * FROM resources_exceptions WHERE resource_id = $1;"
            return await connection.fetchrow(q, resource_id)

    @classmethod
    async def insert(
        cls,
        resource_id: str,
        table_indexes: dict[str, str] | None = None,
        comment: str | None = None,
    ) -> Record | None:
        """
        Insert a new resource_exception
        table_indexes is a JSON object of column names and index types
        e.g. {"siren": "unique", "code_postal": "index"}
        """
        pool = await context.pool("csv")

        # First, check if the resource_id exists in the catalog table
        resource: Record | None = await Resource.get(resource_id)
        if not resource:
            raise ValueError("Resource not found")

        if table_indexes is None:
            table_indexes = {}
        else:
            valid, error = ResourceExceptionSchema.are_table_indexes_valid(table_indexes)
            if not valid:
                raise ValueError(error)

        async with pool.acquire() as connection:
            q = """
                INSERT INTO resources_exceptions (resource_id, table_indexes, comment)
                VALUES ($1, $2, $3)
                RETURNING *;
            """
            return await connection.fetchrow(q, resource_id, json.dumps(table_indexes), comment)

    @classmethod
    async def update(
        cls,
        resource_id: str,
        table_indexes: dict[str, str] | None = None,
        comment: str | None = None,
    ) -> Record | None:
        """
        Update a resource_exception
        table_indexes is a JSON object of column names and index types
        e.g. {"siren": "unique", "code_postal": "index"}
        """
        pool = await context.pool("csv")

        # First, check if the resource_id exists in the catalog table
        resource: Record | None = await Resource.get(resource_id)
        if not resource:
            raise ValueError("Resource not found")

        if table_indexes is None:
            table_indexes = {}
        else:
            valid, error = ResourceExceptionSchema.are_table_indexes_valid(table_indexes)
            if not valid:
                raise ValueError(error)

        async with pool.acquire() as connection:
            q = """
                UPDATE resources_exceptions
                SET table_indexes = $2, comment = $3
                WHERE resource_id = $1
                RETURNING *;
            """
            return await connection.fetchrow(q, resource_id, json.dumps(table_indexes), comment)

    @classmethod
    async def delete(cls, resource_id: str) -> None:
        """
        Delete a resource_exception by its resource_id
        """
        pool = await context.pool("csv")
        async with pool.acquire() as connection:
            q = "DELETE FROM resources_exceptions WHERE resource_id = $1;"
            await connection.execute(q, resource_id)
