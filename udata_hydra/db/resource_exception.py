import json

from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.db.resource import Resource


class ResourceException:
    """Represents a row in the "resources_exceptions" DB table
    Resources that are too large to be processed normally but that we want to have anyway"""

    @classmethod
    async def get_all(cls) -> list[Record]:
        """
        Get all resource_exceptions
        """
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = "SELECT * FROM resources_exceptions;"
            return await connection.fetch(q)

    @classmethod
    async def get_by_resource_id(cls, resource_id: str) -> Record | None:
        """
        Get a resource_exception by its resource_id
        """
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = "SELECT * FROM resources_exceptions WHERE resource_id = $1;"
            return await connection.fetchrow(q, resource_id)

    @classmethod
    async def insert(cls, resource_id: str, table_indexes: dict[str, str] | None = {}) -> Record:
        """
        Insert a new resource_exception
        table_indexes is a JSON object of column names and index types
        e.g. {"siren": "unique", "code_postal": "index"}
        """
        pool = await context.pool()

        # First, check if the resource_id exists in the catalog table
        resource: dict | None = await Resource.get(resource_id)
        if not resource:
            raise ValueError("Resource not found")

        if table_indexes:
            for index_type in table_indexes.values():
                if index_type not in config.SQL_INDEXES_TYPES_SUPPORTED:
                    raise ValueError(
                        "Index type must be one of: "
                        + ", ".join(config.SQL_INDEXES_TYPES_SUPPORTED)
                    )

            async with pool.acquire() as connection:
                q = f"""
                    INSERT INTO resources_exceptions (resource_id, table_indexes)
                    VALUES ('{resource_id}', '{json.dumps(table_indexes)}')
                    RETURNING *;
                """
                return await connection.fetchrow(q)

        else:
            async with pool.acquire() as connection:
                q = f"""
                    INSERT INTO resources_exceptions (resource_id)
                    VALUES ('{resource_id}')
                    RETURNING *;
                """
                return await connection.fetchrow(q)

    @classmethod
    async def delete(cls, resource_id: str) -> None:
        """
        Delete a resource_exception by its resource_id
        """
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = "DELETE FROM resources_exceptions WHERE resource_id = $1;"
            await connection.execute(q, resource_id)
