from udata_hydra import context
from udata_hydra.db.resource import Resource


class ResourceException:
    """Represents a row in the "resources_exceptions" DB table
    Resources that are too large to be processed normally but that we want to have anyway"""

    @classmethod
    async def insert(cls, resource_id: str, indexes: list[str]) -> dict:
        """
        Insert a new resource_exception in the DB
        """
        pool = await context.pool()

        # First, check if the resource_id exists in the catalog table
        resource: dict | None = await Resource.get(resource_id)
        if not resource:
            raise ValueError("Resource not found")

        async with pool.acquire() as connection:
            q = f"""
                INSERT INTO resources_exceptions (resource_id, indexes)
                VALUES ('{resource_id}', '{indexes}')
                RETURNING *;
            """
            return await connection.fetchrow(q)
