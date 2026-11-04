from udata_hydra import context


class ResourceIndex:
    """Represents the rows of the "resources_indexes" DB table for a resource:
    the indexes created on its table at each conversion, whether the resource is an exception or not"""

    @classmethod
    async def get_by_resource_id(cls, resource_id: str) -> dict[str, str]:
        """Get the indexes of a resource as a {column_name: index_type} mapping"""
        pool = await context.pool("csv")
        async with pool.acquire() as connection:
            q = """
                SELECT column_name, index_type FROM resources_indexes
                WHERE resource_id = $1 ORDER BY column_name;
            """
            rows = await connection.fetch(q, resource_id)
        return {r["column_name"]: r["index_type"] for r in rows}

    @classmethod
    async def replace(cls, resource_id: str, indexes: dict[str, str]) -> None:
        """Replace all the indexes of a resource, an empty mapping removes them"""
        pool = await context.pool("csv")
        async with pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "DELETE FROM resources_indexes WHERE resource_id = $1;", resource_id
                )
                await connection.executemany(
                    """
                        INSERT INTO resources_indexes (resource_id, column_name, index_type)
                        VALUES ($1, $2, $3);
                    """,
                    [(resource_id, column, index_type) for column, index_type in indexes.items()],
                )
