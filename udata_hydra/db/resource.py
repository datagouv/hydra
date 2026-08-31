from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.db.resource_job_status import CRAWLABLE_CLAUSE, ResourceJobStatus


class Resource:
    """Represents a resource in the "catalog" DB table"""

    @classmethod
    async def get(cls, resource_id: str, column_name: str = "*") -> Record | None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""SELECT {column_name} FROM catalog WHERE resource_id = '{resource_id}';"""
            return await connection.fetchrow(q)

    @classmethod
    async def insert(
        cls,
        dataset_id: str,
        resource_id: str,
        url: str,
        type: str,
        format: str,
        title: str,
        priority: bool = True,
    ) -> Record | None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            # Insert new resource in catalog table and mark as high priority for crawling
            q = """
                    INSERT INTO catalog (dataset_id, resource_id, url, type, format, deleted, priority, title)
                    VALUES ($1, $2, $3, $4, $5, FALSE, $6, $7)
                    ON CONFLICT (resource_id) DO UPDATE SET
                        dataset_id = $1,
                        url = $3,
                        type = $4,
                        format = $5,
                        deleted = FALSE,
                        priority = $6,
                        title = $7
                    RETURNING *;"""
            return await connection.fetchrow(
                q, dataset_id, resource_id, url, type, format, priority, title
            )

    @classmethod
    async def update(cls, resource_id: str, data: dict) -> Record | None:
        """Update a resource in DB with new data and return the updated resource in DB"""
        columns = data.keys()
        # $1, $2...
        placeholders = [f"${x + 1}" for x in range(len(data.values()))]
        set_clause = ",".join([f"{c} = {v}" for c, v in zip(columns, placeholders)])
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
                    UPDATE catalog
                    SET {set_clause}
                    WHERE resource_id = ${len(placeholders) + 1}
                    RETURNING *;"""
            return await connection.fetchrow(q, *data.values(), resource_id)

    @classmethod
    async def update_or_insert(
        cls,
        dataset_id: str,
        resource_id: str,
        url: str,
        type: str,
        format: str,
        title: str,
        priority: bool = True,  # Make resource high priority by default for crawling
    ) -> Record | None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            # Check if resource is in catalog then insert or update into table
            if await Resource.get(resource_id):
                q = """
                        UPDATE catalog
                        SET dataset_id = $1, url = $3, type = $4, format=$5, priority = $6, title = $7
                        WHERE resource_id = $2
                        RETURNING *;"""
            else:
                q = """
                        INSERT INTO catalog (dataset_id, resource_id, url, type, format, deleted, priority, title)
                        VALUES ($1, $2, $3, $4, $5, FALSE, $6, $7)
                        ON CONFLICT (resource_id) DO UPDATE SET
                            dataset_id = $1,
                            url = $3,
                            type = $4,
                            format = $5,
                            deleted = FALSE,
                            priority = $6,
                            title = $7
                        RETURNING *;"""
            return await connection.fetchrow(
                q, dataset_id, resource_id, url, type, format, priority, title
            )

    @classmethod
    async def delete(
        cls,
        resource_id: str,
        hard_delete: bool = False,
    ) -> None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            if hard_delete:
                q = f"""DELETE FROM catalog WHERE resource_id = '{resource_id}';"""
                await connection.execute(q)
            else:
                # Clear in-progress jobs so a deleted resource is not counted as active.
                async with connection.transaction():
                    await ResourceJobStatus.clear_all(resource_id, connection)
                    q = f"""UPDATE catalog SET deleted = TRUE WHERE resource_id = '{resource_id}';"""
                    await connection.execute(q)

    @staticmethod
    def get_excluded_clause() -> str:
        """Return the WHERE clause to get only resources from the checks which:
        - don't have a URL in the excluded URLs patterns
        - are not deleted
        - are not currently being crawled or analysed (i.e. idle, or only crawler=BACKOFF)
        """
        return " AND ".join(
            [f"catalog.url NOT LIKE '{p}'" for p in config.EXCLUDED_PATTERNS]
            + [
                "catalog.deleted = False",
                CRAWLABLE_CLAUSE,
            ]
        )

    @staticmethod
    async def clean_up_statuses() -> int:
        """Delete stuck per-job status rows based on their since timestamp.
        Returns the number of status rows that were cleaned up."""
        return await ResourceJobStatus.clean_stuck()
