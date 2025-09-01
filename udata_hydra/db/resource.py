from datetime import datetime, timedelta, timezone

from asyncpg import Record

from udata_hydra import config, context


class Resource:
    """Represents a resource in the "catalog" DB table"""

    STATUSES = {
        None: "no status, waiting",
        "BACKOFF": "backoff period for this domain, will be checked later",
        "CRAWLING_URL": "resource URL currently being crawled",
        "TO_ANALYSE_RESOURCE": "resource to be processed for change, type and size analysis",
        "ANALYSING_RESOURCE_HEAD": "currently checking for change, type and size from headers",
        "DOWNLOADING_RESOURCE": "currently being downloaded",
        "ANALYSING_DOWNLOADED_RESOURCE": "currently checking for change, type and size from downloaded file",
        "TO_ANALYSE_CSV": "resource content to be analysed by CSV detective",
        "ANALYSING_CSV": "resource content currently being analysed by CSV detective",
        "VALIDATING_CSV": "resource content being validated using the previous analysis",
        "INSERTING_IN_DB": "currently being inserted in DB",
        "CONVERTING_TO_PARQUET": "currently being converted to Parquet",
        "TO_ANALYSE_GEOJSON": "geojson resource content to be analysed",
        "ANALYSING_GEOJSON": "geojson resource content currently being analysed",
        "CONVERTING_TO_PMTILES": "currently being converted to pmtiles",
        "CONVERTING_TO_GEOJSON": "csv is currently being converted to geojson",
    }

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
        status: str | None = None,
        priority: bool = True,
    ) -> Record | None:
        if status and status not in cls.STATUSES.keys():
            raise ValueError(f"Invalid status: {status}")

        pool = await context.pool()
        async with pool.acquire() as connection:
            # Insert new resource in catalog table and mark as high priority for crawling
            q = """
                    INSERT INTO catalog (dataset_id, resource_id, url, type, format, deleted, status, priority)
                    VALUES ($1, $2, $3, $4, $5, FALSE, $6, $7)
                    ON CONFLICT (resource_id) DO UPDATE SET
                        dataset_id = $1,
                        url = $3,
                        type = $4,
                        format = $5,
                        deleted = FALSE,
                        status = $6,
                        priority = $7
                    RETURNING *;"""
            return await connection.fetchrow(
                q, dataset_id, resource_id, url, type, format, status, priority
            )

    @classmethod
    async def update(cls, resource_id: str, data: dict) -> Record | None:
        """Update a resource in DB with new data and return the updated resource in DB"""
        if "status" in data:
            data["status_since"] = datetime.now(timezone.utc)
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
        status: str | None = None,
        priority: bool = True,  # Make resource high priority by default for crawling
    ) -> Record | None:
        if status and status not in cls.STATUSES.keys():
            raise ValueError(f"Invalid status: {status}")

        pool = await context.pool()
        async with pool.acquire() as connection:
            # Check if resource is in catalog then insert or update into table
            if await Resource.get(resource_id):
                q = """
                        UPDATE catalog
                        SET dataset_id = $1, url = $3, type = $4, format=$5, status = $6, priority = $7
                        WHERE resource_id = $2
                        RETURNING *;"""
            else:
                q = """
                        INSERT INTO catalog (dataset_id, resource_id, url, type, format, deleted, status, priority)
                        VALUES ($1, $2, $3, $4, $5, FALSE, $6, $7)
                        ON CONFLICT (resource_id) DO UPDATE SET
                            dataset_id = $1,
                            url = $3,
                            type = $4,
                            format = $5,
                            deleted = FALSE,
                            status = $6,
                            priority = $7
                        RETURNING *;"""
            return await connection.fetchrow(
                q, dataset_id, resource_id, url, type, format, status, priority
            )

    @classmethod
    async def delete(
        cls,
        resource_id: str,
    ) -> None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            # Mark resource as deleted in catalog table
            q = f"""UPDATE catalog SET deleted = TRUE WHERE resource_id = '{resource_id}';"""
            await connection.execute(q)

    @staticmethod
    def get_excluded_clause() -> str:
        """Return the WHERE clause to get only resources from the check which:
        - don't have a URL in the excluded URLs patterns
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

    @staticmethod
    async def get_stuck_resources() -> list[str]:
        """Some resources end up being stuck in a not null status forever,
        we want to get them back on track.
        This returns all resource ids of such stuck resources.
        """
        threshold = (
            datetime.now(timezone.utc) - timedelta(seconds=config.STUCK_THRESHOLD_SECONDS)
        ).strftime("%Y-%m-%d %H:%M:%S")
        q = f"""SELECT ca.resource_id
            FROM checks c
            JOIN catalog ca
            ON c.id = ca.last_check
            WHERE ca.status IS NOT NULL AND c.created_at < '{threshold}';"""
        pool = await context.pool()
        async with pool.acquire() as connection:
            rows = await connection.fetch(q)
        return [str(r["resource_id"]) for r in rows] if rows else []

    @classmethod
    async def clean_up_statuses(cls):
        stuck_resources: list[str] = await cls.get_stuck_resources()
        for rid in stuck_resources:
            await cls.update(rid, {"status": None})
