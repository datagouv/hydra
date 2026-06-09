from datetime import datetime, timedelta, timezone

from asyncpg import Record

from udata_hydra import config, context

# Idle catalog resources have status = '{}'::jsonb (no entry in this mapping).
JOB_STATUSES: dict[str, set[str]] = {
    "crawler": {
        "CRAWLING_URL",  # resource URL currently being crawled
        "BACKOFF",  # backoff period for this domain, will be checked later
        "TO_ANALYSE_RESOURCE",  # resource to be processed for change, type and size analysis
        "ANALYSING_RESOURCE_HEAD",  # currently checking for change, type and size from headers
        "DOWNLOADING_RESOURCE",  # currently being downloaded
        "ANALYSING_DOWNLOADED_RESOURCE",  # currently checking change, type and size from downloaded file
    },
    "csv": {
        "TO_ANALYSE_CSV",  # resource content to be analysed by CSV detective
        "ANALYSING_CSV",  # resource content currently being analysed by CSV detective
        "VALIDATING_CSV",  # resource content being validated using the previous analysis
        "INSERTING_IN_DB",  # currently being inserted in DB
    },
    "parquet": {
        "TO_ANALYSE_PARQUET",  # parquet resource content to be analysed
        "ANALYSING_PARQUET",  # retrieving parquet column metadata
        "CONVERTING_TO_PARQUET",  # currently being converted to Parquet
    },
    "geojson": {
        "TO_ANALYSE_GEOJSON",  # geojson resource content to be analysed
        "ANALYSING_GEOJSON",  # geojson resource content currently being analysed
        "CONVERTING_TO_GEOJSON",  # csv is currently being converted to geojson
    },
    "pmtiles": {
        "CONVERTING_TO_PMTILES",  # currently being converted to PMTiles
    },
    "ogc": {
        "TO_ANALYSE_OGC",  # OGC service to be analysed
        "ANALYSING_OGC",  # retrieving OGC service metadata
    },
}

CRAWLABLE_STATUS_CLAUSE = """(
    catalog.status = '{}'::jsonb
    OR (
        (catalog.status - 'crawler') = '{}'::jsonb
        AND catalog.status->'crawler'->>'state' = 'BACKOFF'
    )
)"""


class Resource:
    """Represents a resource in the "catalog" DB table"""

    JOB_STATUSES = JOB_STATUSES

    @classmethod
    def job_for_state(cls, state: str) -> str:
        for job, states in cls.JOB_STATUSES.items():
            if state in states:
                return job
        raise ValueError(f"Unknown status state: {state!r}")

    @classmethod
    def _validate_job_status(cls, job: str, state: str) -> None:
        if job not in cls.JOB_STATUSES:
            raise ValueError(f"Invalid job: {job}")
        if state not in cls.JOB_STATUSES[job]:
            raise ValueError(f"Invalid status {state!r} for job {job!r}")

    @classmethod
    async def get(cls, resource_id: str, column_name: str = "*") -> Record | None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""SELECT {column_name} FROM catalog WHERE resource_id = '{resource_id}';"""
            return await connection.fetchrow(q)

    @classmethod
    async def set_job_status(cls, resource_id: str, job: str, state: str) -> Record | None:
        """Atomically set one job status entry via jsonb_set."""
        cls._validate_job_status(job, state)
        now = datetime.now(timezone.utc)
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                UPDATE catalog
                SET status = jsonb_set(
                    status,
                    ARRAY[$2::text],
                    jsonb_build_object('state', $3::text, 'since', $4::timestamptz),
                    true
                )
                WHERE resource_id = $1
                RETURNING *;
            """
            return await connection.fetchrow(q, resource_id, job, state, now)

    @classmethod
    async def clear_job_status(cls, resource_id: str, job: str) -> Record | None:
        """Atomically remove one job status entry."""
        if job not in cls.JOB_STATUSES:
            raise ValueError(f"Invalid job: {job}")
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                UPDATE catalog
                SET status = status - $2::text
                WHERE resource_id = $1
                RETURNING *;
            """
            return await connection.fetchrow(q, resource_id, job)

    @classmethod
    async def update_job_status(
        cls, resource_id: str, from_job: str, to_job: str, state: str
    ) -> Record | None:
        """Atomically clear one job status entry and set another in a single UPDATE."""
        if from_job not in cls.JOB_STATUSES:
            raise ValueError(f"Invalid job: {from_job}")
        cls._validate_job_status(to_job, state)
        now = datetime.now(timezone.utc)
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                UPDATE catalog
                SET status = jsonb_set(
                    status - $2::text,
                    ARRAY[$3::text],
                    jsonb_build_object('state', $4::text, 'since', $5::timestamptz),
                    true
                )
                WHERE resource_id = $1
                RETURNING *;
            """
            return await connection.fetchrow(q, resource_id, from_job, to_job, state, now)

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
                    INSERT INTO catalog (dataset_id, resource_id, url, type, format, deleted, status, priority, title)
                    VALUES ($1, $2, $3, $4, $5, FALSE, '{}'::jsonb, $6, $7)
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
                        INSERT INTO catalog (dataset_id, resource_id, url, type, format, deleted, status, priority, title)
                        VALUES ($1, $2, $3, $4, $5, FALSE, '{}'::jsonb, $6, $7)
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
                # Mark resource as deleted in catalog table
                q = f"""UPDATE catalog SET deleted = TRUE WHERE resource_id = '{resource_id}';"""
            await connection.execute(q)

    @classmethod
    def get_excluded_clause(cls) -> str:
        """Return the WHERE clause to get only resources from the checks which:
        - don't have a URL in the excluded URLs patterns
        - are not deleted
        - are not currently being crawled or analysed (i.e. idle resources, or only crawler=BACKOFF)
        """
        return " AND ".join(
            [f"catalog.url NOT LIKE '{p}'" for p in config.EXCLUDED_PATTERNS]
            + [
                "catalog.deleted = False",
                CRAWLABLE_STATUS_CLAUSE,
            ]
        )

    @staticmethod
    async def clean_up_statuses() -> int:
        """Some resources end up being stuck in a status forever, we want to get them back on track.
        Clear stuck per-job status entries based on their since timestamp.
        This returns the number of such stuck resources that were cleaned up."""
        threshold = datetime.now(timezone.utc) - timedelta(seconds=config.STUCK_THRESHOLD_SECONDS)

        pool = await context.pool()
        async with pool.acquire() as connection:
            # Update all stuck resources in a single query
            q = """
                UPDATE catalog
                SET status = status - stuck.job
                FROM (
                    SELECT ca.resource_id, t.key AS job
                    FROM catalog ca
                    JOIN checks c ON c.id = ca.last_check
                    CROSS JOIN LATERAL jsonb_each(ca.status) AS t(key, entry)
                    WHERE c.created_at < $1
                      AND (entry->>'since')::timestamptz < $1
                ) stuck
                WHERE catalog.resource_id = stuck.resource_id
            """
            result = await connection.execute(q, threshold)
            return int(result.split()[-1]) if result else 0  # Returns the number of affected rows
