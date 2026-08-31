from datetime import datetime, timedelta, timezone

from asyncpg import Record

from udata_hydra import config, context

# Idle catalog resources have no row in this table.
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
        "INSERTING_IN_DB",  # currently being inserted in DB
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

# Resource is crawlable when it has no status row, or only crawler=BACKOFF.
CRAWLABLE_CLAUSE = """NOT EXISTS (
    SELECT 1 FROM resource_job_status s
    WHERE s.resource_id = catalog.resource_id
      AND NOT (s.job = 'crawler' AND s.state = 'BACKOFF')
)"""


def _affected_row_count(result: str) -> int:
    return int(result.split()[-1]) if result else 0


class ResourceJobStatus:
    """Represents a row in the resource_job_status table (one active job per resource)."""

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
    async def set(cls, resource_id: str, job: str, state: str) -> Record | None:
        """Insert or overwrite one job status row."""
        cls._validate_job_status(job, state)
        now = datetime.now(timezone.utc)
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                INSERT INTO resource_job_status (resource_id, job, state, since)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (resource_id, job) DO UPDATE
                SET state = EXCLUDED.state, since = EXCLUDED.since
                RETURNING *;
            """
            return await connection.fetchrow(q, resource_id, job, state, now)

    @classmethod
    async def clear(cls, resource_id: str, job: str) -> None:
        """Remove one job status row."""
        if job not in cls.JOB_STATUSES:
            raise ValueError(f"Invalid job: {job}")
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = """
                DELETE FROM resource_job_status
                WHERE resource_id = $1 AND job = $2;
            """
            await connection.execute(q, resource_id, job)

    @classmethod
    async def update(
        cls, resource_id: str, from_job: str, to_job: str, state: str
    ) -> Record | None:
        """Atomically clear one job and set another in a single transaction."""
        if from_job not in cls.JOB_STATUSES:
            raise ValueError(f"Invalid job: {from_job}")
        cls._validate_job_status(to_job, state)
        now = datetime.now(timezone.utc)
        pool = await context.pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    DELETE FROM resource_job_status
                    WHERE resource_id = $1 AND job = $2;
                    """,
                    resource_id,
                    from_job,
                )
                q = """
                    INSERT INTO resource_job_status (resource_id, job, state, since)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (resource_id, job) DO UPDATE
                    SET state = EXCLUDED.state, since = EXCLUDED.since
                    RETURNING *;
                """
                return await connection.fetchrow(q, resource_id, to_job, state, now)

    @classmethod
    async def for_resource(cls, resource_id: str) -> dict[str, dict[str, str | datetime]]:
        """Return {job: {state, since}} for a resource, or {} if idle."""
        pool = await context.pool()
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT job, state, since
                FROM resource_job_status
                WHERE resource_id = $1;
                """,
                resource_id,
            )
        return {
            row["job"]: {
                "state": row["state"],
                "since": row["since"],
            }
            for row in rows
        }

    @classmethod
    async def clear_all(cls, resource_id: str) -> None:
        """Remove every job status row for a resource (soft-delete / idle)."""
        pool = await context.pool()
        async with pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM resource_job_status WHERE resource_id = $1;",
                resource_id,
            )

    @classmethod
    async def clean_stuck(cls) -> int:
        """Delete job status rows whose since timestamp is older than the stuck threshold."""
        threshold = datetime.now(timezone.utc) - timedelta(seconds=config.STUCK_THRESHOLD_SECONDS)
        pool = await context.pool()
        async with pool.acquire() as connection:
            result = await connection.execute(
                "DELETE FROM resource_job_status WHERE since < $1;",
                threshold,
            )
            return _affected_row_count(result)
