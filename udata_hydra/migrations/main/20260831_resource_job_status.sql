-- Move catalog.status / status_since into a sparse per-job table.
-- Idle resources have no row. One active job = one row.

CREATE TABLE resource_job_status (
    resource_id UUID NOT NULL REFERENCES catalog(resource_id) ON DELETE CASCADE,
    job         TEXT NOT NULL,
    state       TEXT NOT NULL,
    since       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (resource_id, job)
);

CREATE INDEX resource_job_status_job_state_idx ON resource_job_status (job, state);
CREATE INDEX resource_job_status_since_idx ON resource_job_status (since);

-- Backfill leftover VARCHAR statuses. INSERTING_IN_DB is mapped to csv (the
-- historical single-status column cannot distinguish csv vs parquet).
INSERT INTO resource_job_status (resource_id, job, state, since)
SELECT
    resource_id,
    CASE status
        WHEN 'CRAWLING_URL' THEN 'crawler'
        WHEN 'BACKOFF' THEN 'crawler'
        WHEN 'TO_ANALYSE_RESOURCE' THEN 'crawler'
        WHEN 'ANALYSING_RESOURCE_HEAD' THEN 'crawler'
        WHEN 'DOWNLOADING_RESOURCE' THEN 'crawler'
        WHEN 'ANALYSING_DOWNLOADED_RESOURCE' THEN 'crawler'
        WHEN 'TO_ANALYSE_CSV' THEN 'csv'
        WHEN 'TO_ANALYSE_CSVGZ' THEN 'csv'
        WHEN 'TO_ANALYSE_XLS' THEN 'csv'
        WHEN 'TO_ANALYSE_XLSX' THEN 'csv'
        WHEN 'ANALYSING_CSV' THEN 'csv'
        WHEN 'VALIDATING_CSV' THEN 'csv'
        WHEN 'INSERTING_IN_DB' THEN 'csv'
        WHEN 'CONVERTING_TO_PARQUET' THEN 'parquet'
        WHEN 'TO_ANALYSE_PARQUET' THEN 'parquet'
        WHEN 'ANALYSING_PARQUET' THEN 'parquet'
        WHEN 'TO_ANALYSE_GEOJSON' THEN 'geojson'
        WHEN 'ANALYSING_GEOJSON' THEN 'geojson'
        WHEN 'CONVERTING_TO_GEOJSON' THEN 'geojson'
        WHEN 'CONVERTING_TO_PMTILES' THEN 'pmtiles'
        WHEN 'TO_ANALYSE_WFS' THEN 'ogc'
        WHEN 'ANALYSING_WFS' THEN 'ogc'
        WHEN 'TO_ANALYSE_WMS' THEN 'ogc'
        WHEN 'ANALYSING_WMS' THEN 'ogc'
        WHEN 'TO_ANALYSE_OGC' THEN 'ogc'
        WHEN 'ANALYSING_OGC' THEN 'ogc'
    END,
    CASE status
        WHEN 'TO_ANALYSE_CSVGZ' THEN 'TO_ANALYSE_CSV'
        WHEN 'TO_ANALYSE_XLS' THEN 'TO_ANALYSE_CSV'
        WHEN 'TO_ANALYSE_XLSX' THEN 'TO_ANALYSE_CSV'
        WHEN 'TO_ANALYSE_WFS' THEN 'TO_ANALYSE_OGC'
        WHEN 'TO_ANALYSE_WMS' THEN 'TO_ANALYSE_OGC'
        WHEN 'ANALYSING_WFS' THEN 'ANALYSING_OGC'
        WHEN 'ANALYSING_WMS' THEN 'ANALYSING_OGC'
        ELSE status
    END,
    COALESCE(status_since, NOW())
FROM catalog
WHERE status IS NOT NULL
  AND status IN (
      'CRAWLING_URL', 'BACKOFF', 'TO_ANALYSE_RESOURCE',
      'ANALYSING_RESOURCE_HEAD', 'DOWNLOADING_RESOURCE',
      'ANALYSING_DOWNLOADED_RESOURCE',
      'TO_ANALYSE_CSV', 'TO_ANALYSE_CSVGZ', 'TO_ANALYSE_XLS', 'TO_ANALYSE_XLSX',
      'ANALYSING_CSV', 'VALIDATING_CSV', 'INSERTING_IN_DB',
      'CONVERTING_TO_PARQUET', 'TO_ANALYSE_PARQUET', 'ANALYSING_PARQUET',
      'TO_ANALYSE_GEOJSON', 'ANALYSING_GEOJSON', 'CONVERTING_TO_GEOJSON',
      'CONVERTING_TO_PMTILES',
      'TO_ANALYSE_WFS', 'ANALYSING_WFS', 'TO_ANALYSE_WMS', 'ANALYSING_WMS',
      'TO_ANALYSE_OGC', 'ANALYSING_OGC'
  );

ALTER TABLE catalog DROP COLUMN status;
ALTER TABLE catalog DROP COLUMN status_since;

DROP INDEX IF EXISTS status_deleted_idx;
