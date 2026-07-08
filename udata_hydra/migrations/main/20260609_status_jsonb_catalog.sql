-- Migrate catalog.status from VARCHAR to JSONB {job: {state, since}} and drop status_since.
-- Assumes all resources are idle (status IS NULL) when this migration runs.

ALTER TABLE catalog
    ALTER COLUMN status DROP DEFAULT;

ALTER TABLE catalog
    ALTER COLUMN status TYPE jsonb
    USING '{}'::jsonb;

ALTER TABLE catalog
    ALTER COLUMN status SET DEFAULT '{}'::jsonb,
    ALTER COLUMN status SET NOT NULL;

ALTER TABLE catalog
    DROP COLUMN status_since;

DROP INDEX IF EXISTS status_deleted_idx;

CREATE INDEX IF NOT EXISTS catalog_status_gin_idx ON catalog USING GIN (status);

CREATE INDEX IF NOT EXISTS catalog_crawlable_idx ON catalog (priority, last_check)
    WHERE deleted = false
      AND (
          status = '{}'::jsonb
          OR (
              (status - 'crawler') = '{}'::jsonb
              AND status->'crawler'->>'state' = 'BACKOFF'
          )
      );
