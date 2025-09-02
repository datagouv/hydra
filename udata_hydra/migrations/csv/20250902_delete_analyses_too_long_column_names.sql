-- Deleting all rows where any of the column names is too long for Postgres (> 64 characters)
-- so that no previous analysis will be found, to prevent crashes due to columns mismatches
-- between the actual column names and the truncated ones in Postgres.
-- This will allow to gracefully crash on these resources.

DELETE FROM tables_index
WHERE EXISTS (
    SELECT 1
    FROM jsonb_array_elements_text(csv_detective->'header') AS header
    WHERE length(header) > 64
);
