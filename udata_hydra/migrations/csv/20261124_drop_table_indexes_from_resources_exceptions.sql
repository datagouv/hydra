-- The indexes live in resources_indexes since 20261104_add_resources_indexes.sql,
-- which copied them from this column, and api-tabular reads them there.

ALTER TABLE resources_exceptions DROP COLUMN IF EXISTS table_indexes;
