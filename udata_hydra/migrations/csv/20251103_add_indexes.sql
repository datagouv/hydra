-- add indexes on tables_index table, to improve performance when querying depending on resource_id
CREATE INDEX IF NOT EXISTS resource_id_idx ON tables_index (resource_id);
