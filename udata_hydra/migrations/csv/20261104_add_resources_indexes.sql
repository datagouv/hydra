-- Indexes to create on the table of a resource, at each conversion.
-- They used to live in resources_exceptions.table_indexes, which tied them to the exception mechanism:
-- declaring an index also lifted the file size limit and the job timeout of the resource.
-- resources_exceptions.table_indexes is kept for now: the deployed api-tabular reads it, it will be
-- dropped once api-tabular reads resources_indexes.

CREATE TABLE IF NOT EXISTS resources_indexes (
    resource_id UUID NOT NULL,
    column_name VARCHAR NOT NULL,
    index_type VARCHAR NOT NULL,
    PRIMARY KEY (resource_id, column_name)
);

INSERT INTO resources_indexes (resource_id, column_name, index_type)
SELECT e.resource_id, i.key, i.value
FROM resources_exceptions e, jsonb_each_text(e.table_indexes) i
ON CONFLICT (resource_id, column_name) DO NOTHING;
