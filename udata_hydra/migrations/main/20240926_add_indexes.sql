-- add indexes on checks table, to improve performance when querying depending on resource_id and/or created_at
CREATE INDEX IF NOT EXISTS resource_id_idx ON checks (resource_id);
CREATE INDEX IF NOT EXISTS created_at_idx ON checks (created_at);

-- add index on catalog table, to improve performance when querying depending on last_check
CREATE INDEX IF NOT EXISTS last_check_idx ON catalog (last_check);
