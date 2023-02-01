-- create meta table

CREATE TABLE IF NOT EXISTS tables_index (
    id serial PRIMARY KEY,
    parsing_table VARCHAR,
    csv_detective JSONB,
    resource_id UUID,
    url VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);
