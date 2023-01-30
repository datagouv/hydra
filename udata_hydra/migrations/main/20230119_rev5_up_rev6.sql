-- add a csv_analysis table

CREATE TABLE IF NOT EXISTS csv_analysis(
    id serial PRIMARY KEY,
    resource_id UUID,
    url VARCHAR,
    check_id int,
    csv_detective JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    error VARCHAR
);
