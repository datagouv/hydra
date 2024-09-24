-- add resources_exceptions table

-- create a table with a unique resource_id column as foreign key to resources.id, and a indexes column to list the the table indexes to be created
CREATE TABLE resources_exceptions (
    id SERIAL PRIMARY KEY,
    resource_id UUID UNIQUE NOT NULL REFERENCES catalog(resource_id) ON DELETE CASCADE,
    table_indexes JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
