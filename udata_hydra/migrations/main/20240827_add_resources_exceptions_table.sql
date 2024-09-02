-- add resources_exceptions table

-- create a table with a unique resource_id column as foreign key to resources.id, and a indexes column to list the the table indexes to be created
CREATE TABLE resources_exceptions (
    id SERIAL PRIMARY KEY,
    resource_id UUID UNIQUE NOT NULL REFERENCES catalog(resource_id) ON DELETE CASCADE,
    table_indexes JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--- Add rows to the resources_exceptions table
INSERT INTO resources_exceptions (resource_id) VALUES ('f868cca6-8da1-4369-a78d-47463f19a9a3'), ('4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d');
