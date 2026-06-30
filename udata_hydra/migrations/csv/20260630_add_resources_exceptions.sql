-- creates the table from `main/20240827_add_resources_exceptions_table.sql`
-- which migration has been documented in `csv/20250610_migrate_resources_exception.sql` but is a noop
-- this helps having a working env from scratch but does not apply on prod (IF NOT EXISTS)

CREATE TABLE IF NOT EXISTS resources_exceptions (
    id SERIAL PRIMARY KEY,
    resource_id UUID UNIQUE NOT NULL,
    table_indexes JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    comment VARCHAR(255)
);
