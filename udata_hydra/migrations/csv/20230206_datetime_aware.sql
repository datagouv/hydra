-- make timestamp fields timezone aware

ALTER TABLE tables_index
ALTER COLUMN created_at TYPE TIMESTAMPTZ;
