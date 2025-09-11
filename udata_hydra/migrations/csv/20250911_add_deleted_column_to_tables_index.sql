-- Add deleted column to tables_index table
ALTER TABLE tables_index ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
