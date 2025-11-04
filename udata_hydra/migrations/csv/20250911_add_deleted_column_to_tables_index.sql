-- Add deleted_at column to tables_index table
ALTER TABLE tables_index ADD COLUMN deleted_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;
