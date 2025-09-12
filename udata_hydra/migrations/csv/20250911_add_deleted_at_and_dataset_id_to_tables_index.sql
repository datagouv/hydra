-- Add deleted_at and dataset_id columns to tables_index table
ALTER TABLE tables_index ADD COLUMN deleted_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;
ALTER TABLE tables_index ADD COLUMN dataset_id VARCHAR(24);
