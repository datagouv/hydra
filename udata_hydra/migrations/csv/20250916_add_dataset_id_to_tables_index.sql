-- Add dataset_id columns to tables_index table
ALTER TABLE tables_index ADD COLUMN dataset_id VARCHAR(24);
