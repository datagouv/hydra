-- add indexes column to tables_index table

ALTER TABLE tables_index
ADD COLUMN indexes JSONB;
