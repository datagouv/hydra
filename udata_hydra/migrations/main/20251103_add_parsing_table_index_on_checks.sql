-- add parsing_table index on checks table, to improve performance when querying depending on parsing_table
CREATE INDEX IF NOT EXISTS parsing_table_idx ON checks (parsing_table);
