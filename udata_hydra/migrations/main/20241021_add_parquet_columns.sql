-- remove parquet_url and parquet_size columns from catalog table if exists, since it was first created this way in removed 20241016_add_parquet_columns.sql

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'catalog' AND column_name = 'parquet_url') THEN
        ALTER TABLE catalog DROP COLUMN parquet_url;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'catalog' AND column_name = 'parquet_size') THEN
        ALTER TABLE catalog DROP COLUMN parquet_size;
    END IF;
END $$;

-- add parquet_url and parquet_size columns to checks table

ALTER TABLE checks
    ADD parquet_url VARCHAR,
    ADD parquet_size BIGINT;
