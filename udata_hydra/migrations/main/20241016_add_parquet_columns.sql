-- add parquet_url and parquet_size columns to resources/catalog table

ALTER TABLE catalog
    ADD parquet_url VARCHAR,
    ADD parquet_size BIGINT;
