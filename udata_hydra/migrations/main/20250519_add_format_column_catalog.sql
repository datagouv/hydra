-- Add resource type and format to catalog

ALTER TABLE catalog
    ADD COLUMN type VARCHAR,
    ADD COLUMN format VARCHAR;
