-- Add OGC metadata column to checks table

ALTER TABLE checks
    ADD COLUMN ogc_metadata JSONB;
