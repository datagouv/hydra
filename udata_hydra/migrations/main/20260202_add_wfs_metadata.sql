-- Add WFS metadata column to checks table

ALTER TABLE checks
    ADD COLUMN wfs_metadata JSONB;
