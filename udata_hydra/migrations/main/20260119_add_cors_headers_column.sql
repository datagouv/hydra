-- Add CORS headers column to checks table

ALTER TABLE checks
    ADD COLUMN cors_headers JSONB;
