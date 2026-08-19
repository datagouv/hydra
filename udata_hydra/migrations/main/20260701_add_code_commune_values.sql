-- Add code_commune distinct values column to checks table

ALTER TABLE checks
    ADD COLUMN code_commune_values JSONB;
