-- add a csv_analysis column on checks

ALTER TABLE checks
ADD COLUMN csv_analysis JSONB;
