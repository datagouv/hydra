-- make timestamp fields timezone aware

ALTER TABLE checks
ALTER COLUMN created_at TYPE TIMESTAMPTZ,
ALTER COLUMN detected_last_modified_at TYPE TIMESTAMPTZ;

ALTER TABLE catalog
ALTER COLUMN harvest_modified_at TYPE TIMESTAMPTZ;

ALTER TABLE csv_analysis
ALTER COLUMN created_at TYPE TIMESTAMPTZ,
ALTER COLUMN parsing_date TYPE TIMESTAMPTZ;
