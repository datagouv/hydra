-- add a detected_last_modified_at column on checks

ALTER TABLE checks
ADD COLUMN detected_last_modified_at TIMESTAMP;
