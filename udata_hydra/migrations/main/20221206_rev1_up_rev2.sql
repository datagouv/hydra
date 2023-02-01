-- adapt database schema to new catalog

ALTER TABLE catalog
ADD harvest_modified_at TIMESTAMP;
