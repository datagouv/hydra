-- add a column that indicates when the status was last modified

ALTER TABLE catalog
    ADD status_since TIMESTAMPTZ;
