-- remove csv_analysis table and include interesting columns in checks

ALTER TABLE catalog
    ADD status VARCHAR;
