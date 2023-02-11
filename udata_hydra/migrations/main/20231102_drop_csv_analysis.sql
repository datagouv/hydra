-- remove csv_analysis table and include interesting columns in checks

DROP TABLE csv_analysis;

ALTER TABLE checks
    ADD parsing_error VARCHAR,
    ADD parsing_table VARCHAR,
    ADD parsing_started_at TIMESTAMPTZ,
    ADD parsing_finished_at TIMESTAMPTZ;
