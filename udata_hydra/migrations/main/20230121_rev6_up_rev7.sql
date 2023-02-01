-- add csv_analysis parsing columns

ALTER TABLE csv_analysis
    ADD parsing_error VARCHAR,
    ADD parsing_table VARCHAR,
    ADD parsing_date TIMESTAMP;
