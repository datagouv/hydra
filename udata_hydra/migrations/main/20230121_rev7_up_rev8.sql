-- drop csv_detective column (will be stored alongside csv tables)

ALTER TABLE csv_analysis
    DROP COLUMN csv_detective;
