-- Add a new column 'next_check_at' of type DATETIME to the 'checks' table

ALTER TABLE checks
    ADD COLUMN next_check_at DATETIME;
