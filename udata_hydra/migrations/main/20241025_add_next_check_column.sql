-- Add a new column 'next_check' of type DATETIME to the 'checks' table

ALTER TABLE checks
    ADD COLUMN next_check DATETIME;
