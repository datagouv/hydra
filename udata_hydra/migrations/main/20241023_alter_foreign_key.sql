-- Change the column `last_check` of the `catalog` table to be a foreign key to the `check` table.
-- (assuming `last_check` is already an INT)
-- When  a check is deleted, the `last_check` column of the `catalog` table will be set to NULL.

ALTER TABLE catalog
    ADD CONSTRAINT fk_last_check
    FOREIGN KEY (last_check) REFERENCES checks(id)
    ON DELETE SET NULL;
