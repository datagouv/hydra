-- the `datetime_iso` format is not in csv-detective anymore (https://github.com/datagouv/csv-detective/pull/132)
-- to prevent crashes, we replace all references to this format with `datetime_aware`
-- NB: it doesn't matter if it's the right format, if it's not, validation will fail
-- and a new analysis will find the right format

UPDATE tables_index
SET csv_detective = replace(csv_detective::text, 'datetime_iso', 'datetime_aware')::jsonb
WHERE csv_detective::text LIKE '%datetime_iso%';
