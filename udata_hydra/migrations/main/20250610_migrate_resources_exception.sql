-- Migrate resources_exceptions table from main db to csv db.
-- It can't be done from the our migration system since it applies to two distinct dbs.
-- The command that should be run manually (use correct db name):
-- pg_dump -t resources_exceptions dev-hydra | psql dev-hydra-csv
-- Then you can DROP table resources_exceptions;

SELECT 1;
