-- dataset_id is required by the API schemas, enforce it at the database level
ALTER TABLE catalog ALTER COLUMN dataset_id SET NOT NULL;
