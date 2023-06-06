-- Apply unique constraint on resource_id instead of the tuple (dataset_id, resource_id, url)

-- Drop duplicate entries for a resource_id, keeping the latest created one
DELETE FROM catalog WHERE id IN (
    SELECT a.id FROM catalog a, catalog b WHERE a.id < b.id and a.resource_id = b.resource_id
);

ALTER TABLE catalog
    DROP CONSTRAINT catalog_dataset_id_resource_id_url_key,
    ADD UNIQUE (resource_id);
