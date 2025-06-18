-- Add PMTiles fields to checks table

ALTER TABLE checks
    ADD COLUMN geojson_url VARCHAR,
    ADD COLUMN geojson_size BIGINT;
