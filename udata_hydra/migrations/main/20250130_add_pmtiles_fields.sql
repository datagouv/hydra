-- Add PMTiles fields to checks table

ALTER TABLE checks
    ADD COLUMN pmtiles_url VARCHAR,
    ADD COLUMN pmtiles_size BIGINT;
