-- Optional flag from udata webhook: request high-priority RQ path for first analysis (#386)
ALTER TABLE catalog ADD COLUMN IF NOT EXISTS instant_analysis BOOLEAN NOT NULL DEFAULT FALSE;
