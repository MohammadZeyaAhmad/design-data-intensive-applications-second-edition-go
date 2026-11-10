ALTER TABLE catalog.prices ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
ALTER TABLE catalog.quotes ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
