ALTER TABLE payments.intents ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
ALTER TABLE payments.journal ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
ALTER TABLE provider.operations ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
CREATE TRIGGER intent_metadata BEFORE UPDATE ON payments.intents FOR EACH ROW EXECUTE FUNCTION platform.protect_metadata();
CREATE TRIGGER provider_metadata BEFORE UPDATE ON provider.operations FOR EACH ROW EXECUTE FUNCTION platform.protect_metadata();
