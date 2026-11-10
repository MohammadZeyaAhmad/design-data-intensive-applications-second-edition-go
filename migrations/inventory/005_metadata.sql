ALTER TABLE inventory.outbox ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
ALTER TABLE inventory.reservations ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
CREATE TRIGGER reservation_metadata BEFORE UPDATE ON inventory.reservations FOR EACH ROW EXECUTE FUNCTION platform.protect_metadata();
CREATE TRIGGER outbox_metadata BEFORE UPDATE ON inventory.outbox FOR EACH ROW EXECUTE FUNCTION platform.protect_metadata();
