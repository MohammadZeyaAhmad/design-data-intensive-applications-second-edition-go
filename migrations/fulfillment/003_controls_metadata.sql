ALTER TABLE fulfillment.shipments ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
ALTER TABLE fulfillment.shipments ADD COLUMN dispatch_metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(dispatch_metadata));
ALTER TABLE fulfillment.shipments ADD COLUMN paused boolean NOT NULL DEFAULT false;
ALTER TABLE fulfillment.shipments ADD COLUMN control_version bigint NOT NULL DEFAULT 0 CHECK(control_version>=0);
ALTER TABLE fulfillment.events ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
CREATE TRIGGER shipment_metadata BEFORE UPDATE ON fulfillment.shipments FOR EACH ROW EXECUTE FUNCTION platform.protect_metadata();
CREATE FUNCTION fulfillment.protect_dispatch_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.dispatch_metadata->>'version'='1' AND NEW.dispatch_metadata IS DISTINCT FROM OLD.dispatch_metadata
    THEN RAISE EXCEPTION 'immutable dispatch provenance'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER dispatch_metadata BEFORE UPDATE ON fulfillment.shipments FOR EACH ROW EXECUTE FUNCTION fulfillment.protect_dispatch_metadata();
CREATE TABLE fulfillment.control_audit (
    tenant_id text NOT NULL,id uuid NOT NULL,version bigint NOT NULL,
    audit_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    action text NOT NULL CHECK(action IN('pause','resume','retry')),
    reason text NOT NULL CHECK(reason IN('investigation','dependency_restored','operator_retry')),
    before_state jsonb NOT NULL,after_state jsonb NOT NULL,
    metadata jsonb NOT NULL CHECK(platform.valid_metadata(metadata) AND metadata->'version'='1'::jsonb),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,id,version)
);
CREATE TRIGGER control_immutable BEFORE UPDATE OR DELETE ON fulfillment.control_audit FOR EACH ROW EXECUTE FUNCTION fulfillment.reject_mutation();
