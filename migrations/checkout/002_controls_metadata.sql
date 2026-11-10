ALTER TABLE checkout.jobs ADD COLUMN metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(metadata));
ALTER TABLE checkout.jobs ADD COLUMN cancel_metadata jsonb NOT NULL DEFAULT '{"version":0}' CHECK(platform.valid_metadata(cancel_metadata));
CREATE TRIGGER cancellation_metadata BEFORE UPDATE ON checkout.jobs FOR EACH ROW EXECUTE FUNCTION platform.protect_once_metadata();
ALTER TABLE checkout.jobs ADD COLUMN paused boolean NOT NULL DEFAULT false;
ALTER TABLE checkout.jobs ADD COLUMN control_version bigint NOT NULL DEFAULT 0 CHECK(control_version>=0);
CREATE TRIGGER job_metadata BEFORE UPDATE ON checkout.jobs FOR EACH ROW EXECUTE FUNCTION platform.protect_metadata();
CREATE TABLE checkout.command_results (
    tenant_id text NOT NULL,operation text NOT NULL,idempotency_key text NOT NULL,
    request_hash bytea NOT NULL CHECK(octet_length(request_hash)=32),response jsonb,
    PRIMARY KEY(tenant_id,operation,idempotency_key)
);
CREATE TABLE checkout.control_audit (
    tenant_id text NOT NULL,id uuid NOT NULL,version bigint NOT NULL,
    audit_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    action text NOT NULL CHECK(action IN('pause','resume','retry')),
    reason text NOT NULL CHECK(reason IN('investigation','dependency_restored','operator_retry')),
    before_state jsonb NOT NULL,after_state jsonb NOT NULL,
    metadata jsonb NOT NULL CHECK(platform.valid_metadata(metadata) AND metadata->'version'='1'::jsonb),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,id,version)
);
CREATE FUNCTION checkout.immutable_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'workflow control audit is immutable'; END $$;
CREATE TRIGGER control_immutable BEFORE UPDATE OR DELETE ON checkout.control_audit FOR EACH ROW EXECUTE FUNCTION checkout.immutable_audit();
