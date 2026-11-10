ALTER TABLE checkout.jobs ADD COLUMN engine text NOT NULL DEFAULT 'postgres' CHECK(engine IN('postgres','temporal'));
ALTER TABLE checkout.jobs ADD COLUMN temporal_step bigint NOT NULL DEFAULT 0 CHECK(temporal_step>=0);
ALTER TABLE checkout.jobs ADD COLUMN temporal_started_at timestamptz;
CREATE INDEX temporal_start_pending ON checkout.jobs(tenant_id,id) WHERE engine='temporal' AND temporal_started_at IS NULL;

CREATE FUNCTION checkout.protect_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.engine IS DISTINCT FROM OLD.engine OR NEW.request IS DISTINCT FROM OLD.request
    THEN RAISE EXCEPTION 'immutable checkout owner and request'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER checkout_owner BEFORE UPDATE ON checkout.jobs FOR EACH ROW EXECUTE FUNCTION checkout.protect_owner();

CREATE TABLE checkout.temporal_steps (
    tenant_id text NOT NULL,id uuid NOT NULL,step bigint NOT NULL CHECK(step>=0),
    result jsonb NOT NULL,metadata jsonb NOT NULL CHECK(platform.valid_metadata(metadata)),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,id,step),
    FOREIGN KEY(tenant_id,id) REFERENCES checkout.jobs(tenant_id,id)
);
CREATE TRIGGER temporal_step_immutable BEFORE UPDATE OR DELETE ON checkout.temporal_steps FOR EACH ROW EXECUTE FUNCTION checkout.immutable_audit();
CREATE TABLE checkout.temporal_wakes (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id text NOT NULL,id uuid NOT NULL,
    delivered_at timestamptz,
    FOREIGN KEY(tenant_id,id) REFERENCES checkout.jobs(tenant_id,id)
);
CREATE FUNCTION checkout.record_wake() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.engine='temporal' AND (NEW.paused IS DISTINCT FROM OLD.paused
       OR NEW.cancel_requested IS DISTINCT FROM OLD.cancel_requested
       OR NEW.control_version IS DISTINCT FROM OLD.control_version)
    THEN INSERT INTO checkout.temporal_wakes(tenant_id,id) VALUES(NEW.tenant_id,NEW.id); END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER temporal_control_wake AFTER UPDATE ON checkout.jobs FOR EACH ROW EXECUTE FUNCTION checkout.record_wake();
