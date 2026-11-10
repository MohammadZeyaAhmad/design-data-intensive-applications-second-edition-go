CREATE SCHEMA fulfillment;

CREATE TABLE fulfillment.inbox (
    tenant_id text NOT NULL, id uuid NOT NULL, order_id uuid NOT NULL,
    request jsonb NOT NULL, received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,id), UNIQUE(tenant_id,order_id)
);
CREATE TABLE fulfillment.shipments (
    tenant_id text NOT NULL, id uuid NOT NULL, order_id uuid NOT NULL,
    checkout_id uuid NOT NULL, warehouse_id text NOT NULL,
    state jsonb NOT NULL,
    status text GENERATED ALWAYS AS (state->>'status') STORED,
    version bigint GENERATED ALWAYS AS ((state->>'version')::bigint) STORED,
    lease_token bigint NOT NULL DEFAULT 0,
    lease_until timestamptz NOT NULL DEFAULT '-infinity',
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    attempts bigint NOT NULL DEFAULT 0, last_error text,
    PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,checkout_id,warehouse_id),
    FOREIGN KEY(tenant_id,checkout_id) REFERENCES fulfillment.inbox(tenant_id,id),
    CHECK(status IN ('ready','picking','packed','dispatching','shipped','delivered')),
    CHECK(version > 0),
    CHECK(state->>'tenant_id'=tenant_id AND (state->>'id')::uuid=id
      AND (state->>'order_id')::uuid=order_id AND (state->>'checkout_id')::uuid=checkout_id
      AND state->>'warehouse_id'=warehouse_id)
);
CREATE INDEX shipments_order ON fulfillment.shipments(tenant_id,order_id);
CREATE INDEX shipments_dispatch ON fulfillment.shipments(next_attempt_at,lease_until)
    WHERE status='dispatching';
CREATE TABLE fulfillment.allocations (
    tenant_id text NOT NULL, id uuid NOT NULL, shipment_id uuid NOT NULL,
    PRIMARY KEY(tenant_id,id),
    FOREIGN KEY(tenant_id,shipment_id) REFERENCES fulfillment.shipments(tenant_id,id)
);
CREATE TABLE fulfillment.events (
    tenant_id text NOT NULL, shipment_id uuid NOT NULL, version bigint NOT NULL,
    action text NOT NULL, state jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id,shipment_id,version),
    FOREIGN KEY(tenant_id,shipment_id) REFERENCES fulfillment.shipments(tenant_id,id)
);
CREATE TABLE fulfillment.command_results (
    tenant_id text NOT NULL, operation text NOT NULL, idempotency_key text NOT NULL,
    request_hash bytea NOT NULL CHECK(octet_length(request_hash)=32),
    response jsonb, PRIMARY KEY(tenant_id,operation,idempotency_key)
);
CREATE FUNCTION fulfillment.reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'immutable fulfillment fact'; END $$;
CREATE TRIGGER inbox_immutable BEFORE UPDATE OR DELETE ON fulfillment.inbox
    FOR EACH ROW EXECUTE FUNCTION fulfillment.reject_mutation();
CREATE TRIGGER allocations_immutable BEFORE UPDATE OR DELETE ON fulfillment.allocations
    FOR EACH ROW EXECUTE FUNCTION fulfillment.reject_mutation();
CREATE TRIGGER events_immutable BEFORE UPDATE OR DELETE ON fulfillment.events
    FOR EACH ROW EXECUTE FUNCTION fulfillment.reject_mutation();
CREATE FUNCTION fulfillment.protect_shipment_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.tenant_id,NEW.id,NEW.order_id,NEW.checkout_id,NEW.warehouse_id)
        IS DISTINCT FROM ROW(OLD.tenant_id,OLD.id,OLD.order_id,OLD.checkout_id,OLD.warehouse_id)
        OR (NEW.state - ARRAY['status','version','dispatched','tracking_reference'])
           IS DISTINCT FROM (OLD.state - ARRAY['status','version','dispatched','tracking_reference'])
    THEN RAISE EXCEPTION 'immutable shipment identity and allocations'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER shipment_identity BEFORE UPDATE ON fulfillment.shipments
    FOR EACH ROW EXECUTE FUNCTION fulfillment.protect_shipment_identity();
