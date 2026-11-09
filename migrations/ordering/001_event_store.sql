CREATE SCHEMA ordering;
CREATE TABLE ordering.streams (
    tenant_id text NOT NULL,
    id uuid NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id,id)
);
CREATE TABLE ordering.command_results (
    tenant_id text NOT NULL,
    operation text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    command_id uuid NOT NULL DEFAULT gen_random_uuid(),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    response jsonb,
    PRIMARY KEY (tenant_id,operation,idempotency_key)
);
CREATE TABLE ordering.events (
    tenant_id text NOT NULL,
    order_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version>0),
    event_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    command_id uuid NOT NULL,
    event_type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version>0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,order_id,version),
    FOREIGN KEY (tenant_id,order_id) REFERENCES ordering.streams (tenant_id,id)
);
CREATE FUNCTION ordering.reject_event_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'order events are immutable' USING ERRCODE='55000'; END;
$$;
CREATE TRIGGER immutable_order_events BEFORE UPDATE OR DELETE ON ordering.events
    FOR EACH ROW EXECUTE FUNCTION ordering.reject_event_change();

CREATE TABLE ordering.snapshots (
    tenant_id text NOT NULL,
    order_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version>0),
    schema_version integer NOT NULL,
    body jsonb NOT NULL,
    checksum bytea NOT NULL CHECK (octet_length(checksum)=32),
    PRIMARY KEY (tenant_id,order_id),
    FOREIGN KEY (tenant_id,order_id) REFERENCES ordering.streams (tenant_id,id)
);
CREATE TABLE ordering.outbox (
    tenant_id text NOT NULL,
    order_id uuid NOT NULL,
    version bigint NOT NULL,
    event_id uuid NOT NULL REFERENCES ordering.events (event_id),
    available_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    quarantined boolean NOT NULL DEFAULT false,
    PRIMARY KEY (tenant_id,order_id,version),
    FOREIGN KEY (tenant_id,order_id,version) REFERENCES ordering.events (tenant_id,order_id,version)
);
CREATE INDEX ordering_pending ON ordering.outbox (available_at) WHERE processed_at IS NULL AND NOT quarantined;
CREATE TABLE ordering.order_views (
    tenant_id text NOT NULL,
    order_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version>0),
    body jsonb NOT NULL,
    PRIMARY KEY (tenant_id,order_id)
);
CREATE TABLE ordering.projection_receipts (
    tenant_id text NOT NULL,
    event_id uuid NOT NULL REFERENCES ordering.events (event_id),
    PRIMARY KEY (tenant_id,event_id)
);
