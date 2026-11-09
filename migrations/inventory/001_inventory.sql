CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.stock (
    tenant_id text NOT NULL,
    sku text NOT NULL,
    on_hand bigint NOT NULL DEFAULT 0,
    reserved bigint NOT NULL DEFAULT 0,
    allocated bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, sku),
    CHECK (on_hand >= 0 AND reserved >= 0 AND allocated >= 0 AND version >= 0),
    CHECK (allocated <= on_hand AND reserved <= on_hand - allocated)
);

CREATE TABLE inventory.reservations (
    tenant_id text NOT NULL,
    id uuid NOT NULL,
    sku text NOT NULL,
    quantity bigint NOT NULL CHECK (quantity > 0 AND quantity <= 1000000000),
    status text NOT NULL CHECK (status IN ('active', 'committed', 'released')),
    version bigint NOT NULL CHECK (version > 0),
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, sku) REFERENCES inventory.stock (tenant_id, sku)
);

-- Pending records exist only inside the command transaction. A successful
-- command stores its response before committing; failures roll everything back.
CREATE TABLE inventory.command_results (
    tenant_id text NOT NULL,
    operation text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    response jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, operation, idempotency_key)
);

CREATE TABLE inventory.outbox (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id text NOT NULL,
    sku text NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    available_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    quarantined boolean NOT NULL DEFAULT false,
    last_error text,
    UNIQUE (tenant_id, sku, aggregate_version),
    FOREIGN KEY (tenant_id, sku) REFERENCES inventory.stock (tenant_id, sku)
);
CREATE INDEX outbox_pending ON inventory.outbox (available_at, created_at)
    WHERE processed_at IS NULL AND NOT quarantined;
CREATE INDEX outbox_stream_pending ON inventory.outbox (tenant_id, sku, aggregate_version)
    WHERE processed_at IS NULL;

CREATE TABLE inventory.stock_views (
    tenant_id text NOT NULL,
    sku text NOT NULL,
    on_hand bigint NOT NULL,
    reserved bigint NOT NULL,
    allocated bigint NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    PRIMARY KEY (tenant_id, sku),
    CHECK (on_hand >= 0 AND reserved >= 0 AND allocated >= 0),
    CHECK (allocated <= on_hand AND reserved <= on_hand - allocated)
);

CREATE TABLE inventory.projection_receipts (
    projection_name text NOT NULL,
    tenant_id text NOT NULL,
    event_id uuid NOT NULL REFERENCES inventory.outbox (event_id),
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projection_name, tenant_id, event_id)
);
