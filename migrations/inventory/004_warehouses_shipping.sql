-- Expand existing data into the default warehouse without rewriting saved
-- command responses or immutable facts. Older payloads omit warehouse_id.
ALTER TABLE inventory.stock ADD COLUMN warehouse_id text NOT NULL DEFAULT 'default'
    CHECK (warehouse_id ~ '^[a-z][a-z0-9-]{0,63}$');
ALTER TABLE inventory.reservations ADD COLUMN warehouse_id text NOT NULL DEFAULT 'default';
ALTER TABLE inventory.outbox ADD COLUMN warehouse_id text NOT NULL DEFAULT 'default';
ALTER TABLE inventory.stock_views ADD COLUMN warehouse_id text NOT NULL DEFAULT 'default';

ALTER TABLE inventory.reservations DROP CONSTRAINT reservations_tenant_id_sku_fkey;
ALTER TABLE inventory.outbox DROP CONSTRAINT outbox_tenant_id_sku_fkey;
ALTER TABLE inventory.stock DROP CONSTRAINT stock_pkey;
ALTER TABLE inventory.stock ADD PRIMARY KEY (tenant_id, sku, warehouse_id);
ALTER TABLE inventory.reservations ADD FOREIGN KEY (tenant_id, sku, warehouse_id)
    REFERENCES inventory.stock(tenant_id, sku, warehouse_id);
ALTER TABLE inventory.outbox ADD FOREIGN KEY (tenant_id, sku, warehouse_id)
    REFERENCES inventory.stock(tenant_id, sku, warehouse_id);
ALTER TABLE inventory.stock_views DROP CONSTRAINT stock_views_pkey;
ALTER TABLE inventory.stock_views ADD PRIMARY KEY (tenant_id, sku, warehouse_id);
ALTER TABLE inventory.outbox DROP CONSTRAINT outbox_tenant_id_sku_aggregate_version_key;
ALTER TABLE inventory.outbox ADD UNIQUE (tenant_id, sku, warehouse_id, aggregate_version);
DROP INDEX inventory.outbox_stream_pending;
CREATE INDEX outbox_stream_pending ON inventory.outbox(tenant_id,sku,warehouse_id,aggregate_version)
    WHERE processed_at IS NULL;
ALTER TABLE inventory.reservations DROP CONSTRAINT reservations_status_check;
ALTER TABLE inventory.reservations ADD CHECK (status IN ('active','committed','released','expired','reversed','shipped'));
