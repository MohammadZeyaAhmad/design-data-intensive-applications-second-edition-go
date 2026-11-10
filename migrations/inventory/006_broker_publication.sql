ALTER TABLE inventory.outbox ADD COLUMN broker_published_at timestamptz;
CREATE INDEX inventory_broker_pending ON inventory.outbox(tenant_id,sku,warehouse_id,aggregate_version) WHERE broker_published_at IS NULL;
