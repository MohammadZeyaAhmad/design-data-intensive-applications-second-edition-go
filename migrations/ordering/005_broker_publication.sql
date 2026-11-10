ALTER TABLE ordering.outbox ADD COLUMN broker_published_at timestamptz;
ALTER TABLE ordering.checkout_outbox ADD COLUMN broker_published_at timestamptz;
ALTER TABLE ordering.fulfillment_outbox ADD COLUMN broker_published_at timestamptz;
CREATE INDEX ordering_broker_pending ON ordering.outbox(tenant_id,order_id,version) WHERE broker_published_at IS NULL;
