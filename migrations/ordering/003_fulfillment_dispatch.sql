CREATE TABLE ordering.fulfillment_outbox (
    tenant_id text NOT NULL, id uuid NOT NULL, order_id uuid NOT NULL,
    body jsonb NOT NULL, dispatched_at timestamptz,
    PRIMARY KEY(tenant_id,id),
    FOREIGN KEY(tenant_id,order_id) REFERENCES ordering.streams(tenant_id,id)
);
-- Previously confirmed orders also acquire durable requests. Shipping still
-- requires explicit warehouse commands; migration performs no physical effect.
INSERT INTO ordering.fulfillment_outbox(tenant_id,id,order_id,body)
SELECT tenant_id,(payload->>'checkout_id')::uuid,order_id,
    jsonb_build_object('id',payload->>'checkout_id','tenant_id',tenant_id,
      'order_id',order_id::text,'allocations',payload->'allocations')
FROM ordering.events WHERE event_type='order.confirmed';
