ALTER TABLE fulfillment.shipments ALTER COLUMN status SET NOT NULL;
ALTER TABLE fulfillment.shipments ALTER COLUMN version SET NOT NULL;
ALTER TABLE fulfillment.shipments ADD CHECK (
    state ?& ARRAY['tenant_id','id','order_id','checkout_id','warehouse_id','lines','status','version','dispatched']
    AND jsonb_typeof(state->'lines')='array'
    AND jsonb_array_length(state->'lines') BETWEEN 1 AND 100
    AND (state->>'dispatched')::integer BETWEEN 0 AND jsonb_array_length(state->'lines')
);
