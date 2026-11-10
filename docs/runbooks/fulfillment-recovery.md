# Fulfillment recovery

Reproduce with `make up`, then `make verify-fulfillment verify-fulfillment-recovery`. These scenarios create synthetic development stock and orders. The recovery command stops only this Compose project's worker, accepts dispatch requests, restarts its API, verifies saved-response replay, then starts the worker and checks both shipments, physical stock and projections. It retains volumes and credentials. Reports are in ignored `artifacts/fulfillment.json` and `artifacts/fulfillment-recovery.json`.

## Confirmed order has no shipments

Read the authoritative order and checkout status. Only source confirmation authorizes Fulfillment. A failed or compensating checkout should have no shipment request. After confirmation, the request may still be in `ordering.fulfillment_outbox`; confirm the worker is running and migrations are current. Delivery commits the Fulfillment inbox before acknowledging the source row, so restarting the worker is safe. Do not create a second order, charge the customer again or construct shipment allocations manually.

## Dispatch stays pending

Read `GET /v1/shipments/{id}` for status, `dispatched`, `attempts` and the sanitized `last_error`, then inspect its immutable event history and Inventory reservation IDs. A retry code means the last attempt did not establish a validated receipt; it does not prove stock stayed in the warehouse. Inventory may already show `shipped` while Fulfillment is behind. Restart the worker and allow the 30-second lease to expire after an ungraceful stop. It retries the same allocation and recovers the saved physical result.

Do not reset `dispatched`, generate replacement allocation IDs, force a terminal status, reverse shipped stock or refund a confirmed checkout to clear this condition. Persistent contradictory/missing allocation state needs a reviewed repair workflow; a general repair command is not implemented. Keep source facts and operation IDs for diagnosis. Shipping and returns are different business actions.

## Warehouse projection is stale

Compare authoritative stock and its projection with the same `warehouse_id`. Missing warehouse selection reads only `default`. Projection versions and quarantine apply independently to each tenant/SKU/location. Check the first pending/quarantined event for that stream and the worker's fixed error codes. The existing inventory quarantine mechanism does not automatically delete/rewrite an unsupported fact, and an operator-facing all-inventory rebuild remains future work.

## Upgrade from the single-pool increment

Stop the project's API, worker and provider, build the new binary/image, then run `make up`. This retains its PostgreSQL volume and applies checksum-protected migrations. The migration assigns old rows to `default` and backfills requests for confirmed orders without shipping anything. Deploy the new API and worker together; an old projection adapter cannot use the expanded key. Never edit an applied migration or delete volumes to make a checksum mismatch disappear.
