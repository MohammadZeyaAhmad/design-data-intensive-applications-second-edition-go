# Warehouse inventory and Fulfillment

Use the API base URL and tenant credentials described in [Inventory](API.md). Fulfillment receives confirmed orders asynchronously. It owns shipment progress; Inventory owns physical quantities. Checkout compensation finishes before this ownership transfer, and never reverses a confirmed order because a later shipping operation needs recovery.

## Warehouse stock

Receipts and ordinary reservations accept optional `warehouse_id`. For example:

```json
{"quantity":10,"warehouse_id":"east"}
```

```json
{"sku":"BOOK","quantity":2,"warehouse_id":"east","ttl_seconds":900}
```

Use `GET /v1/stock/BOOK?warehouse_id=east` and `GET /v1/stock/BOOK/projection?warehouse_id=east` for authoritative and eventual snapshots. Warehouse IDs are 1–64 lowercase letters/digits/hyphens, starting with a letter. Locations are created implicitly by a receipt; a warehouse registry is future work. Omitting the field, using an empty string, or naming `default` selects the existing default warehouse. Default snapshots continue to omit `warehouse_id` to preserve old saved responses. Named snapshots include it.

Stock versions, row locks, constraints and projection ordering now belong to `(tenant, SKU, warehouse)`. The stock read without a warehouse reports only the default location. It does not sum locations. A poisoned east event blocks east's stream while west can progress for the same SKU.

Checkout automatically selects one location able to supply an entire SKU line. It prefers `default`, then warehouse ID in lexical order, and pins that choice in the reservation. Row locks recheck availability after waiting. Retrying a successful reserve keeps its original location. A line requiring three units fails if east has one and west has two; splitting one SKU across locations is not implemented. Distinct lines can use different warehouses. Stable close markers fence delayed reserve calls across all locations.

## Shipment commands and reads

Confirmation writes a Fulfillment request in the same Ordering transaction as the confirmed fact. A separate dispatcher delivers it at least once. Fulfillment atomically stores an immutable inbox receipt, one shipment per warehouse, allocation ownership and initial shipment history. Redelivery preserves the original identity and creates no extra shipment. An allocation cannot belong to two shipments in a tenant.

| Method/path | Input | Result |
|---|---|---|
| `GET /v1/orders/{id}/fulfillment` | Authentication | 200, array of shipment views; initially empty until request delivery |
| `GET /v1/shipments/{id}` | Authentication | 200, current shipment, checkpoint, retries and sanitized last error; 404 for another tenant |
| `GET /v1/shipments/{id}/events` | Authentication | Ordered immutable local history, including full states |
| `POST /v1/shipments/{id}/pick` | `{"expected_version":1}`; `Idempotency-Key` | 200, ready → picking |
| `POST /v1/shipments/{id}/pack` | `{"expected_version":2}`; `Idempotency-Key` | 200, picking → packed |
| `POST /v1/shipments/{id}/dispatch` | `{"expected_version":3,"tracking_reference":"TEST-123"}`; `Idempotency-Key` | 202, packed → dispatching; durable worker handoff requested |
| `POST /v1/shipments/{id}/deliver` | Current `expected_version`; `Idempotency-Key` | 200, shipped → delivered |

Send JSON with `Content-Type: application/json`. Expected versions are authoritative; `If-Match` is rejected. Unknown fields, tenant overrides, malformed UUIDs and oversized bodies fail validation. Tracking references are 1–128 printable ASCII characters without spaces. Repeating a successful command returns its saved snapshot and `Idempotent-Replay: true`, including after later progress. Reusing its key with changed input returns 409. Another key with a stale version also returns 409. The returned snapshot is an acknowledgment; read current state to observe progress.

Each allocation handoff increments the shipment version once. A shipment with one line normally has versions 1 ready, 2 picking, 3 packed, 4 dispatching, 5 shipped and 6 delivered. Two lines in one warehouse require two handoff checkpoints, so delivery starts from version 6 instead. An order remains `confirmed`; shipment status is a separate read contract. Listing fulfillment for an unknown or other-tenant order returns an empty array.

## Physical handoff and recovery

The worker claims a 30-second token lease, commits that claim, and calls the published Inventory dispatcher outside the Fulfillment transaction. Inventory serializes dispatch and close on the same stable allocation, marks it `shipped`, decreases both `allocated` and `on_hand`, and emits its warehouse event atomically. Repeating dispatch returns the shipped receipt without another deduction. Shipped allocation cannot be reversed or reserved again.

Fulfillment checks the receipt's identity, SKU, quantity and warehouse before saving its checkpoint. The final write checks the current token and database-clock lease after acquiring the row lock. A lost response stays `dispatching` with a sanitized retry error. A worker dying after Inventory commits retries the same allocation after its lease expires. It cannot checkpoint under an expired or superseded token. A shipment becomes `shipped` only after all allocations have receipts; delivery cannot precede this state.

This is an explicit simulated warehouse handoff with an operator-supplied tracking reference. No real carrier, label purchase, delivery webhook, return, stock transfer, single-SKU split, post-confirmation cancellation or automatic refund after shipping is implemented. Persistent dispatch errors remain visible/retryable; generalized operator repair and production warehouse roles are tracked work. Shipment state is authoritative current state with immutable history, whereas Ordering remains event sourced.

Run `make verify-fulfillment verify-fulfillment-recovery` after `make up`. See [recovery](runbooks/fulfillment-recovery.md) and the [design walkthrough](chapters/ch05/fulfillment.md).
