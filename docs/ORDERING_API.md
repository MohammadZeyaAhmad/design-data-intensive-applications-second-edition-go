# Event-sourced Ordering API

Ordering begins with SKU/quantity intent. Checkout now accepts a server-owned Catalog quote, freezes it in an event, and confirms only after the workflow verifies captured payment and committed allocations. See [commerce contracts](COMMERCE_API.md). Confirmation atomically schedules the separate [Fulfillment context](FULFILLMENT_API.md). Shipment progress has its own status and version; the source order remains confirmed.

Use the same tenant-bound development bearer keys as the inventory API. Every command requires `Idempotency-Key`, JSON content type, and a body of at most 64 KiB. Caller-provided tenant/ID fields are not accepted in command bodies. Current order UUIDs are returned by the placement response. Checkout adds `checkout_pending`, `confirmed`, and `checkout_failed` outcomes, plus accepted `pricing`, `checkout_id`, `payment_id` and failure-reason fields when applicable. Payment references stay in operational records, not event payloads.

| Method and path | Body or parameters | Result |
|---|---|---|
| `POST /v1/orders` | `{"lines":[{"sku":"BOOK","quantity":2}]}` | 201, authoritative state at version 1 |
| `POST /v1/orders/{id}/amend` | `{"expected_version":1,"lines":[{"sku":"BOOK","quantity":3}]}` | 200, next version; 409 on stale version |
| `POST /v1/orders/{id}/cancel` | `{"expected_version":2,"reason":"customer_requested"}` | 200, next version with cancelled status |
| `GET /v1/orders/{id}` | None | 200, authoritative state from a consistent event/snapshot read |
| `GET /v1/orders/{id}/projection` | Optional `min_version=3` | 200 when sufficiently fresh; 404 until a first view exists; 409 if below the requested version |
| `GET /v1/orders/{id}/events` | `after_version=0&limit=50` | Keyset history page, maximum limit 100 |

Line count is 1–100. SKUs cannot repeat within an order; quantities are integers from 1 to 1,000,000,000. Cancellation reasons are controlled codes: `customer_requested`, `inventory_unavailable`, or `payment_failed`. Free-form personal text is not accepted in the immutable history.

A state response contains `id`, `tenant_id`, `version`, `status`, `lines`, and an optional `cancellation_reason`. A cancelled or checkout-pending order cannot be amended or cancelled through ordinary order commands. Checkout cancellation uses its own workflow endpoint. A cancelled order cannot be cancelled by another logical command. Retrying its original cancellation key returns the saved successful response. Version checks precede a new command's decision, so two distinct commands both expecting version 1 cannot both append version 2.

Request identity is scoped by tenant and operation. The hash includes typed command fields, resource identity and expected version. Reusing a key with a changed payload returns 409 `idempotency_conflict`. Repeating placement returns the original order UUID, even after later changes; retries return the original saved state, not an implicit fresh query. Rejected commands leave no request, stream, event or outbox record.

`ETag` reports the order version. Use the required JSON `expected_version` field for mutations; `If-Match` is rejected explicitly rather than silently ignored. Authoritative reads return `X-Consistency: authoritative`. Projection reads return `X-Consistency: eventual` and never authorize command decisions.

History returns `events` and `next_after_version`. Each record includes an immutable event UUID, stream version, type, payload schema version, data and recorded timestamp. Request the next page using the returned version; stop when `events` is empty. Pagination is within one tenant/order stream and may observe later appends across pages. Timestamps and UUIDs are not global commit-order cursors.

## Operator functions

From the repository root, with the configured local operator database binding:

```sh
go run ./cmd/admin snapshot-order -tenant acme -id ORDER_UUID
go run ./cmd/admin rebuild-order -tenant acme -id ORDER_UUID
```

A snapshot is built from full replay and stores a schema version, aggregate version and canonical-state checksum. A missing, corrupt, unsupported or inapplicable snapshot falls back to the immutable event history. JSONB formatting differences do not change the canonical checksum. Snapshot checksums detect accidental byte/state corruption; they do not establish correctness against a privileged database operator or a faulty reducer. Full replay and scenario assertions remain necessary.

Rebuild replaces one order's projection, records receipts for its prefix and advances matching local outbox progress in one transaction. It coordinates with consumers using a shared stream-local projection lock and blocks appends briefly at that stream's head. Unrelated orders keep their own locks and order. This is a scoped repair, not yet the planned all-stream shadow-generation migration tool.

Placement event schema 1 (`items` with `qty`) is upcast to schema 2 (`lines` with `quantity`) during replay. Stored facts remain unchanged. Unknown event schemas fail authoritative reads with 503 `unsupported_event_schema`. Consumers retry and then quarantine their stream head; they never skip it. Upgrade the consumer to understand the event before rebuilding. Rebuild also fails if full history is unsupported.

Ordinary SQL updates/deletes of events are rejected by a database trigger. Deployment still needs the planned separate least-privilege application and migration roles; the local operator role is intentionally powerful for the development suite.
