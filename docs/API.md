# Inventory API

Default base URL: `http://127.0.0.1:18081`. Use `make demo` and `make verify-live` without copying credentials into commands or logs. The generated `.env` contains development keys for `acme` and `globex`.

Authenticated requests require `Authorization: Bearer <tenant-key>`. Identity comes from the credential, never `X-Tenant-ID`. Authenticated responses include generated `X-Request-ID` and bounded `X-Correlation-ID` headers; [provenance contracts](OPERATIONS.md) describe accepted inputs and worker lineage. All successful command responses include `Idempotent-Replay: true|false`. All responses use `Cache-Control: no-store`.

| Method and path | Input | Result |
|---|---|---|
| `GET /healthz` | None | Process health, 200 |
| `GET /readyz` | None | Database migration compatibility, 200 or 503 |
| `POST /v1/stock/{sku}/receipts` | JSON `{"quantity":10,"warehouse_id":"east"}` (warehouse optional) and `Idempotency-Key` | 200, stock snapshot |
| `GET /v1/stock/{sku}` | Auth | 200, authoritative stock at optional `?warehouse_id=east`; 404 if absent in this tenant/location |
| `GET /v1/stock/{sku}/projection` | Auth | 200, eventual stock view at optional `?warehouse_id=east`; 404 until first projected event |
| `POST /v1/reservations` | JSON `{"sku":"BOOK","quantity":2,"ttl_seconds":900,"warehouse_id":"east"}` (warehouse optional) and `Idempotency-Key` | 201, stock and reservation snapshots with `expires_at` |
| `GET /v1/reservations/{id}` | Auth | 200, reservation; 404 if absent in this tenant |
| `POST /v1/reservations/{id}/commit` | No body; `Idempotency-Key` | 200, reservation becomes committed and stock reserved → allocated |
| `POST /v1/reservations/{id}/release` | No body; `Idempotency-Key` | 200, active reservation releases stock |
| `POST /v1/reservations/{id}/reverse` | No body; `Idempotency-Key` | 200, committed allocation is compensated once |

Use `Content-Type: application/json` for JSON commands. The API rejects unknown fields, trailing JSON documents, overflowing numbers, bodies over 64 KiB, invalid SKUs, and nonpositive/oversized quantities. An individual command quantity is at most 1,000,000,000. Reservation IDs are UUIDs. SKUs are 1–64 ASCII letters/digits/dot/underscore/hyphen and start with a letter or digit.

Stock keys, versions and projection streams are now tenant/SKU/warehouse scoped. Missing/empty `warehouse_id` or `default` selects the legacy default location; reads without the field do not sum other locations. Named locations appear in snapshots; default snapshots retain the old shape. [Fulfillment contracts](FULFILLMENT_API.md) describe sourcing and shipping.

Command responses have this shape:

```json
{
  "stock": {
    "tenant_id": "acme",
    "sku": "BOOK",
    "on_hand": 10,
    "reserved": 2,
    "allocated": 0,
    "version": 2
  },
  "reservation": {
    "id": "7b2cfc59-f04b-49d0-8813-cfcb5f0abcfe",
    "tenant_id": "acme",
    "sku": "BOOK",
    "quantity": 2,
    "status": "active",
    "version": 1,
    "expires_at": "2030-01-01T00:15:00Z"
  }
}
```

The `reservation` field is absent for a receipt. Available stock is `on_hand - reserved - allocated`. A receipt does not create a catalog product; Catalog is a later context.

Idempotency keys contain 1–128 printable non-space ASCII characters. Scope is `(tenant, operation, key)`, and the request hash includes the typed payload and resource/action identifiers. The two reservation transitions share an operation scope, so use a different key for a different action. The same successful command returns its saved snapshots even if stock changed later. A changed payload returns 409 `idempotency_conflict`. Rejected/rolled-back commands do not reserve the key; they may succeed later if business state changes. Records currently have no expiration and are not automatically deleted.

Repeating the same terminal transition with another key succeeds without another stock mutation or event. Committing a released/expired reservation, or releasing a committed reservation, returns 409. Reversal is a separate action from release and applies only to committed, unshipped allocation in the current inventory model; once reversed it cannot be committed again.

`ttl_seconds` is optional: omitted or zero means 900 seconds, and explicit values must be between 1 and 3600 seconds. The requested TTL participates in request identity; keep the original request representation when retrying. New holds use database time after acquiring stock locks. Commitment checks `clock_timestamp()` after acquiring both reservation and stock locks, so waiting for a lock cannot extend the deadline. A due active hold is unavailable for commitment even if the expiry worker has not yet run. The worker releases it and emits `inventory.reservation-expired.v1` in one transaction. Committed reservations do not auto-expire. These are deadlines relative to the single database clock, not guarantees of globally synchronized physical time.

Successful retries return the original saved snapshot: retrying a reservation after its expiry may still return its original `active` response. Read the reservation endpoint for current state. Idempotency responses written by the first increment remain readable; their absent deadline is omitted on replay rather than invented.

`ETag` on stock reads and command outcomes reports the stock version. Conditional requests using `If-Match` are explicitly rejected until an application-level version precondition is implemented; they are never silently ignored. Projection reads are asynchronous and return `X-Consistency: eventual`; they must not authorize another reservation.

Errors use `{"error":{"code":"..."}}`: 400 invalid input/JSON, 401 unauthorized, 404 not found, 409 insufficient stock/idempotency conflict/invalid transition, 415 unsupported media type, 504 deadline, and 500 internal error. Internal database details are logged server-side rather than returned to clients.

Workflow-owned reservations are managed through the published checkout contract. Ordinary commitment/release/reversal endpoints reject those IDs; cancel a pending checkout to request compensation. After source confirmation, Fulfillment owns physical handoff; checkout cancellation cannot undo it. Permanent step-close markers prevent a delayed workflow reserve from recreating an already compensated hold.

## Published service contracts and request budgets

The [OpenAPI 3.1 reservation/stock subset](../api/openapi/inventory/openapi.json) is served at `GET /openapi/inventory.json`. The [REST/gRPC guide](RPC.md) explains the second adapter, shared command receipts, exact integer/timestamp representations and native transport failures. Inventory requests may provide one `X-Request-Timeout-Ms` value from 1 through 10000; an earlier parent deadline wins. A timeout may follow a commit, so retries must retain the original tenant, operation, key and command.

## Optional session-consistent Inventory reads

See the [session consistency guide](SESSION_CONSISTENCY.md) and [OpenAPI contract](../api/openapi/inventory/openapi.json) for opt-in primary/physical-replica reads, signed tenant/history watermarks, bounded pending/fallback behavior and `GET /v1/inventory/snapshot`. Commands remain primary-authoritative. The asynchronous projection has a separate worker-progress contract.
