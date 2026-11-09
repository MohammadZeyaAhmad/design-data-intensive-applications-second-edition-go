# Inventory API

Default base URL: `http://127.0.0.1:18081`. Use `make demo` and `make verify-live` without copying credentials into commands or logs. The generated `.env` contains development keys for `acme` and `globex`.

Authenticated requests require `Authorization: Bearer <tenant-key>`. Identity comes from the credential, never `X-Tenant-ID`. All successful command responses include `Idempotent-Replay: true|false`. All responses use `Cache-Control: no-store`.

| Method and path | Input | Result |
|---|---|---|
| `GET /healthz` | None | Process health, 200 |
| `GET /readyz` | None | Database migration compatibility, 200 or 503 |
| `POST /v1/stock/{sku}/receipts` | JSON `{"quantity":10}` and `Idempotency-Key` | 200, stock snapshot |
| `GET /v1/stock/{sku}` | Auth | 200, authoritative stock; 404 if absent in this tenant |
| `GET /v1/stock/{sku}/projection` | Auth | 200, eventual stock view; 404 until first projected event |
| `POST /v1/reservations` | JSON `{"sku":"BOOK","quantity":2}` and `Idempotency-Key` | 201, stock and reservation snapshots |
| `GET /v1/reservations/{id}` | Auth | 200, reservation; 404 if absent in this tenant |
| `POST /v1/reservations/{id}/commit` | No body; `Idempotency-Key` | 200, reservation becomes committed and stock reserved → allocated |
| `POST /v1/reservations/{id}/release` | No body; `Idempotency-Key` | 200, active reservation releases stock |

Use `Content-Type: application/json` for JSON commands. The API rejects unknown fields, trailing JSON documents, overflowing numbers, bodies over 64 KiB, invalid SKUs, and nonpositive/oversized quantities. An individual command quantity is at most 1,000,000,000. Reservation IDs are UUIDs. SKUs are 1–64 ASCII letters/digits/dot/underscore/hyphen and start with a letter or digit.

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
    "version": 1
  }
}
```

The `reservation` field is absent for a receipt. Available stock is `on_hand - reserved - allocated`. A receipt does not create a catalog product; Catalog is a later context.

Idempotency keys contain 1–128 printable non-space ASCII characters. Scope is `(tenant, operation, key)`, and the request hash includes the typed payload and resource/action identifiers. The two reservation transitions share an operation scope, so use a different key for a different action. The same successful command returns its saved snapshots even if stock changed later. A changed payload returns 409 `idempotency_conflict`. Rejected/rolled-back commands do not reserve the key; they may succeed later if business state changes. Records currently have no expiration and are not automatically deleted.

Repeating the same terminal transition with another key succeeds without another stock mutation or event. Committing a released reservation, or releasing a committed reservation, returns 409. Compensating allocated stock and expiring active reservations are not implemented yet.

`ETag` on stock reads and command outcomes reports the stock version. Conditional requests using `If-Match` are explicitly rejected until an application-level version precondition is implemented; they are never silently ignored. Projection reads are asynchronous and return `X-Consistency: eventual`; they must not authorize another reservation.

Errors use `{"error":{"code":"..."}}`: 400 invalid input/JSON, 401 unauthorized, 404 not found, 409 insufficient stock/idempotency conflict/invalid transition, 415 unsupported media type, 504 deadline, and 500 internal error. Internal database details are logged server-side rather than returned to clients.
