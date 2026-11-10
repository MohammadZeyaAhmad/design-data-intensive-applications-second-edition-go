# Catalog, checkout and payment contracts

Use the same authenticated development tenants and API base URL as Inventory and Ordering. This increment connects order intent to server-owned prices, durable stock holds, a simulated provider, and an immutable journal. All amounts are integer minor units in one three-letter currency per quote. There is no currency conversion.

## Catalog

| Method/path | Input | Result |
|---|---|---|
| `PUT /v1/catalog/products/{sku}` | `name`, `unit_price_minor`, `currency`, `expected_version`; `Idempotency-Key` | 200, a product with the new price version |
| `GET /v1/catalog/products/{sku}` | Authentication | Current product/price for this tenant |
| `POST /v1/catalog/quotes` | `{"lines":[{"sku":"BOOK","quantity":2}]}`; `Idempotency-Key` | 201, immutable quote with price versions, total, currency and expiry |
| `GET /v1/catalog/quotes/{id}` | Authentication | The original quote fact, including its expiry |

Version 0 creates a product; subsequent publications require the current version. Old price records and quotes reject ordinary updates/deletes. A quote reads every requested current price in one SQL statement/MVCC snapshot, rejects mixed currencies, checks arithmetic overflow and expires after five minutes. Reading an expired fact is allowed; new checkout acceptance checks its deadline under the order lock. Successful checkout retries use the original saved result even after that deadline.

Commands accept 1–100 distinct SKUs, positive quantities up to 1,000,000,000, and positive unit prices up to 1,000,000,000,000. Total quote amount is at most 9,000,000,000,000,000 minor units. A subsequent catalog price update does not change an accepted order's amount.

## Checkout

Create an order intent and a quote for the same lines, then start:

```http
POST /v1/orders/ORDER_UUID/checkout
Authorization: Bearer TENANT_KEY
Idempotency-Key: CLIENT_CHECKOUT_KEY
Content-Type: application/json

{"expected_version":1,"quote_id":"QUOTE_UUID","payment_method":"test-ok"}
```

The response is 202 with a checkout UUID, order UUID, accepted price snapshot and `checkout_pending` state. Request identity includes the order, expected version, quote ID and payment reference. Caller-supplied totals/prices are rejected as unknown fields. The order's lines must match its quote. The accepted event freezes further ordinary amendments/cancellation, and an outbox durably schedules the workflow.

| Method/path | Meaning |
|---|---|
| `GET /v1/checkouts/{id}` | Current workflow stage, amount/currency, reason, retry count, cancellation request, pause/control version, provenance and update time |
| `POST /v1/checkouts/{id}/cancel` | Empty body; idempotently request cancellation of a nonterminal workflow; 202 acknowledges the request |
| `GET /v1/orders/{id}` | Authoritative domain state, including immutable accepted pricing and eventual confirmation/failure |

The workflow record is created asynchronously, so its first read may return 404 briefly after acceptance. Nonterminal phases are `reserving`, `capturing`, `allocating`, `confirming`, and `compensating`. Final workflow states are `confirmed`, `failed`, and `cancelled`.

A cancellation request can lose a race with confirmation. The source aggregate decides which transition wins; a confirmed order is preserved rather than refunded by a stale compensator. On failure, the order may already show `checkout_failed` while the workflow remains `compensating`. Financial/resource cleanup is complete only when the workflow reaches its final state. A paused workflow waits for an operator resume, including after a cancellation request; see [local controls](OPERATIONS.md).

Inventory now selects one warehouse for each whole SKU line and pins it in the allocation. Confirmation atomically schedules Fulfillment, which creates one shipment per warehouse. See [warehouse and shipment contracts](FULFILLMENT_API.md); post-confirmation changes and real carriers remain separate work. Workflow-owned reservation IDs cannot be committed/released/reversed through ordinary reservation endpoints; the published workflow contract owns their lifecycle.

## Simulated payment methods and journal

| Payment reference | Behavior |
|---|---|
| `test-ok` | Successful capture |
| `test-decline` | Persisted decline; no capture journal entry |
| `test-lost-response` | Provider commits the first capture and drops its response; retry reconciles the same operation |
| `test-slow` | Provider work can outlive the client timeout; lookup/retry reconciles it |
| `test-refund-lost-response` | First close/refund response is lost after provider persistence |

These are local test references. No actual card or external payment account is used. The provider runs in a separate authenticated HTTP process at loopback port 18082 for local development. Its token is generated into ignored `.env`, never printed. It has its own transaction/state namespace in the development PostgreSQL instance. Shared hosting is not a cross-context transaction: provider persistence, Payments journal persistence, Ordering events, and workflow checkpoints commit independently.

`GET /v1/payments/{checkout_id}` returns this tenant's observed payment receipt; `GET /v1/payments/{checkout_id}/journal` returns its immutable entries with executor/correlation/cause/origin-request metadata. Metadata never alters the saved money identity. Capture debits `provider_clearing` and credits `customer_liability`; refund reverses those accounts. One row is a balanced two-leg transfer. Uniqueness prevents duplicate capture/refund entries, amount/currency must match the immutable intent, and a deferred constraint requires a capture before refund. Journal updates/deletes are rejected. This representation does not yet implement arbitrary multi-account journals, partial refunds, authorization/capture separation, or webhooks.

An uncertain response stays retryable; timeout does not mean decline. A close operation can atomically void an unseen provider intent, permanently fencing a delayed capture. If the provider captured but Payments missed the result, refund reconciliation records both capture and refund in one local journal transaction.

Payment references remain in operational scheduling/intent records; they are absent from immutable order events and public checkout status. Production identity, secret storage/retention, least-privilege roles, HTTPS trust and a real provider adapter remain explicit deployment work.
