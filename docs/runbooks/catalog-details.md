# Catalog content publication and model repair

Run normal migrations before starting the updated API/worker pair. Migration `catalog/003_product_details.sql` is additive: existing products/prices/quotes remain valid without detail rows. New details require a published product and brand belonging to the same tenant. The static tenant authentication remains the existing local-development mechanism.

Use `PUT /v1/catalog/brands/{id}` with name and expected version, then `PUT /v1/catalog/products/{sku}/details` with brand ID, description, attributes, variants, categories and expected version. Send JSON content type and an idempotency key. Zero is the first expected revision; successful publication advances it. `GET` on those paths returns current state. Price and detail revisions are independent.

On an uncertain HTTP response, retry the identical request with its original key. A changed payload under that key conflicts. A 409 under a new key requires rereading current state and reconciling the change. An earlier receipt can contain the old joined brand name; it does not promise current presentation. A fresh detail read joins the current brand.

Detail publication locks the product, checks its expected content revision, validates the tenant-local brand, and replaces variants/memberships with its saved result/history in one transaction. Removed child rows must disappear. Immutable content history records publication snapshots and server provenance; it is an audit, not the authoritative event-sourced state machine used by Ordering. Do not edit it to change current content.

`make verify-models-live` checks real HTTP reference rename, request replay, tenant isolation, stale revision rejection, child replacement and unchanged price authority. `make verify-models` runs isolated Go/PostgreSQL modeling evidence and drops only its randomly named test database. Its temporary copied representations are not production serving views; their deliberately stale rename state must not be reused as application data.

For the Go lab, close/reopen the same owned bbolt file after a persistence error; the handle is poisoned with `ErrRecovery` because the outcome may be uncertain. `Repair` reconstructs all copies from the normalized facts and publishes one transaction. It cannot repair corrupt source facts. Unknown bbolt buckets are rejected. Existing files are not adopted as empty fixtures. Native process-kill and power-loss certification are outside this increment's evidence.

For generation drift, run `make generate-catalog`, review SQL and generated Go together, then `make check-catalog-generated`. Ordinary application builds need no sqlc binary. Schema-on-read supports only the explicit legacy migration; unknown model versions/types require a new reviewed migration rather than a silent coercion. Production rollout/backfill, retention and backup/restore remain tracked work.
