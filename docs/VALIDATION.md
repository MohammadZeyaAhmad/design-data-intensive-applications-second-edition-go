# Validation

## Third increment: Catalog, durable checkout and simulated payments

Recorded **10 November 2026** using the same Go 1.27.3/PostgreSQL 18.6 development baseline. `make verify` passed: race/unit/architecture tests, vet, all **30 real PostgreSQL integration tests**, source/progress checks, the existing labs/scenarios, and new checkout/restart scenarios. Each integration run used a fresh disposable database with event/journal immutability enabled.

The new database tests verified immutable price versions/quotes, one winner among ten concurrent price updates, accepted-price preservation, cross-tenant quote isolation, one capture after a lost provider response, journal immutability and balance, pending-intent close markers, declined checkout compensation, partial allocation failure with lost refund response, stale lease rejection, closed-resource protection against delayed calls, dispatch redelivery, invalid/expired quote rejection, fixed financial identity, refund constraints, and refund reconciliation of a capture absent locally.

The real HTTP scenarios completed successful, lost-response and declined checkouts. Repricing a product from 1,250 to 9,000 minor units after acceptance still captured the accepted 2,500 total for two items. Successful/lost-response paths each recorded one capture transfer; decline released inventory without a capture entry. Cross-tenant workflow reads returned not found.

`make verify-checkout-recovery` stopped the separate provider, observed a durable retryable capture, stopped/restarted the worker and provider, and reached confirmation with one journal capture. Repeating accepted checkout identity returned the original attempt. Existing inventory/Ordering restart checks also passed. Reports are in ignored `artifacts/checkout.json` and `artifacts/checkout-recovery.json`.

The provider is a development HTTP simulator with its own transaction namespace in the same PostgreSQL instance. These checks do not establish behavior of an actual bank, independent physical infrastructure, authorization/capture separation, partial refunds, webhooks, or Fulfillment. Financial rows implement balanced two-leg transfers; general multi-account accounting and broad operational/audit tooling remain tracked work.

## Second increment: reservation lifecycle and event-sourced Ordering

Recorded **10 November 2026**, using the same pinned Go/PostgreSQL development baseline below. All 19 real PostgreSQL integration tests passed with race detection in a fresh, disposable database. Domain, HTTP and architecture race tests and `go vet` also passed. The Ordering fuzz target completed **78,627 executions** with four workers in a bounded run.

The real HTTP scenarios verified:

- 100 competing reservations for ten units: 10 accepted, 90 rejected.
- A one-second hold was expired by the worker, a late commit was rejected, and explicit allocation reversal was applied once.
- 20 amendments expecting order version 1: one accepted and 19 version conflicts; history contained versions 1, 2 and 3.
- Operator snapshot at version 2, reconstruction at version 3, and agreement with the asynchronous view.
- Both inventory and order commands committed during worker downtime; their projections caught up after restart. Retrying both commands after API restart returned their original saved responses.

Additional database tests cover post-lock expiry time, expiry/release races, committed allocation protection, immutable event guards, transactional rollback of a partly applied event batch, legacy event upcasting, unsupported-schema quarantine, snapshot checksum/schema fallback, keyset pagination, concurrent appends during reconstruction, deletion/reconstruction of a view, and resumption of live processing.

Run `make verify-lifecycle verify-orders verify-recovery` after `make up`. `make integration` now creates and drops its own `atlas_test_run_*` database under the explicit local test binding and requires database-creation permission. It never disables the event immutability trigger to clean up.

Reports are in ignored `artifacts/inventory-lifecycle.json`, `artifacts/orders.json`, and `artifacts/recovery.json`. The two versioned scenario manifests link verified behaviors to package IDs and evidence paths. Entire-package completion remains a separate review gate.

This increment adds current-state order intent without Catalog price authority or payment/shipment orchestration. Per-stream rebuild briefly blocks that stream; all-stream shadow-generation migration, full correlation/actor envelopes, Kafka delivery, retention/erasure, and production database roles remain tracked work.

## First increment evidence

Recorded **10 November 2026** in a managed Linux amd64 development environment, using **Go 1.27.3**, Docker **28.4.0**, and a pinned PostgreSQL image whose running server reports **18.6**. The database, API, and worker were real local processes/containers. These results do not establish the eventual production SLOs or full-book completion.

## Checks and outcomes

| Check | Observed outcome |
|---|---|
| Domain, HTTP and architecture tests with `go test -race ./...` | Passed; invalid quantities/state, aggregate transitions, input limits, credential-bound tenancy, and import boundaries checked |
| `go vet ./...` | Passed |
| Separate lab module build and execution | Passed; explicit lost-update schedule yielded 1 versus 2 for indivisible increments |
| Real PostgreSQL integration suite with race detection | Passed; eight integration tests, including concurrent requests and durable transaction effects |
| 100 competing one-unit reservation requests for ten units | Exactly 10 succeeded and 90 were rejected; stock remained valid |
| 40 concurrent requests with the same idempotency key | Exactly one fresh execution and one reservation; saved responses agreed |
| Same key with a different quantity | Rejected as an idempotency conflict |
| Injected failure after state change and outbox append | State, event and key rolled back; retry subsequently succeeded |
| Commit versus release race | One valid terminal transition; no contradictory second effect |
| Cross-tenant read and mutation | Denied/absent in the requesting tenant; independent tenant state preserved |
| Locked stream head with another eligible stream | Later events did not overtake the lock; unrelated stream progressed |
| Two projection workers and duplicate delivery | Final projection matched authoritative state; no duplicate receipt/effect |
| Poison event quarantine and repair | Affected stream paused, unrelated stream continued, and original-ID repair resumed in order |
| Migration re-entry and checksum drift | Already-applied migrations were safe; changed migration history was rejected |
| Live HTTP test | 100 requests: 10 accepted, 90 rejected; unauthorized and cross-tenant reads failed; projection caught up |
| Worker outage and restart | Commands committed while worker was stopped; their projection appeared correctly after restart |
| API restart and command replay | Retried reservation returned byte-for-byte the same saved response |
| Coverage inventory | 476 sections, 61 glossary terms, 4,215 index occurrences, and 48 pattern entries retained |
| Application build with `GOWORK=off` | Passed independently of educational lab dependencies |

The domain fuzz target additionally completed **74,811 executions** in a short bounded run with four workers. Fuzzing checks that rejected transitions leave state unchanged and available/reserved/allocated counters remain valid. A short run is additional evidence, not exhaustive proof.

## Reproduce

```sh
make up
make verify
go test -fuzz=FuzzStockOperations -fuzztime=3s -parallel=4 ./internal/inventory/domain
GOWORK=off go build ./cmd/atlas ./cmd/guide
```

Integration tests require `ATLAS_TEST_DATABASE_URL` targeting the local `atlas_test` database and fail rather than skip without it. They create unique synthetic tenants and clean up only those tenants. Live scenarios create uniquely named synthetic SKUs in the local development tenants. Recovery scenarios deliberately stop/start this Compose project's worker and restart its API.

Generated `artifacts/live.json` and `artifacts/recovery.json` contain outcome summaries without credentials. The CI workflow runs unit/race checks, real PostgreSQL tests, the source inventory/lab, and live API/restart scenarios. Its remote execution status is separate from these local results.

Not covered by this increment: remote broker acknowledgment, provider-side payment effects, actual database failover, point-in-time restore, shard migration, all possible process-crash instruction boundaries, production identity/TLS, benchmarks against the planned SLOs, or complete chapter implementations. These remain explicit backlog items.
