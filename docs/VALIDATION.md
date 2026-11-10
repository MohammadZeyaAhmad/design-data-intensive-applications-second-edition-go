# Validation

## Fifth increment: provenance and audited workflow controls

Recorded **10 November 2026** with the same Go 1.27.3/PostgreSQL 18.6 baseline. `make verify` passed all existing and new live/restart scenarios. The current PostgreSQL suite has **55 integration tests**, including thirteen new lineage/control tests, with race detection and isolated fresh databases. Domain/architecture/HTTP checks and vet also pass.

New database evidence covers correlation and initiating request across checkout, provider response loss, confirmation, shipment dispatch and physical handoff; idempotent order replay preserving accepted provenance; legacy unknown metadata and malformed-envelope rollback; paused-work exclusion; active-lease rejection; effect completion after pause with stale-checkpoint rejection; concurrent operator version races; concurrent duplicate controls sharing one audit identity; audit-write failure rolling back schedule/request state; recovery of an unknown shipment effect using original IDs; operator scope independent of diagnostic actor metadata; immutable first cancellation/dispatch/origin provenance; and continued hold expiry during pause with expiry-worker lineage and compensating refund after resume. HTTP unit tests reject actor/cause/request-ID spoofing and malformed correlation inputs.

`make verify-operations` exercised the actual local operator CLI and authenticated HTTP APIs. It stopped the provider, paused a retrying checkout, restored the provider, restarted API/worker, verified that paused progress and accepted lineage remained unchanged, replayed the same pause receipt, resumed, and recorded one capture after the simulator's lost response. Resource-local audit pagination returned the two original controls with the local operator actor. Ordering and journal metadata retained the accepted request/correlation; warehouse dispatch followed each persisted dispatch-command branch after pause/resume. The report is in ignored `artifacts/operations.json`.

These controls operate on local scheduling and lease fields only. They do not implement production remote operator authentication, OIDC/RBAC, arbitrary checkpoint/financial repairs, independent reconciliation, broker replay, metrics/distributed tracing or governance. A paused resource may still receive an effect from an expired old worker; the explicit counterexample verifies that the old checkpoint fails and resumption deduplicates that effect. See [contracts](OPERATIONS.md) and [recovery](runbooks/workflow-control.md).

## Fourth increment: warehouse inventory and durable Fulfillment

Recorded **10 November 2026** using Go 1.27.3 and the same pinned PostgreSQL 18.6 baseline. The current suite has **42 real PostgreSQL integration tests**, with race detection, isolated fresh databases, immutable facts and journals, plus domain/architecture/HTTP checks, vet, all earlier live scenarios and the new shipment/restart scenarios.

Twelve added database tests cover location-scoped reservation limits and projections, concurrent automatic sourcing across warehouses, pinned single-location SKU lines, fulfillment dispatch acknowledgment loss and duplicate delivery, source-confirmation/request atomic rollback, expected-version operator races and saved responses, physical stock commit without a shipment checkpoint, expired/superseded token rejection, lost handoff response reconciliation, shipping versus compensation, immutable shipment facts/identity, tenant isolation, absence of fulfillment after declined checkout, named-location compensation and warehouse-specific poison-stream quarantine.

`make verify-fulfillment` accepted a two-line checkout sourced from east and west, built two shipments, rejected premature packing and changed dispatch retries, completed pick/pack/dispatch/delivery, retained one payment capture, deducted each physical quantity once, preserved a third balance for the same SKU at another location, and matched authoritative stock with its location projection. Shipment history contained each version exactly once.

`make verify-fulfillment-recovery` accepted dispatch while the worker was stopped, restarted the API, required byte-identical saved dispatch responses, restarted the worker and reached shipped/delivered with the same stock results. Reports are in ignored `artifacts/fulfillment.json` and `artifacts/fulfillment-recovery.json`. This runtime drill covers accepted work across restart; the database counterexample separately exercises the exact physical-commit/checkpoint-loss boundary.

The migration also applied to the populated earlier development database, preserving default stock and old command/event meanings and creating ready requests for old confirmations. Updated API and worker binaries were deployed together. This does not establish a mixed-version rolling upgrade, real carrier effects, single-SKU location splits, returns, production authorization, or generalized manual repair. See [contracts](FULFILLMENT_API.md) and [recovery guidance](runbooks/fulfillment-recovery.md).

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
