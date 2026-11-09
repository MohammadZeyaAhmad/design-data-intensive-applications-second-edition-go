# Validation of the first implementation increment

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
