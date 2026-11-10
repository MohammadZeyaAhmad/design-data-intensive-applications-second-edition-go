# Validation

## Ninth increment: page-based B+ tree and full-page redo

Recorded **10 November 2026**, using Go **1.27.3**, Linux amd64 and the same pinned bbolt baseline. The custom B+ tree passes map-oracle schedules for orders 4, 6 and 8, with structural checks after mutations. Dedicated schedules verify root growth/collapse, both borrowing and merging, exact separators, linked ranges, copied/binary/empty-value handling, writer exclusion and reuse of the original page allocation after deletion and reopening.

The shared filesystem model passed **1,338 data/WAL mutation cuts**, with **190 additional recovery interruptions** inside those schedules, plus first-open fault schedules. It distinguishes buffered data from early/torn persistence, including partial page overwrites. Every recovered generation matches the exact oracle map at an allowed submitted prefix, with complete page accounting. The native suite covers **18 checkpoint/recovery cases**: ten root-split/checkpoint boundaries and eight recovery boundaries, with a preliminary committed-WAL kill before each recovery case.

Recovery tests repair a torn leaf, torn metadata and truncated DATA from a valid redo transaction. Missing files, complete checksum damage, stale/foreign redo and sequence gaps fail closed. Every incomplete prefix of a valid redo frame is checked, and an exhaustive checksum-valid mutation sweep exercises semantic bounds. Structure-aware fuzzing recomputes framing/page checksums to reach internal decoders: the page target completed **190,138 executions** and the redo target **5,989 executions** in bounded eight-second runs with two workers, without failure. These counts describe finite checks, not proofs over all faults.

The default tree report uses **512 synthetic keys, three repetitions**, synchronous mutations and overlayfs in this workspace. After 171 overwrites and 74 deletes it contains **438 live keys**, height **4**, and **154 allocated non-metadata pages**, of which **54 are free**. Its structural check reports 83 leaves and 17 branches. Deleting the live set, reopening, and reinserting it reuses **134 pages** while keeping the same allocation high-water mark. The independent bbolt alternative has the same logical digest. The combined storage report now includes all **five engines**.

The small eight-key capacity and 256-byte inline-value limit are reported explicitly. Full-page redo writes whole modified paths, and the simple separator implementation does extra reads. Page/WAL byte counters and raw timings are workload evidence; they do not establish a hardware-wide performance ranking. Process kills retain the OS cache, while power-loss claims are limited to the declared model and honest fsync/rename assumptions.

Both storage engines now share the low-level file/lock contract, common key/value types and deterministic fault model. LSM format and write/publication behavior remain unchanged and its regression suite passes. Reports are ignored `artifacts/btree.json` and `artifacts/storage.json`; CI retains both. Scoped Make and root guide verification require no application credentials/services. Application and standalone lab builds remain separate.

The [B+ tree scenario](../scenarios/storage-btree.json) brings the tracker to **nine scenario manifests**, with all 88 original packages retained: **22 in progress, 66 planned**. C04-02 remains in progress for its complete sub-concept audit, overflow pages, denser formats and broader scale/concurrency variants. See the [guide](BTREE.md), [chapter](chapters/ch04/page-based-btree.md) and [runbook](runbooks/btree-recovery.md).

## Eighth increment: persistent LSM and embedded storage comparison

Recorded **10 November 2026** with Go **1.27.3** on Linux amd64 and bbolt **1.4.3**. Application and lab race tests, architecture checks, vet, separate application/lab builds, the scoped storage target and the root guide command passed. The original application integration suite remains 71 PostgreSQL, three Kafka and three Temporal tests; this increment adds storage tests in the independent lab module.

Both compaction styles passed random insert/update/delete/get/scan schedules against a map, with repeated reopening. Dedicated tests cover partial-merge tombstone retention, older data rewritten into a newer file, safe full-compaction deletion, empty values, binary keys, copied results, same-process and cross-process writer exclusion, corruption/missing-file rejection, every incomplete final-frame length, and sparse-index seeks for points and ranges. Repeated overwrites of one large value also force WAL rollover/compaction while the live memtable stays small.

An independent filesystem model explores a crash after every mutation of a fixed multi-checkpoint schedule across both styles and sync/async modes. Its variants distinguish inode contents from directory names, allow early data/manifest-rename/deletion persistence and exercise short writes. Recovery must retain the required acknowledged/fenced prefix and match the complete map at its recovered sequence. The real filesystem suite additionally SIGKILLs child processes at **21 named boundaries** covering WAL writes/syncs, table writes/syncs/renames, both directory fences, manifest publication, WAL reset and compaction cleanup.

The record decoder fuzz target completed **313,440 executions in eight seconds with two workers**, without failure. Real process-kill tests ran on the system temporary filesystem; that validates process recovery with an intact OS page cache. Power-loss evidence comes from the declared filesystem model, not a physical device power cut.

The default comparison used **512 synthetic warehouse keys, three fresh runs per engine**, synchronous individual mutations, 171 overwrites and 74 deletes. Append-log, size-tiered LSM, leveled LSM and bbolt produced identical state digests after reopening. Before full compaction their observed file sizes were 90,136; 95,709; 93,691; and 262,144 bytes respectively. These include each format's metadata/allocation behavior. The full-compaction check preserved live state and reduced the two LSM layouts to about 58.5 KB.

Reports identify Go/OS/architecture, exact engine options, row/repetition counts, sync policy, filesystem type/block size, cache assumptions, raw timings/percentiles and instrumented bytes/counters. Initial system-temp trials reported `tmpfs`; the default report command now creates private temporary children beside its output and reports **overlayfs** in this workspace. Physical backing hardware is unidentified, caches are uncontrolled, and the measurements establish no general performance ranking. `-workdir` allows an explicit parent filesystem without adopting or deleting a caller's database.

Generated comparison evidence is in ignored `artifacts/storage.json`; CI retains it as `storage-comparison`. The [storage manifest](../scenarios/storage-lsm.json) joins the other seven scenario manifests, retaining the original 88-package scope: **21 packages in progress, 67 planned**. C04-01 remains in progress for the full sub-concept audit, compression and broader format/scale stress. Custom B+ tree, controlled warm/cold/device benchmarks and the remaining storage packages are still planned. See the [guide](STORAGE.md), [chapter walkthrough](chapters/ch04/log-structured-storage.md) and [recovery runbook](runbooks/storage-recovery.md).

## Seventh increment: Temporal durable execution

Recorded **10 November 2026**, using Go 1.27.3, PostgreSQL 18.6, Temporal Go SDK **1.37.0** and pinned Temporal CLI **1.4.1** development-server image. The normal suite now has **71 PostgreSQL integration tests**, with **three real-Kafka** and **three real-Temporal** tests behind dedicated profiles. Each real-Temporal worker-loss test covers five stage subcases: reserve, capture, allocation, confirmation and compensation.

The six new database tests verify immutable engine ownership and profile-change request replay, both engines' refusal of the other's work, lost-completion receipt replay, stale checkpoint rejection, pause/native-cancel wake persistence, uncertain start/signal acknowledgments, immutable receipts and rollback when receipt insertion fails. Unit tests verify durable wake scheduling, disconnected cancellation, ordinal-preserving continuation (including repeated activity cancellation) and replay of a committed synthetic v0 history.

`make integration-temporal` passed with race detection against a real isolated server. A stopped SDK worker left an effect without a checkpoint at each external stage; replacements converged without duplicate holds or captures. These cases explicitly expire test leases to avoid thirty seconds per subcase. Native cancellation after capture produced one capture and one refund. A history generated by the pre-version-marker workflow, including an old one-second timer, replayed successfully under the current versioned implementation. Duplicate start calls before and after completion retained the same run.

The Temporal profile passed the existing live contention, inventory lifecycle, Ordering, checkout, checkout recovery, warehouse Fulfillment, shipment recovery, local operator and projection/API recovery checks. `make verify-temporal` additionally accepted/replayed a checkout while Temporal was stopped, SIGKILLed the real server/worker and recovered the same persisted workflow run, then killed a worker during a slow provider activity and recovered after natural lease expiry. Each recovered purchase kept its frozen 2,500 minor-unit total and one capture journal entry. The connection configuration was corrected during validation so gRPC preserves the local service hostname for `NO_PROXY`.

The managed workspace uses an 8 GiB writable layer and Docker's `vfs` storage driver. To fit that budget, the final isolated Kafka/Temporal regressions used the same race-enabled test cases in a precompiled runner, with other service profiles stopped. CI retains the standard Make commands.

Reports are in ignored `artifacts/temporal-recovery.json`, `artifacts/temporal-v0-history.json` and the ordinary HTTP reports. The retained v0 fixture and version branch run in the default unit suite. The seven scenario manifests preserve the original 88-package scope; 20 packages are now in progress and 68 remain planned. No full work package is signed off.

The development server uses one persistent SQLite volume. This validates process restart, without establishing replicated Temporal durability, production history retention/repair, remote TLS/identity or worker deployment routing. PostgreSQL owner records still provide durable business deduplication after Temporal history retention. Combined Kafka/Temporal operation is not certified. See the [profile](TEMPORAL.md) and [recovery runbook](runbooks/temporal-recovery.md).

## Sixth increment: Kafka transport and receipt failure boundaries

Recorded **10 November 2026** with Go 1.27.3, PostgreSQL 18.6 and pinned Apache Kafka **4.1.1**. The normal PostgreSQL suite has **65 integration tests**, with **three additional real-Kafka tests** in the dedicated local broker profile. `make verify` passed in the PostgreSQL profile, including race/unit/architecture checks and vet. The Kafka profile also passed every existing HTTP/lifecycle/restart scenario, including Ordering, checkout, warehouse Fulfillment and operator recovery.

Ten new database tests cover uncertain source publication, locked publication/inbox prefixes, duplicate owner invocation after owner commit, durable invalid-wire rejection, immutable identity conflicts and audited retry, poison-stream isolation on a shared partition, authoritative-source validation, Ordering projection/rebuild convergence, and request identity across Kafka/local profile fallback. These database counterexamples use an injected publisher where appropriate; they are distinct from broker evidence.

The isolated Kafka tests produced two physical records for one logical event after an injected post-broker-acknowledgment error, lost the consumer checkpoint after durable staging, verified redelivery and one projection receipt, injected database-stage failure before receipt, and observed a stable two-member consumer group before removing a member and publishing the remainder of an 18-event stream. All resulting owner projections matched their source prefix. The separate test broker runs at loopback 19093; its PostgreSQL database is disposable. It never uses the development Kafka cluster for these injected failure tests.

`make verify-kafka` stopped the development broker consumer after source publication and observed the absent projection, then restored consumption. It stopped Kafka, accepted another real HTTP stock command, killed the publisher and restored both using retained volumes. The projection caught up. New physical duplicate records retained one owner effect; malformed wire was durably rejected; a conflicting logical identity fenced later work for that stream; and the actual scoped CLI retried the retained original with an immutable audit/saved response. The report is in ignored `artifacts/kafka.json`.

This is one broker, replication factor 1, one application inbox generation and a modular application using owner schemas in one PostgreSQL instance. It does not demonstrate replicated broker failover, certified broker data-loss/retention-overrun recovery, Kafka transactions, CDC, independent databases, production broker security, generalized offset migration or the planned segmented-log implementation. See [contracts](MESSAGING.md) and [recovery](runbooks/kafka-recovery.md).

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
