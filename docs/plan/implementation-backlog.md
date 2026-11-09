# Implementation backlog

This is a build plan. Every package below is **planned**, not implemented. All packages and catalog entries belong to the full repository scope; runtime profiles are only a way to limit local resource use.

Read [the end-to-end plan](ddia-go-mega-project-plan.md) first. The stable package IDs join this backlog to the section, glossary, index, and pattern CSVs and to [the machine-readable backlog](work-packages.json).

## Mandatory completion contract

For every package: implement its full declared behavior, wire it into the shared guide runner and domain datasets, write an original explanation, provide a meaningful baseline/counterexample where applicable, verify recovery and edge cases, and publish a reproducible evidence report. Every mapped source concept and pattern needs a named scenario or decision exercise. Passing a broad smoke test is insufficient. Theory, legal, and societal concepts use explicit assumptions, executable models or synthetic scenarios, and decision records; they are not relabeled as implemented production algorithms.

Commands in this document are target interfaces to implement. They do not exist yet. Expected failures must be asserted as successful demonstrations, never swallowed as generic test failures.

## Milestones

| Milestone | Focus | Focused engineer-weeks | Exit evidence |
|---|---|---:|---|
| M00 | Scope, foundation, and deterministic harness | 1–2 | Source inventory, repository skeleton, data ownership, reproducible simulation, architecture ADRs. |
| M01 | Walking skeleton and transactional domain | 3–5 | Catalog, tenant-aware inventory, one real vertical slice per context, ACID experiments. |
| M02 | Event sourcing, CQRS, and reliable messaging | 3–5 | Versioned event store, snapshots, outbox/inbox, read models, replay, duplicate suppression. |
| M03 | Complete checkout and durable workflows | 3–5 | Provider simulator, balanced journal, saga and Temporal alternatives, identity/API contracts. |
| M04 | Operational baseline and overload behavior | 3–5 | Telemetry, SLOs, load generator, resilience, fault proxy, container and CI baseline. |
| M05 | Storage and indexing implementations | 4–7 | LSM, B+ tree, column storage, text/vector/geospatial indexes, crash and benchmark reports. |
| M06 | Data models and schema evolution | 3–5 | Relational/document/graph/RDF/Datalog/GraphQL comparisons, codecs, migrations, actors, timelines. |
| M07 | Replication, causality, and offline synchronization | 4–6 | Primary/replica profiles, backups, leaderless/multi-leader models, CRDT/OT clients, object storage. |
| M08 | Sharding and transaction semantics | 4–6 | Resharding, routing, hotspots, isolation anomalies/fixes, 2PL/SSI/2PC/3PC models. |
| M09 | Consensus and distributed correctness | 4–7 | Linearizability checker, Raft, etcd, logical clocks, fencing, Byzantine-boundary exercises. |
| M10 | Batch analytics and responsible data preparation | 4–6 | Batch DAG engine, joins, warehouse/lake flows, DataFrames, ML datasets, synthetic bias experiment. |
| M11 | Streaming and change-data capture | 4–6 | Broker/log models, real CDC handoff, windows, temporal joins, checkpoint and sink recovery. |
| M12 | Integrated dataflow and online evolution | 4–6 | Projection rebuild/cutover, extraction comparison, client sync, strict/relaxed constraints, auditing. |
| M13 | Privacy and data lifecycle | 2–4 | Consent/export/erasure/retention and replay/backup governance; hosting responsibility decisions. |
| M14 | Full concept and pattern audit | 2–4 | All section/glossary/index concepts reviewed, guide completed, missing sub-concepts implemented. |
| M15 | Production reference release | 3–5 | Full acceptance, infrastructure support matrix, release/rollback and recovery evidence, v1.0 guide. |

Total planning range: **51–84 focused engineer-weeks** before contingency. These are rough scope estimates, not delivery commitments. Milestones sequence dependencies; work within one milestone follows the package dependency graph.

## M00 — Scope, foundation, and deterministic harness

### APP-01 — Repository, guide runner, and reproducible development

**Mode:** application. **Status:** planned. **Depends on:** none.

**Target location:** `cmd/guide; Makefile; docs; deploy/compose`.

**Build:** Create Go application and lab modules, a go.work development workspace, pinned tooling, seeded fixtures, profile manifests, coverage schema, CI skeleton, and a Go guide CLI. Provide doctor, up, seed, verify, report, and scoped reset commands.

**Acceptance:** From a clean checkout, one documented command starts the core, seeds two tenants, and runs a smoke test. Reset requires an explicit disposable environment and never targets an arbitrary database. Every planned command is registered and documentation examples are checked.

**Verification interface:** `make verify ID=APP-01`.

### C01-01 — Architecture trade-offs and deployment boundaries

**Mode:** decision + runnable comparison. **Status:** planned. **Depends on:** APP-01.

**Target location:** `docs/adr; labs/ch01/architecture`.

**Build:** Create an architecture decision matrix for frontend/backend responsibilities, operational versus distributed deployment, cloud versus self-hosting, microservices/serverless, and HPC assumptions. Run the same Go operation in-process, over HTTP, and as a stateless worker; model latency and operating cost.

**Acceptance:** Record comparable workload, failure, latency, and cost assumptions. Explain which costs and failure modes move between deployment models and select the modular monolith as the initial application shape.

**Verification interface:** `make verify ID=C01-01`.

**Book section IDs:** B01-001, B01-008, B01-009, B01-014, B01-015, B01-016, B01-017.

### C09-06 — System models, safety/liveness, and deterministic simulation

**Mode:** shared Go laboratory infrastructure. **Status:** planned. **Depends on:** APP-01.

**Target location:** `labs/internal/sim; labs/ch09/model-checking`.

**Build:** Build a discrete-event scheduler with injectable clock, random source, network, disk, crashes, and restart; serialize seeds and event traces. Add finite-state exhaustive exploration and an optional TLA+ specification comparison for a lease/commit protocol.

**Acceptance:** A failing seed reproduces the same trace byte-for-byte and can be minimized. Check safety during faults and liveness only after stated delivery/majority assumptions recover. Only code routed through the scheduler is called deterministic; neither arbitrary goroutines nor PostgreSQL are thereby made deterministic.

**Verification interface:** `make verify ID=C09-06`.

**Book section IDs:** B09-036, B09-037, B09-038, B09-039, B09-040, B09-041, B09-042, B09-043, B09-044.

**Additional pattern obligations:** P42; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M01 — Walking skeleton and transactional domain

### APP-02 — Domain model and vertical slice boundaries

**Mode:** application. **Status:** planned. **Depends on:** APP-01.

**Target location:** `internal/{catalog,inventory,ordering,payments,fulfillment}; docs/domain`.

**Build:** Define ubiquitous language, context map, aggregates, state machines, ownership, published contracts, and ports. Build one end-to-end slice per context, with concrete composition roots and adapters; record architecture decisions and enforce dependency rules.

**Acceptance:** A real HTTP request reaches a use case, domain rule, PostgreSQL adapter, and response. Import checks prohibit another context's private domain/persistence types and production imports of lab modules. Domain state transitions have invariant tests.

**Verification interface:** `make verify ID=APP-02`.

**Additional pattern obligations:** P01, P02, P03, P04, P05, P06, P07, P46; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### APP-03 — Inventory reservations and contention

**Mode:** application. **Status:** planned. **Depends on:** APP-02.

**Target location:** `internal/inventory/features; internal/inventory/adapters/postgres`.

**Build:** Implement tenant-scoped stock, reservations, expiration, commit, release, and compensating allocation reversal. Use conditional updates, unique reservation keys, database constraints, and a transaction boundary per inventory aggregate.

**Acceptance:** With stock 10 and 100 concurrent requests, at most 10 reservations succeed. Duplicate, cancellation, expiry, and commit races preserve available/reserved/committed accounting. Payment cannot silently resurrect an expired reservation.

**Verification interface:** `make verify ID=APP-03`.

**Additional pattern obligations:** P02, P08, P19; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C08-01 — ACID and transaction error semantics

**Mode:** database integration lab. **Status:** planned. **Depends on:** APP-03.

**Target location:** `labs/ch08/acid`.

**Build:** Exercise single/multi-object operations, rollback, constraints, fsync/durability configuration, connection loss during commit, and whole-transaction retries. Tie atomicity and application invariants to inventory/journal examples.

**Acceptance:** A failed local transaction has no partial durable business effect. Ambiguous commit outcomes are reconciled by an idempotency key. Record exactly which crash/failure model durability covers; ACID consistency does not validate business rules automatically.

**Verification interface:** `make verify ID=C08-01`.

**Book section IDs:** B08-001, B08-002, B08-003, B08-004, B08-005, B08-006, B08-007, B08-008, B08-009, B08-010, B08-011.

**Additional pattern obligations:** P08; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M02 — Event sourcing, CQRS, and reliable messaging

### APP-04 — Durable event-sourced ordering

**Mode:** application. **Status:** planned. **Depends on:** APP-02, APP-03.

**Target location:** `internal/ordering/domain; internal/ordering/adapters/eventstore`.

**Build:** Build a PostgreSQL event store with expected-version append, tenant-scoped stream uniqueness, immutable domain events, validated commands, deterministic replay, versioned snapshots, and upcasters. Commit events, outbox records, and request deduplication atomically.

**Acceptance:** Concurrent commands on one expected stream version cannot both win. Replaying all events and replaying from a validated snapshot yield identical state. A crash after commit but before response returns the original result on retry. Unknown schema versions stop safely.

**Verification interface:** `make verify ID=APP-04`.

**Additional pattern obligations:** P02, P03, P08, P10, P19, P47; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### APP-06 — Transactional outbox, inbox, and ordering

**Mode:** application. **Status:** planned. **Depends on:** APP-01, APP-04.

**Target location:** `internal/platform/messaging; internal/*/adapters/postgres`.

**Build:** Implement transactional outbox polling and a Kafka adapter, leased publishing, per-stream ordered delivery, consumer inbox deduplication, bounded retries, quarantine, and replay tooling. Make each inbox record and its business effect one local transaction.

**Acceptance:** Crash at each boundary before/after publish, transaction commit, and acknowledgment. Every committed event eventually propagates after recovery; duplicate transport deliveries cause one durable local effect. A stream gap blocks or repairs that stream without skipping its events.

**Verification interface:** `make verify ID=APP-06`.

**Additional pattern obligations:** P11, P12, P18; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C03-06 — CQRS and rebuildable read models

**Mode:** application. **Status:** planned. **Depends on:** APP-04, APP-06.

**Target location:** `internal/ordering/features/getorder; internal/projections`.

**Build:** Separate command handling from read handlers and schemas; project immutable ordering events into order details, tenant dashboards, and fulfillment views. Expose projection versions and freshness tokens; support synchronous-local and asynchronous projection comparisons.

**Acceptance:** Queries use read models without mutating aggregates. Read-your-writes waits to a bounded deadline or returns an explicit pending status. Deleting a projection and replaying events gives the same result; command validation never relies on a stale read model.

**Verification interface:** `make verify ID=C03-06`.

**Book section IDs:** B03-026.

**Additional pattern obligations:** P09; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C08-08 — Exactly-once effects within a transaction boundary

**Mode:** application + lab. **Status:** planned. **Depends on:** APP-06.

**Target location:** `labs/ch08/message-effects; internal/platform/messaging`.

**Build:** Compare XA-style atomic acknowledgment with inbox/deduplication plus a local transaction. Implement idempotency-key retention policies that outlive supported replay and reconciliation windows.

**Acceptance:** A crash after commit but before broker acknowledgment leads to redelivery and one database effect. An external provider call cannot be made exactly once by a local inbox alone; its adapter must use provider idempotency and reconciliation.

**Verification interface:** `make verify ID=C08-08`.

**Book section IDs:** B08-057, B08-063.

**Additional pattern obligations:** P12; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M03 — Complete checkout and durable workflows

### APP-05 — Payments, immutable journal, and provider simulator

**Mode:** application. **Status:** planned. **Depends on:** APP-02, APP-04.

**Target location:** `internal/payments; cmd/provider-sim`.

**Build:** Build a Go payment provider simulator and adapter with authorization, capture, void, refund, webhook delivery, and lookup by idempotency key. Maintain an immutable double-entry journal in fixed minor units, one currency per balanced transaction; use compensating postings for corrections.

**Acceptance:** Lost responses, reordered webhooks, and repeated captures never create duplicate postings. Debits equal credits per currency; refunds never exceed captured funds. Unknown provider outcomes are reconciled before another action. No real payment network is required.

**Verification interface:** `make verify ID=APP-05`.

**Additional pattern obligations:** P02, P04, P33; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### APP-07 — Checkout saga and fulfillment

**Mode:** application. **Status:** planned. **Depends on:** APP-03, APP-04, APP-05, APP-06.

**Target location:** `internal/ordering/features/checkout; internal/fulfillment`.

**Build:** Persist an explicit checkout process manager that reserves stock, authorizes/captures a simulated payment, commits inventory, confirms an order, and requests fulfillment. Persist deadlines, correlation IDs, step idempotency keys, compensations, and manual-recovery states.

**Acceptance:** Test crash/restart and cancellation races at every step. An order becomes confirmed only after inventory commitment and captured payment are known. Failure after payment capture produces a refund/reconciliation workflow; compensation is retriable and never described as ACID rollback.

**Verification interface:** `make verify ID=APP-07`.

**Additional pattern obligations:** P03, P13, P47; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### APP-08 — API, identity, and tenant isolation

**Mode:** application. **Status:** planned. **Depends on:** APP-02, APP-04.

**Target location:** `internal/platform/auth; api/openapi; internal/*/features`.

**Build:** Implement REST/OpenAPI, validation, stable errors, tenant-scoped authorization, RBAC, keyset pagination, request size limits, idempotency keys with request hashes, and read-your-writes tokens. Use an OIDC adapter and an explicit local development identity issuer.

**Acceptance:** Cross-tenant identifiers cannot read or mutate resources. Same key/different payload is rejected; same key/same payload replays its stored result. Invalid/expired/wrong-audience tokens fail. Pagination is deterministic and authorization applies to exports, events, and derived views.

**Verification interface:** `make verify ID=APP-08`.

**Additional pattern obligations:** P05, P15, P37, P38; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C05-05 — Durable execution with Temporal

**Mode:** application alternative adapter. **Status:** planned. **Depends on:** APP-07.

**Target location:** `internal/ordering/workflows/temporal; deploy/compose/workflows`.

**Build:** Implement the checkout saga using Temporal Go workflows/activities alongside the explicit PostgreSQL process manager. Persist timers and signals; use deterministic workflow code, activity idempotency, and supported workflow versioning.

**Acceptance:** Kill workers during each activity and resume without duplicate provider effects. Replay old workflow histories under a compatible code update. Both orchestrators pass the same business outcome suite, with separate profiles preventing simultaneous ownership.

**Verification interface:** `make verify ID=C05-05`.

**Book section IDs:** B05-024, B05-025.

**Additional pattern obligations:** P14, P47; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C13-05 — End-to-end request identity and duplicate suppression

**Mode:** application + failure lab. **Status:** planned. **Depends on:** APP-05, APP-06, APP-07.

**Target location:** `labs/ch13/end-to-end; internal/platform/idempotency`.

**Build:** Carry scoped request IDs through HTTP, saga steps, broker events, provider calls, and journal postings. Persist request hashes and results; specify key expiry, replay horizons, authorization, and reconciliation responsibilities.

**Acceptance:** Retry checkout after failures at all boundaries and assert one logical purchase/payment effect. Demonstrate why broker guarantees or TCP alone do not provide end-to-end exactly-once behavior. Expired keys cannot silently enable historical financial duplication.

**Verification interface:** `make verify ID=C13-05`.

**Book section IDs:** B13-030, B13-031, B13-032, B13-033, B13-034, B13-035, B13-036.

**Additional pattern obligations:** P15; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M04 — Operational baseline and overload behavior

### APP-09 — Caching, resilience, and overload control

**Mode:** application. **Status:** planned. **Depends on:** APP-06, APP-08.

**Target location:** `internal/platform/resilience; internal/catalog/adapters/cache; cmd/loadgen`.

**Build:** Implement cache-aside, version-aware invalidation, TTL jitter, singleflight, negative caching, deadlines, bounded queues, retry budgets with jitter, circuit breakers, bulkheads, admission control, and graceful shutdown. Use Go channels with explicit ownership and bounded concurrency.

**Acceptance:** A cache outage preserves authoritative correctness; a hot-key expiry does not stampede the database. Retry amplification stays within a configured budget. Overload is shed predictably, canceled calls release resources, and recovery succeeds without restarting healthy components.

**Verification interface:** `make verify ID=APP-09`.

**Additional pattern obligations:** P23, P24, P25, P26, P27, P28, P46, P48; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### OPS-01 — Observability and delivery baseline

**Mode:** operations. **Status:** planned. **Depends on:** APP-07, APP-08, APP-09.

**Target location:** `internal/platform/telemetry; deploy; .github/workflows; docs/runbooks`.

**Build:** Add structured redacted logs, OpenTelemetry traces, Prometheus metrics, dashboards, SLO recording rules, readiness/liveness, container builds, migration jobs, supply-chain checks, and release artifacts. Establish Helm and cloud-reference infrastructure plans with separate service credentials.

**Acceptance:** Trace checkout across HTTP, events, workflow steps, and database calls. Alerts trigger on injected consumer lag and integrity failures. Rolling a compatible binary version preserves traffic; migration locking prevents two jobs running concurrently. No secrets or personal payloads appear in telemetry.

**Verification interface:** `make verify ID=OPS-01`.

**Additional pattern obligations:** P36, P39, P40; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C02-02 — Workload models, latency percentiles, and capacity

**Mode:** application + lab. **Status:** planned. **Depends on:** APP-09.

**Target location:** `cmd/loadgen; labs/ch02/performance; docs/slo`.

**Build:** Build a Go open-loop load generator, histogram export, fan-out tail-latency experiments, throughput/queueing curves, and saturation/recovery scenarios. Separate service time, queueing, network delay, and end-to-end response time. Include exact sorted quantiles, an HDR-style histogram, and a Go t-digest implementation/integration with mergeable summaries.

**Acceptance:** Publish p50/p95/p99 from merged histogram samples rather than averaged percentiles. Include timeouts and errors, avoid coordinated omission, record arrival rate, and demonstrate the knee of the capacity curve without claiming unmeasured production capacity. Quantify approximation error across skewed distributions and demonstrate why averaging percentiles is wrong.

**Verification interface:** `make verify ID=C02-02`.

**Book section IDs:** B02-004, B02-005, B02-006, B02-007, B02-008, B02-009, B02-010, B02-018, B02-019, B02-021.

**Additional pattern obligations:** P27, P39; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C02-03 — Reliability, topology, and maintainability

**Mode:** operations + lab. **Status:** planned. **Depends on:** OPS-01.

**Target location:** `labs/ch02/reliability; docs/runbooks; docs/adr`.

**Build:** Compare shared-memory, shared-disk, and shared-nothing resource models; exercise hardware-like, software, and operator faults. Define error budgets, operating procedures, safe configuration changes, and evolvability criteria.

**Acceptance:** Inject a disk-full condition, process failure, bad configuration, and overloaded dependency. Observe bounded failure and documented recovery. Record redundancy limits, blast radius, and maintenance costs; a second process on one host is not a second failure domain.

**Verification interface:** `make verify ID=C02-03`.

**Book section IDs:** B02-011, B02-012, B02-013, B02-014, B02-015, B02-016, B02-017, B02-020, B02-022, B02-023, B02-024, B02-025.

### C09-01 — Partial failure, network uncertainty, and timeout budgets

**Mode:** application fault injection + Go model. **Status:** planned. **Depends on:** APP-09.

**Target location:** `labs/ch09/network; deploy/compose/faults`.

**Build:** Inject loss, delay, duplication, partitions, connection resets, bandwidth/queue limits, and failure-detector false positives using a local proxy and the deterministic transport. Compare TCP/UDP semantics, synchronous/asynchronous assumptions, circuit/packet switching, and latency/utilization.

**Acceptance:** Lost response and lost request produce distinguishable durable histories but identical client uncertainty. Timeouts bound waiting without claiming the peer is dead. Retry budgets and admission control prevent cascading overload after a partition heals.

**Verification interface:** `make verify ID=C09-01`.

**Book section IDs:** B09-001, B09-002, B09-003, B09-004, B09-005, B09-006, B09-007, B09-008, B09-009, B09-010, B09-011, B09-012, B09-013, B09-014.

**Additional pattern obligations:** P26; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C09-03 — Process pauses, runtime behavior, and bounded latency

**Mode:** Go runtime experiment. **Status:** planned. **Depends on:** APP-09.

**Target location:** `labs/ch09/pauses; benchmarks/runtime`.

**Build:** Exercise GC pressure, process stop/resume, scheduler stalls, page faults, and worker starvation. Record Go profiles and compare allocation reduction, bounded worker pools, and isolated critical workers.

**Acceptance:** A resumed worker cannot act on an expired lease without validation. Compare tail latency and resource usage under pause injection. Document that general-purpose Go/OS scheduling does not provide hard real-time guarantees.

**Verification interface:** `make verify ID=C09-03`.

**Book section IDs:** B09-024, B09-025, B09-026.

## M05 — Storage and indexing implementations

### C04-01 — Append log and LSM storage engine

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** APP-01.

**Target location:** `labs/ch04/lsm`.

**Build:** Implement append-only key/value storage, WAL framing/checksums, memory index and memtable, immutable SSTables, sparse indexes, tombstones, Bloom filters, leveled/size-tiered compaction variants, and manifest recovery. Compare an embedded mature engine through a Go adapter.

**Acceptance:** Property-test against an in-memory map. Crash at write/fsync/rename/compaction boundaries and recover according to documented durability settings. Bloom filters have no false negatives; deleted data cannot resurrect after compaction.

**Verification interface:** `make verify ID=C04-01`.

**Book section IDs:** B04-001, B04-002, B04-003, B04-004, B04-005, B04-006, B04-007.

### C04-02 — Page-based B+ tree

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C04-01.

**Target location:** `labs/ch04/btree`.

**Build:** Implement a bounded page format, search, split/merge, range scans, free-page handling, and WAL-based recovery for a B+ tree. Explain latches, copy-on-write variants, and where concurrency/durability assumptions differ from production engines.

**Acceptance:** Random insert/update/delete sequences preserve ordering and page occupancy invariants. Reopening and injected torn-write recovery match the reference model. A documented single-writer restriction is enforced rather than hidden.

**Verification interface:** `make verify ID=C04-02`.

**Book section IDs:** B04-008, B04-009, B04-010.

### C04-03 — Index layout and storage-engine trade-offs

**Mode:** benchmark + integration lab. **Status:** planned. **Depends on:** C04-01, C04-02.

**Target location:** `labs/ch04/indexes; benchmarks/storage`.

**Build:** Compare LSM/B+ tree read, write, and space amplification; sequential/random writes; composite, secondary, clustered, covering, and in-memory indexes. Include PostgreSQL query plans and controlled Go engine benchmarks.

**Acceptance:** Publish warm/cold cache, dataset size, fsync policy, device details, and repeated-run variance. Show a query plan that changes with a covering or multicolumn index. Do not generalize SSD behavior from an unspecified container filesystem.

**Verification interface:** `make verify ID=C04-03`.

**Book section IDs:** B04-011, B04-012, B04-013, B04-014, B04-015, B04-016, B04-017, B04-018, B04-019.

### C04-04 — Column storage and query execution

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C04-01.

**Target location:** `labs/ch04/columnstore`.

**Build:** Build a small column format with dictionary/RLE/bitmap encodings, sorting, immutable segments, row-group pruning, and delta ingestion. Compare scalar interpretation, Go-generated compiled predicate code, and batch-oriented execution; build a materialized aggregate and data cube.

**Acceptance:** All execution modes match a row-store oracle, including nulls and deletes. Compare bytes scanned and CPU time. Clearly distinguish batch execution from actual SIMD; compilation only accepts a bounded safe expression grammar.

**Verification interface:** `make verify ID=C04-04`.

**Book section IDs:** B04-020, B04-021, B04-022, B04-023, B04-024, B04-025, B04-026, B04-027.

### C04-05 — Geospatial, full-text, and vector search

**Mode:** application + Go lab. **Status:** planned. **Depends on:** C04-03.

**Target location:** `labs/ch04/search; internal/search`.

**Build:** Build an inverted index with tokenization/ranking and a multidimensional range index; use PostgreSQL full-text search and a pgvector profile. Implement exact cosine/Euclidean search and a bounded HNSW exercise over deterministic/precomputed embeddings, with versioned embedding metadata.

**Acceptance:** Measure recall@k against exact search and latency/memory trade-offs. Test updates, deletes, tenant filtering, misspellings, and full-text versus semantic behavior. Synthetic vectors demonstrate indexing but are not claimed to demonstrate natural-language understanding.

**Verification interface:** `make verify ID=C04-05`.

**Book section IDs:** B04-028, B04-029, B04-030.

## M06 — Data models and schema evolution

### C02-01 — Fan-out and materialized timelines

**Mode:** lab integrated with application events. **Status:** planned. **Depends on:** APP-06.

**Target location:** `labs/ch02/timeline`.

**Build:** Reuse order/activity events to implement read-time fan-out, write-time fan-out, and a hybrid for high-follower accounts. Supply a small social-network-shaped fixture so the book case study remains recognizable.

**Acceptance:** Compare write amplification, read latency, memory, and staleness under uniform and celebrity-skewed workloads. Duplicate events do not duplicate timeline items; rebuild yields the same ordered view.

**Verification interface:** `make verify ID=C02-01`.

**Book section IDs:** B02-001, B02-002, B02-003.

### C03-01 — Relational and document models

**Mode:** application + lab. **Status:** planned. **Depends on:** APP-02.

**Target location:** `labs/ch03/models; internal/catalog`.

**Build:** Model the same catalog and customer data using normalized SQL, denormalized tables, and PostgreSQL JSONB. Show joins, one-to-many/many-to-many relationships, locality, schema flexibility, declarative queries, and explicit Go/sqlc mapping versus an ORM comparison fixture. Add a wide-column/column-family model with composite partition/clustering keys over the same catalog fixtures.

**Acceptance:** All representations answer equivalent questions on the same fixtures. Demonstrate an update anomaly, schema drift, and the cost of a nested-document update, then measure appropriate indexes and repair paths. Explain and demonstrate the difference between wide-column modeling and analytical column-oriented storage.

**Verification interface:** `make verify ID=C03-01`.

**Book section IDs:** B03-001, B03-002, B03-003, B03-004, B03-005, B03-006, B03-007, B03-008, B03-009, B03-011, B03-012, B03-013, B03-014, B03-015.

### C03-03 — Property graphs, Cypher, and recursive SQL

**Mode:** lab. **Status:** planned. **Depends on:** C03-01.

**Target location:** `labs/ch03/property-graph; deploy/compose/graph`.

**Build:** Represent warehouse routes and supplier relationships as a Go adjacency graph, SQL vertices/edges with recursive CTEs, and a Neo4j profile queried from Go using Cypher.

**Acceptance:** All backends return equivalent reachability/path results, including cycles and disconnected nodes. Explain query expressiveness and index trade-offs; graph queries never bypass tenant boundaries.

**Verification interface:** `make verify ID=C03-03`.

**Book section IDs:** B03-016, B03-017, B03-018, B03-019.

### C03-04 — RDF, SPARQL, and Datalog

**Mode:** lab. **Status:** planned. **Depends on:** C03-03.

**Target location:** `labs/ch03/semantic-graph; deploy/compose/rdf`.

**Build:** Represent the route domain as RDF triples; query a local RDF/SPARQL service from Go and compare with a small Go positive-Datalog evaluator using a documented grammar, fixed-point recursion, and set semantics. Explain URI identity and semantic-web interoperability.

**Acceptance:** Equivalent SQL, graph, SPARQL, and Datalog fixtures agree. Recursive cycles terminate for the finite domain; unsupported negation or language features are rejected explicitly. The evaluator is a completed bounded lab, not a full language implementation.

**Verification interface:** `make verify ID=C03-04`.

**Book section IDs:** B03-020, B03-021, B03-022, B03-023, B03-024.

### C03-05 — GraphQL and read composition

**Mode:** application comparison adapter. **Status:** planned. **Depends on:** APP-08, C03-06.

**Target location:** `api/graphql; labs/ch03/graphql`.

**Build:** Expose an alternative Go GraphQL read endpoint over order/catalog projections with batching, pagination, schema evolution, depth/cost limits, and field authorization. Compare API composition with database graph models.

**Acceptance:** A query over N order lines does not issue N additional database queries. Cross-tenant nested fields are denied and expensive queries are bounded. REST and GraphQL represent the same authorized data.

**Verification interface:** `make verify ID=C03-05`.

**Book section IDs:** B03-025.

**Additional pattern obligations:** P37; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C05-01 — Encoding and Protobuf compatibility

**Mode:** Go lab + contracts. **Status:** planned. **Depends on:** APP-01.

**Target location:** `labs/ch05/encoding; contracts/proto; api/openapi`.

**Build:** Implement Go codecs/fixtures for gob, JSON, XML, MessagePack, and Protobuf; add JSON Schema validation, byte/CPU comparisons, unknown fields, numeric limits, presence/default values, and reserved Protobuf field numbers. Include a Thrift codec compatibility comparison alongside Protobuf, using Go generated clients and schemas.

**Acceptance:** Run old-writer/new-reader and new-writer/old-reader matrices, plus read-modify-write round trips. Demonstrate when unknown fields are lost. Reject malformed or oversized payloads with bounded allocations. Old/new Thrift fixtures must identify field/default behavior without assuming it matches Protobuf.

**Verification interface:** `make verify ID=C05-01`.

**Book section IDs:** B05-001, B05-002, B05-003, B05-004, B05-005, B05-006, B05-007, B05-008, B05-014.

**Additional pattern obligations:** P34; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C05-02 — Avro and schema resolution

**Mode:** Go lab + contracts. **Status:** planned. **Depends on:** C05-01.

**Target location:** `labs/ch05/avro; contracts/avro`.

**Build:** Encode/decode Avro with writer/reader schemas, defaults, unions, promotion, generated versus dynamic schemas, and schema fingerprints/registry lookup. Keep immutable schema versions with each archived dataset.

**Acceptance:** Compatibility fixtures demonstrate permitted and rejected changes in both directions. A missing writer schema fails explicitly; replay of an old file uses its actual schema rather than the latest registry entry.

**Verification interface:** `make verify ID=C05-02`.

**Book section IDs:** B05-009, B05-010, B05-011, B05-012, B05-013.

### C05-03 — Database, archive, and rolling schema evolution

**Mode:** application + lab. **Status:** planned. **Depends on:** APP-04, C05-01, C05-02.

**Target location:** `migrations; labs/ch05/evolution`.

**Build:** Implement expand/backfill/validate/contract migrations, resumable throttled backfills, mixed binary versions, immutable archived formats, and event upcasters. Capture dataflow contracts independently of current Go struct layouts.

**Acceptance:** Run two application versions concurrently during a migration and roll back the compatible binary. Long-running backfills resume without skipping rows or blocking normal writes beyond a stated budget. Destructive contraction waits for reader and rollback-window retirement.

**Verification interface:** `make verify ID=C05-03`.

**Book section IDs:** B05-015, B05-016, B05-017, B05-018.

**Additional pattern obligations:** P34; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C05-04 — REST, RPC, discovery, and transport failures

**Mode:** application + lab. **Status:** planned. **Depends on:** APP-08, C05-01.

**Target location:** `api; labs/ch05/rpc; deploy/compose/routing`.

**Build:** Implement REST/OpenAPI and gRPC adapters over the same use case with deadlines, cancellation, idempotency, health, load balancing, service discovery, and mixed-version contracts. Include a service-mesh responsibility exercise using a local proxy.

**Acceptance:** Demonstrate that a timed-out RPC may have committed. Retry only safe/idempotent operations, propagate budgets, and test an endpoint disappearing during a request. Explain proxy responsibilities and why a mesh does not solve business consistency.

**Verification interface:** `make verify ID=C05-04`.

**Book section IDs:** B05-019, B05-020, B05-021, B05-022, B05-023.

**Additional pattern obligations:** P06, P37, P46; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C05-06 — Actors and asynchronous communication

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** APP-06, C05-01.

**Target location:** `labs/ch05/actors`.

**Build:** Implement entity-keyed actors with bounded mailboxes, supervision, serialization, and remote transport. Compare synchronous requests, transient messages, and durable broker delivery using inventory-shaped entities.

**Acceptance:** Demonstrate mailbox backpressure, actor restart, lost messages, remote timeouts, and rolling message versions. Single-threaded actor execution is not presented as durable storage or distributed exactly-once delivery.

**Verification interface:** `make verify ID=C05-06`.

**Book section IDs:** B05-026, B05-027, B05-028.

## M07 — Replication, causality, and offline synchronization

### C06-01 — Single-leader replication, failover, and backup

**Mode:** real database integration + Go simulation. **Status:** planned. **Depends on:** OPS-01.

**Target location:** `labs/ch06/single-leader; deploy/compose/replication`.

**Build:** Run PostgreSQL primary/standby replication in synchronous and asynchronous modes; implement a small Go leader/follower model. Exercise consistent snapshot plus log catch-up, follower bootstrap, leader promotion, fencing, backup/PITR, and region/zone failure-domain assumptions.

**Acceptance:** Measure acknowledged-write loss and lag under each configured durability mode. Prevent the old primary from continuing writes during promotion. Restore a backup to an independent database and validate invariants; replication is not counted as backup.

**Verification interface:** `make verify ID=C06-01`.

**Book section IDs:** B06-001, B06-002, B06-003, B06-004, B06-006, B06-007, B06-008, B06-015.

**Additional pattern obligations:** P30, P41; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C06-02 — Object-backed storage and conditional publication

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C04-01.

**Target location:** `labs/ch06/object-storage`.

**Build:** Implement immutable data segments and versioned manifests over an object-store port, conditional put/CAS, snapshots, reader caches, and independent compute workers. Provide a local deterministic backend and an S3 integration contract suite.

**Acceptance:** Concurrent writers cannot silently overwrite the same manifest generation. Readers observe a complete published generation; crashes leave recoverable orphan objects. Record consistency/conditional-write assumptions and API-call cost; emulator success alone is not a cloud durability guarantee.

**Verification interface:** `make verify ID=C06-02`.

**Book section IDs:** B06-005.

### C06-03 — Replication log formats

**Mode:** Go lab + database integration. **Status:** planned. **Depends on:** C06-01, C05-03.

**Target location:** `labs/ch06/log-formats`.

**Build:** Compare statement replication, physical WAL shipping, and logical row changes. Implement deterministic fixtures that include nondeterministic SQL, sequences, schema changes, and transaction boundaries; inspect PostgreSQL logical decoding with a Go consumer.

**Acceptance:** Show why replaying a time-dependent statement can diverge. Verify row-change ordering within transactions, snapshot/log handoff, and compatibility constraints of physical versus logical logs.

**Verification interface:** `make verify ID=C06-03`.

**Book section IDs:** B06-009, B06-010, B06-011, B06-012.

### C06-04 — Replica lag and session consistency

**Mode:** application + lab. **Status:** planned. **Depends on:** C06-01, APP-08.

**Target location:** `internal/platform/readrouting; labs/ch06/session-consistency`.

**Build:** Implement primary reads, causal/write-position tokens, bounded replica waits, sticky routing, and monotonic-read watermarks. Demonstrate stale reads, read-your-writes, monotonic reads, and consistent-prefix guarantees under uneven lag.

**Acceptance:** Reproduce each stale-read anomaly before enabling its remedy. A session never regresses below its recorded watermark; timeouts return pending/fallback behavior explicitly. Explain that per-shard tokens alone do not provide an atomic cross-shard snapshot.

**Verification interface:** `make verify ID=C06-04`.

**Book section IDs:** B06-013, B06-014, B06-016, B06-017, B06-018, B06-038.

**Additional pattern obligations:** P30; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C06-05 — Multi-leader replication and conflict policies

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C06-08.

**Target location:** `labs/ch06/multi-leader`.

**Build:** Implement star, ring, and all-to-all replication of offline warehouse notes with loop suppression, causal dependency buffering, region-local writes, conflict avoidance, last-write-wins, and manual/automatic resolution policies.

**Acceptance:** Partition replicas, make conflicting edits, reorder deliveries, and heal. Show lost updates under clock-based last-write-wins and convergence under the selected explicit merge policy. Never use this lab to authorize overselling globally scarce inventory.

**Verification interface:** `make verify ID=C06-05`.

**Book section IDs:** B06-019, B06-020, B06-021, B06-022, B06-026, B06-027, B06-028, B06-029, B06-030, B06-032, B06-040.

**Additional pattern obligations:** P32; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C06-06 — Local-first synchronization, CRDTs, and OT

**Mode:** Go implementation lab + CLI clients. **Status:** planned. **Depends on:** C06-08, APP-08.

**Target location:** `labs/ch06/local-first; cmd/offline-client`.

**Build:** Implement offline Go clients with durable local operation logs, G/PN counters, an observed-remove set, a sequence CRDT, and a bounded two-client OT text exercise. Sync notes and wish lists through the project API with authorization and tombstone/causal metadata.

**Acceptance:** Merge permutation/property tests establish convergence for supported operations; reconnect, duplicate, and delete scenarios do not resurrect removed entries. Record OT assumptions, metadata growth, tombstone retention, and server/device data ownership.

**Verification interface:** `make verify ID=C06-06`.

**Book section IDs:** B06-023, B06-024, B06-025, B06-031.

**Additional pattern obligations:** P32; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C06-07 — Leaderless replication and quorum limits

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C06-08, C04-01.

**Target location:** `labs/ch06/leaderless`.

**Build:** Implement configurable N/R/W replicas, sloppy quorums, hinted handoff, read repair, anti-entropy with Merkle summaries, concurrent siblings, tombstones, and staleness metrics. Compare synchronous leader and leaderless latency on the same workloads. Implement bounded gossip dissemination and peer selection for the anti-entropy comparison.

**Acceptance:** Show counterexamples to treating R+W>N as unconditional linearizability, including failed writes, concurrent writes, and sloppy quorum placement. Under the stated convergence assumptions replicas repair after healing; deleted keys stay deleted through the retention window. Convergence requires connected communication and fair peer selection; report repair traffic and time.

**Verification interface:** `make verify ID=C06-07`.

**Book section IDs:** B06-033, B06-034, B06-035, B06-036, B06-037, B06-039.

### C06-08 — Causality, siblings, and version vectors

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** APP-01.

**Target location:** `labs/ch06/causality`.

**Build:** Implement happens-before histories, concurrent-version detection, version vectors/dotted versions, sibling merge, and causal dependency visualization. Distinguish vector clocks tracking events from per-key version metadata and document bounded actor membership.

**Acceptance:** Property tests distinguish before/after/concurrent and preserve independent edits. Pruning metadata cannot silently assert an order that was not observed. Histories include delayed and duplicated messages and do not derive causality from wall-clock timestamps.

**Verification interface:** `make verify ID=C06-08`.

**Book section IDs:** B06-041, B06-042, B06-043, B06-044, B06-045, B06-046.

### C09-02 — Clock models and uncertainty

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** APP-01.

**Target location:** `labs/ch09/clocks`.

**Build:** Inject wall-clock jumps, skew, drift, NTP-like corrections, and confidence intervals. Separate elapsed-time monotonic clocks from wall timestamps; compare timestamp-based ordering and uncertainty-aware snapshot/commit-wait models.

**Acceptance:** Durations do not become negative after wall-clock changes. Reproduce incorrect last-write-wins ordering. Confidence bounds are explicit simulator assumptions; the application never claims TrueTime-like guarantees from ordinary host clocks.

**Verification interface:** `make verify ID=C09-02`.

**Book section IDs:** B09-015, B09-016, B09-017, B09-018, B09-019, B09-020, B09-021, B09-022, B09-023.

## M08 — Sharding and transaction semantics

### C07-01 — Tenant-aware sharding and routing

**Mode:** application routing adapter + lab. **Status:** planned. **Depends on:** APP-08, C06-01.

**Target location:** `labs/ch07/router; internal/platform/shardrouting`.

**Build:** Implement range/hash tenant routing, a versioned shard directory, request forwarding, tenant placement, and resharding state. Compare sharding, table partitioning, replication, and shared-schema multitenancy.

**Acceptance:** Route every tenant request and event consistently; stale routing epochs cause retry/redirect rather than a second owner. Cross-tenant operations cannot accidentally span uncoordinated databases. A no-sharding baseline remains runnable.

**Verification interface:** `make verify ID=C07-01`.

**Book section IDs:** B07-001, B07-002, B07-003, B07-004, B07-015.

**Additional pattern obligations:** P31; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C07-02 — Hashing, range splits, and safe rebalancing

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C07-01, C06-03.

**Target location:** `labs/ch07/rebalancing`.

**Build:** Implement modulo hashing, fixed virtual shards, hash ranges, a consistent-hash ring, range splits/merges, and a migration protocol: snapshot, log catch-up, verified cutover, then source retirement. Include analytical partition pruning and manual/automatic placement policies. Include highest-random-weight/rendezvous hashing alongside the ring and modulo strategies.

**Acceptance:** Measure key movement and skew when nodes are added. Concurrent migration writes are not lost or accepted by two owners; interrupted migrations resume. Range scans return complete, duplicate-free results for the chosen snapshot semantics. Adding/removing nodes yields the expected ownership and key-movement properties for each strategy.

**Verification interface:** `make verify ID=C07-02`.

**Book section IDs:** B07-005, B07-006, B07-007, B07-008, B07-009, B07-010, B07-011, B07-012, B07-014.

**Additional pattern obligations:** P31; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C07-03 — Hot partitions and workload skew

**Mode:** Go load experiment. **Status:** planned. **Depends on:** C07-02, APP-09.

**Target location:** `labs/ch07/hotspots`.

**Build:** Generate Zipfian workloads, celebrity tenants, sequential timestamps, and hot SKU counters. Compare salting, key splitting, admission control, tenant isolation, and preallocated inventory quotas.

**Acceptance:** Quantify throughput and read fan-out costs. Quota/escrow experiments preserve total allocatable stock; random salting alone is never claimed to solve a single-key invariant. Show when migration increases rather than relieves load.

**Verification interface:** `make verify ID=C07-03`.

**Book section IDs:** B07-013.

**Additional pattern obligations:** P25; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C07-04 — Local and global secondary indexes

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C07-02, APP-06.

**Target location:** `labs/ch07/secondary-indexes`.

**Build:** Implement document-local scatter/gather indexes and separately sharded global indexes for catalog search; maintain them through ordered events, including update and delete handling.

**Acceptance:** Compare write amplification and query fan-out. Expose asynchronous staleness and repair divergence after a failed update. Pagination and index rebuild across shard movement obey documented consistency bounds.

**Verification interface:** `make verify ID=C07-04`.

**Book section IDs:** B07-016, B07-017, B07-018.

**Additional pattern obligations:** P17; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C08-02 — Read committed, MVCC, and snapshot isolation

**Mode:** Go simulator + PostgreSQL lab. **Status:** planned. **Depends on:** C08-01.

**Target location:** `labs/ch08/isolation`.

**Build:** Build controlled interleavings for dirty reads/writes, read skew, nonrepeatable reads, and MVCC snapshots. Implement a small version-visibility model and compare actual PostgreSQL isolation behavior and indexes.

**Acceptance:** Use synchronization barriers rather than timing-dependent sleeps. Label unsupported anomalies as simulator examples: PostgreSQL does not provide a dirty-read mode. A repeatable-read snapshot is not mislabeled as serializable.

**Verification interface:** `make verify ID=C08-02`.

**Book section IDs:** B08-012, B08-013, B08-014, B08-015, B08-016, B08-017, B08-018, B08-019, B08-020, B08-021.

### C08-03 — Lost updates and concurrency control

**Mode:** application + lab. **Status:** planned. **Depends on:** C08-02, APP-03.

**Target location:** `labs/ch08/lost-updates; internal/inventory`.

**Build:** Compare read-modify-write, atomic increments, SELECT FOR UPDATE, expected-version CAS, optimistic retries, and replicated conflict handling against the same reservation/account fixtures.

**Acceptance:** The unsafe variant deterministically loses an update; corrected variants preserve the invariant. Retry the entire logical transaction on serialization conflicts and use deterministic lock ordering to reduce deadlocks.

**Verification interface:** `make verify ID=C08-03`.

**Book section IDs:** B08-022, B08-023, B08-024, B08-025, B08-026, B08-027.

**Additional pattern obligations:** P19, P20; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C08-04 — Write skew, phantoms, and materialized conflicts

**Mode:** database integration lab. **Status:** planned. **Depends on:** C08-02.

**Target location:** `labs/ch08/write-skew`.

**Build:** Implement on-call staffing, booking overlap, and stock-allocation constraints; reproduce write skew and phantom predicates. Compare materialized guard rows, predicate/range protection, database constraints, and serializable retries.

**Acceptance:** A scheduled two-transaction history violates the weak-isolation invariant before the fix. Each repair has a concurrent acceptance test and contention measurement; row locks on existing rows are not credited with locking absent rows.

**Verification interface:** `make verify ID=C08-04`.

**Book section IDs:** B08-028, B08-029, B08-030, B08-031, B08-032.

### C08-05 — Serial execution and two-phase locking

**Mode:** Go implementation lab + database comparison. **Status:** planned. **Depends on:** C08-02.

**Target location:** `labs/ch08/serial-execution; labs/ch08/2pl`.

**Build:** Implement a serial transaction queue and a strict-2PL scheduler with shared/exclusive locks, upgrades, deadlock handling, predicate/index-range examples, and shard coordination. Compare stored-procedure versus interactive round trips.

**Acceptance:** Generated schedules are serializable under the model; deadlocked victims abort and release locks. Measure contention and long-transaction effects. Explicitly distinguish two-phase locking from two-phase commit.

**Verification interface:** `make verify ID=C08-05`.

**Book section IDs:** B08-033, B08-034, B08-035, B08-036, B08-037, B08-038, B08-039, B08-040, B08-041, B08-042, B08-043, B08-044.

### C08-06 — Serializable snapshot isolation

**Mode:** Go dependency model + PostgreSQL lab. **Status:** planned. **Depends on:** C08-04, C08-05.

**Target location:** `labs/ch08/ssi`.

**Build:** Visualize read/write dependency graphs, stale premises, dangerous structures, and SSI abort/retry behavior. Compare optimistic SSI with pessimistic locking using the same workload.

**Acceptance:** Detect known write-skew histories and validate PostgreSQL SERIALIZABLE outcomes. The teaching dependency model states its simplifications and does not claim to be PostgreSQL's complete SSI implementation; production correctness uses the real database.

**Verification interface:** `make verify ID=C08-06`.

**Book section IDs:** B08-045, B08-046, B08-047, B08-048, B08-049, B08-050.

**Additional pattern obligations:** P20; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C08-07 — Distributed atomic commit, 2PC, 3PC, and XA

**Mode:** Go protocol lab + PostgreSQL integration. **Status:** planned. **Depends on:** C08-01, C09-06.

**Target location:** `labs/ch08/atomic-commit`.

**Build:** Implement durable coordinator/participant logs, prepare/commit/abort, in-doubt recovery, and a bounded 3PC simulator. Use PostgreSQL prepared transactions with two databases to demonstrate actual lock retention; model XA resource-manager contracts and compare database-internal distributed commits.

**Acceptance:** Crash the coordinator after each phase; a participant never guesses abort after a durable commit decision. Demonstrate blocking and document 3PC timing/partition assumptions with a failing counterexample. Publish cleanup tooling for prepared transactions in disposable lab databases.

**Verification interface:** `make verify ID=C08-07`.

**Book section IDs:** B08-051, B08-052, B08-053, B08-054, B08-055, B08-056, B08-058, B08-059, B08-060, B08-061, B08-062.

**Additional pattern obligations:** P21; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M09 — Consensus and distributed correctness

### C09-04 — Locks, leases, and fencing tokens

**Mode:** application coordination adapter + lab. **Status:** planned. **Depends on:** C10-05, C09-03.

**Target location:** `labs/ch09/fencing; internal/platform/leases`.

**Build:** Implement lease ownership with monotonically increasing fencing tokens from a linearizable allocator. Enforce token ordering at the resource being mutated, including a multi-replica model and compare-and-set alternative.

**Acceptance:** Pause an old owner, grant a new lease, then deliver the old write; storage rejects it. A lock timestamp in application memory is insufficient. Document where fencing cannot be enforced, such as an external API without token support.

**Verification interface:** `make verify ID=C09-04`.

**Book section IDs:** B09-027, B09-028, B09-029, B09-030, B09-031.

**Additional pattern obligations:** P29; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C09-05 — Byzantine faults and integrity checks

**Mode:** Go model + integrity exercise. **Status:** planned. **Depends on:** C09-06.

**Target location:** `labs/ch09/byzantine`.

**Build:** Implement checksum corruption detection and a small authenticated-message quorum model with an equivocating node. Contrast crash faults, stale information, accidental corruption, and Byzantine behavior; show the extra assumptions of BFT protocols.

**Acceptance:** Inject corrupted messages and conflicting signed votes and detect them in the stated model. Include a counterexample showing that checksums or a crash-fault-tolerant majority alone do not provide Byzantine consensus. No production BFT capability is claimed.

**Verification interface:** `make verify ID=C09-05`.

**Book section IDs:** B09-032, B09-033, B09-034, B09-035.

### C10-01 — Linearizability, serializability, and partition choices

**Mode:** Go history checker + experiment. **Status:** planned. **Depends on:** C06-07, C09-06.

**Target location:** `labs/ch10/consistency`.

**Build:** Implement a bounded linearizability checker for register/CAS histories with invocation/response intervals and uncertain operations. Compare stale replicas, quorum counterexamples, cross-channel races, uniqueness, and consistency/availability choices under partition.

**Acceptance:** Known legal/illegal histories pass/fail, including overlapping operations. Compare serializable transactions with real-time linearizability separately. A finite history check is evidence within a model, not a proof of all future executions or a simplistic choose-two CAP claim.

**Verification interface:** `make verify ID=C10-01`.

**Book section IDs:** B10-001, B10-002, B10-003, B10-004, B10-005, B10-006, B10-007, B10-008, B10-009, B10-010, B10-011, B10-012.

### C10-02 — Logical clocks and ID generation

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C06-08, C09-02, C10-05.

**Target location:** `labs/ch10/ids`.

**Build:** Implement Lamport clocks, hybrid logical clocks, vector-clock comparisons, UUID-based identity, and a linearizable sequence allocator over the consensus-backed adapter. Examine causality, uniqueness, monotonicity, and linearizability as distinct properties. Include a Snowflake-style timestamp/worker/sequence ID generator as a comparison.

**Acceptance:** Test clock rollback, node restart, overflow, duplicate node identity, and reordered messages. HLC timestamps never masquerade as proof of causality or global serialization; generated IDs are not treated as database commit order. Duplicate worker identity and clock rollback must expose the uniqueness/availability trade-off rather than being ignored.

**Verification interface:** `make verify ID=C10-02`.

**Book section IDs:** B10-013, B10-014, B10-015, B10-016, B10-017, B10-018, B10-019, B10-020.

### C10-03 — Consensus formulations and impossibility boundaries

**Mode:** Go protocol models. **Status:** planned. **Depends on:** C09-06.

**Target location:** `labs/ch10/consensus-models`.

**Build:** Implement bounded single-value agreement, compare-and-set, replicated-log, sequencer/fetch-and-add, and atomic-commit examples under explicitly stated server failure and storage assumptions. Explore bivalent/delayed schedules and distinguish consensus abstractions from primitive hardware consensus numbers. Build a bounded single-decree Paxos prepare/accept model and a Multi-Paxos stable-leader extension. Add Viewstamped Replication and Zab leader/epoch/log transition traces to the shared protocol model and explain their distinguishing recovery responsibilities.

**Acceptance:** Validate agreement, validity, and integrity; exhibit nontermination under allowed adversarial scheduling. Explain FLP assumptions and the role of eventual synchrony/randomization. A bare fetch-and-add primitive is not asserted to solve arbitrary wait-free consensus. Persistent promises and accepted values survive modeled restart; competing proposers cannot choose different values. Each named protocol comparison has a runnable trace, while only the declared subset is claimed implemented.

**Verification interface:** `make verify ID=C10-03`.

**Book section IDs:** B10-021, B10-022, B10-023, B10-024, B10-025, B10-026, B10-027, B10-028.

### C10-04 — Raft replicated state machine

**Mode:** Go implementation lab + mature-library adapter. **Status:** planned. **Depends on:** C10-01, C10-03, C04-01.

**Target location:** `labs/ch10/raft`.

**Build:** Implement terms, elections, log replication, quorum commit, persistence, restart, snapshots, membership-change protocol, and safe linearizable reads. Pair the teaching implementation with a maintained Raft library adapter for comparison; document Paxos-family relationships as protocol alternatives.

**Acceptance:** Run partition, restart, stale-leader, duplicate-message, and membership-change histories through invariants and the checker. A minority cannot acknowledge committed writes; reads use a valid quorum/read-index barrier. Educational Raft code is excluded from the application data plane.

**Verification interface:** `make verify ID=C10-04`.

**Book section IDs:** B10-029, B10-030, B10-031, B10-032, B10-033, B10-034.

### C10-05 — Coordination services and work allocation

**Mode:** real-system integration + Go client. **Status:** planned. **Depends on:** C09-06.

**Target location:** `labs/ch10/coordination; internal/platform/coordination`.

**Build:** Use etcd through Go for compare-and-set, watches, leases, leader election, configuration generations, service discovery, and work assignment. Exercise watch resumption, compaction, and placement epochs; compare application responsibility with the consensus service.

**Acceptance:** A compacted watch resumes from a fresh snapshot without silently skipping configuration. Reassigned work uses fencing at its destination. Loss of quorum fails operations according to the configured consistency contract; cached discovery is labeled stale.

**Verification interface:** `make verify ID=C10-05`.

**Book section IDs:** B10-035, B10-036, B10-037, B10-038.

**Additional pattern obligations:** P29; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M10 — Batch analytics and responsible data preparation

### C01-02 — Systems of record, warehouse, lake, and storage/compute separation

**Mode:** application + lab. **Status:** planned. **Depends on:** APP-06, C11-02.

**Target location:** `internal/analytics; labs/ch01/dataflows; docs/data-lineage`.

**Build:** Classify every dataset as authoritative or derived. Build OLTP-to-star-schema ETL, immutable object snapshots, a lakehouse-style manifest/CAS demonstration, and stateless Go query workers over shared data. Compare schema-on-read/write and storage/compute scaling.

**Acceptance:** Delete a derived table and rebuild it from documented sources. Restart query workers without data loss; reject concurrent manifest publication conflicts. Label the manifest exercise as a subset, not a complete Iceberg implementation.

**Verification interface:** `make verify ID=C01-02`.

**Book section IDs:** B01-002, B01-003, B01-004, B01-005, B01-006, B01-007, B01-010, B01-011, B01-012.

### C03-02 — Analytical schemas, DataFrames, matrices, and arrays

**Mode:** lab + analytics. **Status:** planned. **Depends on:** C11-01.

**Target location:** `labs/ch03/analytics-models; internal/analytics`.

**Build:** Build star and snowflake schemas, typed Go column batches with filter/group/join/pivot operations, sparse purchase matrices, one-hot encoding, and an array-shaped geospatial fixture. Use these datasets in later batch jobs.

**Acceptance:** Compare dimensional joins and redundant storage; round-trip typed records and sparse matrices without silently confusing missing values with zero. Include nullable, categorical, and temporal data tests.

**Verification interface:** `make verify ID=C03-02`.

**Book section IDs:** B03-010, B03-027.

### C11-01 — Composable Go batch tools and external sorting

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** APP-01.

**Target location:** `labs/ch11/unix-style; cmd/batch`.

**Build:** Build streaming parse/filter/map/group/sort tools using stdin/stdout with explicit schemas and exit codes. Compare in-memory hashing with bounded-memory external merge sort on the same synthetic access/order logs.

**Acceptance:** Pipelines match the reference aggregate, propagate backpressure/cancellation, and clean up temporary files on errors. An input larger than configured memory completes without unbounded allocation. Explain Unix composition using Go implementations.

**Verification interface:** `make verify ID=C11-01`.

**Book section IDs:** B11-001, B11-002, B11-003, B11-004.

**Additional pattern obligations:** P48; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C11-02 — Distributed storage and job orchestration

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C06-02, C09-06, C10-05.

**Target location:** `labs/ch11/storage; labs/ch11/scheduler`.

**Build:** Build a small replicated chunk/metadata filesystem model and an object-store adapter; compare block/file/object semantics. Implement a Go DAG scheduler with worker leases, resource limits, dependencies, retries, straggler handling, and immutable task outputs with atomic manifest publication. Add a Go erasure-coding integration, comparing k-of-n recovery against full replication for immutable chunks.

**Acceptance:** Worker death and duplicate task execution produce one published result generation. Detect missing/corrupt chunks. Speculative execution cannot duplicate external effects; workflow restarts resume from durable task state. Recover from supported missing/corrupt shards and fail visibly beyond the configured redundancy bound; report storage and repair costs.

**Verification interface:** `make verify ID=C11-02`.

**Book section IDs:** B11-005, B11-006, B11-007, B11-008, B11-009, B11-010, B11-011, B11-012.

**Additional pattern obligations:** P43; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C11-03 — MapReduce, dataflow, and shuffle

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C11-01, C11-02.

**Target location:** `labs/ch11/dataflow`.

**Build:** Implement map, combine, partition/shuffle, sort, reduce, and pipelined DAG operators. Compare materialized intermediate files with pipelined execution and reuse the commerce activity dataset. Add iterative PageRank over the supplier graph to demonstrate iterative dataflow, partitioned graph state, and convergence.

**Acceptance:** Distributed results equal a single-process oracle under worker failures and retries. Combiner use is restricted to valid algebraic operations. A skewed key demonstrates stragglers; shuffle is explained as redistribution, not randomness. Distributed iterations match a small single-process oracle within a declared tolerance and stop at a bounded iteration/convergence limit.

**Verification interface:** `make verify ID=C11-03`.

**Book section IDs:** B11-013, B11-014, B11-015, B11-016, B11-017, B11-018.

**Additional pattern obligations:** P43; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C11-04 — Joins, DataFrame APIs, and execution plans

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C11-03, C03-02.

**Target location:** `labs/ch11/joins`.

**Build:** Implement sort-merge, hash, broadcast, and partitioned joins; group/aggregate operators; a bounded relational/DataFrame API; and a simple query-plan interpreter with pushdown and partition pruning.

**Acceptance:** Join modes agree across nulls, duplicates, unmatched rows, and skew. Planner choices are reproducible and measured. A relation to warehouse SQL engines is documented without claiming the small Go evaluator is a general SQL compiler.

**Verification interface:** `make verify ID=C11-04`.

**Book section IDs:** B11-019, B11-020, B11-021, B11-022.

### C11-05 — ETL, analytics, training data, and serving outputs

**Mode:** application + Go lab. **Status:** planned. **Depends on:** C11-04, C05-02, C14-01.

**Target location:** `internal/analytics; labs/ch11/training-data`.

**Build:** Create daily sales/inventory marts, point-in-time feature datasets, Parquet exports, dataset lineage/version manifests, and an atomic serving-generation switch. Include a simple Go model-training/feature pipeline on synthetic purchases.

**Acceptance:** Re-running the same bounded input yields matching aggregates and reproducible data splits. No future data leaks into historical features. Bad input is quarantined with reconciliation counts, and readers never see a partially published output.

**Verification interface:** `make verify ID=C11-05`.

**Book section IDs:** B11-023, B11-024, B11-025, B11-026, B11-027.

**Additional pattern obligations:** P22; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C14-01 — Predictive analytics, bias, and accountability

**Mode:** Go synthetic experiment + decision record. **Status:** planned. **Depends on:** C11-01.

**Target location:** `labs/ch14/responsible-analytics; docs/governance`.

**Build:** Build a synthetic Go predictor/decision pipeline with dataset provenance, subgroup error metrics, biased sampling and feedback-loop simulations, explanation records, and human review/appeal workflow. Use fabricated attributes and record intended/forbidden uses.

**Acceptance:** Show how selection bias and feedback change outcomes despite acceptable aggregate accuracy. Record conflicting fairness objectives and decision ownership; no metric is claimed to establish ethical acceptability or suitability for real credit/employment decisions.

**Verification interface:** `make verify ID=C14-01`.

**Book section IDs:** B14-001, B14-002, B14-003, B14-004.

## M11 — Streaming and change-data capture

### C12-01 — Messaging models and delivery semantics

**Mode:** Go lab + real broker integration. **Status:** planned. **Depends on:** APP-06.

**Target location:** `labs/ch12/messaging`.

**Build:** Implement direct channels, a bounded queue/broker model, request/reply, pub/sub, competing consumers, fan-out, routing, acknowledgment, redelivery, expiry, dead-letter handling, and backpressure. Compare durable and transient messaging.

**Acceptance:** Crash consumers before/after acknowledgment, fill buffers, and exercise poison messages. Show loss/duplication boundaries explicitly and contrast message queues with database query/history semantics. Ordering-sensitive streams cannot silently skip a poison event.

**Verification interface:** `make verify ID=C12-01`.

**Book section IDs:** B12-001, B12-002, B12-003, B12-004, B12-005, B12-006, B12-007.

**Additional pattern obligations:** P16, P18, P28; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C12-02 — Partitioned log broker and Kafka operations

**Mode:** Go implementation lab + Kafka integration. **Status:** planned. **Depends on:** C04-01, APP-06.

**Target location:** `labs/ch12/log-broker; deploy/compose/kafka`.

**Build:** Build a segmented append-log broker with per-partition offsets and consumer groups, then run equivalent experiments on Kafka. Cover retention, replay, rebalances, partition keys, slow consumers, lag, disk exhaustion, and producer/broker durability settings.

**Acceptance:** A restarted consumer resumes from its durable progress. Adding partitions exposes any per-key ordering migration assumption. Test retention overtaking a consumer and controlled recovery from archive; at-least-once delivery is measured rather than assumed.

**Verification interface:** `make verify ID=C12-02`.

**Book section IDs:** B12-008, B12-009, B12-010, B12-011, B12-012, B12-013, B12-014.

**Additional pattern obligations:** P16; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C12-03 — CDC, snapshot handoff, and log compaction

**Mode:** application + integration lab. **Status:** planned. **Depends on:** C06-03, C05-03, C12-02.

**Target location:** `internal/analytics/cdc; labs/ch12/cdc`.

**Build:** Implement a Go PostgreSQL logical-replication consumer and a Debezium comparison profile. Handle a consistent initial snapshot plus WAL position, transaction ordering, schema evolution, replication slots, compaction/tombstones, and change-stream API contracts.

**Acceptance:** Concurrent writes during snapshotting appear exactly in the eventual reconstructed state, with idempotent application of duplicates. Restart from a persisted position, detect slot/log loss, bound retained WAL, and resnapshot safely. Row CDC is distinguished from intentional domain events.

**Verification interface:** `make verify ID=C12-03`.

**Book section IDs:** B12-015, B12-016, B12-017, B12-018, B12-019, B12-020, B12-021, B12-022, B12-023.

**Additional pattern obligations:** P22; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C12-04 — Event immutability, concurrency, and retention

**Mode:** application + lab. **Status:** planned. **Depends on:** APP-04, C12-03.

**Target location:** `labs/ch12/event-history; internal/ordering`.

**Build:** Compare state-table CDC with event sourcing, build several projections from one history, enforce expected stream versions, and define archival, correction, retention, and personal-data-reference policies.

**Acceptance:** A concurrent command cannot append an invalid history. Replaying events never re-executes payment or notification side effects. Test deletion/redaction references and archive gaps; immutable events do not imply infinite retention of personal data.

**Verification interface:** `make verify ID=C12-04`.

**Book section IDs:** B12-024, B12-025, B12-026, B12-027, B12-028.

**Additional pattern obligations:** P10; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C12-05 — Streaming operators and incremental views

**Mode:** Go implementation lab + application projections. **Status:** planned. **Depends on:** C12-01, C12-02, C03-06.

**Target location:** `labs/ch12/operators; internal/projections`.

**Build:** Implement filter/map/enrich, split/aggregate, CEP sequences, stream search, incremental aggregates, and materialized views. Add a small choreography-based fulfillment notification flow to compare with the orchestrated checkout workflow. Add HyperLogLog-based approximate distinct counts with a Go implementation/integration and merging across partitions.

**Acceptance:** Incremental results match a full recomputation, including updates/deletes and duplicate events. CEP timers survive restart. Choreography exposes ownership, correlation, and failure handling rather than hiding business workflow inside an unbounded event chain. Compare estimates against exact cardinality across known seeded distributions and expose error/memory trade-offs.

**Verification interface:** `make verify ID=C12-05`.

**Book section IDs:** B12-029, B12-030, B12-031, B12-032, B12-033, B12-034, B12-035, B12-036.

**Additional pattern obligations:** P13, P17, P22, P48; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C12-06 — Event time, windows, watermarks, and late data

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C12-05, C09-02.

**Target location:** `labs/ch12/event-time`.

**Build:** Implement processing/event-time modes, tumbling/sliding/session windows, per-partition watermarks, idle-partition handling, allowed lateness, late-event correction/retraction, and source-clock policy.

**Acceptance:** Out-of-order fixtures give correct results under the declared lateness contract. Idle sources do not stall all output; watermarks do not advance on wall-clock guesses that discard valid events. Compare latency, completeness, and state retention.

**Verification interface:** `make verify ID=C12-06`.

**Book section IDs:** B12-037, B12-038, B12-039, B12-040, B12-041.

**Additional pattern obligations:** P44; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C12-07 — Stream and table joins

**Mode:** Go implementation lab. **Status:** planned. **Depends on:** C12-06, C11-04.

**Target location:** `labs/ch12/joins`.

**Build:** Implement windowed stream-stream joins, stream-table enrichment, and table-table incremental joins. Add versioned/as-of lookup to show processing-order versus event-time dependence and define retractions for changed dimensions.

**Acceptance:** Replay with different delivery orders produces the specified temporal result. Bound join state with a documented lateness/retention contract and verify missing, duplicate, late, and deleted records.

**Verification interface:** `make verify ID=C12-07`.

**Book section IDs:** B12-042, B12-043, B12-044, B12-045, B12-046.

**Additional pattern obligations:** P17, P44; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C12-08 — Checkpoints, microbatches, and transactional sinks

**Mode:** Go implementation lab + Kafka integration. **Status:** planned. **Depends on:** C12-07, APP-06.

**Target location:** `labs/ch12/recovery`.

**Build:** Implement microbatch boundaries, state checkpoints, changelog restore, and atomic sink-state/input-offset commits. Compare Kafka read-process-write transactions with PostgreSQL inbox transactions and external-service idempotency.

**Acceptance:** Crash between every state/output/offset action; restored output matches the reference computation. Kafka transactional guarantees are confined to participating Kafka operations and compatible consumers; external database/API side effects use their own explicit correctness protocol.

**Verification interface:** `make verify ID=C12-08`.

**Book section IDs:** B12-047, B12-048, B12-049, B12-050, B12-051.

**Additional pattern obligations:** P43; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M12 — Integrated dataflow and online evolution

### C13-01 — Data integration and causal ordering

**Mode:** application capstone. **Status:** planned. **Depends on:** C12-03, C12-08.

**Target location:** `internal/integration; docs/data-lineage`.

**Build:** Connect authoritative contexts to search, analytics, and customer views through a versioned event/dataflow contract. Record per-stream causality, independent ordering domains, deduplication, replay, and derived-store repair.

**Acceptance:** Inject cross-store dual-write failures in an unsafe comparison, then demonstrate outbox/CDC recovery. Cross-shard events carry necessary dependencies; no global total order is invented from timestamps or unrelated offsets.

**Verification interface:** `make verify ID=C13-01`.

**Book section IDs:** B13-001, B13-002, B13-003, B13-004, B13-005, B13-006.

### C13-02 — Online reprocessing and batch/stream convergence

**Mode:** application capstone. **Status:** planned. **Depends on:** C13-01, C11-05.

**Target location:** `internal/projections/rebuild; labs/ch13/reprocessing`.

**Build:** Implement shadow projection generations, bounded snapshot plus catch-up, versioned transforms, high-watermark barriers, checksum reconciliation, atomic reader cutover, and rollback. Reuse a pure transform for bounded batch and unbounded streaming inputs. Run a lambda-style batch-plus-speed-view comparison and a kappa-style log-replay comparison using the same pure business transformation.

**Acceptance:** Accept live writes throughout a projection change and prove no skipped/duplicated final effects. Explicitly test transactions committing out of numeric ID order; a BIGSERIAL maximum is not a safe commit watermark. Rollback switches a compatible reader generation. Both converge to the same bounded input result; report duplicate logic, replay cost, and correction behavior.

**Verification interface:** `make verify ID=C13-02`.

**Book section IDs:** B13-007, B13-008, B13-009, B13-010, B13-011.

**Additional pattern obligations:** P09, P35; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C13-03 — Unbundled data systems and service extraction

**Mode:** application comparison deployment. **Status:** planned. **Depends on:** C13-02, APP-02.

**Target location:** `labs/ch13/dataflow; deploy/compose/extracted-services`.

**Build:** Compose durable log, compute, storage, indexes, and caches; expose lineage and checkpoint metadata. Extract inventory or fulfillment behind its published contract using branch-by-abstraction and a strangler route, with one owner of writes at every stage.

**Acceptance:** The monolith and extracted deployment pass the same domain contract suite. Replay rebuilds state independently of worker deployment. Compare operational complexity and failure modes against an integrated PostgreSQL baseline before recommending extraction.

**Verification interface:** `make verify ID=C13-03`.

**Book section IDs:** B13-012, B13-013, B13-014, B13-015, B13-016, B13-017, B13-018, B13-019, B13-020, B13-021, B13-022.

**Additional pattern obligations:** P04, P07, P36; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C13-04 — Observing derived state and client synchronization

**Mode:** application + Go CLI clients. **Status:** planned. **Depends on:** C06-06, C13-02, APP-08.

**Target location:** `internal/streamingapi; cmd/offline-client; labs/ch13/clients`.

**Build:** Implement SSE or WebSocket state notifications with resume tokens, snapshot-plus-delta handoff, client cache versions, authorized read events, and multishard result assembly. Reuse offline clients and document cache/materialized-view boundaries.

**Acceptance:** Reconnect after missed notifications and recover through a snapshot when history expired. Client views converge without treating delivery as authoritative state; revoked access stops subscriptions. Multishard freshness and non-atomic reads are explicit.

**Verification interface:** `make verify ID=C13-04`.

**Book section IDs:** B13-023, B13-024, B13-025, B13-026, B13-027, B13-028, B13-029.

**Additional pattern obligations:** P23, P24; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C13-06 — Constraints, timeliness, and coordination avoidance

**Mode:** application + Go comparison lab. **Status:** planned. **Depends on:** C07-03, C10-05, C13-05.

**Target location:** `labs/ch13/constraints`.

**Build:** Implement strict per-scope uniqueness with database constraints/consensus, ordered-log ownership, multi-shard request workflows, and quota/escrow alternatives. Contrast strict stock/payment integrity with eventually fresh dashboards and explicitly relaxed business constraints. Demonstrate monotonic versus nonmonotonic computations and the coordination implications of negation/uniqueness.

**Acceptance:** A partition cannot create duplicate ownership for a strict key. Escrow allocation never exceeds total stock, while availability limits are visible. Track invariant violations separately from projection lag; compensations or apologies are an explicit domain choice. A monotonic union converges under reordered delivery while the selected nonmonotonic constraint needs coordination or an explicit business relaxation.

**Verification interface:** `make verify ID=C13-06`.

**Book section IDs:** B13-037, B13-038, B13-039, B13-040, B13-041, B13-042, B13-043, B13-044.

### C13-07 — Auditing and independent integrity verification

**Mode:** application + Go audit tools. **Status:** planned. **Depends on:** C13-02, C13-06.

**Target location:** `cmd/audit; internal/reconciliation; docs/runbooks`.

**Build:** Build independent stock, order, payment, and journal reconciliation from authoritative records, lineage hashes, checksummed archives, anomaly reports, and audited repair commands. Use a separate straightforward oracle so validation does not repeat the same faulty derivation.

**Acceptance:** Inject a buggy projection, corrupted record, missed event, and duplicate financial effect; the audit identifies each divergence. Repair is resumable and authorization-checked, preserves an audit trail, and never rewrites financial history silently.

**Verification interface:** `make verify ID=C13-07`.

**Book section IDs:** B13-045, B13-046, B13-047, B13-048, B13-049, B13-050.

**Additional pattern obligations:** P33; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M13 — Privacy and data lifecycle

### C01-03 — Operational responsibility and legal data boundaries

**Mode:** operations + decision. **Status:** planned. **Depends on:** OPS-01, C14-02.

**Target location:** `docs/operations; docs/data-governance`.

**Build:** Document who owns backups, upgrades, tenancy, access, and incident response in each hosting model. Link data classification, retention, jurisdiction, and purpose to the implemented privacy controls and architecture decisions.

**Acceptance:** A restore and deletion drill identifies every responsible component and derived copy. A decision exercise explains the effect of changing hosting region and provider responsibility; legal compliance is not inferred from passing code tests.

**Verification interface:** `make verify ID=C01-03`.

**Book section IDs:** B01-013, B01-018.

**Additional pattern obligations:** P45; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

### C14-02 — Consent, minimization, retention, and erasure

**Mode:** application + decision exercises. **Status:** planned. **Depends on:** APP-08, C13-07.

**Target location:** `internal/privacy; docs/governance; labs/ch14/privacy`.

**Build:** Implement purpose-scoped consent, withdrawal, export, retention jobs, access audit, deletion workflows across authoritative/derived stores, and replay suppression for erased personal data. Keep personal payloads outside immutable domain events where feasible; document lawful-retention exceptions and backup expiry.

**Acceptance:** Withdraw consent and stop the covered optional processing; export only authorized data. Erase a synthetic subject and verify caches, search, objects, snapshots, events/references, and restored-backup handling. A deletion ledger prevents replay resurrection. Explain surveillance, power, accountability, and regulation in concrete design exercises; do not claim legal certification.

**Verification interface:** `make verify ID=C14-02`.

**Book section IDs:** B14-005, B14-006, B14-007, B14-008, B14-009, B14-010, B14-011.

**Additional pattern obligations:** P45; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M14 — Full concept and pattern audit

### OPS-02 — Full-concept audit and complete guide

**Mode:** operations. **Status:** planned. **Depends on:** C13-07, C14-01, C14-02.

**Target location:** `docs/coverage; docs/chapters; docs/patterns; cmd/guide`.

**Build:** Audit every section, glossary term, and indexed concept against executable evidence. Add missing sub-concepts found in prose. Publish original chapter walkthroughs, concept-to-code links, diagrams, fixed/broken comparisons, and application integration scenarios.

**Acceptance:** No book concept is silently deferred. Each distinct concept has a named explanation and either executable evidence or a justified decision exercise with observable consequences. Every backend pattern catalog entry has a complete example. Vendor names map to the concept they illustrate, not an obligation to clone every product.

**Verification interface:** `make verify ID=OPS-02`.

**Additional pattern obligations:** P42; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).

## M15 — Production reference release

### OPS-03 — Production reference release and recovery evidence

**Mode:** operations. **Status:** planned. **Depends on:** OPS-01, OPS-02.

**Target location:** `deploy/helm; deploy/terraform/aws; docs/runbooks; release`.

**Build:** Complete a portable Kubernetes profile, an AWS reference Terraform deployment, release promotion and rollback, backup/PITR drills, tenant migration, capacity reports, dependency upgrades, and a threat model. Freeze the tested support matrix and remaining environment assumptions.

**Acceptance:** In a clean environment run full acceptance, restore into a separate database, verify journal and inventory integrity, rebuild projections, roll forward/back compatible binaries, and exercise failover. Publish measured results with hardware and configuration. Actual cloud provisioning requires an account and budget; local tests do not establish multi-zone availability.

**Verification interface:** `make verify ID=OPS-03`.

**Additional pattern obligations:** P38, P40, P41; implement the contracts in [the pattern catalog](backend-pattern-catalog.csv).
