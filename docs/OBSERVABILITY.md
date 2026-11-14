# Operational evidence and release artifacts

OPS-01 connects Atlas checkout to real OpenTelemetry/OTLP, private Prometheus metrics, an independent source/view audit, provisioned Grafana/Jaeger, and checked release artifacts. It reuses the existing historical/current binary rollout drill. The [original baseline](plan/ddia-go-mega-project-plan.md) remains the complete implementation commitment.

```sh
make up
make verify ID=OPS-01
make up-observe
make verify-observe-live
make restore-default-observe
make down
```

The scoped check needs the local PostgreSQL test binding and database-creation permission. It runs telemetry/lineage race tests, three new PostgreSQL tests, one additional native Prometheus test, actual rule tests, release artifact checks, Helm guards, Terraform formatting and the existing rolling-schema drill. Missing required services fail. Prometheus and deployment CLI caches default to `/tmp/atlas-operations-tools`; `ATLAS_OPERATIONS_TOOLS` changes that location. Archives are pinned by SHA-256; Helm is built from source **3.17.1** with compression dependency **1.18.7**, using the Go module checksum mechanism. No cloud deployment or release publication occurs during this check.

## Trace and identity boundaries

The application uses the actual OpenTelemetry Go SDK **1.43.0**, a bounded batch processor and OTLP/HTTP export. HTTP, PostgreSQL query, Ordering checkout publication/delivery, durable checkout-step and provider-client/server spans use the same actual trace. Native tests construct independent API, worker and provider runtimes and validate persisted producer parentage and provider-client parentage after SQL delivery. This is a PostgreSQL checkout tracing increment; complete gRPC, Temporal, Kafka and all-context instrumentation remains in the package audit.

Span names and attributes come from authored operational categories. Empty/background SQL polling increments finite operation metrics without creating an unrelated root trace for every poll. SQL text/parameters, URLs/query strings, credentials, bodies, tenant/SKU/order/payment identities and arbitrary error messages are excluded. Logs default to redacting unknown messages/attributes. W3C version-00 trace IDs are diagnostic identifiers, never tenant authorization or command keys. Incoming baggage/tracestate are not exported or persisted; provider calls remove them. Exact trace/span IDs can correlate permitted logs without using resource IDs as metric dimensions.

`ATLAS_TELEMETRY_ENABLED=false` is the default. When enabled, `ATLAS_OTLP_ENDPOINT` selects a validated HTTP(S) endpoint and `ATLAS_TRACE_SAMPLE` selects 0..1 root sampling; remote sampled-parent behavior follows the SDK parent-based sampler. Sampling, finite queue overflow and export failure can lose spans. A batch holds at most **1024 spans**, exports at most **128** at once, and uses a **500 ms** export budget without SDK retry amplification. Failed exports increment a counter and suppress raw endpoint errors. They cannot roll back or alter a successful business command. The provider simulator preserves its deliberate continuing-work behavior with `context.WithoutCancel` while retaining diagnostic context.

Durable propagation adds an optional `traceparent` to the existing version-one provenance envelope. The additive SQL reader expansion is migration **000_platform/002**; historical migration checksums and old rows are unchanged. It validates the same bounded 55-byte subset as Go. `ATLAS_DURABLE_TRACE=false` keeps producers compatible with old strict readers during rollout. Enable it only after every affected reader is upgraded. The chart requires an explicit `rollout.traceReadersReady` gate before enabling emission. After new metadata exists, setting the flag off does not erase persisted history or make old strict binaries safe; retain compatible readers through the complete replay/retention horizon.

Trace context is outside command payload hashes. Retrying an HTTP checkout with a different trace parent returns the same saved response and appends **one** checkout-started event. A source message keeps its original stored carrier across publication retries; delivery attempts do not rewrite its identity/hash. Each worker step starts from that durable producer parent. Traces supplement the immutable actor/correlation/cause/request provenance.

## Metrics, audit and SLOs

The separate metrics listener is optional (`ATLAS_METRICS_ADDR`) and accepts only a dedicated bearer reader credential (`ATLAS_TELEMETRY_TOKEN`, at least 32 characters). Tenant API keys cannot read it. Its HTTP server bounds headers, deadlines and concurrent exposition. `make configure` generates a separate private token and a mode-0600 token file for Prometheus; credential contents are never printed. The development Prometheus process runs with the current local UID/GID to read the private bind-mounted secret. Public config/provisioning files contain no token values.

HTTP route/method/status categories, SQL operation/outcome, workflow stage/outcome, runtime measurements and trace-export counters have finite labels. Unknown routes/resources cannot create one series per identifier. Handler latency includes admission waiting. The dedicated availability denominator has no status split, avoiding loss of a newly appearing failure status in a `rate()` window. 5xx and rate exhaustion count as unavailable responses. See [the SLO definitions](slo/atlas-reference.md) for eligibility, histogram approximation, zero traffic, example targets and production-policy limits.

One API-owned, cancellation-aware audit reads Inventory source/view evidence in a single SQL statement with a one-second budget. It measures unprocessed PostgreSQL source count/oldest age and value disagreement **at equal versions**. A stale projection is not classified as same-version corruption. Audit-enabled/up/last-success signals distinguish ownership, failed evidence and a healthy zero. The provider/worker do not falsely advertise an unstarted audit. Missing configured scrape targets have a separate alert. This audit is not a full invariant checker or a Kafka-offset freshness monitor.

Real Prometheus **3.2.1** scrapes the authenticated endpoint and evaluates the actual recording/alert rules. An isolated PostgreSQL fixture delays an Inventory event, observes `AtlasConsumerLag` firing, drains it, introduces same-version projection drift, observes `AtlasIntegrityFailure`, and repairs only that fixture. Both alerts clear. Synthetic rule cases supplement this native execution. The example availability and merged-histogram p95 records are queried through native PromQL. No alert notification destination is contacted.

## Observation profile

The [Compose overlay](../deploy/compose/observe.yaml) pins Linux amd64 Prometheus **3.2.1**, Grafana **11.6.0** and Jaeger all-in-one **1.66.0** by image digest. All public bindings are loopback:

| Component | Local endpoint |
| --- | --- |
| API / worker / provider private metrics | 19181 / 19182 / 19183 |
| Prometheus | 19290 |
| Grafana provisioned dashboard | 19300, UID `atlas-operations` |
| Jaeger OTLP/HTTP / query UI | 19418 / 19686 |

The overlay enables durable carriers only for the fully upgraded local API, worker and provider. The live guide confirms one checkout, queries the actual Jaeger trace across those three services, checks dedicated reader credentials at all metric endpoints, verifies absence of fixture identities/secrets, and reads the provisioned Grafana dashboard. Prometheus data and Jaeger traces are finite/ephemeral development observations, not durable audit records. Grafana grants anonymous **Viewer** access only on the loopback profile.

`make restore-default-observe` recreates the named application services with baseline configuration and removes only this project's Prometheus/Grafana/Jaeger containers. PostgreSQL data is retained. Runtime metrics/tracing are deliberately allowed to fail independently from authoritative business work.

## Delivery and infrastructure references

`make release-artifacts` builds static Linux **amd64 and arm64** executables independently of the lab workspace. `ATLAS_RELEASE_ARCHES=amd64` narrows an explicit local check; CI checks both. Each archive contains:

- The actual executable and build-info-derived source revision/dirty state, compiler, OS/architecture and dependency count.
- A CycloneDX **1.6** SBOM validated offline against the pinned upstream specification schemas. Module `h1` tree hashes remain named Go module properties; they are not mislabeled as artifact file hashes.
- A manifest and SHA-256 inventory for the executable, SBOM and manifest.

The checker verifies module sums against `go.sum`, rejects replaced/unchecksummed dependencies, rereads executable build information, and checks the SBOM's actual dependency names/versions/sums. `-verify` verifies an existing bundle. Local dirty builds report their state; CI sets `ATLAS_RELEASE_REQUIRE_CLEAN=true`. File checksums detect corruption relative to a trusted inventory; they do not by themselves authenticate a publisher or establish vulnerability clearance.

The manually invoked [candidate workflow](../.github/workflows/release-candidate.yml) requires a successful reference CI run for the exact SHA in the same repository, builds clean artifacts and uses pinned GitHub provenance attestation. It is implemented but not invoked by ordinary verification. All direct action sources are pinned to immutable commits. Artifact signing/registry promotion requires that deliberate candidate operation; this increment publishes no signed release and applies no cloud infrastructure.

The [Helm chart](../deploy/helm/atlas/Chart.yaml) requires an immutable image digest, distinct API/worker/provider/migration database secret references and an explicit trace-reader gate. It supplies a pre-upgrade migration Job, non-root/read-only pods, no mounted service-account token, readiness/liveness boundaries, a two-replica API with zero-unavailable rolling policy and a disruption budget. A worker database check is readiness, not a dependency-coupled liveness restart loop. Different Secret names alone do not grant least-privilege SQL roles.

The [AWS module/reference plan](../deploy/terraform/aws-reference/README.md) establishes an immutable registry, four empty credential containers and a retained log group. Provider archive hashes are pinned for Linux amd64/arm64. It contains no credential values and creates no cluster/database. CI validates provider schema without applying resources; account-specific plans, identity bindings, actual SQL grants, encryption, network policy, backups and full cloud deployment remain OPS-03/release work.

Compatible historical/current traffic, old-reader expansion, guarded contraction and rollback reuse the [existing executable evolution drill](EVOLUTION.md). Actual advisory-lock tests observe two blocked migration jobs, cancel a third waiting caller, then allow both jobs to finish with exactly one expansion record. Broader canary traffic policy, runtime-flag retirement and production rollout evidence remain explicit audits rather than claims inferred from a template.

Reports are `artifacts/operations/{checkout-trace,audits,migration-serialization,prometheus}.json`, existing evolution reports, `artifacts/operations-live.json` and release bundles. See [ADR 0023](adr/0023-diagnostic-context-and-release-evidence.md) and the [runbook](runbooks/observability-and-release.md). All 88 original packages retain their complete scope.
