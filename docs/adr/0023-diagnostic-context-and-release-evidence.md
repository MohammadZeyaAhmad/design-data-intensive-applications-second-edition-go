# 0023 — Diagnostic context, independent audit and checked release evidence

Date: 14 November 2026. Status: accepted for the reference increment.

Trace context must survive asynchronous checkout without becoming an authorization capability or a command identity. An exporter, slow scrape, high-cardinality resource label or accidental payload log must not change the business result or exhaust unlimited memory. A fast response does not prove freshness or integrity, and a checksum alone does not authenticate a release publisher.

Use the real OpenTelemetry SDK with finite nonblocking batching, bounded OTLP export, fixed operational attributes and redaction by default. Preserve one optional validated trace carrier in diagnostic provenance, outside command payload hashes. Expand readers first and keep production of the new optional field disabled during mixed-reader deployment. Persist the original message carrier instead of rewriting it on each retry. Retain compatible readers after emission/replay begins.

Expose authenticated metrics on a separate listener with a dedicated reader key. Keep HTTP labels finite and measure the availability denominator without a status split. Independently audit unprocessed PostgreSQL source age and equal-version source/view drift; unavailable audit evidence is not a healthy zero. Use native Prometheus evaluation and an actual SQL fixture to verify firing and repair. Dashboard/alert thresholds are local examples, not production SLO evidence.

Derive release dependency inventory and VCS state from actual static binaries, validate the SBOM with pinned upstream schemas, and verify artifact hashes. Require clean builds in CI. The candidate workflow gates provenance attestation on a successful same-repository, exact-SHA verification run. Do not infer authentication from unsigned checksums or vulnerability clearance from a module inventory.

Reuse the existing historical/current application rollout and rollback drill, and prove migration serialization with real advisory-lock waits. Helm and AWS files establish reviewable references and distinct credential boundaries without applying infrastructure. Full production grants, network/identity operation, canary policy, telemetry retention/sampling operation, broader transport instrumentation and complete book audits remain open.

Evidence: [guide](../OBSERVABILITY.md), [scenario](../../scenarios/observability-delivery.json), [native tracing/audit/migration checks](../../tests/integration/observability_test.go), [actual Prometheus check](../../tests/integration/observability_prometheus_test.go), [release verifier](../../cmd/releasecheck/main.go) and [deployment chart](../../deploy/helm/atlas/Chart.yaml).
