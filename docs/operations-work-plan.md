# OPS-01 implementation increment

Date: 14 November 2026. The complete baseline in `docs/plan/` remains unchanged.

1. Add real OpenTelemetry SDK/OTLP export, allowlisted logs and finite Prometheus labels at HTTP, SQL, event publication, workflow and provider boundaries. Optional durable trace context uses an additive reader expansion and a producer flag that stays off during a mixed-version rollout. Tracing never changes command identity.
2. Add independent freshness/integrity audits, Prometheus recording/alert rules and Grafana dashboard definitions. Verify actual rule evaluation against injected lag and projection drift, including repair and telemetry-backend failure.
3. Verify native checkout trace parentage across persisted messages and independent runtime instances. Reuse compatible historical/current API traffic and guarded rollback/contraction, and exercise competing migration jobs against a disposable database. Weighted canary/mesh and broader graceful-traffic audits remain explicit release work.
4. Build checksum-verified binaries, build-info-derived SBOMs and release manifests; retain CI artifacts and pin action sources. Add a renderable Helm reference with separate credential references and an AWS deployment plan, without applying cloud infrastructure.
5. Run scoped and existing regression checks, save measured evidence and limits, update the 88-package tracker, commit/push and verify CI for the exact SHA.

This is one verified increment. Distributed telemetry operation, cloud credentials/provisioning, broader audit and production deployment remain part of the original release scope.
