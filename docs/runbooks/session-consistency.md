# Inventory session read recovery

Run `make verify ID=C06-04` before changing the admitted read topology. Keep the [HTTP contract and limits](../SESSION_CONSISTENCY.md) available to client owners. Logs, tokens and reports must not disclose signing keys or connection strings.

| Symptom | Action |
| --- | --- |
| 425 `replica_pending` | Retain the same token. Retry within caller budget or explicitly select primary fallback. Inspect receiver/replay lag and replica health. |
| 200 with `replica-timeout-primary` | Business data came from the primary. Retain the returned token; investigate lag before depending on replica capacity. |
| Successful command with `X-Session-Status: unavailable` | Its business commit is still successful. Require a primary read, or retry the exact command/key for its saved receipt and a new fence. |
| Lost command reply | Reuse the original idempotency key and payload. The session client conservatively requires primary reads until an issued fence arrives. |
| 400 invalid token | Check tenant credential, header duplication/size and signing configuration. Do not log the credential/token or silently substitute another tenant. |
| 410 expired token | Reconcile the session explicitly and begin another with a primary read. Expiry does not authorize sending an older token. |
| 409 history changed | Stop admitting old-history traffic. Confirm which primary is fenced, which epoch is admitted and whether clients need reconciliation for lost acknowledged writes. |
| 503 read authority unavailable | Restore the admitted primary/endpoint; strict output fencing depends on it. Do not issue a fabricated watermark. |
| Projection consistency rejected | Use authoritative stock/reservation endpoints; measure the projection worker's own source progress separately. |

For a topology change, confirm all old writers are fenced using the existing [replication runbook](replication-recovery.md), rotate `platform.writer_fence.epoch` with the migration/operator identity, then restart API processes against the admitted primary. Keep the same source UUID for a physical lineage. A separately initialized source has another UUID. Do not compare LSN numbers across different UUIDs/epochs or manually rewrite old tokens. A prior-history replica must replay the authority change before it can be selected. Signing-key rotation and session retirement must be coordinated explicitly; this increment uses one active key, without a rolling key ring.

Removing a replica label invalidates only its sticky preference. Another eligible replica must still meet the session watermark; pending/fallback remains explicit. Never promote a failed replica, reuse an epoch or disable validation merely to clear a lag timeout. Automatic election, distributed fencing and production role/topology audits remain separate baseline obligations.

The native profile owns only newly allocated temporary directories/processes and does not operate on an existing cluster. Its report contains observations and counters rather than live secrets. CI retains that report with executable SHA-256/revision/dirty provenance. Fixture credentials and data directories are removed after child processes stop.
