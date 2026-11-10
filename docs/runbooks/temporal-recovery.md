# Temporal checkout recovery

Use the local [Temporal profile](../TEMPORAL.md). Start with the checkout API view's `engine`, `status`, `paused`, `cancel_requested` and `attempts`. Use the existing tenant-scoped `admin workflow-status` and control-history commands for persisted leases and audit. Locate the execution in <http://127.0.0.1:8233> by `atlas-checkout/<tenant>/<checkout-UUID>`.

| Observation | Inspect | Recovery |
|---|---|---|
| Accepted but still reserving; no workflow | Temporal server/dispatcher health and `temporal_started_at` | Restore server and `temporal-dispatcher`; it retries the stable ID |
| Pending activity and unavailable SDK worker | `temporal-worker` logs/health; task queue | Restore that worker; wait for activity timeout and the thirty-second lease |
| Capturing with retry attempts | Provider health, local payment receipt and journal | Restore provider; retry/reconciliation uses the original intent |
| Paused job | Control version and immutable control history | Resolve the investigation; resume through the scoped local CLI |
| Cancellation requested while paused | First cancellation metadata and pause audit | Resume; compensation preserves an already confirmed source outcome |
| Repeated owner/step fence failure | Engine, retained step receipts and workflow history | Investigate incompatible code or conflicting execution; preserve evidence |
| Temporal execution failed/terminated but business job pending | History failure and source outcome | Restore compatible code and plan a reviewed history repair; ordinary workflow retry only wakes a healthy execution |

Do not change `checkout.jobs.engine`, clear receipts, reset progress, or create a new checkout/payment ID to force a retry. The database rejects owner/request edits and receipt mutation. Do not terminate a purchase to cancel it; use the authenticated checkout cancellation API. Termination does not run compensation. The current local admin command does not implement Temporal history reset or automatic repair of failed/terminated executions.

For a local server or worker outage, preserve volumes and run:

```sh
docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/temporal.yaml -p ddia-go up -d --wait temporal
docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/temporal.yaml -p ddia-go start temporal-dispatcher temporal-worker
```

`make verify-temporal` reproduces acceptance during server downtime, server/worker SIGKILL with persisted run identity, and worker death during a slow provider activity. The last case lets the original provider request continue and waits for natural lease expiry. Each completed purchase must retain one capture journal entry and the frozen quote total. Generated evidence is in ignored `artifacts/temporal-recovery.json`.

Before an upgrade, replay the retained v0 fixture and relevant exported synthetic histories with the updated workflow. Coordinate deployment of incompatible activity or phase schemas. Keep the version branch until histories using it have completed or have an explicit migration path. The profile's SQLite persistence demonstrates process restart, without establishing backup, multi-node failover or production disaster recovery.
