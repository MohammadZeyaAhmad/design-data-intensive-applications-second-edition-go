# Scoped workflow recovery

Use [operator contracts and commands](../OPERATIONS.md) for Checkout and Fulfillment. The CLI requires the local operator database binding, an explicit tenant/resource/kind, controlled reason, stable request key and current control version. It creates no public administrative HTTP route.

## Investigate and pause

Inspect `workflow-status` and the corresponding public business state. Preserve the original checkout/payment/allocation IDs. If another operator changed the control version, read status before issuing a new action. If a lease is live, wait for the attempt or lease expiry; a failed control has no saved success or scheduling effect. Do not clear leases manually to make a command pass.

Pause at the observed control version using reason `investigation`. A successful response includes an audit UUID and saved before/after scheduling snapshots. Read current status separately. Pausing stops new claims, and an old external call may still complete. Use payment journal/Inventory receipts and immutable source facts when deciding whether an effect occurred; a timeout or paused checkpoint alone cannot answer that question.

## Restore and resume

Restore the failed dependency or deploy the required compatible code. Resume the paused resource with a new logical request key and the current control version, using reason `dependency_restored`. The worker keeps its original operation IDs and completed progress. Checkout can remain compensating until cleanup finishes; a confirmed source order is preserved. A shipment can remain partly dispatching until every original allocation has a validated receipt.

For an unpaused retrying resource whose dependency is healthy, `retry` with reason `operator_retry` schedules its existing attempt immediately. It neither resets diagnostic counters nor bypasses leases, terminal states, resource closure or source confirmation/failure fences. Shipment retry applies only to dispatching work. Repeating a previously successful control key returns its saved receipt, including after the resource finishes; use status to see the new state.

## Review the audit

`workflow-history -after-version N -limit 50` pages by resource-local control version. Compare its action/reason, executor metadata and before/after snapshot. Audit writes and scheduling mutations commit together; a failed audit insert leaves neither a control result nor a partial scheduling change. Ordinary audit update/delete is rejected. Store incident notes in an appropriate external system; controlled reason codes keep free-form personal data out of this history.

These controls do not repair corrupted event payloads, replay broker offsets, change checkpoints, invent missing allocations, refund confirmed purchases or undo shipped stock. Those cases require their owning business/reconciliation workflow. Projection quarantine still uses its existing scoped procedure in [projection recovery](projection-recovery.md). Production operator authentication, access roles and a generalized reviewed repair framework remain tracked work.

`make verify-operations` performs a local provider-outage, pause/restart/resume drill through the actual CLI and authenticated HTTP APIs. Reports are in ignored `artifacts/operations.json`. The PostgreSQL suite separately exercises the expired-worker/effect-after-pause boundary and an injected audit-write failure.
