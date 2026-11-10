# Checkout recovery

Inspect `GET /v1/checkouts/{id}`, authoritative `GET /v1/orders/{order_id}`, and the tenant-scoped payment/journal endpoints. Checkout status excludes the operational payment reference. Do not print `.env` or the internal provider token.

- `capturing` during a provider outage remains pending and retries. A timeout does not justify a new payment ID or a decline.
- `compensating` means the failure fence is recorded or being retried, but money/resource cleanup is incomplete. Refund response loss may require reconciliation before inventory closure completes.
- `confirmed` requires a captured payment and committed allocations. A cancellation request can lose to confirmation; a stale compensator then preserves the confirmed purchase.
- `failed`/`cancelled` are final workflow outcomes after payment closure and closure of every possible stock step. The source order can show `checkout_failed` earlier while compensation is still pending.

Worker claims expire after thirty seconds. A restarted worker may wait for the old claim, then resumes using the same operation IDs. Do not reset payment IDs, delete financial entries, reopen closed inventory steps, or force an unknown payment to “declined.” Preserve request identity and let reconciliation recover the actual result.

To exercise the local recovery path, run `make verify-checkout-recovery`. It stops only this project's `provider` and `worker` services, restarts them, and checks one resulting capture. `make verify-checkout` checks normal/lost-response/declined outcomes and preservation of accepted pricing. The integration suite additionally forces partial allocation failure, lost refund acknowledgment, dispatch redelivery and a superseded lease.

Development configuration upgrades add a random provider token and a loopback provider URL while preserving existing credentials. For a coordinated source upgrade, stop `api worker provider` with this repository's Compose command, then run `make up`. The database volume is preserved. Use [scoped workflow controls](workflow-control.md) for audited pause/resume/retry without resetting progress. A paused checkout retains cancellation requests until resume. Production mixed-version rollout, remote operator identity, arbitrary repair and alerting remain future work.
