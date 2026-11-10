# ADR 0007: Temporal schedules the shared checkout feature

Status: accepted for the seventh implementation increment.

Add a real Temporal alternative to compare durable execution with the explicit PostgreSQL process manager. Keep orchestration in the Checkout context, which already owns this process; the original backlog's proposed `ordering/workflows` location would mix scheduling ownership with Ordering's authoritative outcome facts.

Select the engine at quote acceptance and retain it in the dispatch request and job. Enforce immutable ownership in SQL and filter both engines' claims/checkpoints. Existing jobs default to PostgreSQL. Profile changes never change business idempotency or migrate accepted work implicitly.

Run the existing advance feature in an activity. The workflow schedules it using tenant, checkout ID and a global ordinal. Each successful progress transition and its exact return value commit atomically in PostgreSQL. A repeated activity returns that receipt; a crash before checkpoint retries the original resource IDs under a new lease. The scheduler still relies on each resource owner's idempotency and reconciliation rules.

Bridge start and control intents through separate database acknowledgments. Use stable workflow IDs, duplicate-start rejection and type validation. Database flags remain authoritative; durable wake signals and timers prompt another check. Native cancellation uses a disconnected context to complete business compensation. Bounded workflow runs carry their ordinal across continuation. Supported version markers preserve old timer histories, with an actual v0 replay fixture in CI.

This adds another persistent service and a history compatibility obligation. The local profile uses one pinned development server with SQLite persistence. Production Temporal deployment, history retention/repair, encryption and worker routing require further work; C05-05 remains in progress rather than closing the entire package.
