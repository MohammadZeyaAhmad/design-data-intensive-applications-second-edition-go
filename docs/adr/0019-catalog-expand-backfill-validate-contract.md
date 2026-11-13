# ADR 0019: separate Catalog binary rollout from data and column retirement

Status: accepted, 13 November 2026.

Use the real Catalog brand name as a concrete rolling evolution. Add display_name without rewriting old tuples, bridge old/new writes in the database, and expose the owner through a stable read view. Keep the public HTTP name and optimistic business version unchanged. Build the actual historical application from pinned commit 393f703 and run it concurrently with the current binary against an owned database.

Ship only expansion in normal startup migrations. Prepare the partial index concurrently, and backfill small transactions using the remaining-data predicate with SKIP LOCKED. Do not advance a saved cursor past skipped rows. Commit receipts, parameter identity and progress with the data, enforce database/context time budgets, and persist throttling. Do not create business events or revisions for a representation repair.

Commit NOT VALID constraint metadata before validating in a separate transaction. Keep old columns/bridge during a real binary rollback. Allow destructive contraction only after successful validation, explicit retirement of declared old readers, no observed tagged legacy database sessions, elapsed nonshortenable rollback support and an audited retirement decision. Replace the view before dropping its old dependency. Treat deployment inventory/session labels as trusted operational inputs, with stronger production roles/fencing still required.

Version the immutable Catalog history payload independently of aggregate versions and SQL layout. New writers explicitly emit schema 2; historical writers retain schema 1. Use published wire types/upcasters and retain original JSONB source/metadata images. Archive both writer definitions and hashes, publish immutable complete files and replay offline after contraction. Bound every export and reject unknown future schemas rather than guessing current Go layout.

Verify ordinary PostgreSQL behavior, interrupted index repair, skipped rows, durable retries, actual worker SIGKILL, mixed-version HTTP writes, rollback and complete contraction. Retain raw latency/process evidence and source digests. The local scenario is not certification of arbitrary reader inventories, device power loss, full backup/restore or production retention policy.
