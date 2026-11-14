# C06-03 implementation increment

Date: 14 November 2026. Preserve the complete baseline under `docs/plan/`.

1. Compare actual PostgreSQL statement replay, physical WAL shipping and logical row changes for warehouse stock and receipts. Include clocks/session state, random values, sequences, rollback, ordered multi-row changes and schema changes.
2. Implement a bounded Go pgoutput consumer and relational projection sink. Validate relation/type/identity contracts, preserve NULL/unchanged TOAST semantics, apply a complete source transaction together with its durable checkpoint, and acknowledge only that committed checkpoint.
3. Export a logical-slot snapshot and import it into a repeatable-read transaction before streaming. Bootstrap a target atomically while source writes continue, then verify the snapshot/log handoff and complete row state.
4. Exercise actual consumer-process interruption before target commit and after target commit/before source acknowledgment. Verify replay/deduplication, checkpoint identity fences, source slot progress, schema pause/migrate/resume and physical-versus-logical sequence/DDL boundaries.
5. Provide a pinned native/Compose PostgreSQL profile, scoped verifier, bounded protocol fuzzing, reports, guide/runbook/heading mapping and CI artifacts. Update all 88 retained package IDs, commit/push, and verify exact-SHA CI.

This is a bounded replication-log lab, not automatic schema replication, logical-slot failover, production CDC ingestion or a distributed election protocol. Full semantic, compatibility, operational and scale audits remain in the baseline.
