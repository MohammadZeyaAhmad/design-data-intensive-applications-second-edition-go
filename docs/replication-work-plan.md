# C06-01 implementation increment

Date: 14 November 2026. The complete baseline in `docs/plan/` remains unchanged.

1. Implement a bounded Go leader/follower model with separate local durability, remote flush and replay stages, explicit acknowledgment rules, snapshot/suffix bootstrap, retention gaps, intent reconciliation, fenced promotion and a split-brain counterexample. Model zone/region common-mode failures explicitly.
2. Add an opt-in Inventory database writer epoch. Configure it on every Atlas connection, guard effect/receipt/projection tables together, and hold an admission lock until transaction completion. Verify stale/missing tokens, transaction ordering and restricted-role permissions against PostgreSQL.
3. Drive actual Atlas HTTP receipts through isolated PostgreSQL primary/standby processes. Measure WAL positions, byte lag, acknowledgment latency and acknowledged loss for asynchronous, synchronous remote-flush and remote-apply modes. Include follower outage/catch-up, uncertain commits, stale replica reads, an LSN wait, confirmed primary fencing, promotion and actual `pg_rewind` rejoin.
4. Take a real online physical backup during application writes, verify its manifest and a corruption counterexample, archive complete immutable WAL, replicate a logical operator error, and restore an independent database to a named point before that error. Validate stock, receipt and event invariants through SQL and the real API; record local RTO and deliberately excluded acknowledgments.
5. Provide a pinned, isolated Compose sandbox and a native PostgreSQL override. Save report and binary provenance artifacts, document source-heading/pattern mappings and limitations, run scoped/regression checks, update the 88-package tracker, push, and verify exact-SHA CI.

These are the implemented scenario boundaries. Production election/STONITH, independent failure domains, cross-context epoch enforcement, comprehensive replica routing, backup retention/encryption/offsite storage and full semantic/scale audits remain open in the original plan. One scenario does not complete C06-01, P30 or P41.
