# ADR 0026: Logical row projection and durable feedback

Status: accepted for the C06-03 increment. Date: 14 November 2026.

## Decision

Use native PostgreSQL to compare SQL replay, physical WAL shipping and logical row changes. Implement the Go client with pinned pglogrepl protocol v1 while retaining the existing pgx version. Validate wire counts/lengths before invoking the parser; buffer complete bounded transactions.

Bootstrap through an exported logical-slot snapshot imported before reads/streaming. Commit the copied state and consistent-point checkpoint together. Apply each later source transaction, audit and end-LSN checkpoint in one target PostgreSQL transaction. Acknowledge only that committed end position. Fence checkpoint reuse by source identity/incarnation and refuse lost-slot or forward-source-position gaps.

Treat relation metadata as a typed schema contract, without automatically executing source DDL. Preserve NULL/empty/unchanged-TOAST distinctions and old key images. Require explicit target migration before replaying an incompatible transaction. Treat sequence state and logical-slot failover as separate operational concerns.

## Consequences and limits

Retained replay can repeat a transaction after an acknowledgment is lost; the durable target end position makes projection idempotent. This does not establish exactly-once external effects or distributed election. A source administrator changing a slot/publication must coordinate incarnation and rebootstrap policy.

Physical replay requires a compatible base image/format and reproduces WAL-backed DDL and sequence state. Logical row values can cross independent clusters, while target schema and sequence operation remain explicit. Only PostgreSQL 18 is exercised natively; different-major guard tests are preflight contract tests.

Fuzzing exposed an unchecked allocation driven by a malformed truncate count. Preflight now bounds every variable wire count/length, with the failing corpus retained. Supported transactions and table/type/identity forms remain intentionally finite; unsupported forms stop without acknowledging past durable target state.
