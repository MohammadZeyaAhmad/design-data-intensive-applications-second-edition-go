# Replication log representations

| Book section | Executable evidence |
| --- | --- |
| B06-009 — Implementation of Replication Logs | One native warehouse fixture compared across controlled statement replay, actual physical WAL and a Go pgoutput consumer |
| B06-010 — Statement-based replication | Identical SQL under controlled timezone/random/sequence context produces four different resolved fields; logical rows preserve source values |
| B06-011 — Write-ahead log shipping | Real consistent base backup and streaming standby, row/DDL/sequence behavior and physical format/cluster checks |
| B06-012 — Logical (row-based) log replication | Exported snapshot handoff, relation/tuple metadata, ordered committed row changes, TOAST/key images, schema pause/migrate/resume, atomic target checkpoints and retained replay |

The [Go decoder and sink](../../../labs/ch06/log-formats) implement protocol-v1 transaction projection over four allowlisted tables. They do not implement PostgreSQL's physical WAL engine. The [native fixture](../../../labs/ch06/log-formats/native.go) verifies real source/physical/logical state equality before independent promotion probes. The [framing checks](../../../labs/ch06/log-formats/framing.go) bound counts before allocation; the [tests](../../../labs/ch06/log-formats/decoder_test.go) retain an independent state oracle and the discovered malformed-wire regression.

The [guide](../../LOG_FORMATS.md), [scenario](../../../scenarios/replication-log-formats.json) and [validation](../../VALIDATION.md) distinguish row facts from business events, provider format assumptions from actual compatibility tests, and source acknowledgment from target durability. Full semantic, cross-version, schema lifecycle, production role/retention and scale audits remain against the unchanged baseline.
