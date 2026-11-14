# Inventory session consistency

C06-04 adds an opt-in read contract over the existing Inventory authority. Stock receipts and reservations still commit on the primary. Authoritative stock, reservation and combined snapshot HTTP reads can use physical replicas after they reach the client's recorded WAL fence. The native lab uses the actual Atlas binary and PostgreSQL primary/two-standby processes.

## Run

```bash
make verify ID=C06-04
# Or select the layers:
make session-test session-postgres session-demo
go run ./cmd/guide verify-session-consistency
```

The default native runner uses the pinned PostgreSQL 18.6 image, an unprivileged user, no external network and disposable tmpfs. For an already installed PostgreSQL 18 runtime, set `ATLAS_SESSION_PG_BIN` to its absolute executable directory and optionally `ATLAS_SESSION_PG_SHARE` to its initdb shared files. The runtime must be executable by the current unprivileged user. All owned API/database processes stop before their temporary directories are removed. Reports and binary hashes/revision/dirty provenance are written under ignored `artifacts/session-consistency/` and `artifacts/session-binaries/`. The model also runs 15,625 finite six-operation schedules against a separate maximum-observation oracle.

## Enable on the API

Apply migrations, then set these variables on `atlas serve`:

```dotenv
ATLAS_READ_ROUTING_ENABLED=true
ATLAS_READ_TOKEN_KEY=<separate random secret of 32 to 256 bytes>
ATLAS_READ_REPLICAS={"replica-a":"<physical standby DSN>","replica-b":"<physical standby DSN>"}
```

Configuration is optional and defaults off. Replica labels are server-owned lowercase slugs; at most eight pools are opened with four connections each. Commands always use the primary pool. A restricted application role needs schema USAGE, its existing Inventory table grants and EXECUTE on `platform.read_identity()`; it does not need access to PostgreSQL control files or permission to update the private source/epoch tables. The lab administrator is a disposable fixture role, not a production privilege recommendation. Never put live DSNs or signing keys in committed configuration.

## HTTP contract

Supported reads are `GET /v1/stock/{sku}`, `GET /v1/reservations/{id}` and `GET /v1/inventory/snapshot?sku=BOOK&reservation_id=<uuid>&warehouse_id=default`. The last route requires the reservation to match the authenticated tenant, SKU and warehouse, and reads both rows in one read-only repeatable-read transaction. Commands accept a session token and return a new fence after their successful commit or saved-response replay. The [OpenAPI contract](../api/openapi/inventory/openapi.json) documents these headers.

| Request header | Meaning |
| --- | --- |
| `X-Session-Token` | Latest opaque signed token retained by this session; optional for its first operation |
| `X-Read-Consistency` | `primary` (default) or `replica`; read routes only |
| `X-Read-On-Lag` | `pending` (default) or explicit `primary` fallback |
| `X-Read-Wait-Ms` | Replay-wait budget 0–2000, default 100 |

Successful observations include `X-Session-Status: issued`, `X-Session-Token`, a server-owned `X-Read-Source` label and `X-Read-Reason`. Personalized responses use `Cache-Control: no-store`. Authoritative 404 observations also carry a fence. A lag timeout returns **425** with `Retry-After: 1` and no advanced token, or **200** from the primary with reason `replica-timeout-primary`. No successful stale row is substituted. Invalid/tampered/wrong-tenant tokens return **400**, expired tokens **410**, incompatible source/history **409**, and unavailable authority **503**. Canceled caller contexts and bounded SQL failures retain their existing HTTP error handling. Duplicate consistency headers, invalid modes/budgets and read-mode headers on commands are rejected.

The wait budget covers retrying replay probes. Wait zero performs one bounded probe per configured replica; there is no intentional replay retry. Each pool/probe operation has a 100 ms context, capture has 500 ms and the data transaction has 2 seconds, beneath HTTP's existing request limit. PostgreSQL cancellation/connection cleanup can add the configured cancellation grace. These are finite operation budgets, not an exact end-to-end latency SLA. Sticky preference is tried first, then other configured endpoints. A missing or failed sticky endpoint never removes the watermark.

The asynchronous `/projection` route rejects consistency headers: WAL replay does not establish that its application worker has processed the source outbox. Other bounded contexts, GraphQL, gRPC and internal workflow calls retain their existing contracts. They do not gain this guarantee by sharing the database or signing key.

## What a returned token establishes

1. Validate HMAC, tenant subject, expiry, source UUID and history epoch. Reject another history instead of comparing unrelated LSNs.
2. Probe a replica's replay position **before** starting its data transaction. It must be at least the incoming local-shard watermark. Check live endpoint role/history again when establishing the repeatable-read snapshot.
3. Read every row of the requested view in that single snapshot.
4. After the observation, capture the live primary's **WAL insert position**, verify its identity/history and sign a new token at least as large as the incoming local watermark. Insert position includes already visible asynchronous commits; primary flush position alone need not.

The post-read primary fence conservatively bounds all data the physical replica could have returned. It can include unrelated or later commits that this response's snapshot did not contain. The output token is a **minimum replay position for future reads**, not an exact snapshot timestamp and not a claim that this response included everything up to its output token. Sampling a replica replay pointer after opening a snapshot would not establish that boundary. Strict reads therefore retain a small primary query and a primary-availability dependency.

Write fences are captured after the business commit. A failure there leaves the original successful receipt intact with `X-Session-Status: unavailable`. Reporting a failed business write would misrepresent the commit. Use a primary read before returning to replica reads, or retry the identical command/idempotency key to retrieve its saved receipt and a fresh fence. Token capture never changes that saved receipt or replays its business effect.

The signed payload has a five-minute lifetime, SHA-256 tenant subject, format version and a sorted vector of at most eight `(shard, source UUID, epoch, LSN, sticky label)` entries. Tokens/headers are capped at 4096 bytes and the HMAC key at 256 bytes. Only the Inventory component is used by the application increment. Other signed components are retained but are not proof of a distributed snapshot or automatically discovered domain dependencies. Keys belong to a trusted routing authority; replicas cannot mint their own arbitrary fences.

## Client ownership of the session

The [Go session client](../internal/platform/readrouting/client.go) serializes calls within one session using a context-cancelable gate, retains the last returned token, starts on the primary and forces a primary read after a mutation without a valid issued fence or after a failed exchange. It bounds its HTTP time, declines redirects and accepts only paths under its configured origin. Pass the same idempotency key when retrying a command. Separate clients represent independent sessions; ordinary concurrent independent HTTP requests remain concurrent on the server.

The stateless server cannot prevent a caller from deliberately dropping or reverting its own token. This guarantee requires forwarding the latest recorded watermark. Parallel branches sharing one logical session need serialization or an explicit authenticated vector-join protocol; this increment implements serialization. Beginning another session after expiry/history change is an explicit reconciliation decision, followed by a primary read. Silently deleting an incompatible token would erase the guarantee.

## Reproduced anomalies and limits

With private telemetry enabled, `atlas_read_replica_probe_up`, `atlas_read_replica_fence_gap_bytes` and `atlas_read_replica_probe_timestamp_seconds` describe the latest request-driven probe for each configured label. Interpret gap only with a successful, recent role/history probe. It measures distance from that request's session fence, not a continuously sampled primary head or elapsed-time staleness. Unobserved timestamps start at zero; idle replicas need a separate active health sampler if required operationally. `atlas_read_pending_total` and `atlas_read_primary_fallback_total` expose policy outcomes. Scrapes query bounded in-memory state, without database calls or tenant/session/token/DSN labels. The native drill separately samples actual primary/replay LSN byte gaps before/after catch-up and verifies the real private metrics endpoint during lag/recovery. Byte gaps alone cannot be converted to seconds; a stale last-replay timestamp during an idle workload does not establish user-data lag.

| Observation before the remedy | Implemented remedy |
| --- | --- |
| Primary acknowledges BOOK on-hand 20 while paused standby returns 10 | Write token gates replay; pending or labeled primary fallback |
| Alternating raw replicas returns stock version 2 then 1 | Retain the maximum observation fence across routing changes |
| Stale stock shows reserved 0 while a faster replica shows its committed reservation of 15 | Single endpoint/transaction combined snapshot returns reserved 15 with that reservation |
| Known child at shard B is combined with an older parent from A | Model explicitly propagates A's dependency and re-reads A to its required fence |
| Debit A1 is visible but credit B1 is not, producing A=0/B=0 | Per-shard watermarks permit this causal prefix; atomic cross-shard transfer visibility needs another protocol |

The cross-shard case is a deterministic Go model, not a deployed sharded Inventory system. It distinguishes causal-prefix requirements from distributed atomic snapshots. The native drill separately verifies delayed replay within a wait, endpoint stop/restart, unavailable sticky preference, role mismatch, history rotation, tenant/tamper rejection, saved receipt replay, authoritative absence and twelve concurrent tenant-isolated observations. Unit/integration tests cover postcommit fence failure, session-client watermark retention, cancellation, private authority grants, unrelated source rejection and an actual committed write interleaved inside a repeatable-read snapshot.

Source UUID is stable under physical replication. The existing writer epoch identifies an admitted history. Operators must fence the former primary and rotate authority before admitting a new writable history, then restart API instances. An old API stops serving strict results when its live authority differs; an old token is rejected by a new API, and an old-history replica is excluded until it catches up. This is explicit authority management, not automatic failover or consensus. Production topology/grant audits, cross-context/transport integration, general causal dependency exchange, distributed snapshot protocols, TTL/key lifecycle and scale remain open in the unchanged baseline.

See [heading evidence](chapters/ch06/session-consistency.md), [decision](adr/0027-inventory-session-fences-and-snapshot-reads.md), [runbook](runbooks/session-consistency.md), [work plan](session-consistency-work-plan.md) and [validation](VALIDATION.md). C06-04 and pattern P30 remain in progress.
