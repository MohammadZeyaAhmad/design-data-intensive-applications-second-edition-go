# Actors and asynchronous messaging laboratory

From the repository root:

```sh
make actors-demo
make actors-postgres
make integration-actors
make verify ID=C05-06
```

The first command builds actual native actor-node processes and requires no database. The remaining commands require the local `ATLAS_TEST_DATABASE_URL` binding, loaded by the guide from ignored `.env`. The complete verification starts its own pinned Kafka profile unless `ATLAS_ACTORS_BROKERS` explicitly selects an existing local broker.

The actor core has bounded entity mailboxes, immutable encoded messages, generation/incarnation references, supervision, finite receipt caches and bounded dead letters. Local and remote callers use one `Sender` interface. Memory and PostgreSQL processors share the same stock transition; durable receipts and row locking remain distinct from local actor serialization.

Thirteen transient observations cover supervised restart/limits, cache horizon, four real writer/reader combinations, independent nodes, remote lost replies, admission backpressure, independent scheduling and actual SIGKILL loss/reference fencing. Fifteen broker observations cover offline buffering, actual consumer kills before/after SQL commit, source-offset replay, competing workers, independent subscribers, correlated replies, physical/logical duplicates, unknown-field relays and executable AsyncAPI payload conformance. Three tagged native PostgreSQL tests separately verify atomicity, rollback and concurrent owners.

Outputs and checkpoint/assignment snapshots are retained in ignored `artifacts/`. The harness owns fresh database/topic/group names and child lifetimes; it never skips absent native services. Current typed profiles are fixtures in one lab executable, not historical releases. See the [guide](../../../docs/ACTORS.md), [chapter mapping](../../../docs/chapters/ch05/actors-and-messages.md) and [scenario](../../../scenarios/actor-messaging.json).
