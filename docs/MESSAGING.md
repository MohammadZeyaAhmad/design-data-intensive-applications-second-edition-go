# Kafka transport and durable receipt boundaries

The optional Kafka profile carries Inventory events, Ordering events, checkout requests and Fulfillment requests through a real broker. PostgreSQL remains authoritative for business facts, workflow state and local receipts. The default PostgreSQL profile remains runnable and uses the same business handlers.

```sh
make up-kafka
make verify-kafka
ATLAS_MESSAGE_TRANSPORT=kafka make verify-live verify-orders verify-checkout verify-fulfillment verify-operations
make integration-kafka
make down-kafka
```

The profile runs pinned **Apache Kafka 4.1.1**, digest `sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40`, in single-node KRaft mode. Its external listener binds loopback port **19092**. A separate publisher and broker consumer run alongside the API, provider and durable worker. `make integration-kafka` uses an isolated broker project on **19093** and a fresh test database. Its volume contains only synthetic fixtures and is retained by shutdown. Use `make down-kafka-test` after a failed test run to stop that test project.

## Four delivery lanes

| Topic | Stable identity and stream | Application effect |
|---|---|---|
| `atlas.inventory.v1` | Source event UUID; tenant/SKU/warehouse version | Inventory stock projection |
| `atlas.orders.v1` | Source event UUID; tenant/order version | Ordering projection through its existing reducer |
| `atlas.checkout.v1` | Accepted checkout UUID; tenant/order, sequence 1 | Durable checkout job |
| `atlas.fulfillment.v1` | Checkout UUID; tenant/order, sequence 1 | Fulfillment inbox and warehouse shipments |

Each topic has three partitions. The key is the JSON array `[tenant_id, stream_id]`. Inventory stream IDs contain `SKU/warehouse`; other lanes use canonical order UUIDs. The producer explicitly chooses `FNV-1a-32(key) % 3`, independent of a client's default partitioner. Startup checks the expected partition count and refuses a different topology. Increasing partitions is not a supported live routing migration in this increment.

A version-one wire envelope carries message UUID, tenant, stream, sequence, event type, payload schema version, data and the existing provenance envelope. Encoding/decoding is strict and bounded to 256 KiB. IDs are canonical lowercase UUIDs; stream and actor inputs are bounded. Kafka offsets, source stream versions and correlation IDs have distinct meanings. None supplies a global business order.

## Publication

The source transaction already contains its outbox record. The publisher locks one unpublished stream head, sends it with all-in-sync-replica acknowledgments and client idempotence, then commits `broker_published_at`. The pending-predecessor predicate prevents another publisher from overtaking a locked or uncertain head. Other eligible streams can be selected. Network attempts are bounded and errors back off. A process crash releases its row lock.

The broker may accept a record before its acknowledgment or the PostgreSQL publication checkpoint is lost. A retry can create another physical Kafka record with the same logical message UUID. Consumer/owner receipts handle that duplicate. Client idempotence does not eliminate the cross-store commit boundary or duplicates across publisher restarts.

Publication uses row locks during bounded network calls. This is a concrete polling-outbox implementation; a persistent leased publisher and CDC/Debezium alternative remain separate comparisons. The profile has one broker, replication factor 1 and minimum ISR 1: all-ISR acknowledgment here acknowledges that one broker. It does not demonstrate replicated broker durability or failover.

## Broker receipt and application receipt

The consumer disables automatic offset commits and polls one record at a time. It blocks rebalance while staging and committing that record, then permits reassignment. A staging or commit failure recreates the consumer session from committed offsets. It never polls a later record after a failed staging transaction and then acknowledges past it.

The staging transaction commits a raw transport-delivery fingerprint and the logical PostgreSQL inbox record before Kafka offset acknowledgment. Delivery identity is `(group, topic, partition, offset)`. Logical identity is `(topic, message UUID)`, with an additional uniqueness check on stream/sequence. This is one application inbox generation; independent fan-out/read-model generations need separate namespaces and remain future work. Identical redelivery is safe; changed bytes at an observed broker offset fail closed. A changed logical message/sequence is recorded as a conflict and fences the retained stream instead of rewriting its accepted payload.

Kafka acknowledgment means durable receipt into PostgreSQL, including a durable rejection for malformed wire. It does not mean the projection, checkout or shipment already changed. The API remains able to commit source commands while the broker is unavailable; a newly accepted workflow may temporarily have no job view until its request arrives.

The application worker drains the durable inbox by stream/sequence. A pending or quarantined predecessor blocks later work for that stream. Five failed application attempts quarantine its head; unrelated streams can progress even on the same Kafka partition because broker receipt is independent of application execution. Unsupported domain schemas are retained rather than skipped. Invalid wire without a trustworthy stream identity goes to a bounded raw rejection record and produces no business effect; owner version checks still reject gaps in valid projection streams.

Application handlers verify message identity, payload, schema and provenance against their owning context's source facts. Inventory/Ordering receipt and projection changes commit together in the owner transaction. Checkout job acceptance and Fulfillment inbox/shipments use their existing atomic/idempotent stores. The transport inbox's completion marker commits afterward. A crash between owner commit and that marker repeats the handler; its owner receipt prevents another local effect. Payments and physical stock still require their own stable resource operations and reconciliation.

These handlers currently run inside one modular application with separate schemas in the same PostgreSQL instance. Source validation reads the owning context's source, and the root composes the published handlers. This is not a deployment of independent databases/microservices or a distributed transaction.

## Profiles, replay and repair

Publication progress is separate from the local delivery/application cursor. Projection receipt completion marks the source projection outbox as processed. Kafka request publication leaves the local request `dispatched_at` cursor unchanged. Returning to the PostgreSQL profile can therefore deliver an unconsumed request or redeliver one already accepted; the same job/Fulfillment inbox identity handles both. To switch profiles cleanly, run `make down-kafka` and then `make up`, or `make down` and then `make up-kafka`. Volumes and credentials are retained. Mixed delivery is designed to deduplicate, but operators should use a coordinated switch rather than leave unnecessary broker processes running.

Use `go run ./cmd/atlas broker-status` for aggregate inbox/rejection counts. Its database binding is local operator configuration; no broker connection is needed. Inspect a specific stream and its original source before repair. The narrow retry command preserves the retained message, fingerprint, sequence, IDs and lifetime attempt count:

```sh
go run ./cmd/admin inbox-retry -tenant acme -topic atlas.inventory.v1 -id MESSAGE_UUID \
  -reason consumer_upgraded -key INCIDENT_RETRY_KEY -expected-control-version 0
```

`consumer_upgraded` applies after a compatible handler fix; `source_verified` records review of a conflicting delivery against the retained original source. Retry requires the scoped local operator capability and current inbox control version. It resets the attempt cycle, makes the retained record eligible, and atomically saves an immutable audit and response. Repeating the same key returns its original receipt. It cannot replace payloads, delete facts, bypass source validation or authorize a financial correction.

The broker uses seven-day delete retention, not compaction or an archive. Retention-overrun detection/catch-up, offset migration, broker data-loss recovery, multi-broker failure, SASL/TLS/ACLs, throughput tuning, Kafka transactions, general consumer-offset rewind tooling and CDC remain tracked work. Do not delete broker volumes or reset offsets as a substitute for an explicit recovery design. Read [the recovery runbook](runbooks/kafka-recovery.md) and [failure-boundary walkthrough](chapters/ch12/reliable-messaging.md).
