# Chapter 5: event-driven architectures and actors

`make verify ID=C05-06` connects the [actor guide](../../ACTORS.md), [runtime](../../../labs/ch05/actors/runtime.go), [stock transition](../../../labs/ch05/actors/stock.go), [real node](../../../labs/cmd/actor-node), [durable processor](../../../labs/ch05/actors/durable), [Kafka worker](../../../labs/ch05/actors/broker) and [native drills](../../../labs/ch05/actors/drill).

| Source ID | Named concept | Concrete behavior or decision exercise |
|---|---|---|
| B05-026 | Event-Driven Architectures | Tell returns admission, not completion; sender/recipient decoupling through logical topics; real buffering while consumers are absent; replacement redelivery; fan-out and a correlated reply on a separate channel. |
| B05-027 | Message brokers | Actual competing consumer-group members versus independent subscribers; opaque broker bytes and authored AsyncAPI schemas; durable local receipts before source acknowledgment; retained Kafka history/finite expiry versus delete-after-consumption queues and indefinite event-sourcing requirements; real unknown-field relay loss. Commercial, open-source and cloud broker families remain explicit comparative choices. |
| B05-028 | Distributed actor frameworks | Entity-owned memory, one-message execution, bounded mailboxes/registry, independent scheduling, supervised restart budgets, generation/incarnation fencing, transient loss, actual remote encoding and four rolling writer/reader combinations. Akka/Orleans/Erlang facilities are compared with the implemented subset. |

The local and HTTP `Sender` interfaces intentionally share operations, while both use encoded messages rather than mutable object references. This matches actor location transparency more closely than treating network calls as ordinary function calls: loss is already part of the local actor model. The same interface still has different latency and failure boundaries remotely.

One actor's serialization and finite ID cache cannot protect state across two independently activated nodes. Three PostgreSQL tests make durability explicit: complete receipt/state/source commit, cancellation rollback, and competing actor owners using shared row locks to avoid overselling. Consumer process SIGKILL before/after transaction commit then exercises the separate Kafka offset boundary.

The optional note is a compatible nonsemantic field. Consumers that relay complete bytes retain it; older typed reconstruction loses it and conflicts with the saved canonical body hash. New semantic operations and unsupported formats reject rather than guessing intent.

Global actor placement/fencing, durable activation, framework-specific behavior, richer broker semantics, external registry services, cross-language codecs and larger-scale operation remain in the original baseline. These verified scenarios do not close the full package or replace the application's existing transactional outbox/inbox and event-sourced Ordering contracts.
