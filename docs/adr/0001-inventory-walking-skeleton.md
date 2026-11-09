# ADR 0001: Begin with a transactional Inventory slice

Status: accepted for the first implementation increment.

The full Atlas plan needs a realistic invariant before its architectural abstractions and distributed examples are useful. Start with inventory, where concurrency can cause a concrete business failure: multiple reservations consuming the same available units.

Use pure domain aggregates and feature-owned handlers. The PostgreSQL adapter exposes a tenant-scoped transaction interface; feature handlers load locked aggregates, apply domain behavior, persist state, and append an outbox event. HTTP decoding/authentication are separate adapters. Domain packages depend only on the standard library, and features cannot import concrete adapters; architecture checks enforce this.

Inventory owns all its tables. A reservation transition and corresponding stock change share one local transaction because the invariant spans both records within this context. Every command also persists request identity and its response, so a lost response can be retried without a second effect. Composite keys and every query include tenant identity bound by authentication.

Use PostgreSQL as the first durable dispatcher. The worker writes its projection receipt, projection state, and outbox progress in one transaction. Only the earliest pending event for a tenant/SKU is eligible; `SKIP LOCKED` is used to parallelize different streams, not to skip their predecessors. A failed event backs off, then quarantines after five failures, while remaining a barrier for its stream.

This is a useful local correctness baseline for the later Kafka comparison. It is not presented as a distributed acknowledgment protocol, an event-sourced order aggregate, or production infrastructure. Inventory tables remain authoritative. The lost-update lab and real PostgreSQL contention test connect the first code to the book's transaction concepts.

Use development tenant keys and loopback-only published ports to make the first slice reproducible without an external identity provider. OIDC, RLS, TLS trust for remote services, telemetry, catalog price authority, and other production requirements remain explicit work. The initial Go CLI and separate lab module will expand as real scenarios are added.
