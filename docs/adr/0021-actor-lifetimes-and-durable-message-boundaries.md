# ADR 0021: separate actor admission, processing, durability and acknowledgment

Status: accepted, 13 November 2026.

Implement the actor model in the independent Go lab. Reuse one pure inventory-shaped transition across volatile and PostgreSQL processors. Actors own state and accept bounded immutable messages; references include entity, process incarnation and supervised generation. A finite receipt cache and dead-letter ring remain explicitly volatile. A bounded restart policy cannot be bypassed by re-resolving a stopped cell.

Define Tell as admission and Ask as waiting. Caller cancellation stops waiting without retracting already admitted work. Processing uses a runtime-owned context, while shutdown remains cooperative. Hard failures use actual process SIGKILL. Keep fault gates in the lab executable, outside production application servers.

Make durability an adapter choice. Commit shared stock, command identity/hash, original bytes, saved result and attempt metadata together in PostgreSQL. Require row locking and complete receipts even if each process locally serializes actors; actor placement is not a distributed uniqueness guarantee.

Use real Kafka to compare buffering, competing workers, independent subscriptions, consumer restart and separate request/reply channels. Publish the saved reply after SQL commit and acknowledge the source last. Redelivery resolves through the local receipt; duplicate replies still require correlation. Pin the broker, declare single-replica/all-ISR and finite-retention limits, and delete only fresh owned resources.

Validate actual command/reply values against authored AsyncAPI payload schemas and bounded runtime preflight. Keep compatible writer/reader profiles explicit. Preserve complete source bytes through relays; typed reconstruction can lose unknown fields and must not replace a saved same-ID body. Broader schema governance, cluster actor ownership and framework/operations behavior remain in the complete plan.
