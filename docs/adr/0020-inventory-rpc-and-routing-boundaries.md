# ADR 0020: preserve business command identity across service transports

Status: accepted, 13 November 2026.

Expose the existing Inventory reservation feature through REST/OpenAPI and generated gRPC. Keep domain/features independent of transport, reuse normalized command identity, and save the receipt, aggregate updates, original response and outbox event in the same PostgreSQL transaction. Bind tenant and lineage to server authentication in both adapters.

Give one published caller ownership of bounded retries: safe reads or reservation commands with a stable snapshotted identity, inside an inherited total deadline. The proxy forwards once. Return permanent/business errors immediately, reject unsupported request semantics and preserve protobuf response unknown fields. Demonstrate actual old/current contract calls without duplicating global descriptor registrations or claiming an unreleased historical server.

Use standard gRPC resolver/balancer/health APIs behind a small standalone proxy. A versioned, bounded operator file drives live discovery for the local deployment. Accept explicit empty snapshots as withdrawal; reject stale/malformed updates. Keep TLS identity, registry consensus, heartbeat leasing and distributed telemetry in the operational backlog rather than representing the exercise as a complete service mesh.

Propagate contexts across every RPC hop and through the use case to PostgreSQL. Send native CancelRequest for live cancellations. Configure independent server-side statement/lock/idle-transaction limits because a socket closed by SIGKILL may leave SQL waiting on a lock. Preserve timeout error identity on failed Inventory commands without rewriting a successful commit into rollback.

Verify the lost-reply ambiguity, receipt sharing, tenant bounds, pre-commit rollback, actual backend SIGKILL, surviving-peer discovery, health exclusion, full withdrawal and contract versions. Preserve reports and executable hashes. The full book-package audit remains open beyond these verified scenarios.
