# ADR 0005: Provenance independent of authority and scheduling

Status: accepted for the fifth implementation increment.

Persist versioned actor/correlation/causation/origin-request metadata outside domain payloads. Authentication selects the tenant actor and generates request IDs. Correlation input is a bounded UUID. Workers inherit the persisted asynchronous parent and identify themselves as executor. Original request IDs survive worker/provider response loss and restart. Metadata is excluded from business request hashes, reducers and snapshot checksums; old facts explicitly retain unknown provenance.

Add local operator scheduling controls for Checkout and Fulfillment through owner adapters and a common limited control primitive. Its in-process tenant capability is separate from metadata. The local composition root holds the privileged development database binding; remote identity and production roles remain explicit future work.

Controls require the current control version and reject live leases and terminal work. Pause, resume or retry changes only scheduling/lease control fields. A transaction commits that change, an immutable before/after audit record and the saved response together. Rejected attempts leave no accepted request result. A successful retry reuses its original audit identity.

A pause prevents new claims while an expired old call may still complete externally. Advancing the token blocks that worker's checkpoint. Resumption relies on the already implemented stable resource identities and source outcome fences. Neither operator controls nor diagnostic lineage grants permission to rewrite money, allocations, immutable facts or workflow progress.

Keep full business repair, production operator authorization, distributed tracing/metrics, broker replay controls, retention and independent lineage reconciliation in their existing work packages. This slice provides reproducible local operational controls without claiming those broader capabilities.
