# ADR 0003: Price authority and resource-fenced checkout

Status: accepted for the third implementation increment.

Catalog owns price versions and immutable quotes. Ordering accepts a quote only when its lines agree and its database-time deadline has not passed after acquiring the order head lock. Accepted pricing is an immutable event fact. Repeated acceptance uses saved request identity before re-evaluating its old deadline.

The checkout process manager integrates published Catalog, Ordering, Inventory and Payments contracts. No bounded context imports another context's private domain, features or persistence. Architecture checks cover all five contexts. Request hashing is an I/O-free shared utility; local transaction/deduplication support is an adapter utility, not a shared business repository.

Ordering commits a checkout-dispatch intent with its event and response. Dispatch creates one idempotent workflow record and acknowledges afterward. Work claims use monotonic tokens and database leases; each step executes outside the workflow transaction and checkpoints only while its token/lease remain current.

External side effects need stronger protection than checkpoint fencing. Inventory closes a stable step even when its reservation does not yet exist. The provider closes a stable payment intent even before capture. A delayed call cannot reopen either resource. The source order records failure before compensation; confirmation and failure are serialized by the aggregate, preventing stale confirmation after resources were unwound.

Payments observes provider truth before changing its local intent and journal. The provider models persistent declines, capture, void-before-capture, full refund, delayed processing and lost responses. A response loss is uncertainty, not evidence that no side effect happened. Reconciliation can discover a capture missing locally and atomically record its capture/refund pair.

Each journal row represents a balanced two-account transfer, with immutable money identity and guarded refund history. Future accounting can extend to general posting sets without pretending this initial representation already implements them.

The local provider is a separate HTTP process using a separate namespace in the same development PostgreSQL instance. The code never joins provider and application effects in a transaction. Production deployment still needs separate access roles and provider credentials, security/retention controls, and evidence for its actual infrastructure/failure domains.
