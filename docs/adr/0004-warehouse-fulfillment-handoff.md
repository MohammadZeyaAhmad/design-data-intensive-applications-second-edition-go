# ADR 0004: Warehouse allocation and Fulfillment ownership

Status: accepted for the fourth implementation increment.

Inventory owns a balance per tenant, SKU and warehouse. Existing data maps to `default`; old payloads retain their meaning. Checkout sources a whole SKU line from one location and pins the result in its stable allocation. Compensation closes that allocation's original warehouse, and a permanent resource intent fences any delayed reserve independent of location.

Ordering confirmation atomically writes a Fulfillment request. Fulfillment consumes through its published contract and owns one shipment per warehouse. Its inbox, allocation ownership, initial states and immutable history commit together. A lost delivery acknowledgment is handled by replaying the same request. Private contexts remain independent, with imports checked across all six contexts.

Warehouse commands use expected version and saved request identity. Dispatch is asynchronous. The worker claims a token lease, commits before calling Inventory, validates the physical receipt and saves only under its live token. Inventory records shipment and physical deduction atomically, once per stable allocation. Closing and shipping serialize; after shipping, compensation cannot recreate stock.

The source order's confirmed/failure facts define checkout's compensation fence. Shipping failure does not trigger a checkout refund. A partial handoff remains explicitly dispatching and retryable. Returns, post-confirmation cancellation, real carriers, same-SKU splits and generalized manual repair need separate contracts.

The upgrade adds the warehouse key to authoritative and projection tables. Stop old API/worker binaries before migration and deploy the new pair together. This additive payload compatibility does not make the key change compatible with old persistence adapters. Full expand/backfill/contract and rolling-deployment comparisons remain planned under C05-03.
