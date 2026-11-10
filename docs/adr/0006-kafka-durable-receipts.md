# ADR 0006: Kafka publication, durable receipt and owner effect

Status: accepted for the sixth implementation increment.

Provide an optional real Kafka profile for Inventory/Ordering events and checkout/Fulfillment requests. Keep the PostgreSQL profile and business handlers. Route keys include tenant and stream, with a pinned three-partition mapping and startup topology validation.

Separate the broker publication cursor from owner processing/local request delivery. Publish under a bounded source-row lock and acknowledge publication only after broker success. Retrying an uncertain publication preserves the logical ID. The polling implementation deliberately uses bounded row locks; leased publication and CDC remain comparisons to implement.

Persist transport delivery and logical inbox records before Kafka offset commit. On a staging/commit failure, recreate the consumer session from committed offsets. Stage independently of business execution so poison application streams do not block unrelated streams on one partition. Retain malformed wire and identity conflicts as durable evidence.

Apply messages through owning context contracts after validating them against owner source facts. Each owner commits its local receipt and effect together. Transport completion follows afterward and can cause safe redelivery. External effects retain their existing workflow/resource idempotency protocols. Source facts and canonical payloads are not repaired by rewriting inbox records.

Expose a narrow, tenant-scoped, versioned and audited inbox retry. Preserve payloads, sequence, IDs and lifetime attempt counts. Keep arbitrary replay/offset edits, archive catch-up, replicated broker operation, production security, retention-overrun recovery, Kafka transactions and independent reconciliation in their broader packages.
