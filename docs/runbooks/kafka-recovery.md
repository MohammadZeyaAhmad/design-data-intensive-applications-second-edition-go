# Kafka transport recovery

Run the optional profile with `make up-kafka`. Use `make verify-kafka` for the local outage/crash/conflict drill and `make integration-kafka` for isolated real-broker acknowledgment/rebalance tests. Reports are in ignored `artifacts/kafka.json`; the guide deliberately mutates only synthetic development stock and this Compose project's services. Shutdown retains volumes.

## Source accepted, read model or workflow absent

Read the authoritative owner state first. A committed outbox record is accepted work even while Kafka is down. Inspect the appropriate source's `broker_published_at`; it reports publication only. Check `broker-publisher`, broker health and `broker-consumer` separately. After publication, inspect the logical record in `messaging.inbox` and its delivery receipt. A newly accepted checkout can lack its job view before request delivery. An absent job or payment intent is not a decline.

Restart the affected local service or restore its dependency. The publisher retries the same logical ID; the consumer resumes from committed offsets. A crash before acknowledgment can produce duplicates. Preserve source/message IDs and let the owner receipt reconcile them. Do not create another checkout/payment merely because a broker acknowledgment or view is delayed.

## Inbox is pending or quarantined

`go run ./cmd/atlas broker-status` prints aggregate counts without message payloads. Identify the exact tenant/topic/message/stream, its first unresolved predecessor, `attempts`, `cycle_attempts`, `last_error` and original source fact. An application failure retries five times in a cycle before quarantine. Later records in that stream wait; another stream on the same Kafka partition may continue because transport receipt is already durable.

Deploy the compatible consumer fix, or investigate an identity conflict against the immutable retained payload and authoritative source. Use the scoped `inbox-retry` command in [messaging contracts](../MESSAGING.md) with `consumer_upgraded` or `source_verified`, a stable request key and the current control version. It saves audit and scheduling state atomically, retains the original message and lifetime attempts, and resets only the attempt cycle. Replaying the same request returns the original saved receipt.

A retry cannot make a changed payload agree with its source or restore a missing version. Do not rewrite source facts or wire envelopes, skip an unresolved head, or assign replacement resource IDs to clear the queue. General repair, archive replay and independent reconciliation remain separate work.

## Rejected wire or broker identity mismatch

Malformed/oversized wire and partition-key mismatches are retained in `messaging.rejected` with a bounded record prefix and fingerprint in the delivery table. The broker offset can advance after that rejection commits, but no business handler is invoked. Inspect these records only through the privileged local database binding; they can contain operational references. The normal CLI does not print their payloads.

Changed bytes at an already observed `(group,topic,partition,offset)` fail consumption. Investigate broker data replacement, topic recreation or mismatched database/broker recovery. Do not erase receipts or reset the group to conceal the mismatch. This increment has no certified broker data-loss/retention-overrun repair procedure.

## Profile and topology changes

The profile pins three partitions per topic and a fixed mapping. A partition-count mismatch fails startup validation. Live repartitioning is not implemented. The broker has one node and delete retention; its successful process restart does not demonstrate multi-replica durability, failover or archive recovery.

For a coordinated return to PostgreSQL delivery, use `make down-kafka`, then `make up`. Publication and local delivery cursors are independent, so requests published but not locally acknowledged can be redelivered through the local path. Existing jobs/Fulfillment receipts deduplicate already accepted requests. When returning to Kafka later, older projection events may also replay harmlessly through owner receipts. Keep both PostgreSQL and Kafka volumes and the existing `.env` unless operating an explicitly disposable test environment.
