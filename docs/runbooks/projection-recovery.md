# Recovering the local inventory projection

The dispatcher never skips a pending predecessor for the same tenant/SKU. After five failed attempts it sets `quarantined=true` and records `last_error='projection_failed'`. Later events from that stream wait; other streams continue.

Use the worker's structured logs and inspect the specific stream in the local database. Credentials are in `.env`; do not paste them into logs or issue reports. The current development adapter has no public administrative endpoint.

1. Identify the event and verify its type, payload tenant/SKU/version, authoritative stock history, and existing projection receipt.
2. Fix the consumer/schema problem where possible. If this is deliberately corrupted synthetic lab data, restore the original event fixture using its original event ID; do not invent a different business event to silence the error.
3. In a transaction restricted to that event, clear quarantine, reset attempts, and set its available time to now. Preserve event identity and sequence.
4. Let the worker retry. Confirm that the stream catches up in order, the projection matches authoritative state, and receipts are unique.

The integration test `TestPoisonEventBlocksOnlyItsStreamAndCanBeRepaired` demonstrates this procedure against isolated synthetic data. `make verify-recovery` tests worker downtime and API restart. Full authorized replay/rebuild tooling, event-store historical auditing, and production restore procedures remain future packages.

`make down` stops this Compose project and preserves its database. No default command deletes volumes. Keep the existing `.env` with an initialized volume: changing only its password will not rotate PostgreSQL's stored password.
