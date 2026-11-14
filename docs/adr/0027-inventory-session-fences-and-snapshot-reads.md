# 0027 — Inventory session fences and snapshot reads

Status: accepted for the bounded C06-04 Inventory HTTP increment.

Physical replication lag makes acknowledged stock updates invisible on a standby and permits a session to regress when its routing changes. Reading related rows from independently lagging snapshots can combine effects with an older cause. Sticky routing alone cannot retain progress after endpoint loss.

Use explicit opt-in primary/replica reads, authenticated tenant/source/history WAL vectors, a replay gate before snapshot creation, and one repeatable-read transaction for a combined Inventory view. Commands remain primary-authoritative. Sticky labels are signed preferences, subordinate to the fence. Waits are bounded; lag produces 425 or a labeled primary fallback selected by the caller.

Capture a primary WAL insert position after each successful observation or business commit. This conservative output fence bounds the observation for future reads while allowing unrelated commits beyond the current snapshot. A post-snapshot replica replay sample would not identify which replay position the snapshot included. Primary insert position also avoids treating a flush position as a bound on locally visible asynchronous commits. The resulting strict-read path retains a primary availability and query dependency.

Preserve successful command receipts when postcommit fence capture fails. Expose unavailable session metadata, and make the client require primary reads until a valid fence is obtained. Serialize calls in the provided session client instead of maintaining an unbounded server session registry or silently losing maxima to reordered responses. Validate tokens before admitting commands.

An immutable source UUID distinguishes independently initialized stores; the existing operator-controlled writer epoch distinguishes admitted histories. Rotate epochs only with proper former-writer fencing and restart API instances. Reject incompatible tokens rather than discarding them. The private authority tables are exposed through a fixed-search-path security-definer read function.

This decision covers authoritative Inventory HTTP views. The worker-driven projection, other contexts/transports, automatic failover, general dependency propagation and distributed atomic snapshots retain separate contracts. Per-shard tokens can represent known causal prerequisites; they do not make independently read shard states an atomic transfer view. See [guide](../SESSION_CONSISTENCY.md) for bounds and evidence.
