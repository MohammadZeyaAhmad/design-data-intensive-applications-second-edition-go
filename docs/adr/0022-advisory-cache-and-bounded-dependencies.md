# 0022 — Advisory Catalog cache and bounded dependency work

Date: 2026-11-13. Status: accepted for the local reference implementation.

Catalog exposes immutable prices and frozen quotes. A cache and a source transaction cannot commit atomically through ordinary Redis writes. Failed invalidation, a late fill, negative entries after creation, integer rounding, dependency retries and unbounded waiters can otherwise change apparent correctness or exhaust finite resources.

Opt-in display reads use a validated tenant-scoped cache decorator. Default reads and pricing decisions retain PostgreSQL authority. Positive/negative TTLs, retained integer version floors, local load invalidation and transactional SQL notifications limit ordinary display staleness; floor/notification loss is explicitly possible. A minimum-version hint asks for known progress without making the cache a strong consistency service. Decimal-string Lua comparisons preserve all supported integer versions.

Apply separate finite HTTP read/write admission, body slots, per-tenant/global origin limits, cache/provider bulkheads, owned coalescing and deadline-aware circuit/retry policies. A dependency-wide retry budget bounds amplification. The payment workflow owns mutation retries by default; optional immediate same-intent retries are a comparison against the simulator's durable fence. Redis SDK retries are disabled.

The consequence is availability of authoritative product/quote work during cache failure, with deliberate load shedding when the database's own finite budget is exhausted. A successful business commit cannot be changed into a failed command merely because invalidation failed. Display reads can regress after advisory metadata is lost; callers requiring authority must request it. NOTIFY is not durable CDC. Rate limits are per process/workload, without a distributed tenant quota claim. Cache outage does not promise that an already overloaded database will serve all fallback traffic.

Write-through and bounded refresh-ahead are optional comparison modes. Volatile write-behind loss and reversed delivery are confined to an independent finite lab. Broader CDC, distributed invalidation, production provider contracts, capacity sizing, telemetry/SLOs and deployment remain separately tracked.

Evidence: [guide](../RESILIENCE.md), [scenario](../../scenarios/cache-resilience.json), [native SQL tests](../../tests/integration/resilience_test.go), [native Redis tests](../../tests/integration/resilience_redis_test.go), [drill](../../cmd/guide/resilience.go) and [write-behind comparison](../../labs/ch02/cache/comparison.go).
