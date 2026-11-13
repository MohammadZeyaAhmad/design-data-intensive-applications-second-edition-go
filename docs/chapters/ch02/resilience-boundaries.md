# Finite work and advisory state in Atlas

APP-09 connects the book's reliability/overload and derived-data tradeoffs to the same Catalog and payment workflows used by the rest of the project. [The executable guide](../../RESILIENCE.md) maps P23–P28, P46 and P48 to production adapters and comparison code. Related full semantic audits remain C02-02, C07-03, C09-01 and C13-04.

A display cache is derived state with a different authority boundary from an immutable quote. Simultaneous misses should share finite source work; source overload still needs a finite rejection policy. A fast cache does not increase the number of PostgreSQL transactions the system can safely run when that cache fails.

The native checks distinguish a rolled-back invalidation from a committed notification, exact integer versions from rounded Lua numbers, one canceled waiter from abandoned shared work, and accepted HTTP work from rejected/waiting requests. A real Redis stop/start verifies recovery without a healthy API restart. The bounded load command records shed requests as well as successful latency samples.

The independent write-behind counterexample deliberately acknowledges a value that its modeled volatile crash loses. Reversing two pending writes regresses the source unless version checks reject stale delivery. That comparison explains why production price publication remains a PostgreSQL transaction and why a cache commit failure cannot reverse a business success. The finite model is not a physical durability experiment.

The current queue/rate limits are explicit local defaults. Workload-derived sizing, distributed quotas, durable invalidation and richer operational telemetry remain part of the original scope, rather than being inferred from a small local scenario.
