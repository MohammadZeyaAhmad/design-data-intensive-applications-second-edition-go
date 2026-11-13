# Actual rolling migration drill

From the repository root, run `make verify ID=C05-03` with the local `atlas_test` PostgreSQL binding. `make evolution-demo` builds historical/current Atlas plus the schema operator and runs the process scenario. The historical application comes from immutable commit `393f703d7e097f96f09847b8196ead2c4429f260`; fetch full Git history if a shallow clone lacks it.

The standalone lab command is:

```sh
go run ./cmd/lab evolution -rows 128 -binaries ../artifacts/evolution-binaries -output ../artifacts/evolution.json
```

It requires `ATLAS_TEST_DATABASE_URL` to identify local atlas_test with CREATE DATABASE permission. It creates/drops only a fresh owned database and starts/drains its own loopback processes. Seeding, HTTP requests, worker transactions, concurrent index creation and physical contraction are real. Both complete application binaries are verified against recorded SHA-256 digests.

The report retains expanded/backfilled/validated/contracted states, transactional batch receipts, foreground HTTP samples, an actual killed worker, compatible rollback, explicit retirement and offline archive replay. The selected historical source/metadata images and writer definitions remain in a new `evolution-run-*` child beside the report. Existing development data and unrelated services are outside the drill.

See the [guide](../../../docs/EVOLUTION.md) and [operator runbook](../../../docs/runbooks/schema-rollout.md) for budgets, schema-version separation, trusted reader inventory and the larger operational scope still tracked in C05-03.
