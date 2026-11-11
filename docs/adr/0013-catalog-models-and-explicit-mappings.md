# ADR 0013: Normalized Catalog details and explicit read mappings

Status: accepted, 11 November 2026.

Catalog needs richer discovery content without changing immutable price/quote authority. Add shared tenant-qualified brands, product details, ordered variant rows and unique category memberships. Keep bounded flexible attributes in JSONB. Product details and brands have independent expected revisions; child replacement, command receipts and immutable provenance/history commit together.

Read current brand presentation through a normalized join. Brand renames do not advance product detail/price revisions or rewrite every product. Earlier command receipts remain historical responses. Generate the bounded nested read from reviewed SQL with pinned sqlc 1.30.0, retaining checked-in Go and enforcing reproducible generation in CI.

Keep GORM 1.31.1 in an actual comparison fixture, with composite tenant keys and explicit lazy/eager association strategies. Count executed statements and compare complete results. Do not introduce ORM auto-migration or domain/feature dependencies on infrastructure.

Use isolated temporary PostgreSQL representations and a separate Go lab for denormalization, document drift/locality and ordered family keys. Deliberately inconsistent copies belong only to those synthetic experiments. Repair reconstructs copies from shared facts. Temporary nested-update targets are unlogged, so their zero WAL counters are not a production durability comparison.

The family exercises model partition/clustering access using bbolt/PostgreSQL; they provide no Cassandra/HBase operational implementation. Analytical columns remain a separate package. The [guide](../MODELS.md) records the quality/measurement boundaries and remaining scale, migration and customer/application work.
