# Chapter 3: Relational, document and wide-column catalog models

`make verify ID=C03-01` runs the [modeling guide](../../MODELS.md) experiments against shared catalog/customer fixtures. This table names the concept exercise; the complete source inventory remains the final coverage gate.

| Source section | Experiment or worked decision |
|---|---|
| B03-001 — Declarative query terminology | Compare explicit Go reference loops with four SQL queries; inspect ordinary optimizer plans before/after indexes |
| B03-002 — Relational versus document models | Ask the same category/price/favorite questions of shared references, copied rows and JSONB documents; explain SQL/NoSQL as families with overlapping capabilities |
| B03-003 — Object-relational mismatch | Reconstruct nested variants/categories from rows; compare that work with an already nested document |
| B03-004 — Object-relational mapping | Execute generated sqlc, lazy GORM associations and eager preloading; check equal full-detail results and actual statement counts |
| B03-005 — Document one-to-many relationships | Read variants embedded in the product document and compare a nested field update with one variant row |
| B03-006 — Normalization, denormalization and joins | Rename one shared brand versus copied names, expose stale copies and repair them |
| B03-007 — Normalization tradeoffs | Inspect read joins, copy fanout, child replacement and reference constraints on the same fixture |
| B03-008 — Social-network denormalization | Use customer favorite relationships as the concrete many-to-many fixture; work through copied display names versus IDs in a feed below |
| B03-009 — Many-to-one and many-to-many relationships | Brand references, category memberships and customer favorites use tenant-qualified identities rather than accidental repeated-name equality |
| B03-011 — Model choice | Evaluate each question and update frequency; identify which locality and integrity requirements it favors |
| B03-012 — Schema flexibility | Introduce a legacy string weight, migrate only the supported version, distinguish absent/null/zero and reject incompatible v2 input |
| B03-013 — Read/write locality | Inspect actual JSONB/TOAST relation allocation and buffer work for a nested update; keep logical serialization counts separate |
| B03-014 — Document query languages | Execute JSONB containment, field extraction, array expansion and JSONB favorite membership in SQL |
| B03-015 — Model convergence | Combine SQL joins/constraints with flexible JSONB attributes, generated Go reads and a query-shaped family table in PostgreSQL |

## Choose identity before copying presentation

A customer favorites a product by SKU, and that product references a brand by ID. If a feed embeds product/brand display names at publication time, renaming the shared brand does not rewrite those copies automatically. Keeping the IDs enables current-name joins; pinning the copied presentation instead gives a historical snapshot. Neither policy should be accidental.

The fixture's partial brand rename makes this visible. For a social feed, extend the same reasoning to author/organization names: estimate fanout across favorite/feed entries, select current versus historical presentation, and define a versioned backfill plus failure/retry policy before copying. The current code verifies reference/copy divergence and full repair; it does not yet implement a production feed fanout worker or Customer context.

## Nested reads and shared references solve different problems

Variants are usually read with their product, so embedding offers useful read locality. A brand shared by many products has a different update pattern. The application therefore keeps ordered variants/categories as rows and brand identity as a shared reference, while allowing product-specific attributes in JSONB. Its query reconstructs a bounded nested API view in one MVCC statement.

A document model can also use references and joins, and a relational system can store/query nested values. The experiments compare explicit representations and access patterns rather than treating a database label as a complete design decision. Many-to-many categories/favorites are where a purely nested tree often requires references, duplicated edges or additional query tables.

## A key order is a query commitment

The family partition chooses tenant/category; price and SKU choose clustering order. That answers one price-bounded category question without a global scan. Favoriting or querying by brand does not magically change that order. Measure the extra filtering or design another materialization and its update/repair obligations. This is wide-column data modeling; compressed analytical columns in Chapter 4 answer a different workload.

The remaining package audit includes all source sub-concepts/index occurrences, broader scale, production customer boundaries and safe schema/backfill rollout. A passing named scenario does not close those obligations.
