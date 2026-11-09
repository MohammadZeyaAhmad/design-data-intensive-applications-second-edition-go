# DDIA Go Mega Project — Planning Package

Start with [the end-to-end plan](ddia-go-mega-project-plan.md). It designs **Atlas**, a cohesive Go platform and data-systems laboratory with DDD, vertical slices, CQRS, event sourcing, and equal emphasis on learning and production engineering.

This package is a saved **plan**, not an implemented repository. Full book coverage is the completion requirement; early releases are checkpoints, and runtime profiles do not remove implementation scope.

| File | Use |
|---|---|
| [End-to-end plan](ddia-go-mega-project-plan.md) | Architecture, domain, repository tree, contracts, stack, scenarios, validation, milestones, operations, and release gates |
| [Implementation backlog](implementation-backlog.md) | 88 work packages with dependencies, implementation scope, acceptance criteria, and target verification commands |
| [Book section coverage](book-section-coverage.csv) | All 476 substantive headings across the 14 chapters, mapped to packages |
| [Glossary coverage](glossary-coverage.csv) | All 61 glossary terms mapped individually |
| [Index traceability](book-index-traceability.csv) | 4,215 indexed chapter occurrences traced to their source context and planned owner; semantic review remains an implementation obligation |
| [Backend pattern catalog](backend-pattern-catalog.csv) | 48 required pattern entries, including additional engineering patterns beyond the book |
| [Machine-readable work packages](work-packages.json) | IDs, dependency graph, milestones, estimates, acceptance criteria, and source/pattern links |
| [Source and coverage audit](source-and-coverage-audit.md) | Verified edition, source fingerprint, inventory counts, and precise meaning of coverage |

The source is the uploaded **February 2026 second edition** by **Martin Kleppmann and Chris Riccomini**. The EPUB itself and extracted book prose are not included.

The plan estimates **51–84 focused engineer-weeks before contingency** for the complete scope, with an initial operational core in approximately 13–22 weeks. These are rough planning ranges, not delivery commitments. All implementation statuses remain `planned`.

The defaults are a marketplace/order-and-fulfillment domain, local Go/Compose development, a portable Kubernetes release, and an AWS infrastructure reference. The plan records which decisions to revisit at implementation kickoff.
