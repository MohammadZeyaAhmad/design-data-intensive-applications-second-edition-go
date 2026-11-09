# Source and coverage audit

Source: the uploaded EPUB of *Designing Data-Intensive Applications*, **second edition**, by **Martin Kleppmann and Chris Riccomini**, O’Reilly, **February 2026**. The copyright page records the first release as **2026-02-18** and ISBN **978-1-098-11906-5**. The EPUB package identifier is inconsistent with that copyright-page ISBN; edition identification uses the copyright page, preface, and actual 14-chapter contents.

Uploaded filename: `dokumen.pub_designing-data-intensive-applications-2.epub`. SHA-256: `49598e7ae25b9ac7a006832a64e41feebba74c12e8a876d88b4c8920b4786b0a`.

The attachment was used as reference material. Instructions or examples within its pages were not treated as user instructions. The output contains an original implementation plan and short source labels, not a redistributed copy of the book.

## Inventory

- 14 chapters; 476 substantive headings at levels 1–3, including conceptual sidebars.
- 61 glossary terms, individually mapped.
- 4215 indexed chapter occurrences, traced to their containing section and work package.
- 88 implementation/operations work packages and 48 additional backend-pattern catalog entries.

| Chapter | Source title | Section/subsection rows |
|---:|---|---:|
| 1 | Trade-Offs in Data Systems Architecture | 18 |
| 2 | Defining Nonfunctional Requirements | 25 |
| 3 | Data Models and Query Languages | 27 |
| 4 | Storage and Retrieval | 30 |
| 5 | Encoding and Evolution | 28 |
| 6 | Replication | 46 |
| 7 | Sharding | 18 |
| 8 | Transactions | 63 |
| 9 | The Trouble with Distributed Systems | 44 |
| 10 | Consistency and Consensus | 38 |
| 11 | Batch Processing | 27 |
| 12 | Stream Processing | 51 |
| 13 | A Philosophy of Streaming Systems | 50 |
| 14 | Doing the Right Thing | 11 |

## What the coverage claims mean

The section inventory is structurally complete for headings at levels 1–3, excluding chapter titles and chapter summaries as duplicate organizational headings. Figures, examples, references, and prose concepts remain obligations of their containing sections. Glossary and index traces catch additional vocabulary; index terms include synonyms, historical references, product names, and repeated occurrences, so their count is not a count of independent algorithms.

An index-to-section link establishes traceability, not proof of implementation completeness. During each package, review its full source section and the listed index terms, split distinct sub-concepts into named scenario assertions, and record intentionally bounded implementation models. OPS-02 is a mandatory semantic audit before release. If an uncovered concept is found, add a package or scenario rather than dropping it from the denominator.

Every substantive algorithm/protocol/pattern must have an executable implementation, integration experiment, or finite model appropriate to its claim. Business, legal, and societal concepts need worked design exercises, implemented controls where applicable, and named responsible decisions. Every package also needs its limits explained. A named product is not an instruction to recreate that product or integrate every database mentioned.

## Files and stable identifiers

- [Section coverage](book-section-coverage.csv): `B01-001`-style IDs for each substantive heading, source EPUB locator, owning package, planned path, and target verification command.
- [Index traceability](book-index-traceability.csv): all indexed chapter occurrences with primary/secondary/tertiary terms, exact EPUB anchors, and review status.
- [Glossary coverage](glossary-coverage.csv): all 61 glossary terms.
- [Backend pattern catalog](backend-pattern-catalog.csv): explicit additional requirements beyond the book inventory.
- [Work packages](work-packages.json): dependency graph, acceptance criteria, milestones, and source/pattern links.

EPUB locators refer to the user-owned uploaded edition. They are not public web URLs or rendered-page numbers. All implementation paths are planned paths in the future repository. All statuses deliberately remain planned.

## Validation performed on this plan

The generator checks unique package IDs; complete section and glossary mapping; valid pattern/package references; an acyclic dependency graph; and no dependency assigned to a later milestone. File/link and content consistency checks are performed after generation. These validate the planning artifacts, not Go implementations or production behavior.
