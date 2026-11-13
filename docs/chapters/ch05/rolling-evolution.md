# Chapter 5: database, archive and rolling schema evolution

`make verify ID=C05-03` connects the [rolling guide](../../EVOLUTION.md), [Catalog migration](../../../migrations/catalog/004_brand_display.sql), [operator](../../../cmd/schema), [published history contracts](../../../contracts/catalog/history.go) and [process drill](../../../labs/ch05/evolution). It uses an actual historical Atlas build and the current executable, rather than simulated version flags.

| Book source ID | Named concept | Concrete implementation |
|---|---|---|
| B05-015 | Modes of Dataflow | Independently named HTTP, SQL/view, journal and archive contracts; explicit direction/ownership and version metadata. Service transport is the next package. |
| B05-016 | Dataflow Through Databases | Real old/new concurrent API processes, old and new writes through a database bridge, online metadata expansion, resumable backfill, validation and guarded contraction. |
| B05-017 | Different values written at different times | Pre-expansion null tuples, explicit v1/v2 immutable journal payloads, stable business revisions, in-memory upcasting, compatible binary rollback and preserved command responses. |
| B05-018 | Archival storage | Actual frozen source bytes/metadata, explicit writer definitions and digests, immutable file publication and offline replay after physical column retirement; complements the earlier native Avro OCF archives. |

A database reader can encounter records written by many historical applications. Deploying a new binary does not update all those records, and deploying it everywhere does not prove that every batch, export, replica or rollback reader has retired. The scenario treats binary rollback, data backfill and destructive contraction as separate operations.

The pending predicate and transactional receipts demonstrate a resumability trap: `SKIP LOCKED` plus a cursor that advances can omit the locked row forever. This backfill reselects the remaining predicate and distinguishes no available work from no remaining work. DDL validation also needs separate transactions to release the initial metadata lock before scanning.

Current Go struct names do not define the meaning of stored old history or an archive. Independent schema versions select the appropriate upcaster; aggregate version, physical layout and archive format have separate meanings. Source images are copied and verified, never rewritten during replay. Full production reader inventories, fencing, larger scale and retention/restore policy remain explicit obligations in the original baseline and P34.
