# Design: Secondary Indexes on Dynamic Columns (Feature C)

**Status:** Draft
**Date:** 2026-05-15
**Author:** nlakshmanan (persona brainstorm)
**Branch:** `feature/json-indexes`
**Scope:** Apache Phoenix 5.x — master branch
**Predecessor:** `2026-05-05-bson-path-functional-indexes-design.md` (Features A + B — shipped on this branch)
**Successor candidate:** Phase 6 of the prior spec — GIN-style multi-valued path indexes.

---

## Summary

Today, Phoenix lets a query reference a *dynamic column* via the `t(name TYPE)` syntax in the `FROM` clause, but those columns are per-statement: they are never written to `SYSTEM.CATALOG`, two queries can declare different types for the same name, and they are not indexable. This design introduces **dynamic-column secondary indexes** through a *promote-then-index* contract: `CREATE INDEX idx ON t(extra VARCHAR DYNAMIC)` atomically (a) registers `extra` as a *virtual* `PColumn` in `SYSTEM.CATALOG` and (b) builds a sync-global secondary index over it. Sparse rows (UPSERTs that don't supply the column) skip the index entry. Conflicting inline types are rejected at compile time. `DROP INDEX` un-promotes the column when no other index references it.

Feature C is orthogonal to the BSON path-index work (Features A + B) already merged on this branch. Both features share the sparse-skip branch in `IndexMaintainer.buildRowKey`; otherwise they touch disjoint code.

---

## Problem statement

Dynamic columns (`t(extra VARCHAR)` in `FROM`) are widely used for tenant-defined sparse attributes — every row has its own subset of metadata. Today, every query that filters on `extra` does a full table scan because there is no schema entry for the column to attach an index to. Users wanting to index a sparse column today have only one workaround: `ALTER TABLE t ADD COLUMN extra VARCHAR; CREATE INDEX idx ON t(extra);`. This works, but it permanently widens the table schema (column appears in `SELECT *`, mixes into row-version checks, increases default projection cost) — losing the "dynamic" UX entirely.

We want a third option: **a column that is committed in `SYSTEM.CATALOG` for indexing purposes but stays invisible to default schema discovery.**

---

## Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Index contract | Promote-then-index | Indexes need fixed (qualifier, type, presence). Pure dynamic columns provide none. |
| Promotion model | Virtual: indexable, not in `SELECT *` | Preserves dynamic UX. New `PColumn.isVirtual` flag. Sparse rows still don't materialize the column. |
| Type conflict policy | Reject at compile time | Strong correctness, no silent index corruption. New error: `PHOENIX_DYNAMIC_TYPE_CONFLICT`. |
| Feature scope | Plain typed scalars only (VARCHAR, INTEGER, BIGINT, etc.) | Orthogonal to BSON path indexes. No path expressions in v1. |
| DDL form | New `DYNAMIC` token in `index_pk_constraint` | Atomic CREATE INDEX with explicit user intent. |
| DROP INDEX semantics | Un-promote when last index referencing column drops | Symmetric with CREATE INDEX. Cells in HBase remain (data not deleted). |
| Consistency mode | Sync global only (v1) | Matches Features A + B. EC indexes deferred. |
| Encoded-qualifier tables | Reject | Mirrors today's dynamic-column restriction (qualifier must be literal name bytes). |

---

## Architecture

```
                       CREATE INDEX idx ON t(extra VARCHAR DYNAMIC)
                                      │
                                      ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │ MetaDataClient.createIndex()                                     │
   │   1. Resolve t. extra not in PTable → DYNAMIC token requires     │
   │      promotion; type required (no defaulting).                   │
   │   2. Promote: insert PColumn(extra, VARCHAR, isVirtual=true,     │
   │      isDynamic=false) into SYSTEM.CATALOG via the same           │
   │      addColumnMutations path used by ALTER TABLE ADD COLUMN.     │
   │   3. Build sync-global index over the now-resolvable column.     │
   │   4. Bump base table's LastDDLTimestamp once for the whole op.   │
   └──────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                              SYSTEM.CATALOG
                              (extra is now real, virtual)

   UPSERT t(pk, extra) VALUES (1, 'hello')
        │
        ▼ UpsertCompiler resolves extra, sees isVirtual=true.
          If user supplied an inline ColumnDef with a different type,
          throw PHOENIX_DYNAMIC_TYPE_CONFLICT.
        ▼
   IndexMaintainer.buildRowKey(row)
        - extra absent → sparse-skip (existing branch from BSON Phase 2).
        - extra present → standard KeyValueColumnExpression evaluation.

   SELECT *  FROM t                  → ProjectionCompiler skips isVirtual cols.
   SELECT extra FROM t WHERE extra='hello' → resolves, hits idx.

   DROP INDEX idx
        │
        ▼ MetaDataClient.dropIndex() — after dropping the index, scan
          remaining indexes on t. If no other index references extra,
          remove the virtual PColumn from SYSTEM.CATALOG.
```

---

## Components and contracts

| # | Component | Where | Contract |
|---|---|---|---|
| 1 | `DYNAMIC` grammar token | `PhoenixSQL.g`, in `index_pk_constraint` → `dyn_index_column` | Soft keyword (won't break existing identifiers). Position: after the type, e.g. `extra VARCHAR DYNAMIC`. Type is **mandatory**; no defaulting. |
| 2 | `PColumn.isVirtual()` flag | `PColumnImpl`, plus protobuf `PTableProtos.PColumn` field | New boolean, default false. Persisted; survives serialization/deserialization. Bumps `PTABLE_IMPL_V`. |
| 3 | `MetaDataClient.createIndex` promotion branch | `phoenix-core-client/.../schema/MetaDataClient.java` | When the parser delivered an `IndexKeyConstraint` with `dynamic=true` for a missing column: emit an `addColumnMutations` for the virtual column **inside the same RPC batch** as the index-table creation. Single `LastDDLTimestamp` bump. |
| 4 | `UpsertCompiler` type-conflict guard | `phoenix-core-client/.../compile/UpsertCompiler.java` | When the UPSERT `ColumnDef` for a name collides with a registered `isVirtual` column, throw `PHOENIX_DYNAMIC_TYPE_CONFLICT` (new error code in `SQLExceptionCode`). |
| 5 | `ProjectionCompiler` virtual filter | `phoenix-core-client/.../compile/ProjectionCompiler.java` | In `*` expansion, skip columns where `isVirtual()`. Explicit reference (`SELECT extra`) still resolves. |
| 6 | `MetaDataClient.dropIndex` un-promotion | same file | After successful index drop, scan `PTable.getIndexes()` for any remaining reference to each virtual column previously indexed. If none, emit a column-drop mutation for the virtual column. Single `LastDDLTimestamp` bump for the whole op. |
| 7 | Sparse-skip in `IndexMaintainer.buildRowKey` | already exists from BSON Phase 2 | No change — the existing null-result skip branch handles "row didn't UPSERT this column." |
| 8 | Cache invalidation | `LastDDLTimestamp` mechanism | Existing. Must bump on both create-index-with-promotion and drop-index-with-un-promotion. |
| 9 | `SYSTEM.CATALOG` migration | `UpgradeUtil` | Backfill `IS_VIRTUAL=false` for all existing PColumn rows during upgrade. Existing pattern. |
| 10 | Validators | various | Reject: `DYNAMIC` on encoded-qualifier tables; `DYNAMIC` on a name that already exists as a regular column; `DYNAMIC` without a type. |

### What explicitly does not change

- On-disk index format.
- `IndexMaintainer`, `IndexRegionObserver`, `PhoenixIndexCodec`, WAL.
- Existing dynamic-column UX (`SELECT extra FROM t(extra VARCHAR)` continues to work for un-promoted cases).
- BSON path index code from the earlier phases (Features A + B).
- Transaction or EC index integration (deferred).

---

## Incremental delivery (4 phases)

Each phase merges independently. After phase N, master is shippable; users get a coherent subset.

### Phase 0 — `isVirtual` plumbing (no user-visible change)

- Add the boolean to `PColumnImpl`, `PTableProtos.PColumn`, `PColumn` interface.
- Bump `PTABLE_IMPL_V`.
- Add `UpgradeUtil` backfill.
- `ProjectionCompiler.*` skips `isVirtual`.
- Unit tests: serialize/deserialize round-trip, `*`-expansion test, regression on an existing IT proves no behavior change.

**Exit criteria:** all existing tests pass; `mvn install -pl phoenix-core` clean; new unit tests at 100% line coverage on the new flag.

### Phase 1 — Grammar token + DDL acceptance (DDL works, no index yet)

- `DYNAMIC` soft keyword in `PhoenixSQL.g` for `index_pk_constraint`.
- Parser emits `IndexKeyConstraint` with a `dynamic=true` marker per column.
- `MetaDataClient.createIndex`: if `dynamic=true` and column missing → promote (write virtual `PColumn` to catalog) within the same RPC; then build the index over it. Atomic.
- New error codes: `DYNAMIC_INDEX_REQUIRES_TYPE`, `DYNAMIC_INDEX_NAME_CONFLICTS_WITH_REGULAR_COLUMN`, `DYNAMIC_INDEX_NOT_ALLOWED_ON_ENCODED_TABLE`.

**Exit criteria:** `DynamicColumnIndexCreationIT` — create index + assert `SYSTEM.CATALOG` row exists with `IS_VIRTUAL=true`; assert `SELECT *` doesn't return the column; assert `SELECT extra` does. Cover all three error codes.

### Phase 2 — Write path (UPSERTs maintain the index correctly)

- `UpsertCompiler` type-conflict guard with `PHOENIX_DYNAMIC_TYPE_CONFLICT`.
- Confirm sparse-skip from BSON Phase 2 fires for absent columns.

**Exit criteria:** `DynamicColumnIndexWriteIT` — full table-and-index correctness. UPSERTs with the column → index entry exists. UPSERTs without the column → index entry absent (sparse). UPSERTs with conflicting inline `ColumnDef` → `SQLException` with the new error code. Side-by-side query against table-with-index vs. `ALTER INDEX … DISABLE` returns identical results across a 200-row randomized fixture.

### Phase 3 — DROP INDEX un-promotion + observability + docs

- `MetaDataClient.dropIndex` un-promotion logic, gated by "no other index references this column."
- Counters: `phoenix.index.dyncol.promotions`, `phoenix.index.dyncol.unpromotions`, `phoenix.index.dyncol.type_conflict_rejects`. Reuse the `BsonPathMetrics` pattern from Phase 5 of the BSON work.
- User-facing doc page: when to use vs. ADD COLUMN, sparse-skip semantics, type-conflict rejection rule.

**Exit criteria:** `DynamicColumnIndexDropIT` — single index → drop → column gone. Two indexes on same column → drop one → column persists; drop second → column gone. Counters incremented.

---

## Rollback plan

- **Phase 0:** strictly additive; rollback by reverting commit.
- **Phase 1:** feature flag `phoenix.index.dynamic.enabled` (default true). Setting false rejects all `DYNAMIC` DDL at parse time. Existing promoted columns remain readable.
- **Phase 2:** the same feature flag covers the type-conflict guard. Disabling it preserves write availability at the cost of correctness; documented as emergency-only.
- **Phase 3:** cosmetic.

---

## Local testing plan

- Reuse the docker-based local IT runner (`bin/run-it.sh` from commit `31e1b2055f`) for the new ITs.
- Per-phase: unit tests + one IT + EXPLAIN-plan goldens (extend the classifier added in commit `45675ffa6f`).
- Cross-phase invariant: results-with-index = results-with-index-disabled across a randomized 200-row dataset (mirrors `BsonPath` consistency IT pattern).
- Upgrade test: build a table on master without this feature, bounce to a build with this feature, run `UpgradeUtil`, confirm `IS_VIRTUAL=false` backfilled and existing tables work unchanged.

---

## Open questions for review

1. **`SELECT *` virtual filter — should there be an opt-in to include virtuals?** e.g., `SELECT * INCLUDE VIRTUAL FROM t`. v1 says no; document workaround as explicit column reference.
2. **Multi-tenant view interaction.** A tenant view atop a base table with a virtual column: does the view inherit the virtual column? Default proposal: yes, virtual columns inherit through views like regular columns; tenants cannot promote through a view.
3. **CDC stream interaction.** Should virtual columns appear in CDC change images? Default proposal: yes if the column was written by the row's UPSERT; absent otherwise (matches sparse semantics). Validate against `CDCGlobalIndexRegionScanner` behavior.

These three are scoped to clarify in the implementation plan, not blockers for the design.

---

## Out of scope

- BSON path expressions on promoted virtual columns (covered by Features A + B if user opts to declare a `BSON DYNAMIC` virtual column; specific UX deferred).
- GIN-style multi-valued indexes (Phase 6 of the prior spec).
- Transactional or eventually-consistent variants.
- `ALTER TABLE ADD VIRTUAL COLUMN` standalone DDL (not strictly needed; promotion is always coupled to `CREATE INDEX` in v1).
