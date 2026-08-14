# Standby local-index regeneration via a pre-image-backed `LocalHBaseState`

**Status:** proposed (2026-06-23)
**Branch:** `eliminate-index-replication-v2` (PHOENIX-7931)
**Test driving it:** `ReplicationLogGroupIT#testAppendAndSync` (local index `L_*` enabled at line 341 + 422)

## Problem

On the standby, the local-index branch of `preBatchMutateWithExceptions`
(`IndexRegionObserver.java:2510`) regenerates local-index entries from the replayed data
mutations. It currently fails the cross-cluster cell-equality assertion: the standby's
local index is **missing the covered column `val2`** and **missing the old-key
`DeleteFamily` tombstones** (confirmed from the test's own table dumps, run 2026-06-23).

### Root cause (verified, not assumed)

Local indexes live in the data table's own region (`L#0` column family), so the build runs
inside `preBatchMutate` *before* the batch's data is applied. The builder reads the **prior
row state** via `LocalHBaseState.getCurrentRowState`, which `CachedLocalTable`
(`CachedLocalTable.java:118-181`) services by **scanning the data-table region**.

- **Active:** one `preBatchMutate` per `conn.commit()`. Each commit's data is materialized in
  the region before the next commit's build runs, so the region holds the correct prior state.
- **Standby:** `ReplicationLogProcessor.processLogFile` concatenates multiple active-side
  batches into one replay batch → one `preBatchMutate`. Nothing from this batch is written
  until after it returns, so all `(row, ts)` groups build against the **same** pre-batch region
  state (empty for these rows). A val1-only group can't see the prior val2 → no covered cell,
  and can't see the old indexed value → no old-key tombstone.

This is a **region-state visibility** problem, not a pre-image problem at the builder level.
The builder never asks for a pre-image — it asks `getCurrentRowState` for prior state. On the
standby we already ship that prior state per `(row, ts)` group as the `PRE_IMAGE` attribute
(used today by the global path). The fix is to feed the local builder its prior state from the
pre-image instead of from a region scan.

### Why not the alternatives

- **Replicate `L#0` cells from the active** (ship local-index cells in the data record, skip
  regeneration): viable but loses the local-index write-volume savings and needs verification
  that global regeneration ignores foreign-family cells in the reconstructed Put.
- **Replay per source-batch** (don't concatenate when local indexes present): matches active
  semantics exactly but trades directly against the coalescing throughput work.
- **Pre-image-backed `LocalHBaseState`** (this plan): keeps regeneration + coalescing, removes
  the region-state dependency by construction, and reuses the pre-image infra already built and
  validated for global indexes. Lowest risk.

## Corner case: local-only table (no global/uncovered/transform index)

Pre-image capture is gated entirely on the global block: `prepareDataRowStates` +
`capturePreImageCells` run **only** inside `if (context.hasGlobalIndex || hasUncoveredIndex ||
hasTransform)` (`IndexRegionObserver.java:2478`, body 2483-2487). The index flags are mutually
exclusive per maintainer (`IndexRegionObserver.java:2218-2226`), and `capturePreImageCells` has
no other call site. So a **local-only** table ships **no pre-image at all** — `PreImageLocalTable`
would have nothing to read. `testAppendAndSync` has both index types and would pass while the
local-only case stays silently broken, so this must be handled explicitly.

### Is the global pre-image the same as what the local index needs?

Not identical, but the global pre-image is a **superset** that fully serves the local index. Both
are a data-table `Put` of the prior committed row state; they differ only in **column scope**:

- Global (`getCurrentRowStates` → `readDataTableRows`, `IndexRegionObserver.java:1777-1803`):
  scans the **entire row, all columns** (no column filter, just the skip-scan row filter), builds
  a `Put` with every cell.
- Local (`CachedLocalTable.preScanAllRequiredRows`, `CachedLocalTable.java:132-139`): scans
  **only index-relevant columns** (all maintainers' columns + the empty-KV qualifier).

The local builder pulls only the columns it tracks via `toCover`/`columnSet`
(`LocalTableState.java:159-178`); extra columns in the pre-image sit unused. So a single
`PreImageLocalTable` wrapping the full-row global pre-image serves global-only, local-only, and
mixed tables with no per-case logic. The only real divergence is **existence**, not scope.

### Resolution (option A): capture the pre-image for local-only **only when replication is enabled**

Hoist `prepareDataRowStates` + `capturePreImageCells` so they also run for a local-only table —
but **gated on `shouldReplicate && context.logGroup.isPresent()`**, not on `hasLocalIndex`
unconditionally. The pre-image exists solely to serve standby regeneration, so a local-only table
that is not being replicated must pay nothing new.

- `shouldReplicate` (`IndexRegionObserver.java:598`, set at setup from `SYNCHRONOUS_REPLICATION_ENABLED`
  + `SchemaUtil.shouldReplicateTable`, lines 700-706) is the active-side "this table is replicated
  to a standby" signal. It is **distinct** from `context.isReplication` (`IndexRegionObserver.java:2389`),
  which means "this batch is a replayed batch *on* the standby." Capture is an **active-side**
  concern, so it keys off `shouldReplicate`, the same guard `captureReplicationCells` already uses
  (lines 1565, 1568).
- The global/uncovered/transform block already implies replication intent for those index types;
  the new local-only branch makes the replication gate explicit so we don't widen the read for
  non-replicated local-only tables.

**Cost (replicated local-only table only):** a full-row read + a shipped pre-image cell it does not
pay today (today's local path scans only index-relevant columns via `CachedLocalTable` in
`preBatchMutate`, and ships nothing). The active *already* reads prior row state for local indexes,
so the read is relocated/widened, not strictly new. **Non-replicated** local-only tables are
entirely unaffected. Narrowing the replicated local-only capture to index-relevant columns (to
match `CachedLocalTable`'s scope and trim read + payload) is a deferred optimization; correctness
holds with the superset.

**Rejected alternative (option B):** regenerate-from-pre-image when one exists, else replicate
`L#0` cells. Two code paths + the foreign-family-cell verification. Option A is one uniform
mechanism.

## Key facts established (with sources)

- `IndexBuilder.getIndexUpdate(Mutation, IndexMetaData, LocalHBaseState)`
  (`IndexBuilder.java:75-76`) **already takes `LocalHBaseState` as a parameter** — the interface
  is the injection seam. `NonTxIndexBuilder.getIndexUpdate` (`NonTxIndexBuilder.java:53-56`)
  forwards it straight into `new LocalTableState(localHBaseState, mutation)`.
- The **only** place that decides which `LocalHBaseState` to use for the local path is
  `IndexBuildManager.getIndexUpdates` line 97 (`CachedLocalTable.build(...region...)`). The
  per-mutation loop (lines 100-107) is source-agnostic.
- `getCurrentRowState` returning **null** is already handled as "no prior row":
  `LocalTableState.getIndexedColumnsTableState` (`LocalTableState.java:176`) →
  `addUpdateCells(null, false)` → `if (list == null) return;` (`LocalTableState.java:94`). So the
  "active saw empty row" sentinel (`decodePreImage` → null, `IndexRegionObserver.java:1474-1476`)
  maps cleanly to first-time-insert semantics.
- `groupMutationsByRowTs` (`IndexRegionObserver.java:1199`) is still required: `NonTxIndexBuilder`
  demands uniform-ts mutations (`NonTxIndexBuilder.java:89-90`). The grouping supplies uniform
  timestamps; the pre-image supplies prior state. Both needed, neither redundant.

## The intermediate-state subtlety (the one real design decision)

A row can recur across multiple `(row, ts)` groups in one concatenated replay batch, and each
group has its **own** pre-image. A plain `row → cells` map collapses these, so a later group
would read the earliest group's pre-image — re-introducing the same staleness, relocated from
region to map.

**Decision (revised 2026-06-23): key the prior-state map by `(row, ts)` and return each group's
own shipped pre-image. No chaining.** Rationale:

- The local builder needs only **prior row state**, never `nextState`. `NonTxIndexBuilder` applies
  the pending mutation itself (`addCleanupForCurrentBatch` uses prior state; then
  `applyPendingUpdates` + `addUpdateForGivenTimestamp`). So the local path never consumes
  `deriveNextState`.
- The active ships **one pre-image cell per row per batch**, so group N's shipped pre-image
  *already equals* the row state after groups 1..N−1 — exactly the prior state that batch saw on
  the active. It is the **authoritative** source of truth.
- `getCurrentRowState(m, …)` receives the mutation; each standby group is a uniform-ts
  `MultiMutation`, so `(row, IndexUtil.getMaxTimestamp(m))` recovers the group key for lookup.

So keying `PreImageLocalTable` by `(row, ts)` and returning that group's pre-image makes each
group's local build a faithful, independent reproduction of the corresponding active
`preBatchMutate` (prior = pre-image, pending = group's cells). This is **more** correct than
chaining: chaining would feed group N+1 the standby-*derived* `nextState(N)` instead of the
active's authoritative pre-image(N+1), risking divergence. The "collapse" worry is solved purely
by the `(row, ts)` key, not by chaining. (This also largely moots the step-3 open question about
`hasNewerTimestamps` suppression, since each group stands alone.)

`ReplicatedRowGroup.nextState` is retained — the **global** path (step 2) still needs it — but the
local path ignores it.

## Code reuse between global and local (the second ask)

The global path (`prepareReplicatedIndexMutations`, `IndexRegionObserver.java:1427-1457`) already
does, per `(row, ts)` group:
1. group the mini-batch by `RowTsKey` (lines 1431-1439),
2. `decodePreImage(group.get(0))` (line 1444),
3. `deriveNextState(preImage, groupMutations)` (line 1445),
4. hand `(preImage, nextState)` to index generation.

The local path needs exactly steps 1-3 to produce its prior-state map. **Extract a shared
helper** so both paths consume one grouping + derive pass:

```java
/** Per (row, ts) group on the standby: the group's mutations, its decoded pre-image,
 *  and the derived next-row state. Built once, consumed by both global and local paths. */
static final class ReplicatedRowGroup {
  final ImmutableBytesPtr row;
  final long ts;
  final List<Mutation> mutations;
  final Put preImage;     // null = active saw empty row
  final Put nextState;    // null = row empty after this group
}

/** Group a replicated mini-batch by (row, ts), decode each group's pre-image, and fold the
 *  group's mutations to derive next-state. Groups for the same row are returned in ascending
 *  ts so callers can chain prior-state. */
private static List<ReplicatedRowGroup> buildReplicatedRowGroups(
    MiniBatchOperationInProgress<Mutation> miniBatchOp, IndexBuilder builder) { ... }
```

- `prepareReplicatedIndexMutations` is rewritten to iterate `ReplicatedRowGroup`s instead of its
  own inline `LinkedHashMap<RowTsKey, List<Mutation>>` — behavior-preserving refactor.
- The local path builds its `PreImageLocalTable` from the same `ReplicatedRowGroup`s, using
  `nextState` as the chained prior state.
- `groupMutationsByRowTs` (the temporary helper added during diagnosis) is **removed** — its job
  is subsumed by `buildReplicatedRowGroups` + `PreImageLocalTable`. The verbose diagnostic
  logging added to it is removed too.

This is the genuine shared seam: one grouping + pre-image + derive pass, two consumers.

## Implementation steps

0. **Active: capture the pre-image for a local-only table when replication is enabled.** Today the
   capture at `IndexRegionObserver.java:2478` runs only for global/uncovered/transform. Add a branch
   so `prepareDataRowStates` + `capturePreImageCells` also run when `context.hasLocalIndex` **and**
   `shouldReplicate && context.logGroup.isPresent()` (the active-side replication gate, not
   `context.isReplication`). Mixed tables already capture via the global branch; this only adds the
   local-only-replicated case. Non-replicated local-only tables are unchanged.
   → verify: (a) replicated local-only table ships a pre-image cell in its record; (b) a
   non-replicated local-only table does **not** capture/ship one (no new read or payload).

1. **`ReplicatedRowGroup` + `buildReplicatedRowGroups`** in `IndexRegionObserver`.
   → verify: new unit tests in `IndexRegionObserverReplayTest` for (a) two ts groups on one row,
   (b) chained prior-state across groups, (c) empty-row sentinel.

2. **Refactor `prepareReplicatedIndexMutations`** to consume `ReplicatedRowGroup`s.
   → verify: global-index-only `testAppendAndSync` config still passes cell-for-cell (it does
   today — this must remain green).

3. **`PreImageLocalTable implements LocalHBaseState`** next to `CachedLocalTable`
   (`covered/data/`). Backed by `Map<RowTsKey, List<Cell>>` (the group's own pre-image cells, null
   = empty-row sentinel). `getCurrentRowState(m, …)` looks up `(row, getMaxTimestamp(m))` and
   returns those cells (null ok). `toCover` / `ignoreNewerMutations` are no-ops (we hold the exact
   per-group snapshot — document why). No chaining; each group is independent.
   → verify: unit test returning null and non-null states; a row with two `(row, ts)` groups
   returns each group's own pre-image.

4. **Injection overload** in `IndexBuildManager`:
   `getIndexUpdates(indexUpdates, miniBatchOp, mutations, indexMetaData, LocalHBaseState)`. The
   existing 4-arg method delegates to it with `CachedLocalTable.build(...)`. The loop
   (lines 100-107) is shared.
   → verify: compiles; existing (active) callers unchanged.

5. **`handleLocalIndexUpdates`**: on `context.isReplication`, build the `PreImageLocalTable` from
   the `ReplicatedRowGroup`s (keyed `(row, ts)` → pre-image cells) and call the new overload; else
   unchanged. Feed it the uniform-ts `MultiMutation`s sourced from the groups.
   → verify: full `testAppendAndSync` (global + local) passes cell-for-cell across clusters.

6. **Cleanup**: remove `groupMutationsByRowTs` and its diagnostic logging.
   → verify: `mvn spotless:apply`; recompile; full `ReplicationLogGroupIT`.

## Verification checklist

- [ ] `mvn install -pl phoenix-core-server -DskipTests` **before** every IT run (IT links the
      installed jar, not freshly-compiled classes — see memory `reference_it_stale_server_jar`).
- [ ] `IndexRegionObserverReplayTest` green (existing + new cases).
- [ ] `ReplicationLogGroupIT#testAppendAndSync` green with local index enabled (lines 341, 422).
- [ ] **Local-only-index table** (no global index) replays correctly cell-for-cell — the corner
      case that motivated step 0. Add explicit coverage; `testAppendAndSync` alone does not exercise it.
- [ ] Global-index-only path unchanged (no regression in `prepareReplicatedIndexMutations`).
- [ ] `mvn spotless:apply` clean.
- [ ] No wildcard / unshaded-Guava / commons-logging imports introduced.

## Open question to confirm during step 3

When prior state comes from a pre-image Put rather than a raw region scan, confirm
`LocalTableState`'s memstore + `ColumnTracker` timestamp logic
(`NonTxIndexBuilder.addCurrentStateMutationsForBatch:189-211`) treats the pre-image cells'
timestamps correctly — the pre-image carries the active's original cell timestamps, which is what
we want, but the `hasNewerTimestamps()` out-of-order guard should be checked against a
multi-group row to be sure no group's tombstone is suppressed.
