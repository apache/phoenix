# Unify sync + WAL-restore replication on one local-index cell filter

## Motivation

Two paths ship a batch's cells to the replication log, and they exclude
local-index (`L#`) cells by *different* mechanisms:

- **Sync** (`replicateMutations`, POST): ships `context.replicationCellsByRow`,
  which `captureReplicationCells` built at PRE **before** `handleLocalIndexUpdates`
  injected the `L#` cells. Exclusion is by **timing** — the snapshot predates the
  injection.
- **WAL-restore** (`replicateEditOnWALRestore`, crash recovery): ships
  `logEdit.getCells()` verbatim. That persisted WAL edit is built from
  `familyCellMaps[index]`, into which HBase merged the `L#` cells at
  `checkAndMergeCPMutations` (HRegion.java:3860). There is **no exclusion** here.

The WAL-restore path therefore leaks `L#` local-index cells to the standby, which
regenerates its own local index from the data record — so the shipped `L#` cells
are at best redundant and at worst corrupting (they carry the *active's*
`encodedRegionName` in the rowkey). Confirmed real; recovery-path-only, which is
why ITs (that replay the replication log, not the RS WAL) never caught it.

Root cause is the asymmetry: two exclusion mechanisms, only one of which the
WAL-restore path can use (it has no `BatchMutateContext`, no PRE-phase snapshot).
The fix is to make **both** paths exclude `L#` the same way — by column family —
so the standby-facing invariant ("no `L#` cells cross the wire") is enforced in
one place, on both paths, and the timing-based snapshot machinery
(`replicationCellsByRow`) can be deleted.

## Key source facts (verified)

- `MetaDataUtil.isLocalIndexFamily(byte[] cf)` (MetaDataUtil.java:1156) —
  `Bytes.startsWith(cf, LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES)` ("L#"). The filter
  predicate. Global-index cells are in a *separate physical table*, never in this
  region's batch; on-dup/conditional-TTL cells are legitimate data and stay.
- At POST, `getOperation(i).getFamilyCellMap()` = data + on-dup/TTL + merged `L#`
  cells (HRegion.java:3859 preBatchMutate injects, :3860 merges into
  `familyCellMaps[index]`, which is the same map object as
  `getOperation(i).getFamilyCellMap()`).
- `getWalEdit(0)` still holds the pre-image cells at POST: `buildWALEdits` reads
  `walEditsFromCoprocessors[index]` (HRegion.java:3583) without clearing it.
- `capturePreImageCells` writes one pre-image cell per replicated row to
  `miniBatchOp.getWalEdit(0)` (IRO:1719). This is the **sole** mechanism that gets
  the pre-image onto the WAL-restore path: that path has no `miniBatchOp`, no
  `BatchMutateContext`, no `dataRowStates` — it reads only the persisted
  `logEdit.getCells()`, into which `buildWALEdits` merged the slot-0 cells. The
  pre-image therefore *must* live in the WAL edit regardless of what the sync path
  does; the sync path should consume that same artifact rather than produce a
  second, parallel one.
- Pre-image cells have family `WALEdit.METAFAMILY`, not `L#`, so an
  `isLocalIndexFamily` filter over the WAL edit keeps them.
- `ignoreReplicationFilter` (mutations tagged `IGNORE_REPLICATION_ATTRIB` by
  atomic-op ignore, IRO:1050) must still be honored on the sync path. On the
  WAL-restore path those mutations were already excluded from the persisted edit
  by the active — no per-mutation attribute is available at cell granularity there
  anyway, so behavior is unchanged.

## Design

One shared static filter, applied by both paths:

```java
/** Local-index (L#) cells must never cross the wire: the standby regenerates its
 *  own local index from the data record, and a replicated L# rowkey carries the
 *  active's encodedRegionName. Drop them on every replication path. */
static boolean isReplicableCell(Cell c) {
  return !MetaDataUtil.isLocalIndexFamily(CellUtil.cloneFamily(c)); // or a no-copy variant
}
```

(Prefer a no-copy family read — `isLocalIndexFamily` over the cell's family
array/offset/length — to avoid a per-cell allocation in this loop. Check whether a
byte[]+offset+len overload exists; if not, add one alongside the existing
`isLocalIndexFamily(byte[])` rather than cloning.)

### 1. `replicateMutations` (sync, POST) — read data cells from miniBatchOp, pre-image from the WAL edit

Replace the `replicationCellsByRow` read with a POST read of the (now-final,
merged) mutation cells filtered by `isReplicableCell`, then append the pre-image
cells from the same WAL slot the WAL-restore path reads. **One producer of
pre-image cells (`capturePreImageCells`), both paths consume it** — no
re-derivation, so no drift between the two paths' shipped bytes:

```
for i in miniBatchOp:
  m = getOperation(i)
  if ignoreReplicationFilter.test(m): continue
  for cell in flattenCells(m):
    if isReplicableCell(cell): flattened.add(cell)   // drops L#, keeps data + on-dup/TTL
WALEdit preImageEdit = miniBatchOp.getWalEdit(0)      // pre-image cells capturePreImageCells wrote
if preImageEdit != null:
  flattened.addAll(preImageEdit.getCells())           // METAFAMILY family, no L# to filter
```

- `replicateMutations` no longer touches `dataRowStates` at all. The pre-image is
  read back from slot 0, byte-identical to what WAL-restore ships.
- The old no-index early-exit fallback branch collapses into this single loop:
  a table with no indexes has an empty/absent slot-0 WAL edit (no pre-image) and
  no `L#` cells (nothing filtered) — identical output to today's fallback. Delete
  the `if (replicationCellsByRow != null) … else …` split.
- Slot-0 caveat: `getWalEdit(0)` is populated by `callPreMutateCPHook`
  (HRegion.java:3743) from per-mutation `prePut`/`preDelete` CP hooks. For a
  replicated Phoenix table the only slot-0 writer is `capturePreImageCells`. Were a
  foreign coprocessor stacked below Phoenix to deposit cells there, WAL-restore
  already forwards them verbatim — so reading slot 0 on the sync path stays the
  faithful match to WAL-restore rather than a divergence.
- Keep the four existing guards (`!shouldReplicate`, `!logGroup.isPresent()`,
  `context.isReplication`, `originalMutations.isEmpty()`) and the
  `flattened.isEmpty()` skip.

### 2. `replicateEditOnWALRestore` (WAL-restore) — filter the cell stream

```
List<Cell> cells = logEdit.getCells();
if (cells == null || cells.isEmpty()) return;
List<Cell> replicable = new ArrayList<>(cells.size());
for (Cell c : cells) if (isReplicableCell(c)) replicable.add(c);
if (replicable.isEmpty()) return;
logGroup.append(tableName, -1, replicable, replicationAttrs);
logGroup.sync();
```

Pre-image (METAFAMILY) cells survive the filter; `L#` cells are dropped. Update
the method javadoc — the current text claims "no per-mutation filtering is
required here," which is exactly the bug.

### 3. Delete the snapshot machinery

- Delete `captureReplicationCells` (IRO:1654) and its call at IRO:2610.
- Delete the `replicationCellsByRow` field (IRO:496) and every reference
  (IRO:1703, 1710, 1741, 3445, 3448, 2626-2632 comment).
- `capturePreImageCells` (IRO:1701): drop the `entry.getValue().add(preImageCell)`
  sync-path dual-write; keep the `walEdit.add(preImageCell)` +
  `setWalEdit(0, walEdit)` — the WAL-restore path still needs the pre-image cells
  persisted in the WAL edit. It no longer iterates `replicationCellsByRow`; iterate
  `dataRowStates` (or the replicated rows) directly to know which rows get a
  pre-image cell. Guard becomes `dataRowStates == null || isEmpty` instead of
  `replicationCellsByRow`.
- `captureLocalIndexPreImageCells` (IRO:1738) and the local-only branch gate at
  IRO:2632: today "is this a replicated batch?" is answered by
  `replicationCellsByRow != null`. Replace that signal with a direct one —
  `shouldReplicate && !ignoreSyncReplicationForTesting && context.logGroup.isPresent()`
  (the same condition `captureReplicationCells` self-guarded on). Extract it to a
  helper (`context.isReplicated()` or a local boolean) so the global branch and the
  local-only branch share one definition.

### 4. Verify the `dataRowStates`-only pre-image gate is complete

`capturePreImageCells` previously appended a pre-image for every row in
`replicationCellsByRow` that also had a `dataRowStates` entry. After the change it
keys off `dataRowStates` alone. Confirm every replicated row that needs a
pre-image has a `dataRowStates` entry:
- global/uncovered/transform: `prepareDataRowStates` populates it for every
  enabled mutation (applyPending{Put,Delete}Mutations) — matches.
- local-only replicated: `captureLocalIndexPreImageCells` populates it — matches.
- A row present in the batch but absent from `dataRowStates` gets no pre-image
  today either (the `rowState == null` continue at IRO:1714) — matches.

## Test — WAL-restore recovery path (new; the gap that hid the leak)

No existing test drives `preWALRestore`. Add one for a local-indexed replicated
table that asserts **no `L#` cell** reaches the log via the WAL-restore path:

- Prefer a focused test that calls `preWALRestore` with a hand-built `WALEdit`
  containing data + `L#` + METAFAMILY pre-image cells and asserts the appended
  stream (capture the `logGroup.append` cells) contains the data + pre-image cells
  and zero `isLocalIndexFamily` cells. This is a unit-level assertion on the filter
  and needs no mini-cluster WAL replay.
- If an IT-level exercise of real WAL replay is cheap to reach via existing
  `ReplicationLogGroupIT` scaffolding, add it; otherwise the focused test is
  sufficient and the region-split/replay IT is already tracked as a follow-up.

Also add a sync-path assertion (extend `ReplicationLogGroupIT` local-index case)
that the shipped stream for a local-indexed table contains zero `L#` cells — this
now shares the exact filter with WAL-restore, so one predicate is under test from
both entry points.

## Verification

- `mvn -o compile -pl phoenix-core-server` clean.
- `IndexRegionObserverReplayTest` (16) green.
- `MutationCellGrouperTest` green (reader side unchanged).
- `ReplicationLogGroupIT` incl. `testConcurrentUpserts` green (sync path,
  global + local end-to-end cross-cluster equality).
- New WAL-restore filter test green; asserts zero `L#` cells shipped.
- `mvn spotless:apply` before commit. Commit source + tests only (not this plan).

## Out of scope / non-goals

- No change to the reader/reconstruct side (`MutationCellGrouper`,
  `PreImageLocalTable`) — the wire format is unchanged (data cells + METAFAMILY
  pre-image cells; `L#` cells were never *supposed* to be on the wire).
- No change to `handleLocalIndexUpdates` — it still injects `L#` via
  `addOperationsFromCP(0, …)`; we now simply filter them back out on the way to the
  log instead of relying on snapshot timing.
- Does not touch the standby carve (`preBatchMutateReplication`) landed in
  `7584f108a9`; this builds on top of it.
