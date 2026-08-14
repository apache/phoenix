# Refactor `IndexRegionObserver` into a thin coprocessor + named collaborators

## Context

`phoenix-core-server/.../hbase/index/IndexRegionObserver.java` (IRO) is the most important class on Phoenix's write path and has grown to ~2607 lines on `master` today. Over time it has accumulated many distinct responsibilities — atomic ON DUP KEY processing, conditional TTL (PHOENIX-7170 / PHOENIX-7667), index-mutation generation (verified/unverified), row locking, concurrent-batch coordination, WAL annotation, slow-call metrics, and pre/post index-write phases. The hot path `preBatchMutateWithExceptions` is a ~100-line orchestration method that touches every cluster.

`PHOENIX-7562-feature-new` is about to merge into `master`. The merge is purely additive: it brings synchronous replication (HAGroup-aware mutation blocking, replication filtering, WAL-restore replication, `Optional<ReplicationLogGroup>` on the batch context) on top of everything master has today. The eventually-consistent-index CDC path — `IndexCDCConsumer`, `cdcPreMutationsBytes` / `cdcPostMutationsBytes`, `prepareEventuallyConsistentIndexMutations`, `serializeCDCMutations` / `compressCDCMutations` — stays. **This refactor targets the post-merge state of master, which is current-master + sync-replication.**

Goals:
- **Readability:** collapse `preBatchMutateWithExceptions` into a short, scannable pipeline of named stages.
- **Modularity:** establish clean seams so sync-replication has a clean home today and future write-path concerns (e.g., Variant JSON encoding on the index path — see [[project_variant_json]]) can plug in without touching the orchestrator.

One PR overall, composed of small reviewable commits — each builds clean, runs the IT regression bar, and is independently revertable.

Pattern to follow: the in-repo precedent of thin observer + delegate helpers (`BaseScannerRegionObserver` → `RegionScannerFactory`, `UngroupedAggregateRegionObserver` → scanner classes). Wiring stays plain Java — `start()` constructs each collaborator with `new` and passes what it needs as constructor arguments. No Spring/Guice; no annotations.

## Collaborator class list

All new types live in `org.apache.phoenix.hbase.index.batch` (sibling to `index.builder`, `index.write`). IRO stays in `org.apache.phoenix.hbase.index`.

- **`BatchMutateContext`** — promoted from inner to top-level. Single state object for one `preBatchMutate` invocation. Field visibility narrows to private; outside world goes through methods. `volatile currentPhase` and synchronized latch list keep their semantics.
- **`PendingRow`** — promoted to top-level next to `BatchMutateContext`. No new behavior.
- **`BatchPhaseCoordinator`** — owns the cross-batch concurrency machinery: the `pendingRows` map, `lastTimestamp` / `batchesWithLastTimestamp`, `assignBatchTimestamp` (today `getBatchTimestamp` + `shouldSleep`), `waitForPreviousConcurrentBatch`, `removePendingRows`. Constructed once per IRO so `pendingRows` stays per-region.
- **`MutationClassifier`** — stateless. Stamps classification flags onto `BatchMutateContext` (collapses `identifyIndexMaintainerTypes`, `identifyMutationTypes`, `isPartialUncoveredIndexMutation`, `isStrictTTLEnabled`). Also detects the new replication-sourced-batch attribute and stamps `markReplicationSourced` accordingly — see §"Replication-sourced batches".
- **`RowLockCoordinator`** — thin wrapper over `LockManager`. Owns `populateRowsToLock`, `lockRows`, `unlockRows`, `releaseLocksForOnDupIgnoreMutations`. Encapsulates the TreeSet ordering invariant (PHOENIX-6871 / HBASE-17924).
- **`DataRowStateLoader`** — owns `getCurrentRowStates`, `readDataTableRows`, `prepareDataRowStates`, `applyPendingPutMutations`, `applyPendingDeleteMutations`, `applyOnePendingDeleteMutation`. Self-contained read-and-merge subsystem. The "where do I get the current data row state for a row key that isn't already pending in memory" question is delegated to a `DataRowStateSource` strategy (see below) — that's the seam replication-sourced batches and future Variant JSON read paths plug into.
- **`DataRowStateSource`** (functional interface) — supplies current row state(s) to `DataRowStateLoader` for the rows that aren't already in-memory from a concurrent batch. Two impls today:
  - `RegionScanDataRowStateSource` — the existing path: takes a set of row keys, issues a `Scan` against the region (BloomFilter point-gets if `useBloomFilter` else `SkipScanFilter` on a multi-row scan), populates `context.dataRowStates`.
  - `BatchSuppliedDataRowStateSource` — the replication path: extracts current row state from mutations tagged with the replication-sourced attribute. No region scan happens. The replication producer guarantees the state mutation appears alongside the change mutation for each row.

  The interface signature roughly:
  ```java
  interface DataRowStateSource {
    void load(ObserverContext<RegionCoprocessorEnvironment> c,
              BatchMutateContext ctx,
              MiniBatchOperationInProgress<Mutation> miniBatchOp,
              Set<ImmutableBytesPtr> rowKeysNeedingState) throws IOException;
  }
  ```
  `DataRowStateLoader` decides which source to call (one or the other; not both) using `ctx.isReplicationSourced()`. Selection happens once per batch.
- **`AtomicMutationProcessor`** — owns the ON DUP KEY pipeline: `preIncrementAfterRowLock`, `addOnDupMutationsToBatch`, `generateOnDupMutations`, `extractExpressionsAndColumns`, plus the cell helpers `updateCurrColumnCellExpr`, `checkCellNeedUpdate`, `addEmptyKVCellToPut`. The largest single cluster (~600 LOC) and the strongest justification for extraction.
- **`ConditionalTtlProcessor`** — owns `updateMutationsForConditionalTTL`. Small but distinct seam.
- **`IndexMutationPlanner`** — owns `prepareIndexMutations`, `preparePreIndexMutations`, `preparePostIndexMutations`, `prepareEventuallyConsistentIndexMutations`, `handleLocalIndexUpdates`, `groupMutations`. The static `IndexRegionObserver.generateIndexMutationsForRow` STAYS where it is — external callers exist.
- **`IndexWritePipeline`** — owns `doPre`, `doPost`, `doIndexWritesWithExceptions`, slow-call metric emission for index writes, and reading the testing-fail toggles. Wraps `preWriter` / `postWriter`. Composes a `ReplicationFilter` (default identity; sync-replication impl filters via `ignoreReplicationFilter`).
- **`WalAnnotationWriter`** — owns `preWALAppend`, `appendMutationAttributesToWALKey`, `preWALRestore` / `replicateEditOnWALRestore` / `splitCellsIntoMutations` (sync-replication WAL-restore path), and the static WAL helpers (`appendToWALKey`, `getAttributeValueFromWALKey`, `getAttributeValuesFromWALKey`).
- **`SyncReplicationGate`** — owns the HAGroup-aware mutation-blocking check that today lives at the top of `preBatchMutate`: `getHAGroupFromBatch`, `isHAGroupOnClientStale`, per-HAGroup `isMutationBlocked`. Throws `MutationBlockedIOException` / `StaleClusterRoleRecordException`. Also owns the `Optional<ReplicationLogGroup> logGroup` resolution stored on `BatchMutateContext`. Constructed once; consulted before `preBatchMutateWithExceptions` runs and again to populate the context's `logGroup`.

No `MutationCellUtil` umbrella class. Static helpers are redistributed to where they're actually used (functional interfaces are reserved for the §"Modularity seams" plug points where callers swap implementations — not for leaf helpers):

- `transferAttributes`, `getDeleteIndexMutation`, `flattenCells` — keep as `public static` on `IndexRegionObserver`. External callers exist (`PhoenixIndexBuilder`, `GlobalIndexRegionScanner`); same rationale as `generateIndexMutationsForRow`.
- `mergeCells`, `checkCellNeedUpdate`, `addEmptyKVCellToPut`, `updateCurrColumnCellExpr` — atomic-update-internal only; move with `AtomicMutationProcessor` as `private static` members.
- `setTimestamps`, `setTimestampOnMutation` — orchestrator-internal only; stay `private static` on `IndexRegionObserver`.

What else stays on IRO (deliberately):
- `start` / `stop` lifecycle (coprocessor contract surface).
- The static testing toggles `setIgnoreIndexRebuildForTesting`, `setFailPreIndexUpdatesForTesting`, `setFailPostIndexUpdatesForTesting`, `setFailDataTableUpdatesForTesting`, `setIgnoreWritingDeleteColumnsToIndex` — tests reference them by `IndexRegionObserver.setFail...`.
- The `static generateIndexMutationsForRow` method (external callers).
- The ThreadLocal `batchMutateContext` and its set/remove pair (only writer).
- `getPhoenixIndexMetaData` (protected hook).

## `BatchMutateContext` API sketch (methods, not fields)

```java
public final class BatchMutateContext {
  BatchMutateContext(int clientVersion);

  // classification flags (set by MutationClassifier, read by everyone)
  void markHasAtomic();         boolean hasAtomic();
  void markHasGlobalIndex();    boolean hasGlobalIndex();
  void markHasUncoveredIndex(); boolean hasUncoveredIndex();
  void markHasLocalIndex();     boolean hasLocalIndex();
  void markHasTransform();      boolean hasTransform();
  void markHasRowDelete();      boolean hasRowDelete();
  void markHasConditionalTtl(); boolean hasConditionalTtl();
  void markImmutableRows();     boolean immutableRows();
  void markReplicationSourced(); boolean isReplicationSourced(); // PHOENIX-7562 add
  void setReturnResult(boolean v);  boolean returnResult();
  void setReturnOldRow(boolean v);  boolean returnOldRow();
  boolean needsCurrentRowStates();   // collapses the giant if-condition

  // phase
  BatchMutatePhase getCurrentPhase();
  void transitionTo(BatchMutatePhase next);   // single writer for currentPhase

  // rows / locks / data states
  Set<ImmutableBytesPtr> rowsToLock();        // backed by TreeSet
  void addRowToLock(ImmutableBytesPtr ptr);
  List<RowLock> rowLocks();
  void initDataRowStates(int expectedSize);
  Map<ImmutableBytesPtr, Pair<Put, Put>> dataRowStates();
  Put getNextDataRowState(ImmutableBytesPtr rowKeyPtr);

  // index updates
  ListMultimap<HTableInterfaceReference, Mutation> preIndexUpdates();
  ListMultimap<HTableInterfaceReference, Mutation> postIndexUpdates();
  ListMultimap<HTableInterfaceReference, Pair<Mutation, byte[]>> indexUpdates();

  // concurrency
  CountDownLatch getCountDownLatch();
  void countDownAllLatches();
  int getMaxPendingRowCount();  void setMaxPendingRowCount(int v);
  Map<ImmutableBytesPtr, BatchMutateContext> lastConcurrentBatchContext();
  void setLastConcurrentBatchContext(Map<ImmutableBytesPtr, BatchMutateContext> m);

  // originals / atomic helpers
  void populateOriginalMutations(MiniBatchOperationInProgress<Mutation> op);
  List<Mutation> getOriginalMutations();
  Map<ColumnReference, Pair<Cell, Boolean>> currColumnCellExprMap();
  Map<ColumnReference, Pair<Cell, Boolean>> oldRowColumnCellExprMap();
  void setCurrColumnCellExprMap(...); void setOldRowColumnCellExprMap(...);

  // CDC byte buffers (eventually-consistent index path; produced by IndexMutationPlanner)
  Map<ImmutableBytesPtr, byte[]> cdcPreMutationsBytes();
  Map<ImmutableBytesPtr, byte[]> cdcPostMutationsBytes();

  // sync-replication — set by SyncReplicationGate before the orchestrator runs
  Optional<ReplicationLogGroup> logGroup();
  void setLogGroup(Optional<ReplicationLogGroup> g);

  int getClientVersion();
}
```

`logGroup` is the only sync-replication state on the context; everything else (filtering, blocking, WAL restore) lives on the collaborator that owns it. The CDC byte buffers stay on the context because the planner produces them inside `preparePostIndexMutations` and downstream code reads them — this is unchanged from today.

## Target shape of `preBatchMutateWithExceptions`

`preBatchMutate` (the public hook) consults `SyncReplicationGate` first — throws `MutationBlockedIOException` / `StaleClusterRoleRecordException` early — then resolves the `logGroup`, then calls `preBatchMutateWithExceptions`:

```java
public void preBatchMutateWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c,
    MiniBatchOperationInProgress<Mutation> miniBatchOp) throws Throwable {

  PhoenixIndexMetaData meta = getPhoenixIndexMetaData(c, miniBatchOp);
  BatchMutateContext ctx = new BatchMutateContext(meta.getClientVersion());
  ctx.setLogGroup(syncReplicationGate.resolveLogGroup(miniBatchOp));
  setBatchMutateContext(c, ctx);

  // 1. classify: stamps hasAtomic/hasGlobalIndex/... onto ctx
  classifier.classify(miniBatchOp, meta, ctx);
  ctx.populateOriginalMutations(miniBatchOp);
  if (ctx.hasRowDelete()) ServerIndexUtil.setDeleteAttributes(miniBatchOp);

  // 2. lock: populate TreeSet, acquire locks, transition INIT->PRE
  if (!lockCoordinator.populateAndLock(miniBatchOp, ctx)) return;

  // 3. read current row states (only if classification says we need them)
  if (ctx.needsCurrentRowStates()) dataRowLoader.loadCurrent(c, ctx, meta, miniBatchOp);

  // 4. conditional TTL: rewrite mutations whose row version expired
  if (ctx.hasConditionalTtl()) ttlProcessor.apply(miniBatchOp, ctx);

  // 5. atomic ON DUP KEY: rewrite batch, release IGNORE locks, fast-exit if empty
  if (ctx.hasAtomic() || ctx.returnResult()) {
    if (atomicProcessor.processAndMaybeShortCircuit(miniBatchOp, ctx)) return;
  }

  // 6. timestamps + index plan + pre-index write + reacquire + wait + post-index plan
  TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
  long ts = phaseCoordinator.assignBatchTimestamp(ctx, table);
  setTimestamps(miniBatchOp, builder, ts, classifier.isStrictTtl(miniBatchOp));
  if (ctx.hasGlobalIndex() || ctx.hasUncoveredIndex() || ctx.hasTransform()) {
    dataRowLoader.prepareNext(c, miniBatchOp, ctx, ts);
    indexPlanner.preparePreIndex(ctx, ts, meta);
    lockCoordinator.unlock(ctx);
    writePipeline.doPre(ctx);
    lockCoordinator.relock(ctx);
    if (ctx.lastConcurrentBatchContext() != null) phaseCoordinator.waitForPrevious(table, ctx);
    indexPlanner.preparePostIndex(ctx, ts, meta);
  }
  if (ctx.hasLocalIndex()) indexPlanner.handleLocalIndex(table, miniBatchOp, ctx, meta);

  if (failDataTableUpdatesForTesting) throw new DoNotRetryIOException("Simulating ...");
}
```

`postBatchMutateIndispensably` collapses similarly: phase transition → `phaseCoordinator.removePendingRows` → `writePipeline.doPost` (or signal failure) → `lockCoordinator.unlock` → `removeBatchMutateContext`.

## Commit sequence (11 commits, leaf → root)

Order: state object → side-effect-free helpers → stateful coordinators → orchestrator collapse last. Each commit compiles and passes the IT regression bar.

1. **Promote `BatchMutateContext` and `PendingRow` to top-level types** in `org.apache.phoenix.hbase.index.batch`. Add the §"BatchMutateContext API sketch" methods but keep package-private accessors as deprecated bridges so the orchestrator still compiles unchanged. No behavior change.
2. **Extract `WalAnnotationWriter`.** `preWALAppend`, `appendMutationAttributesToWALKey`, the static WAL helpers, plus the sync-replication `preWALRestore` / `replicateEditOnWALRestore` / `splitCellsIntoMutations`. IRO's `preWALAppend` and `preWALRestore` become one-liner delegates.
3. **Extract `MutationClassifier`.** Pure relocation of the four classification methods behind a single `classify(...)` entry point. First cheap unit-test seam. Adds detection of the PHOENIX-7562 replication-sourced-batch attribute, stamping `ctx.markReplicationSourced()`.
4. **Extract `RowLockCoordinator`.** `populateRowsToLock`, `lockRows`, `unlockRows`, `releaseLocksForOnDupIgnoreMutations`. Constructor takes `LockManager` + `rowLockWaitDuration`. The TreeSet ordering invariant lives in this class' javadoc.
5. **Extract `DataRowStateLoader` + `DataRowStateSource`.** All current/next-state methods move into `DataRowStateLoader`. Define `DataRowStateSource` and the `RegionScanDataRowStateSource` impl that wraps today's `Scan` / `BloomFilter` / `SkipScanFilter` logic. Add `BatchSuppliedDataRowStateSource` for replication-sourced batches; selection is via `ctx.isReplicationSourced()`. `start()` wires both. See §"Replication-sourced batches".
6. **Extract `ConditionalTtlProcessor`.** Trivial body but lands the seam.
7. **Extract `AtomicMutationProcessor`.** The big one (~600 LOC). Carries its private-static cell helpers (`mergeCells`, `checkCellNeedUpdate`, `addEmptyKVCellToPut`, `updateCurrColumnCellExpr`) along with it. At this point IRO has shed two-thirds of its body. Run `OnDuplicateKeyIT` / `OnDuplicateKey2IT` as the bar.
8. **Extract `IndexMutationPlanner` + `IndexWritePipeline` + `ReplicationFilter`.** Plan/prepare methods into the planner; `doPre` / `doPost` / `doIndexWritesWithExceptions` into the pipeline. Define `ReplicationFilter` and wire the predicate-composed impl built today inside `start()` through it; the pipeline applies the filter before `preWriter.write` / `postWriter.write`. Slow-call metric emission moves with `doPre` / `doPost`. The `public static` helpers `generateIndexMutationsForRow`, `getDeleteIndexMutation`, `flattenCells`, `transferAttributes` stay on IRO (external callers).
9. **Extract `SyncReplicationGate`.** Move `getHAGroupFromBatch`, `getHAGroupFromWALKey`, the `isHAGroupOnClientStale` / per-HAGroup `isMutationBlocked` checks today at the top of `preBatchMutate`, plus `resolveLogGroup` (the single point that produces `Optional<ReplicationLogGroup>` for the batch). `preBatchMutate` calls the gate before delegating to `preBatchMutateWithExceptions`.
10. **Extract `BatchPhaseCoordinator` and collapse the orchestrator.** Move `pendingRows`, `lastTimestamp`, `batchesWithLastTimestamp`, `getBatchTimestamp` / `shouldSleep`, `waitForPreviousConcurrentBatch`, `removePendingRows`. Rewrite `preBatchMutateWithExceptions` to the target pseudocode shape and `postBatchMutateIndispensably` similarly. Delete the deprecated bridge accessors from commit 1.
11. **Encapsulate the HBASE-18127 ThreadLocal.** Move the ThreadLocal field onto `BatchMutateContext` and route every read/write through `attachToThread` / `detachFromThread` / `fromThread`. Delete the three IRO static helpers and update `preWALAppend` / `preIncrementAfterRowLock` to read from `fromThread`. See §"Encapsulate the HBASE-18127 ThreadLocal hack".

Optional commit 12: dead-import cleanup, visibility tightening, package javadoc.

## Replication-sourced batches

PHOENIX-7562-feature-new is adding a new mutation attribute that marks a batch as having been sourced from replication rather than from a Phoenix client. When the attribute is present, two things change about the write path:

1. **Current data row state is supplied, not read.** The replication producer ships the pre-mutation state of each row alongside the change mutation, tagged to identify it as the current state. IRO must NOT issue a region scan for the current data row state in this case; it must extract the supplied state from the batch.
2. **Index mutation generation otherwise proceeds normally.** Once `context.dataRowStates` is populated (regardless of source), the rest of the path is unchanged.

The refactor handles this cleanly through the `DataRowStateSource` strategy seam introduced on `DataRowStateLoader`:

- `MutationClassifier` reads the new attribute off the batch and stamps `ctx.markReplicationSourced()`.
- `DataRowStateLoader.loadCurrent(...)` selects its strategy based on `ctx.isReplicationSourced()`:
  - `false` → `RegionScanDataRowStateSource` (existing behavior, unchanged).
  - `true` → `BatchSuppliedDataRowStateSource` (new; extracts from tagged mutations in `miniBatchOp`).
- The in-memory concurrent-batch lookup (`pendingRows.putIfAbsent` → `lastContext.getNextDataRowState(...)`) runs the same way regardless of source, so live concurrent updates between replicated batches and same-region client batches still resolve correctly. Only the "row key wasn't already pending in memory" branch differs.

This is exactly the kind of seam that earns a real interface (two genuine impls now) rather than the no-op-default plug-in pattern. It also keeps IRO and the orchestrator unaware of "replication" as a concept — the new path is a strategy choice on one collaborator, scoped to one decision point.

The `BatchSuppliedDataRowStateSource` impl is added in the same commit that defines `DataRowStateSource` and extracts `DataRowStateLoader` (commit 5). The `MutationClassifier` flag is added in commit 3. The wiring (`start()` builds both source impls and `DataRowStateLoader` is given a factory or selector function over them) lives in IRO's `start()` per the §"What NOT to do" plain-Java rule.

## Encapsulate the HBASE-18127 ThreadLocal hack

`IndexRegionObserver.java:311-313` carries:

```
// Hack to get around not being able to save any state between
// coprocessor calls. TODO: remove after HBASE-18127 when available
```

HBASE-18127 hasn't shipped — the ThreadLocal stays. What the refactor *can* fix is the encapsulation: today the ThreadLocal field, three static helpers (`setBatchMutateContext` / `getBatchMutateContext` / `removeBatchMutateContext`), and five call sites all live on IRO, with collaborators reaching back through them. The fix moves the ThreadLocal onto `BatchMutateContext` itself as a `private static final ThreadLocal<BatchMutateContext> CURRENT`, exposing three methods on the type:

```java
static void attachToThread(BatchMutateContext ctx);   // only writer (entry)
static void detachFromThread();                       // only writer (exit)
static BatchMutateContext fromThread();               // only reader
```

`preBatchMutateWithExceptions` calls `attachToThread` once at entry; `postBatchMutateIndispensably`'s `finally` calls `detachFromThread`. `preWALAppend` and `preIncrementAfterRowLock` (the only paths today that don't receive the context as an argument) call `fromThread`. The HBASE-18127 TODO comment moves with the field and stays accurate. The three IRO static helpers are deleted.

This is purely an encapsulation win, not a behavior change — but it removes the "ThreadLocal touched in five places" smell and lands the TODO comment next to its actual mechanism.

## Sync-replication is in-tree post-merge — extracted, not pluggable

After the PHOENIX-7562 merge, sync-replication is a present concern, not a future plug-in. The refactor extracts each piece into the collaborator it naturally belongs in. Two functional interfaces exist, because each has two genuine impls today:

- `ReplicationFilter` — identity (replication off) and `PredicateReplicationFilter` (replication on).
- `DataRowStateSource` — `RegionScanDataRowStateSource` (client batches) and `BatchSuppliedDataRowStateSource` (replication-sourced batches). See §"Replication-sourced batches".

Per-piece extraction:

- HAGroup-aware mutation blocking → **`SyncReplicationGate`** (consulted at the top of `preBatchMutate`, before the orchestrator).
- Per-batch `Optional<ReplicationLogGroup>` resolution → **`SyncReplicationGate`** writes it onto `BatchMutateContext.logGroup`.
- WAL-restore replication (`preWALRestore`, `replicateEditOnWALRestore`, `splitCellsIntoMutations`) → **`WalAnnotationWriter`** (already owns the WAL surface).
- Mutation-side replication filtering (`ignoreReplicationFilter`, the `IGNORE_REPLICATION` / `NOT_TENANT_ID_ROW_KEY_PREFIX` / `NOT_CHILD_LINK_TENANT_VIEW` predicates, `getSynchronousReplicationFilter`) → **`ReplicationFilter`** functional interface, composed into `IndexWritePipeline`. Two impls: identity (replication off) and `PredicateReplicationFilter` (replication on, table-type-driven).
- Replication-sourced batch detection + state extraction → **`MutationClassifier`** stamps `markReplicationSourced`; **`DataRowStateLoader`** dispatches to the `BatchSuppliedDataRowStateSource` impl of **`DataRowStateSource`**. See §"Replication-sourced batches".

The `shouldReplicate`, `ignoreReplicationFilter`, and `abortable` fields move off IRO. They live on `SyncReplicationGate` and `IndexWritePipeline` constructor parameters wired in `start()`.

## Future-feature seam

The one seam left genuinely future-shaped:

- **Variant JSON encoding on the index write path** ([[project_variant_json]]). `IndexMutationPlanner` produces `Mutation` objects; introduce `interface IndexMutationEncoder { void encode(BatchMutateContext, Mutation); }` invoked between plan and write. Default no-op; a Variant JSON encoder slots in there. Seam at the cell-encoding step rather than row-derivation, which is what variant-typed callers want.

Do not add other speculative interfaces. Adding a `PostIndexStrategy` plug, a `ConcurrencyGate` plug, a `WalAnnotator` plug, etc. on master before there's a second implementation would be over-engineering — there's only one impl of each, and the extracted classes are themselves the seams.

## Risks and invariants the refactor must preserve

- **ThreadLocal lifetime.** Through commits 1–10 the `batchMutateContext` ThreadLocal stays on IRO; only `setBatchMutateContext` / `removeBatchMutateContext` write to it; collaborators receive `BatchMutateContext` as an argument and never touch the ThreadLocal. Commit 11 moves the ThreadLocal onto `BatchMutateContext` itself — same lifetime semantics, narrower surface.
- **`pendingRows` ownership.** Per-region (per IRO instance), not static. `BatchPhaseCoordinator` is owned 1:1 by IRO and constructed in `start()`. `PendingRow` keeps its back-reference to the map. We do **not** convert this to a different structure in this refactor.
- **Lock ordering (PHOENIX-6871 / HBASE-17924).** Only `RowLockCoordinator.populateAndLock` constructs the TreeSet of `rowsToLock`. `BatchMutateContext.rowsToLock()` returns `Set<ImmutableBytesPtr>` typed broadly but backed by `TreeSet` — preserves iteration order.
- **`volatile currentPhase` + waitlist/latch semantics.** All transitions go through `BatchMutateContext.transitionTo`. The double-check pattern in `waitForPreviousConcurrentBatch` (recheck phase after `getCountDownLatch` returns null, recheck after `await`) is copied byte-for-byte into `BatchPhaseCoordinator.waitForPrevious` — this is the trickiest code in the file and deserves the literal copy.
- **Static testing toggles** stay on IRO. Internal readers (e.g., `IndexWritePipeline`) read the same statics; do not duplicate.
- **Slow-call metric parity.** Each `metricSource.update*Time` and `incrementSlow*` call moves with the block it measures: `updateDuplicateKeyCheckTime` into `AtomicMutationProcessor`, `updateIndexPrepareTime` into `IndexMutationPlanner`, `updatePreIndexUpdateTime` / `updatePostIndexUpdateTime` into `IndexWritePipeline`. `MetricsIndexerSource` injected at construction.
- **Checked exceptions.** Every extracted method keeps its existing `throws` clause exactly so call sites in IRO don't ripple.

## What NOT to do

- No dependency-injection framework (Spring/Guice/etc.). IRO's `start()` plainly does `this.lockCoordinator = new RowLockCoordinator(lockManager, rowLockWaitDuration);` and so on — every collaborator is constructed by hand and given exactly what it needs as constructor arguments. No annotations, no service locator, no factory registry. Phoenix doesn't use a DI framework today; this refactor doesn't introduce one.
- No `<T extends Mutation>` generification — HBase API is `Mutation`-typed.
- Do not change the public API of `IndexRegionObserver.generateIndexMutationsForRow` — external callers exist (rebuild paths, IT scaffolding). `IndexMutationPlanner` calls it as `IndexRegionObserver.generateIndexMutationsForRow(...)`.
- Do not move `pendingRows` to a static or swap its data structure.
- Do not move `start` / `stop` into a helper.
- Do not add speculative interfaces (`PostIndexStrategy`, `ConcurrencyGate`, `WalAnnotator`, etc.) on top of single implementations. The extracted classes are themselves the seams; only `ReplicationFilter` (two real impls today) and `IndexMutationEncoder` (Variant JSON, planned) get functional interfaces.

## Critical files

- `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/IndexRegionObserver.java` (~2607 lines — the subject)
- `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/builder/IndexBuildManager.java` (collaborator, unchanged)
- `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/write/IndexWriter.java` (collaborator, unchanged)
- `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/LockManager.java` (collaborator, unchanged)
- `phoenix-core-server/src/main/java/org/apache/phoenix/index/PhoenixIndexBuilder.java` (atomic-op execution, unchanged)
- New package: `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/batch/` (all new collaborator types)

## Verification

Per-commit:
- `mvn -pl phoenix-core-server -am compile -DskipTests` (must build clean).
- Targeted unit tests for each new collaborator extracted that commit (added in the same commit).

Pre-merge regression bar:
- `OnDuplicateKeyIT`, `OnDuplicateKey2IT` — atomic update parity (commit 7 gate).
- `IndexMaintenanceIT`, `BaseIndexIT` family, `BaseImmutableIndexIT`, `BaseIndexWithRegionMovesIT` — index correctness across paths.
- `IndexRegionObserverMutationBlockingIT` — mutation-blocking parity (commit 9 gate; `SyncReplicationGate` extraction).
- `ConcurrentMutationsExtendedIT`, `PartialIndexRebuilderIT` — concurrency / rebuild parity (commit 10 gate; this is where the `pendingRows` + waitForPrevious refactor gets stressed).
- `ConditionalTTLExpressionIT` — conditional TTL parity (commit 6 gate).
- The sync-replication ITs that arrive with the PHOENIX-7562 merge — must continue to pass post-refactor.
- `NonTxIndexBuilderTest` — existing unit test must still pass.
