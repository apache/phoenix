# Replay Ordering Analysis: Standby-Side Index Generation Blocker

## Problem Statement

When replaying replication logs on the standby cluster, mutations for the same row can be applied out of order. This is safe for data tables (HBase cell versioning resolves to the correct value regardless of application order) but breaks standby-side index generation because IndexRegionObserver reads the current row state to generate index mutations.

## Root Cause

Within a single replication round, there are multiple shard files — one per RegionServer. If a region moves between RSes during the round (e.g., region balancing, RS crash), mutations for the same row can land in different shard files.

### How shards are assigned

`ReplicationShardDirectoryManager.getShardDirectory(long fileTimestamp)` assigns shards based on time:
```
shardIndex = (secondsSinceStartOfDay / replicationRoundDurationSeconds) % numShards
```

All RSes writing within the same time window write to the **same shard directory**, but each RS creates its own file within the shard (file name includes serverName).

### How files are processed

`ReplicationLogDiscovery.processNewFilesForRound()` calls `processOneRandomFile(files)` in a loop:
```java
while (!files.isEmpty() && isRunning()) {
    processOneRandomFile(files);
    files = replicationLogTracker.getNewFilesForRound(replicationRound);
}
```

`processOneRandomFile` picks a **random** file from the available files:
```java
Path file = files.get(ThreadLocalRandom.current().nextInt(files.size()));
```

Files within a round are processed **one at a time, in random order, sequentially**.

### Within a single file

Records are read sequentially from the file (write order preserved). Mutations are batched by table via `tableToMutationsMap` and applied via `AsyncTable.batchAll()`. Within a single `batchAll()` call, the HBase async client's `AsyncBatchRpcRetryingCaller.groupAndSend()` does async region location lookups which can reorder mutations for the same region.

## Concrete Failure Scenario

### Out-of-order partial update

1. M1 (ts=100): Full row insert `{PK=1, COL1=A, COL2=X}` — written to file A (RS1)
2. M2 (ts=200): Partial update `{PK=1, COL1=B}` — written to file B (RS2, after region move)
3. Index covers `(COL1) INCLUDE (COL2)`

**Correct order (M1 then M2):**
- M1 applied → row is `{COL1=A@100, COL2=X@100}`
- M2 applied → IRO reads current row, gets COL2=X → index entry: `(B, X)` at ts=200 ✓

**Out of order (M2 then M1):**
- M2 applied → IRO reads empty row → COL2=null → index entry: `(B, null)` at ts=200
- M1 applied → row is `{COL1=A@100, COL2=X@100}`, M2's COL1=B@200 wins
- Final data row: `{COL1=B@200, COL2=X@100}` ✓ (correct due to cell versioning)
- Index still has `(B, null)` at ts=200 ✗ **Permanent inconsistency**

### Reproducer test

`ReplicationLogProcessorTestIT.testOutOfOrderReplayProducesInconsistentIndex` on the `eliminate-index-replication` branch reproduces this:
- Creates table with index
- Generates M1 (full insert) and M2 (partial update) with explicit timestamps
- Writes them to separate log files
- Replays file2 first, then file1
- Verifies data table is correct but index table has COL2=null instead of COL2=X

## Why data tables are safe today

HBase stores each cell with its own timestamp. When M1 (ts=100) and M2 (ts=200) are applied in any order, the final visible row state is the same — HBase reads return the latest version per cell. Pre-computed index mutations from the primary also carry correct timestamps, so they produce the correct final index state regardless of application order.

## What was built on the `eliminate-index-replication` branch

The branch has a working implementation of standby-side index generation (all tests passing) but is blocked by this ordering issue:

- **LogFileCodec**: Serialize/deserialize mutation attributes (REPLICATION_ATTR_KEYS)
- **REPLICATED_MUTATION attribute**: Tells IRO to preserve original cell timestamps instead of overwriting with standby's current time
- **IRO changes**: Skip index mutation replication in `replicateMutations()`, `addGlobalIndexMutationsToWAL()`, `replicateEditOnWALRestore()`; use original batchTimestamp for replicated mutations in `getBatchTimestamp()`
- **WAL key**: Save replication attributes for crash recovery path
- **TestConnectionFactory fix**: Per-server connection cache key (real bug, also needed on main branch)
- **assertTablesEqualAcrossClusters**: Cell-level cross-cluster comparison using `Result.compareResults`
- **E2E test**: `ReplicationLogGroupIT.testIndexRegenerationOnStandby`

## Ordering guarantees in the current architecture

| Layer | Ordered? | Notes |
|-------|----------|-------|
| Replication log write (primary) | Yes | Row locks serialize same-row writes on the same RS |
| Log file read | Yes | Sequential file read |
| processLogFile() batching | Yes | ArrayList preserves insertion order |
| Cross-file within a round | **No** | `processOneRandomFile` picks randomly |
| `batchAll()` within a file | **Maybe not** | Async region location lookups can reorder |
| Cross-batch within a file | Yes | `processReplicationLogBatch` is synchronous |

## Requirements for safe standby-side index generation

For IRO on the standby to generate correct index mutations, mutations for the same row must be applied in timestamp order. This requires ordering guarantees at:

1. **Cross-file within a round**: All shard files in a round must be processed in an order that preserves per-row timestamp ordering
2. **Within batchAll()**: Mutations for the same region must be applied in list order

## Open questions for solution design

1. Can we merge all shard files in a round before replaying? What's the memory/performance impact?
2. Can we sort mutations by (row key, timestamp) before applying?
3. Can we process files in a deterministic order that preserves per-row ordering?
4. Is the region move scenario (same row in different shard files) actually common?
5. Could we use the commit ID or timestamp in the log records to establish ordering?
6. Could we apply mutations one at a time (not batched) to preserve order within batchAll()?
7. Is there a lighter-weight solution that doesn't require full ordering — e.g., idempotent index generation that doesn't depend on current row state?
