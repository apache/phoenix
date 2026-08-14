# Standby CDC-index regeneration — Test A (plain CDC index)

**Status:** proposed (2026-06-24)
**Branch:** `eliminate-index-replication-v2` (PHOENIX-7931)
**Test home:** `ReplicationLogGroupIT` (new method)

## Goal

Prove the standby regenerates a **plain CDC index** correctly from the replicated data record +
per-(row,ts) PRE_IMAGE, with ZERO index replication records — same as global/local indexes.

"Plain" = `CREATE CDC <name> ON <table>` with **no** downstream EVENTUAL secondary index. With no
EVENTUAL index, `hasEventuallyConsistentIndexes()` is false, so the standby `IndexCDCConsumer`
stays dormant (exits at IRO consumer:413-417) — no background regeneration races. The test is
deterministic.

## What makes the CDC index different from global/local

The CDC index rowkey is `[32-byte PARTITION_ID][ROW_TIMESTAMP][data PK]`
(IndexMaintainer.java:852-860; `PARTITION_ID_LENGTH = 32`, PartitionIdFunction.java:38).
`PARTITION_ID()` = the **encoded data-table region name**, which differs between active and standby.
This is the central rationale for regenerating indexes on the standby at all: a replicated index
row would carry the active's partition_id (wrong on the standby). The standby fork
(`prepareReplicatedIndexMutations`, IRO:1486-1487) passes the standby's OWN `encodedRegionName`, so
the regenerated rowkey leads with the standby's partition_id.

Everything else is identical across clusters:
- CDC index is **STRONG** consistency (created `ASYNC`, no CONSISTENCY prop → default STRONG), so
  it is NOT skipped by the async-skip in `buildIndexTablesList` (IRO:2008-2013) and IS written
  inline on the standby replay path.
- It is **uncovered**, so the empty cell is written `UNVERIFIED` on the active (IRO:1928) and
  `UNVERIFIED` on the standby (IRO:1487). **There is no post phase for an uncovered index** to flip
  it to VERIFIED. Cell values are byte-identical across clusters.
- `SALT_BUCKETS=0` and `buildRowKey` skips the salt prefix for CDC indexes (IndexMaintainer.java:777
  `isIndexSalted = !isLocalIndex && !isCDCIndex && ...`), so partition_id is truly at byte offset 0.

Net: the ONLY cross-cluster physical difference in the entire CDC index table is the leading 32
bytes of each rowkey.

## Verification: raw scan + strip partition_id (decided 2026-06-24)

Because cell values are byte-identical and only the leading 32 rowkey bytes differ, the existing
byte-equal `assertTablesEqualAcrossClusters` can't be used directly (it compares rowkeys), but a
small variant is the **simplest and strongest** check:

`assertCDCIndexEqualAcrossClusters(physicalCdcIndexTableName)`:
- raw HBase `Scan().readAllVersions()` on both clusters' CDC index physical tables
- for each row pair, assert the rowkey **suffix after byte 32** is equal, and the leading 32 bytes
  are each non-empty (sanity: both wrote a real partition_id)
- compare all cells with row-key comparison disabled — i.e. reuse `Result.compareResults` semantics
  on the cell families/qualifiers/timestamps/values, OR rebuild each `Result` with the partition_id
  stripped from the rowkey before `Result.compareResults(r1, r2, true)`.

Rejected alternatives:
- **SQL event compare** (`SELECT /*+ CDC_INCLUDE(PRE,POST) */ ...`, decode JSON, compare
  event_type/pre_image/post_image): more test code AND a weaker physical guarantee (won't catch a
  wrong ROW_TIMESTAMP in the rowkey or a bad status byte). It proves the read path, not regeneration.
  Defer; the data-table read path is already covered elsewhere.

## Physical CDC index table name

CDC index logical name = `PHOENIX_CDC_INDEX_<cdcName>` (CDCUtil.CDC_INDEX_PREFIX). Resolve the
physical HBase table name the same way the test resolves other index physical names (via the
data table's PTable / index PTable physical name), not by string-building.

## Test steps

1. `CREATE TABLE <t> (pk ..., a ..., b ...)` on the active.
2. `CREATE CDC <cdc> ON <t>` on the active (auto-creates `PHOENIX_CDC_INDEX_<cdc>`).
3. Drive a mix on the active: inserts, updates of indexed/covered columns, deletes — across several
   `commit()`s so the active produces multiple batches (exercises the standby's batch-boundary
   recovery via per-(row,ts) groups).
4. Replay to cluster 2 (reuse the existing replay machinery; create the same DDL on cluster 2 first).
5. Verify the **data table** via the existing byte-equal `assertTablesEqualAcrossClusters`.
6. Verify the **CDC index** via the new strip-partition_id assert.

## Out of scope (Test B, deferred)

EVENTUAL downstream secondary index behind a CDC index, with a live standby consumer, under both
`serializeCDCMutations` values. Needs a convergence/await harness. Tracked separately.
