# Standby CDC-index regeneration — Test B (CDC index behind an EVENTUAL secondary index)

**Status:** proposed (2026-06-24)
**Branch:** `eliminate-index-replication-v2` (PHOENIX-7931)
**Test home:** `ReplicationLogGroupIT` (new method[s])

## Scope (user-directed)

First step: prove the **CDC index physical table** auto-created behind an eventually-consistent
secondary index has identical contents on active and standby (modulo the leading partition_id),
under **both** values of `phoenix.index.cdc.mutation.serialize`. The `IndexCDCConsumer` and the
downstream secondary-index convergence are **deferred** — get the CDC-index comparison passing
first.

## Mechanics (read from code, not assumed)

- `CREATE INDEX ... CONSISTENCY=EVENTUAL` triggers `createCDCForEventuallyConsistentIndex`
  (MetaDataClient.java:1925), which runs `CREATE CDC IF NOT EXISTS "CDC_<dataTableName>" ON <t>`
  (MetaDataClient.java:2473-2475). So the CDC index physical table is
  `PHOENIX_CDC_INDEX_CDC_<dataTableName>` = `CDCUtil.getCDCIndexName("CDC_" + tableName)`.
- The CDC index is STRONG/uncovered, written inline on the data path. On the standby it is
  regenerated via the existing per-(row,ts) pre-image path with the standby's own partition_id
  (same as Test A).
- `serializeCDCMutations=true`: `prepareEventuallyConsistentIndexMutations` (IRO:2120) serializes
  the downstream eventual-index Put/Delete mutations into the CDC index row as the
  `_IDX_PRE_`/`_IDX_POST_` payload columns (IRO:2076-2081). This runs on the replication path too,
  reading `context.indexUpdates`. The embedded secondary-index rowkeys carry NO partition_id, so the
  payload is identical across clusters.
- `serializeCDCMutations=false` (default): no payload column; the CDC index row is lightweight.
- `buildIndexTablesList` (IRO:2008-2013) includes async maintainers only when
  `serializeCDCMutations=true` — so the standby regenerates the eventual-index payload only in that
  mode. This is the precise behavioral difference under test.
- The eventual **secondary** index table is NOT written inline by IRO (async maintainers skipped at
  IRO:2058-2064); only the consumer populates it. With the consumer disabled it stays empty on both
  clusters, so the test compares only the CDC index table + the data table.

## Determinism

Disable the consumer on both clusters: `phoenix.index.cdc.consumer.enabled=false`
(`IndexRegionObserver.PHOENIX_INDEX_CDC_CONSUMER_ENABLED`). The consumer never mutates the CDC index
table itself (it reads CDC + writes the secondary index), so the CDC-index comparison is
deterministic either way, but disabling it removes background threads / CDC_STREAM-partition lookups
on the standby and matches the "consumer deferred" stance. Test A (plain CDC) stays green — its
consumer was already dormant.

## Verification

Reuse Test A's `assertCDCIndexEqualAcrossClusters` (strips the leading
`PartitionIdFunction.PARTITION_ID_LENGTH` bytes, compares the rowkey suffix + every cell). It
compares all cells generically, so it covers the `serialize=true` payload columns with no change.
Data table verified byte-equal via the existing `assertTablesEqualAcrossClusters`.

## The `serialize` knob is class-level

`serializeCDCMutations` is read once at IRO `start()` (IRO:694), so it is fixed per mini-cluster
(set in `@BeforeClass`). Plan:
1. Add the test using the class default (`serialize=false`); get it green first.
2. For `serialize=true`, add a thin subclass that flips
   `PHOENIX_INDEX_CDC_MUTATION_SERIALIZE=true` in its own `@BeforeClass` (mirrors the
   `MultiTenantEventualIndexGenerateIT extends MultiTenantEventualIndexIT` idiom) — OR, if rerunning
   the full suite under the subclass is too costly, factor the eventual-index test into a small
   dedicated IT pair. Decide after step 1 is green.

## Test steps (step 1, serialize=false)

1. `CREATE TABLE <t> (pk integer primary key, a varchar, b varchar)` on the active.
2. `CREATE INDEX <i> ON <t> (a) INCLUDE (b) CONSISTENCY=EVENTUAL` (auto-creates CDC_<t> + its CDC
   index).
3. Drive inserts/updates/deletes across several commits on the active.
4. Replay to cluster 2 (recreate the same DDL on cluster 2 first).
5. Verify the data table byte-equal; verify the CDC index modulo partition_id.

## Out of scope

IndexCDCConsumer behavior and downstream secondary-index convergence on the standby (both
`serialize` paths). Tracked for a later step.
