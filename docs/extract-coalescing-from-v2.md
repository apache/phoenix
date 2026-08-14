# Extract per-batch coalescing from v2 as a standalone PR

## Context

The `eliminate-index-replication-v2` branch contains two layered changes:

1. **Cell-oriented log format + per-batch coalescing of replication appends** — one record per (table, batch) carrying a flat cell stream, instead of one record per mutation. Cuts ring-buffer producer events by ~Nx and shrinks consumer drain work proportionally. This is the change that should drive the largest `phoenixWALSyncRingBufferTime` reduction predicted by the simulator (Model B at pc=10).
2. **Index-mutation elimination + standby pre-image plumbing** — stop replicating index mutations; standby IRO regenerates them from data mutations using shipped pre-images and replication attributes. Blocked on the out-of-order replay bug.

Goal: ship change (1) now while change (2) is being stabilized. This unblocks a perf run that measures coalescing's impact independently — a clean intermediate data point between the current pre-v2 baseline and full v2.

## Approach

The minimum viable extraction is essentially **just the cell-oriented log format commit (`7eded29ac8`)**, plus two follow-ups it requires to compile and to actually deliver coalescing:

1. **Resolve `7eded29ac8`'s one symbol-level dependency on the prior commit.** `LogFileRecord.setMutation` references `ReplicationLogGroup.REPLICATION_ATTR_KEYS`, which was added in `73bf621ca6` (index-elimination). Without index elimination, there are no batch-uniform replication attributes to filter — so drop that filter and copy the mutation's attribute map verbatim (or empty), restoring the pre-v2 behavior of carrying no attributes through the codec.

2. **Update the standby consumer to use the multi-mutation accessor.** The convenience `record.getMutation()` wrapper at `7eded29ac8` throws when a record has more than one mutation. The single in-tree caller that matters is `ReplicationLogProcessor.java:249`. Switch it to iterate `record.getMutations()`; the surrounding apply-loop is the only change needed. `LogFileAnalyzer` is a CLI tool — same fix.

3. **Add a single-record batch path on the producer.** New public overload `ReplicationLogGroup.append(String tableName, long commitId, List<Cell> cells)` (no attribute parameter — this PR does not ship batch-uniform attributes). Internally publishes a `Record` carrying the flat cell stream; the existing `append(table, commitId, Mutation)` path stays for callers that emit one mutation at a time. `IndexRegionObserver.replicateMutations` calls the new overload once per data table + once per index table.

### What to take from v2 (commit `7eded29ac8` only)

- `phoenix-core-server/.../replication/log/LogFile.java` — `Record` interface gains `getCells/setCells`, `getAttributes/setAttributes`, `getMutations` (plural). Legacy `getMutation`/`setMutation` retained as single-mutation convenience.
- `phoenix-core-server/.../replication/log/LogFileRecord.java` — backing fields switch to `List<Cell> cells` + `Map<String, byte[]> attributes`; `setMutation` keeps the existing single-mutation conversion. **Modification on cherry-pick:** drop the `REPLICATION_ATTR_KEYS` filter — copy the mutation's full attribute map (or none) instead.
- `phoenix-core-server/.../replication/log/LogFileCodec.java` — wire format mirrors HBase WALEdit (flat cells with full coordinates, plus a record-level attribute map).
- `phoenix-core-server/.../replication/MutationCellGrouper.java` (new) — `splitCellsIntoMutations(Iterable<Cell>)`. Self-contained; no dependency on `REPLICATION_ATTR_KEYS` or any v2-only symbol at this commit.
- `phoenix-core/src/test/.../replication/log/LogFileCodecTest.java` and `LogFileTestUtil.java` — codec round-trip assertions rewritten for cells + attributes.

### Local additions on top of `7eded29ac8`

- `IndexRegionObserver.replicateMutations`: rewrite the body so it builds one `List<Cell>` per target table (data table + each index table referenced by `preIndexUpdates`/`postIndexUpdates`) **inline at POST time, directly from `miniBatchOp` and the index-update maps already on `BatchMutateContext`** — no PRE-phase capture, no `BatchMutateContext` field for cells. Then emits one record per table. Keep:
  - the existing `ignoreReplicationFilter` per-mutation skip
  - the existing `getOperationsFromCoprocessors(i)` branch that splits coprocessor-merged mutations via the legacy mutation-level split
  - the existing `metricSource.updateReplicationSyncTime` timing block
- `ReplicationLogProcessor.java:249` consumer loop: switch `record.getMutation()` to `record.getMutations()` and iterate.
- `LogFileAnalyzer.java:142, 201`: same switch (CLI tool, low-risk).

### What NOT to take from v2

- `BatchMutateContext.replicationCellsByRow` and PRE-phase per-row capture (cell list is built inline in `replicateMutations`, no context field needed)
- `RowTsKey`, `prepareReplicatedIndexMutations`, `decodePreImage`, `applyDeleteToPut`, `capturePreImageCells`, `BatchMutateContext.isReplication`
- `REPLICATED_MUTATION`, `PRE_IMAGE`, `PRE_IMAGE_WAL_QUALIFIER` constants
- `ReplicationLogGroup.REPLICATION_ATTR_KEYS`
- `appendReplicationAttributesToWALKey` / WAL-key attribute round-trip
- `getHAGroupFromWALKey` signature change
- `replicateEditOnWALRestore` rewrite (keep current per-mutation behavior — single-mutation records still round-trip correctly through `getMutation()`)

### Files modified

Production:
- `phoenix-core-server/.../replication/log/LogFile.java`
- `phoenix-core-server/.../replication/log/LogFileRecord.java`
- `phoenix-core-server/.../replication/log/LogFileCodec.java`
- `phoenix-core-server/.../replication/MutationCellGrouper.java` (new)
- `phoenix-core-server/.../replication/reader/ReplicationLogProcessor.java` (consumer loop)
- `phoenix-core-server/.../replication/tool/LogFileAnalyzer.java` (CLI)
- `phoenix-core-server/.../hbase/index/IndexRegionObserver.java` (`replicateMutations` rewrite)
- `phoenix-core-server/.../replication/ReplicationLogGroup.java` (new `append(String, long, List<Cell>)` overload + internal `Record` shape that carries either a `Mutation` or a `List<Cell>`; ring-buffer publish path is shared)

Tests:
- `phoenix-core/src/test/.../replication/log/LogFileCodecTest.java` (rewritten — comes from cherry-pick)
- `phoenix-core/src/test/.../replication/log/LogFileTestUtil.java` (cell comparison helpers — comes from cherry-pick)
- `phoenix-core/src/test/.../replication/ReplicationLogGroupTest.java` — three callers of `record.getMutation()` for assertion in tests should keep working (each test produces single-mutation records). Add:
  - A new `testFramingMicrobenchmark` (opt-in like the simulator) that runs the per-mutation vs per-batch A/B and prints `appendNs` / `wireBytes` / `bytesPerCell` / `nsPerCell` for each mode.
  - Extend `testReplicationSyncPathSimulator` (or add a sibling `testReplicationSyncPathSimulatorPerBatch`) so the producer loop can publish cells via the new batch overload, gated by `-Dtest.recordFraming=permutation|perbatch`. Existing default behavior (per-mutation) preserved.
- `phoenix-core/src/test/.../replication/log/LogFileWriterTest.java` — keep as-is unless writer-side coalescing is exercised here.
- One new IT or test extending `ReplicationLogGroupIT` covering: a batch with mixed Put/Delete + coprocessor-merged cells produces a single record on the data table and one record per index table; the standby consumer reads all mutations from each.

## Verification

1. Build: `mvn package -DskipTests -pl phoenix-core-server,phoenix-core-client,phoenix-core`
2. Spotless: `mvn spotless:apply` before commit.
3. Unit tests: `mvn test -pl phoenix-core -Dtest='LogFileCodecTest,ReplicationLogGroupTest,LogFileWriterTest'`
4. Integration tests: `mvn verify -pl phoenix-core -Dit.test='ReplicationLogGroupIT'` plus the existing HABaseIT subset before push.
5. Simulator regression: `mvn test -pl phoenix-core -Dtest=ReplicationLogGroupTest#testReplicationSyncPathSimulator -Dtest.runSimulator=true` — confirm the decomposition equation still closes after framing change.
6. **Framing A/B microbenchmark** (new test, gated like the simulator). Goal: isolate the codec + producer-loop wins from consumer/HDFS noise, then plug the same workload through `testReplicationSyncPathSimulator` to see whole-path impact.
   - Build a workload generator: `N` mutations per batch (parameterized: `cellsPerMutation`, `mutationCount`, `valueSize`, `rowsPerBatch`).
   - Mode A (per-mutation): for each mutation, build a `LogFileRecord` via `setMutation`, encode via `LogFileCodec`, sum the resulting bytes; time the encode loop and the wire size.
   - Mode B (per-batch): build one `LogFileRecord` via `setCells` carrying the flat cell stream of the whole batch, encode once; time and wire size.
   - Reported metrics per mode: `appendNs` (sum of encode time), `recordCount`, `wireBytes`, `bytesPerCell`, `nsPerCell`. Print A/B ratios.
   - Then run `testReplicationSyncPathSimulator` with mode A vs mode B selectable via `-Dtest.recordFraming=permutation|perbatch` (the simulator's producer loop chooses which append API to call). Compare `phoenixWALSyncTime` p50/p99/max, `phoenixWALSyncRingBufferTime`, and `phoenixWALFsSyncTime` between modes. The expected signature: ringBufferTime drops sharply in mode B, fsSyncTime stays flat or shrinks (fewer/larger writes), syncTime tracks ringBufferTime + fsSyncTime.
   - This validates two separate things: (a) framing-level cost on the producer thread (step b vs step a above), and (b) the end-to-end sync path benefit at production-like contention (the simulator). The microbenchmark is fast feedback; the simulator is the ground truth.
7. Diff audit: confirm no `REPLICATED_MUTATION`, `PRE_IMAGE`, `RowTsKey`, `replicationCellsByRow`, `REPLICATION_ATTR_KEYS`, or `appendReplicationAttributesToWALKey` symbols leaked from v2.

## Decisions confirmed with user

- **Consumer update:** update both `ReplicationLogProcessor.java:249` and `LogFileAnalyzer.java` to iterate `record.getMutations()`.
- **Producer API:** add a new public overload `ReplicationLogGroup.append(String tableName, long commitId, List<Cell> cells)`.
- **Index-table coalescing:** coalesce per-index-table — one record per index table per batch.
