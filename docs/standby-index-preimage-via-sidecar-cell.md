# Cell-Oriented Replication Log Format (with Pre-Image as METAFAMILY Sidecar)

## Status (as of 2026-06-22 — shipped)

Branch `eliminate-index-replication-v2`, rebased onto `tkhurana/PHOENIX-7562-feature-new` at
`3faada180b` ("PHOENIX-7931 Coalesce per-batch replication appends into a single record"). PHOENIX-7931
upstreamed a refined version of the cell-oriented log format that subsumes Layer-A foundation work, so
the branch now carries only what is genuinely new on top of upstream. **Steps 1–9 plus 8a are
committed.** What remains is the integration-test scenario matrix and the open-question verification
(steps 10/11).

The branch is 9 commits on top of `3faada180b` (newest first):

```
eb42f45ea4 Standby: normalize INDEX_UUID to empty in the replication envelope
88175fafb6 Standby IRO: correct the rationale for skipping concurrent-batch wait
999862ea6f Standby IRO: throw when PRE_IMAGE is missing on indexed-table mutation
59d72d8506 Standby IRO: forked path with per-(row, ts) groups
4d880019b4 Standby pre-image: split capture, REPLICATED_MUTATION/PRE_IMAGE attrs
6860a091f4 Rebase fixups: ProtobufUtil import + REPLICATED_MUTATION refs
2cdd51d2c6 Standby pre-image plumbing: capture, append, envelope (checkpoint)
b53a2d68db Cell-oriented replication log format (codec + record + grouper)
45d12ad749 Eliminate index mutation replication (experimental)
```

Working tree clean; spotless clean; `MutationCellGrouperTest` (14 tests) passing; server module builds.
Local only — not yet force-pushed. Backup branch `eliminate-index-replication-v2-backup-2026-06-22`
(`9f7532c8837`) exists for recovery.

| Step | What | State |
|------|------|-------|
| 1 | `LogFileCodec` cell-oriented framing | committed (upstream `3faada180b` + `b53a2d68db`) |
| 2 | `LogFile.Record` cells/attrs + `MutationCellGrouper.splitCellsIntoMutations` | committed (`b53a2d68db`) |
| 3 | Per-batch `append(t, c, cells, attrs)` on `Writer`/`LogFileWriter`/`ReplicationLogGroup` | committed (`b53a2d68db`) |
| 4 | `BatchMutateContext` replication-cell capture hook | committed (`2cdd51d2c6`, reshaped by 8a in `4d880019b4`) |
| 5 | `ReplicationLogProcessor` consumes cells via `record.getMutations()` (peels pre-image cells) | committed (`b53a2d68db`/`2cdd51d2c6`) |
| 6 | `replicateEditOnWALRestore` forwards cells via batch `append`, no per-mutation filtering | committed (`4d880019b4`) |
| 7 | `capturePreImageCells` PRE-phase capture + WAL-edit injection | committed (`4d880019b4`) |
| 8 | POST `replicateMutations` rewritten to flatten + append | committed (`2cdd51d2c6`) |
| **8a** | Capture replication cells unconditionally; `LinkedHashMap<row, List<Cell>>` | committed (`4d880019b4`) |
| 8b | `MutationCellGrouperTest` — 14 tests | committed (`b53a2d68db`/`2cdd51d2c6`) |
| 9 | Standby IRO consumes pre-image — `prepareReplicatedIndexMutations` + per-`(row, ts)` groups | committed (`59d72d8506`/`999862ea6f`/`88175fafb6`) |
| — | Normalize `INDEX_UUID` to empty in the replication envelope (not in original plan) | committed (`eb42f45ea4`) |
| 10 | HA IT scenario matrix tests (13 scenarios incl. global+local must-have, out-of-order replay) | mostly done — single-scenario matrix complete except #11 (global+CDC, DEFERRED: blocked by a stock local-index + CDC-index Phoenix bug, see row #11); cross-cutting tests partial (see below) |
| 11 | Build, spotless, full unit-test pass | partial (server build + MutationCellGrouperTest green; full suite not run) |

**How the shipped design diverged from the earlier draft of this plan** (the draft is preserved below
in the "Superseded sketches" callouts):

1. **Two attributes, not one.** The earlier draft reused a single `REPLICATED_MUTATION` attribute to
   carry the pre-image bytes. The shipped code splits the contract:
   `IndexRegionObserver.REPLICATED_MUTATION = "_ReplicatedMutation"` (IRO:258) is a bare presence
   marker stamped on every reconstructed mutation; `IndexRegionObserver.PRE_IMAGE = "_PhoenixPreImage"`
   (IRO:265) carries the per-row PB-encoded pre-image bytes and is attached only when the active wrote
   a pre-image cell. The WAL/log cell qualifier is a separate byte-array constant
   `PRE_IMAGE_WAL_QUALIFIER = Bytes.toBytes("_PhoenixPreImage")` (IRO:271).
2. **Step-9 seam is `prepareReplicatedIndexMutations`, not `RowStateSource`/`ToLongFunction`.** No
   functional-interface strategy seam and no per-row-ts function threaded through the prepare methods.
   Instead a dedicated standby method groups the mini-batch by `RowTsKey(row, ts)` (IRO:362, 1377) and
   calls the existing `generateIndexMutationsForRow` per group with that group's ts. The fork is a
   one-line `if (context.isReplication)` inside `preparePreIndexMutations` (IRO:1881).
3. **`hasPreImage` shipped as `isReplication`** (IRO:474), computed once after
   `populateOriginalMutations` (IRO:2315). The `Preconditions.checkState` guard lives at the top of
   `preBatchMutateWithExceptions` (IRO:2319), gating on `isReplication`, not inside the index branch.
4. **`captureReplicationBatch` split into two methods**: `captureReplicationCells` (IRO:1485,
   unconditional, after `setTimestamps`) and `capturePreImageCells` (IRO:1532, inside the
   global/uncovered/transform branch). The draft called the second one `emitPreImageCells`.
5. **`peelSidecarsAndReconstruct` shipped as `MutationCellGrouper.reconstructMutations`**
   (MutationCellGrouper:104), and `LogFileRecord.getMutations()` calls it (LogFileRecord:93).
6. **`applyDeleteToPut(Delete, Put)`** (IRO:1441) — argument order is `(delete, put)`, opposite the
   draft's `(Put, Delete)`.
7. **`decodePreImage` throws** `DoNotRetryIOException` when `PRE_IMAGE` is absent on an indexed-table
   mutation (IRO:1424), and returns `null` for the zero-length sentinel.
8. **The per-mutation `append(t, c, Mutation)` overload was removed from `LogFile.Writer` and
   `LogFileWriter`** (only `append(...cells)` and `append(...cells, attrs)` remain — LogFile.java:379,
   392) — upstream PHOENIX-7931 already removed it. It is **kept** on `ReplicationLogGroup`
   (ReplicationLogGroup.java:579) for test callers. The old "Out of Scope" note about keeping it on the
   Writer is obsolete.
9. **`INDEX_UUID` normalization** (`eb42f45ea4`, not in the original plan):
   `MutationCellGrouper.extractReplicationAttributes` now always writes `INDEX_UUID` as
   `HConstants.EMPTY_BYTE_ARRAY` regardless of its value on the mutation (MutationCellGrouper:90). A
   non-empty UUID is an active-cluster server-cache key that would fail `INDEX_METADATA_NOT_FOUND` on
   the standby; the empty value forces the standby down the server-PTable resolution path, rebuilding
   maintainers from the `SCHEMA_NAME`/`LOGICAL_TABLE_NAME`/`TENANT_ID` attributes in the same envelope.

Cleanups applied while implementing (still accurate):
- `REPLICATED_MUTATION` lives on `IndexRegionObserver` (server-side; it's an IRO-owned standby contract),
  not on the client-side `BaseScannerRegionObserverConstants`.
- `REPLICATED_MUTATION` and `PRE_IMAGE` are NOT in `REPLICATION_ATTR_KEYS`
  (ReplicationLogGroup.java:199). The reader synthesizes both from the record body (the marker
  unconditionally, the pre-image bytes from the peeled METAFAMILY cell); primary code never sets them
  as record attributes.
- `MutationCellGrouper` owns `flattenCells`, `extractReplicationAttributes`, `splitCellsIntoMutations`,
  and `reconstructMutations`. The duplicate `splitCellsIntoMutations(Iterable<Cell>)` and
  `isNewRowOrType` previously in `IndexRegionObserver` were deleted; IRO calls into
  `MutationCellGrouper`.
- `LogFileTestUtil.cellsOf` deduplicated to delegate to `MutationCellGrouper.flattenCells`.
- The two `LogFileWriter.append` overloads deduplicated — the 3-arg overload delegates to the 4-arg
  with `Collections.emptyMap()` (LogFileWriter.java:84-87).
- `IndexRegionObserver.addGlobalIndexMutationsToWAL` deleted (index mutations are no longer written to
  the WAL for replication).

## Context

Eliminating index-mutation replication on the standby (the goal of `eliminate-index-replication-v2`)
requires that the standby cluster regenerate index mutations from replicated data mutations by
re-running `IndexRegionObserver`. That is blocked by a replay-ordering hazard (see
`docs/replay-ordering-analysis.md`): same-row mutations from one replication round can be re-applied out
of order on the standby, and IRO's `getCurrentRowStates` then reads a row that already reflects a later
mutation, producing wrong index entries for partial updates.

The fix is to ship the **primary-side pre-image** (the row state IRO captured under the row lock on the
primary) alongside each replicated mutation, so the standby can compute correct index updates
regardless of replay order.

An earlier plan tried to ship the pre-image as a per-mutation attribute plus a per-row WAL sidecar cell
— two carriers, two encodings. The deeper mismatch is that Phoenix's old replication log was
mutation-oriented while HBase's WALEdit is cell-oriented; upstream PHOENIX-7931 resolved that with a
cell-oriented codec + record.

This plan ships the pre-image as a single METAFAMILY sidecar cell that rides through both the
replication log and the WALEdit uniformly, and the standby reader translates that cell into two
mutation attributes: a `REPLICATED_MUTATION` marker and the `PRE_IMAGE` bytes.

## Sidecar cell shape — design choice

After exploring three encodings (`IndexedKeyValue`, packed-qualifier per-cell, plain PB-blob) we
**locked in**:

- **One METAFAMILY cell per replicated row.**
- `family = WALEdit.METAFAMILY`
- `qualifier = "_PhoenixPreImage"` (constant `IndexRegionObserver.PRE_IMAGE_WAL_QUALIFIER`)
- `timestamp = HConstants.LATEST_TIMESTAMP`
- `type = Put`
- `value = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, preImage).toByteArray()` — or
  `HConstants.EMPTY_BYTE_ARRAY` when the primary observed an empty row at lock time (positive "no
  pre-image" sentinel; distinguishes from "no information shipped").

On the standby reader, this cell is peeled out of the body and its value bytes are attached to the
reconstructed mutation as the `PRE_IMAGE` attribute; every reconstructed mutation also gets the bare
`REPLICATED_MUTATION` marker.

Rationale (the alternatives we rejected):

- **`IndexedKeyValue`** (PB-blob via `KeyValueCodec.write` `-1` length marker). Works on the WAL path
  because `IndexedWALEditCodec` recognizes it. **Does not work on the replication-log path** because
  our `LogFileCodec` has no `KeyValueCodec` hook; it would serialize the placeholder `value` bytes and
  silently lose the PB Mutation. Adding a hook re-introduces the single-cell-per-row size cap concern
  uniformly.
- **Packed-qualifier per-pre-image-cell** (qualifier = vint(famLen) + famBytes + qualBytes, value =
  original cell value). Avoids the per-row-size-cap concern. Rejected because: (a) pre-images don't
  traverse `RSRpcServices.checkCellSizeLimit` — the standby reader strips them out before calling
  `AsyncTable.batchAll`, and `checkCellSizeLimit` doesn't inspect attributes; (b) the replication-log
  path doesn't traverse `checkCellSizeLimit` at all; (c) per-cell sidecars require row-keyed
  reassembly on the reader (`Map<row, Put>` → PB → attribute round-trip) versus a one-step value-byte →
  attribute copy for the PB-blob shape.

Cell-size-limit accounting:
- `RSRpcServices.checkCellSizeLimit` only inspects `m.cellScanner()` cells of an inbound RPC mutation.
  Mutation attributes are not checked.
- Pre-image sidecar lives in the file/WALEdit cell stream, never in a mutation cell stream that reaches
  `region.batchMutate`. Reader peels the sidecar into the `PRE_IMAGE` attribute before `batchAll`.
- `WALEdit` METAFAMILY cells are filtered out of HBase cross-cluster replication by
  `ScopeWALEntryFilter.filterCell` (user table CF descriptors don't define METAFAMILY →
  `hasGlobalScope` returns false → cell dropped) — same mechanism that protected `IndexedKeyValue`
  historically.

## Approach

A log record is a **batch**, not a mutation. Each record has:

- **Envelope** — `tableName`, `commitId`, and the batch-uniform `REPLICATION_ATTR_KEYS` attributes
  (`INDEX_UUID`, `SCHEMA_NAME`, `LOGICAL_TABLE_NAME`, `TENANT_ID`). `INDEX_UUID` is always normalized to
  empty (see status note #9).
- **Body** — a single ordered list of cells with full per-cell coordinates. Includes:
  - **Data cells** of the batch's mutations (post-CP-merge).
  - **Pre-image sidecar cells** — one METAFAMILY/`_PhoenixPreImage` cell per row that has a pre-image
    entry on the primary.

The standby reader (`MutationCellGrouper.reconstructMutations`, called from
`LogFileRecord.getMutations()`):

1. Reads the envelope.
2. Walks the body once, partitioning METAFAMILY/`_PhoenixPreImage` cells into a `Map<row, byte[]>`
   pre-image bucket. Remaining cells go into the data-cell stream.
3. Reconstructs Put/Delete mutations from the data-cell stream via `splitCellsIntoMutations`.
4. Per mutation: applies envelope attrs, stamps the bare `REPLICATED_MUTATION` marker
   (`EMPTY_BYTE_ARRAY`), and — when a pre-image entry exists for the row — attaches the pre-image bytes
   as the `PRE_IMAGE` attribute (the bytes are the sidecar value, which may itself be `EMPTY_BYTE_ARRAY`
   for the "primary saw empty row" sentinel).
5. Hands the mutations to `AsyncTable.batchAll` — sidecars never enter the cell stream that goes over
   the wire.

Because the body is a flat cell stream, the **WAL-restore path** (`replicateEditOnWALRestore`) is
symmetric: same partition, same reconstruction, same attribute attach.

## Implementation (shipped)

### 1, 2. Log file format and record contract

`LogFileCodec` writes/reads `tableName + commitId + attrs + cells`. `LogFile.Record` exposes
`getCells/setCells/getAttributes/setAttributes/getMutations`. `LogFileRecord.getMutations()` calls
`MutationCellGrouper.reconstructMutations(cells, attributes)` (LogFileRecord:93). `getMutation()`
remains only as a guard that throws when a record holds more than one mutation, steering callers to
`getMutations()` (LogFileRecord:99-102). Upstream PHOENIX-7931 (`3faada180b`) landed the codec/record;
`b53a2d68db` grafted the grouper helpers.

### 3. Per-batch append API

`LogFile.Writer` (LogFile.java:379, 392) exposes only the cell-oriented overloads:

```java
boolean append(String tableName, long commitId, List<Cell> cells) throws IOException;
boolean append(String tableName, long commitId, List<Cell> cells,
               Map<String, byte[]> attributes) throws IOException;
```

`LogFileWriter` implements both; the 3-arg form delegates to the 4-arg with `Collections.emptyMap()`
(LogFileWriter.java:84-97). `ReplicationLogGroup` keeps a per-mutation `append(t, c, Mutation)`
(ReplicationLogGroup.java:579) for test callers, plus the cells+attrs `append`
(ReplicationLogGroup.java:600) used by the production write path; both publish one `Record` event onto
the Disruptor ring via the private `publish(Record)` (ReplicationLogGroup.java:609).

### 4, 7, 8a. PRE-phase capture in IRO

Constants (IRO:258, 265, 271):

```java
public static final String REPLICATED_MUTATION = "_ReplicatedMutation";
public static final String PRE_IMAGE = "_PhoenixPreImage";
public static final byte[] PRE_IMAGE_WAL_QUALIFIER = Bytes.toBytes("_PhoenixPreImage");
```

`BatchMutateContext` state (IRO:474, 484):

```java
private boolean isReplication;  // batch came from the standby reader (marker on first mutation)
private LinkedHashMap<ImmutableBytesPtr, List<Cell>> replicationCellsByRow;  // per-row cell buckets
```

Envelope attrs are NOT stored on context — they are recomputed from `originalMutations.get(0)` in
`replicateMutations`.

`isReplication` is computed once at the top of `preBatchMutateWithExceptions` after
`populateOriginalMutations` (IRO:2315):

```java
context.isReplication = !context.getOriginalMutations().isEmpty()
    && context.getOriginalMutations().get(0).getAttribute(REPLICATED_MUTATION) != null;
Preconditions.checkState(
    !context.isReplication
        || (!context.hasAtomic && !context.returnResult && !context.hasConditionalTTL),
    "replicated batch must not carry active-side resolution flags");
```

**`captureReplicationCells`** (IRO:1485) is called **unconditionally** from
`preBatchMutateWithExceptions` after `setTimestamps`, guarded only by `!context.isReplication`
(IRO:2391-2402). For each non-ignored mutation it appends the mutation's family cells (and any
CP-injected cells visible at this call site) to that row's bucket in `context.replicationCellsByRow`.
The map's key set is the set of replicated rows — a row whose only mutation was filtered by
`ignoreReplicationFilter` never enters the map.

**`capturePreImageCells`** (IRO:1532) runs inside the `hasGlobalIndex || hasUncoveredIndex ||
hasTransform` branch, immediately after `prepareDataRowStates` populates `dataRowStates` (IRO:2413).
For each row with a `dataRowStates` entry it builds one METAFAMILY pre-image cell
(`ProtobufUtil.toMutation(PUT, preImage).toByteArray()`, or `EMPTY_BYTE_ARRAY` when the primary saw an
empty row) and appends it to BOTH the row's bucket and `miniBatchOp.getWalEdit(0)` so the WAL-restore
path ships the same payload.

> **Superseded sketch:** the earlier draft used a single `captureReplicationBatch` driven by a separate
> `replicatedRows` set and stored a flat `List<Cell> replicationCells`. 8a replaced that with the
> two-method split and the row-keyed `LinkedHashMap`; the map's key set is the replicated-row set, so
> no separate field is needed.

Key decisions (still accurate):
- **Capture is unconditional** so local-index-only, atomic-only, conditional-TTL-only, and pure-data
  tables also ship a replication record. Pre-image cells are an *additional* payload added only when
  `prepareDataRowStates` runs — not a gate on whether the record exists.
- **Empty-value sentinel** for primary-observed-empty-row. The reader treats the zero-length sidecar
  value as "primary saw nothing here"; no cell at all means "row not visited; no information shipped."
- **Single pre-image cell per row.** Driven by iterating `replicationCellsByRow.entrySet()`.
- **Placement after `setTimestamps`, before the index branches**, so captured cells carry final
  timestamps and reflect post-on-dup / post-conditional-TTL resolution; before `handleLocalIndexUpdates`
  (which runs via `addOperationsFromCP(0, …)`) so local-index updates stay out of the replication log.

### 8. POST-phase `replicateMutations` (IRO:3166)

```java
private void replicateMutations(RegionCoprocessorEnvironment env,
                                MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                BatchMutateContext context) throws IOException {
  if (!shouldReplicate || ignoreSyncReplicationForTesting) return;
  if (!context.logGroup.isPresent()) return;
  if (context.replicationCellsByRow == null || context.replicationCellsByRow.isEmpty()) return;
  if (context.getOriginalMutations().isEmpty()) return;
  List<Cell> flattened = new ArrayList<>();
  for (List<Cell> bucket : context.replicationCellsByRow.values()) flattened.addAll(bucket);
  Map<String, byte[]> replicationAttributes =
      MutationCellGrouper.extractReplicationAttributes(context.getOriginalMutations().get(0));
  ReplicationLogGroup logGroup = context.logGroup.get();
  logGroup.append(dataTableName, -1, flattened, replicationAttributes);
  logGroup.sync();
}
```

`extractReplicationAttributes` is the single source of truth for the envelope contract (and applies the
`INDEX_UUID`→empty normalization).

### 5. Standby reader — `MutationCellGrouper.reconstructMutations` (MutationCellGrouper:104)

```java
public static List<Mutation> reconstructMutations(Iterable<Cell> cells,
    Map<String, byte[]> replicationAttrs) throws IOException {
  Map<ImmutableBytesPtr, byte[]> preImages = new HashMap<>();
  List<Cell> dataCells = new ArrayList<>();
  for (Cell c : cells) {
    if (CellUtil.matchingFamily(c, WALEdit.METAFAMILY)
        && CellUtil.matchingQualifier(c, IndexRegionObserver.PRE_IMAGE_WAL_QUALIFIER)) {
      preImages.put(new ImmutableBytesPtr(CellUtil.cloneRow(c)), CellUtil.cloneValue(c));
    } else {
      dataCells.add(c);
    }
  }
  List<Mutation> mutations = splitCellsIntoMutations(dataCells);
  for (Mutation m : mutations) {
    if (replicationAttrs != null) {
      for (Map.Entry<String, byte[]> e : replicationAttrs.entrySet()) {
        m.setAttribute(e.getKey(), e.getValue());
      }
    }
    m.setAttribute(IndexRegionObserver.REPLICATED_MUTATION, HConstants.EMPTY_BYTE_ARRAY);
    byte[] preImageBytes = preImages.get(new ImmutableBytesPtr(m.getRow()));
    if (preImageBytes != null) {
      m.setAttribute(IndexRegionObserver.PRE_IMAGE, preImageBytes);
    }
  }
  return mutations;
}
```

`ReplicationLogProcessor.processLogFile` iterates `record.getMutations()` (ReplicationLogProcessor:254),
which routes through the above.

> **Superseded sketch:** the earlier draft named this `peelSidecarsAndReconstruct` and set the
> pre-image bytes onto `REPLICATED_MUTATION` directly. The shipped split — bare `REPLICATED_MUTATION`
> marker plus separate `PRE_IMAGE` bytes — lets the standby distinguish "this is a replicated batch"
> (marker, always present) from "the active shipped a pre-image for this row" (`PRE_IMAGE`, present only
> on indexed-table rows). That distinction is what `decodePreImage` keys on to throw when an
> indexed-table mutation is missing its pre-image.

### 6. WAL-restore path — `replicateEditOnWALRestore`

Forwards `logEdit.getCells()` as a single batch via the cells+attrs `append`. The WALEdit cells already
contain both data cells and METAFAMILY pre-image cells (added by `capturePreImageCells` at PRE phase),
so there is no per-mutation filtering and no per-mutation `copyWALKeyAttributesToMutation` — the
envelope is rebuilt from the WAL-key attributes the same way the synchronous path rebuilds it from the
first mutation.

### 9. Standby IRO consumes pre-image — `prepareReplicatedIndexMutations` (IRO:1377)

> **Superseded sketch:** the earlier draft proposed a `RowStateSource` functional-interface seam plus a
> `ToLongFunction<ImmutableBytesPtr>` per-row timestamp threaded through `prepareIndexMutations`,
> `preparePreIndexMutations`, `preparePostIndexMutations`, and
> `prepareEventuallyConsistentIndexMutations`. **None of that shipped.** The prepare methods keep their
> `long batchTimestamp` signatures. Instead, a dedicated standby method owns the whole standby row-state
> + per-row-ts derivation, and the only orchestrator change is a one-line fork.

The fork lives inside `preparePreIndexMutations` (IRO:1881):

```java
if (context.isReplication) {
  prepareReplicatedIndexMutations(miniBatchOp, context, maintainers);
} else {
  prepareIndexMutations(context, maintainers, batchTimestamp);
}
```

`prepareReplicatedIndexMutations` (IRO:1377):

1. Groups the mini-batch's index-enabled mutations by `RowTsKey(row, IndexUtil.getMaxTimestamp(m))`
   (IRO:362 defines `RowTsKey`). Different `(row, ts)` groups for the same row are kept distinct —
   that's how the standby recovers the active-batch boundary the reader's coalescing can erase.
2. Per group: `decodePreImage(groupMutations.get(0))` (all mutations in a group share one pre-image
   because the active wrote one pre-image cell per row per batch). Builds `nextState` by applying the
   group's cells on top of the pre-image (`applyNew` for Puts, `applyDeleteToPut` for Deletes).
3. Calls the existing `generateIndexMutationsForRow(rowKeyPtr, preImage, nextState, ts,
   encodedRegionName, UNVERIFIED_BYTES, indexTables, idxUpdates)` so resulting index cells carry the
   group's ts, then merges into `context.indexUpdates`.

Skips `getCurrentRowStates` entirely (unsafe under out-of-order replay).

`decodePreImage` (IRO:1424): returns `null` for the zero-length sentinel; throws
`DoNotRetryIOException` when `PRE_IMAGE` is absent on an indexed-table mutation (contract violation —
the active always writes a pre-image cell when an index is present).

`applyDeleteToPut(Delete delete, Put put)` (IRO:1441): applies a Delete's cells to a Put, returning the
Put or `null` if the row goes empty. Note the arg order `(delete, put)`.

Other standby short-circuits, all keyed on `context.isReplication`:
- `setTimestamps` / `getBatchTimestamp` are skipped (IRO:2391) — replicated cells already carry final
  per-cell timestamps; re-stamping would clobber the per-row ts that index entries must align with.
- `waitForPreviousConcurrentBatch` is skipped (IRO:2431) — each replicated batch is self-sufficient via
  its `PRE_IMAGE`, the standby never reads from the data table, and every index cell carries the
  active's timestamp, so interleaved PRE/POST writes resolve to the same final index state as the
  active via HBase cell versioning. (Rationale corrected in `88175fafb6`.)
- The `if (hasLocalIndex)` block (IRO:2436) is unchanged: the standby regenerates the local index from
  the replicated data cells via `handleLocalIndexUpdates`. No double-write, because local-index updates
  are added after the global-index block and so were never in the captured cell stream that shipped.

## Key Files

| File | Step | State |
|------|------|-------|
| `phoenix-core-server/.../replication/log/LogFileCodec.java` | 1 | committed (upstream + graft) |
| `phoenix-core-server/.../replication/log/LogFileRecord.java` | 2, 5 | committed — `getMutations()` → `reconstructMutations` |
| `phoenix-core-server/.../replication/log/LogFile.java` | 2, 3 | committed — Writer is cells / cells+attrs only |
| `phoenix-core-server/.../replication/log/LogFileWriter.java` | 3 | committed — 3-arg delegates to 4-arg |
| `phoenix-core-server/.../replication/MutationCellGrouper.java` | 2, 3, 5 | committed — flatten/extract/split/reconstruct; INDEX_UUID normalize |
| `phoenix-core-server/.../replication/ReplicationLogGroup.java` | 3 | committed — cells+attrs append; per-mutation append kept for tests |
| `phoenix-core-server/.../hbase/index/IndexRegionObserver.java` | 4, 7, 8, 8a, 9 | committed |
| `phoenix-core-server/.../replication/reader/ReplicationLogProcessor.java` | 5 | committed — uses `record.getMutations()` |
| `phoenix-core/.../replication/MutationCellGrouperTest.java` | 8b | committed — 14 tests |

## Verification

### Build / format

1. `mvn package -pl phoenix-core-server -am -DskipTests` — passes.
2. `mvn spotless:apply` before any commit — clean.
3. Full unit-test suite (`mvn test -pl phoenix-core`) — **not yet run end-to-end** on the rebased
   branch; `MutationCellGrouperTest` passes in isolation.

### Unit tests (status)

- `MutationCellGrouperTest` (14 tests) covers `splitCellsIntoMutations` + `reconstructMutations`
  round-trips, pre-image peeling, `REPLICATED_MUTATION` stamping, `PRE_IMAGE` attach, replication-attr
  filtering (including the `INDEX_UUID`→empty normalization), and NULL-upsert mixed Put+Delete cells.
  **Done.**
- `IndexRegionObserverTest` unit test for `(row, ts)` grouping in `prepareReplicatedIndexMutations`
  (mocked region env) — **TODO** (cross-cutting test #17 below).

### HA IT scenario matrix (BatchMutateContext flag combinations) — TODO (step 10)

**Replication model (shipped):**

- Every Phoenix-write batch ships through Phoenix replication and lands in the standby's
  `preBatchMutate` → standby IRO.
- The standby reader stamps every reconstructed mutation with `REPLICATED_MUTATION` (generic marker)
  and, when the active wrote a pre-image cell, also with `PRE_IMAGE` (per-row PB-encoded primary-side
  Put bytes; zero-length sentinel = "active observed empty row").
- The standby IRO sets `context.isReplication = (firstMutation.getAttribute(REPLICATED_MUTATION) !=
  null)` and asserts (via `Preconditions.checkState`) that replicated batches do NOT carry active-side
  resolution flags (`hasAtomic`, `returnResult`, `hasConditionalTTL`).
- On the indexed-table branch (`hasGlobalIndex || hasUncoveredIndex || hasTransform`),
  `isReplication == true` routes into `prepareReplicatedIndexMutations`. That helper groups the
  mini-batch's mutations by `(row, ts)`, decodes `PRE_IMAGE` per group, applies the group's cells to
  derive `nextDataRowState`, and calls `generateIndexMutationsForRow` per group. Multiple groups per
  data row are handled — they recover the active-batch boundary the reader's coalescing can erase.
- `decodePreImage` throws `DoNotRetryIOException` when `PRE_IMAGE` is missing on a replicated
  indexed-table mutation (contract violation: the active always emits one pre-image cell per row when an
  index is present).
- Tables without an index ship through replication; on the standby they don't enter the indexed-table
  branch, and `populateRowsToLock` produces an empty set (because `INDEX_UUID` is absent →
  `builder.isEnabled` is false → no rows added). Standby IRO early-returns; HBase's normal write path
  applies the data cells.
- `serializeCDCMutations == true` is supported on the standby. `cdcPreMutationsBytes` /
  `cdcPostMutationsBytes` are keyed by `RowTsKey(row, ts)` so per-(row, ts) groups don't collide;
  consumers in `preparePreIndexMutations` / `preparePostIndexMutations` derive the lookup ts from the
  index Mutation's own cells via `IndexUtil.getMaxTimestamp(m)`.

Each scenario below should ship through the active cluster's IRO, replicate to the standby, and assert
standby state matches active state.

| # | Active flags | Pre-image attached? | Standby path | Assertions |
|---|---|---|---|---|
| 1 | `hasGlobalIndex` only | **Yes** | `isReplication == true` → `prepareReplicatedIndexMutations` | (a) standby index rows match active; (b) per-row ts preserved verbatim from active; (c) no `getCurrentRowStates` scan on standby; (d) **out-of-order replay** regression — ship batch B before batch A for the same row, verify final standby index matches active. |
| 2 | `hasUncoveredIndex` only | **Yes** | Same as (1) | Same as (1) plus: uncovered index marks UNVERIFIED in PRE and is **never marked VERIFIED in POST** — `preparePostIndexMutations` / `prepareEventuallyConsistentIndexMutations` skip the verified Put when `indexMaintainer.isUncovered()` (IRO:2248, IRO:2152). The reader resolves correctness by joining unverified index rows back to the data table. Assert: PRE writes UNVERIFIED, POST writes no verified Put, and a query through the uncovered index returns correct results on the standby. Note: the standby never reads current data rows regardless (pre-image supplies prior state); on the active, `getCurrentRowStates` is gated on `isPartialUncoveredIndexMutation` (IRO:2517). |
| 3 | `hasLocalIndex` only | **No** (local-index-only path doesn't run `prepareDataRowStates`) | `isReplication == true`, indexed-table branch is NOT entered. Standby's `if (hasLocalIndex)` block runs with the replicated mutations. | (a) replication log record exists (data cells only, no pre-image cells); (b) standby regenerates local index from data cells via `handleLocalIndexUpdates`; (c) `decodePreImage` is never called on this path. |
| 4 ✅ | **`hasGlobalIndex` + `hasLocalIndex` (must-have test)** | **Yes** (data + pre-image cells; local-index updates NOT captured because `handleLocalIndexUpdates` runs AFTER `capturePreImageCells`) | `prepareReplicatedIndexMutations` for global; standby regenerates local in its own `if (hasLocalIndex)` branch | **DONE** — `ReplicationLogGroupIT#testSingleBatchRecordCount` (commit `d9408dc09d`): (a) standby global index matches active; (b) standby local index regenerated (its L#0 cells live in the data table, verified by cross-cluster data equality); (c) captured cell stream explicitly asserted to contain no `L#` cells; (d) no double-write (data equality would fail otherwise). Also covered by `testAppendAndSync`. |
| 5 | `hasAtomic` only (no index) | No | `isReplication == true`, no indexed-table branch. Active resolves the on-dup before capture, so resolved cells flow through. | (a) replication log ships post-on-dup resolved cells, no `ATOMIC_OP_ATTRIB`; (b) standby's IRO does NOT re-resolve; (c) `Preconditions.checkState` does not fire. |
| 6 ✅ | `hasAtomic` + `hasGlobalIndex` | **Yes** | Same as (1), with on-dup-resolution cells captured into the same `(row, ts)` group | **DONE** — `ReplicationLogGroupIT#testOnDuplicateKeyUpdateWithIndex` (commit `b2e9f4f69f`): on-dup updates + NULL update on an indexed column; cross-cluster cell equality on data and index confirms the standby regenerates consistently and does NOT re-resolve (no `ATOMIC_OP_ATTRIB` reaches the standby, so `hasAtomic` stays false and `checkState` does not fire — resolves open-Q #1). |
| 7 | `hasConditionalTTL`, no index | No | `isReplication == true`, no indexed-table branch | (a) replication log ships post-TTL-evaluation cells; (b) `Preconditions.checkState` does not fire; (c) standby IRO does NOT re-evaluate TTL. |
| 8 ✅ | `hasConditionalTTL` + `hasGlobalIndex` | **Yes** | `prepareReplicatedIndexMutations` | **DONE** — `ReplicationLogGroupIT#testConditionalTTLWithIndex` (commit `d58e7f7d40`): global index on a conditional-TTL table (index covers `expired` since the TTL expr references it), updates the indexed column on expired rows; cross-cluster cell equality on data and index confirms the standby applies post-TTL cells without re-evaluating (no `TTL` attribute reaches the standby, so `hasConditionalTTL` stays false and `checkState` does not fire — resolves open-Q #2). |
| 9 ✅ | **CDC index (async), `serializeCDCMutations=false`** | **Yes** | `prepareReplicatedIndexMutations` runs; CDC entries emitted by `generateIndexMutationsForRow`. `prepareEventuallyConsistentIndexMutations` NOT called (gated by `serializeCDCMutations`). | **DONE** — plain CDC index: `ReplicationLogGroupIT#testCDCIndex` (commit `e59f9e63c7`); CDC index behind an EVENTUAL secondary index: `ReplicationLogGroupEventualIndexIT#testEventualIndexCDCTable` (commit `4e749816b9`). Both run under the class-default `serializeCDCMutations=false` and assert standby CDC entries match active modulo partition_id. |
| 10 ✅ | **CDC index, `serializeCDCMutations=true`** | **Yes** | `prepareReplicatedIndexMutations` populates `indexUpdates`; `prepareEventuallyConsistentIndexMutations` writes `cdcPre/PostMutationsBytes` keyed by `RowTsKey(row, ts)`; consumers look up by `(row, IndexUtil.getMaxTimestamp(m))`. | **DONE** — `ReplicationLogGroupEventualIndexWithSerializeCDCIT` (commit `9b78eb9091`) inherits the eventual-index test and flips `serializeCDCMutations=true`; `assertCDCIndexPayloadMatchesConfig` asserts the serialized `_IDX_PRE_`/`_IDX_POST_` payload is present and matches across clusters. |
| 11 ⏸️ | `hasGlobalIndex` + CDC index | **Yes** | Paths from (9)/(10) plus global-index regeneration | Both index types regenerate correctly; per-(row, ts) ts honored across both. **DEFERRED** — blocked by a pre-existing, replication-independent Phoenix bug. Tried adding a CDC index to `testAppendAndSync` (which also has a local index); it failed on the **active** cluster's first commit with `IllegalArgumentException: Encoded region name is required for a CDC index` (`IndexMaintainer.buildRowKey:772`). Cause: a table with a local index runs the legacy `handleLocalIndexUpdates` → `NonTxIndexBuilder` → `PhoenixIndexCodec.getIndexUpserts` path, which iterates **all** maintainers (no local-only filter) and calls the `buildUpdateMutation` overload that omits `encodedRegionName`. For a CDC maintainer that overload throws; for every other maintainer type the redundant build is silently discarded by `handleLocalIndexUpdates`'s `removeAll`, so the bug stayed latent. This is stock client code (`PhoenixIndexCodec`/`IndexMaintainer`, untouched by this branch) — local-index + CDC-index on the same table is broken on a plain single cluster, independent of replication. To cover #11 we'd need either a Phoenix fix (skip non-local maintainers on the local path) or a global+CDC table with NO local index. |
| 12 | `returnResult` (single-row UPSERT with RETURN ROW) | Depends on whether also has global index | If global index present: `prepareReplicatedIndexMutations`. Active strips `returnResult` before capture, so `Preconditions.checkState` doesn't fire. | Active returns correct row; standby applies cells without returning anything. |
| 13 | Pure data row (no index, no atomic, no TTL, no CDC) | No | `isReplication == true`; `populateRowsToLock` returns empty; IRO early-returns. | Standby applies data cells via HBase's normal write path; standby IRO is a no-op for indexes. |

### Cross-cutting tests — TODO

7. **Out-of-order replay** (the original blocker): for any scenario with a pre-image attached ({1, 2,
   4, 6, 8, 9, 10, 11}), ship two batches that touch the same row in reverse order; assert final standby
   state matches active.
8. **Reader coalescing of same-row batches**: two `LogFile.Record`s for the same row are coalesced by
   `ReplicationLogProcessor.processLogFile` into one `batchAll` call, landing as one mini-batch on the
   standby IRO. Verify `prepareReplicatedIndexMutations` groups by `(row, ts)` and produces two distinct
   index-update sets — one per active-side batch's pre-image. Test in {1, 6, 8, 9, 10, 11}.
9. **Empty pre-image sentinel**: row had no entry on active at lock time → pre-image cell value is
   `EMPTY_BYTE_ARRAY` → `decodePreImage` returns `null` → `prepareReplicatedIndexMutations` derives
   `nextState` from the mutation alone. Test in {1, 4, 9, 10}.
10. **Missing-PRE_IMAGE contract violation**: synthesize a replicated mutation with `REPLICATED_MUTATION`
    set but no `PRE_IMAGE`, route through the standby's indexed-table branch. Expect
    `DoNotRetryIOException` mentioning `_PhoenixPreImage`. (Negative test for `decodePreImage`.)
11. **No pre-image at all**: scenarios {3, 5, 7, 13} where the active didn't write a pre-image cell —
    confirm `populateRowsToLock` empties for pure-data tables and local-index/atomic/TTL paths don't
    enter `prepareReplicatedIndexMutations`.
12. ✅ **`ignoreReplicationFilter`** — **DONE**: covered by `ReplicationLogGroupIT#testAppendAndSync`.
    The test issues `UPSERT ... ON DUPLICATE KEY IGNORE` against rows that already exist (IT:206-217),
    which is the real production trigger: `addOnDupMutationsToBatch` generates an empty mutation list
    for the already-present row and stamps `IGNORE_REPLICATION_ATTRIB` (IndexRegionObserver.java:1026),
    feeding the `IGNORE_REPLICATION` predicate (IRO:605) → `ignoreReplicationFilter` (IRO:640).
    `assertEquals(0, executeUpdate())` confirms the ignore fired; cross-cluster cell equality after
    replay confirms nothing erroneous replicated. (No targeted "ignored row's cells/pre-image absent
    from the log while siblings present" assertion — the equality covers it indirectly; tighten only
    if desired.)
13. **WAL-restore path equivalence**: after a primary RS crash and WAL replay,
    `replicateEditOnWALRestore` ships the same payload as the synchronous replicate. Crash-recovered
    batches produce identical standby state, including `(row, ts)` grouping.
14. **`ReplicationLogGroupIT.testIndexRegenerationOnStandby`** continues to pass.
15. **`Preconditions.checkState` rejects active-side resolution flags**: synthesize a replicated
    mutation that causes `hasAtomic` (or `returnResult` / `hasConditionalTTL`) on the standby's
    `BatchMutateContext` and confirm the orchestrator rejects it.
16. **Concurrent same-row batches on the standby** (validates skipping `waitForPreviousConcurrentBatch`):
    drive two replicated batches for the same row at `ts1 < ts2` through the standby IRO from two
    threads, force interleaving (test latch on `doPre` or `injectFault*` hooks). Assert: (a) final index
    state matches active; (b) no `getCurrentRowStates` scan fires; (c) all index cells carry active-side
    timestamps; (d) reader queries during the window fall back to the data table on unverified cells.
17. ✅ **Unit test for `(row, ts)` grouping** — **DONE**:
    `IndexRegionObserverReplayTest#testBuildReplicatedRowGroupsMultiRowMultiTsIsolation`. Rather than
    mock a `MiniBatchOperationInProgress` + region env, the grouping + fold is reached through the pure
    `@VisibleForTesting static IndexRegionObserver.buildReplicatedRowGroups(List<Mutation>)` (the same
    helper `prepareReplicatedIndexMutations` calls). The test feeds one list of four mutations —
    `(R1, ts1, Put A)`, `(R1, ts1, Delete C)`, `(R1, ts2, Put B)`, `(R2, ts1, Put X)`, each stamped
    with its own `PRE_IMAGE` — and asserts: (a) exactly three groups `(R1, ts1)`, `(R1, ts2)`,
    `(R2, ts1)` in first-seen order; (b) the `(R1, ts1)` group's `nextState` folds both Put A and
    Delete C onto the `ts1` pre-image; (c) the `(R1, ts2)` group's `nextState` folds only Put B onto
    its own `ts2` pre-image, with no leak from the `ts1` group; (d) each group carries its own
    `(row, ts)`. Item (e) (serialize=true → three distinct `RowTsKey` entries in
    `cdcPreMutationsBytes`) is NOT in this unit test: it lives in the index-build path
    (`prepareReplicatedIndexMutations` → `prepareEventuallyConsistentIndexMutations`), which needs
    maintainers + a region env, and the per-(row, ts) CDC keying is already covered end-to-end by
    `ReplicationLogGroupEventualIndexWithSerializeCDCIT`. The single-row split/merge cases are pinned
    by the sibling `testBuildReplicatedRowGroups{SplitsByTimestamp,EachGroupKeepsItsOwnPreImage,
    MergesSameRowTs}` tests in the same file.

### Existing IT base classes to extend

- `HABaseIT` — 18 tests already extend this; extend with the matrix above.
- `ReplicationLogBaseTest` (server-side log harness).
- `ReplicationLogGroupIT.testIndexRegenerationOnStandby` — already exercises happy path; extend for
  matrix coverage including CDC and out-of-order replay.

### Open questions to resolve before writing matrix tests

1. ~~**Scenarios 5/6 (atomic)**: confirm `ATOMIC_OP_ATTRIB` does not reach the standby so
   `identifyMutationTypes` doesn't set `hasAtomic`.~~ **RESOLVED.** `ATOMIC_OP_ATTRIB` is not in
   `REPLICATION_ATTR_KEYS`, so it is never carried into the standby's reconstructed mutations, and
   `isAtomicOp` keys solely on that attribute (`PhoenixIndexBuilder.java:108`). `hasAtomic` stays
   false on the standby; `Preconditions.checkState` does not fire. Confirmed by source + passing
   `testOnDuplicateKeyUpdateWithIndex` (scenario #6).
2. ~~**Scenarios 7/8 (conditional TTL)**: same concern for `hasConditionalTTL`.~~ **RESOLVED.** The
   `TTL` attribute is not in `REPLICATION_ATTR_KEYS` and `hasConditionalTTL` keys solely on it
   (`PhoenixIndexBuilder.java:297`). `hasConditionalTTL` stays false on the standby. Confirmed by
   source + passing `testConditionalTTLWithIndex` (scenario #8).
3. **Scenario 10 (`serializeCDCMutations=true`)**: confirm `RowTsKey`-keyed `cdcPreMutationsBytes` map
   sizes are bounded under heavy multi-batch coalescing on the standby.
4. **Per-row ts verification on standby**: pick the approach. Recommendation: direct cell-timestamp
   comparison via scan of the standby's index region, fail loudly if any cell ts differs from the
   active's per-(row, ts) ts.

## Out of Scope

- Compression / dedup of identical pre-images for a wide batch (revisit only if production telemetry
  shows it).
- Conditional-TTL evaluation that depends on row metadata not in the pre-image cells. If a gap surfaces
  during testing, ship the additional metadata as another METAFAMILY sidecar cell.
- Bumping the log-file format version. Branch is experimental; older log files cannot be replayed by the
  new reader.
- Removing the per-mutation `append(t, c, Mutation)` overload from `ReplicationLogGroup`. It was already
  removed from `LogFile.Writer`/`LogFileWriter` by upstream PHOENIX-7931; it survives on
  `ReplicationLogGroup` only for test callers. Schedule for a follow-up cleanup once the IT matrix lands.

### Future cleanup: BatchMutateContext state consolidation (deferred — not this PR)

`BatchMutateContext` accumulates several row-keyed maps with overlapping keys but different lifecycles:
`rowsToLock` (set), `dataRowStates`, `lastConcurrentBatchContext`, `cdcPreMutationsBytes`,
`cdcPostMutationsBytes`, `replicationCellsByRow`, plus `multiMutationMap` (local-index-only) and
`originalMutations` (a List whose only used element is the first one). The accretion is real.

Possible consolidation: a canonical `Map<ImmutableBytesPtr, RowState>` where `RowState` aggregates
per-row data. This would eliminate `multiMutationMap` (→ local in `groupMutations`), eliminate
`originalMutations` (→ `firstMutation: Mutation`), and collapse 4–5 row-keyed maps into one. Keep
`replicationCellsByRow` separate-ish (it's cell-stream and cell ordering across rows matters) and keep
atomic-flow state separate (single-row scope, distinct lifecycle).

Why deferred: this is a refactor PR in its own right. Each field's distinct lifecycle stage encodes
ordering invariants via implicit nullability checks; merging them re-discovers those invariants.
Land the IT matrix first to lock in correctness, then revisit as a clean-slate refactor.

Rule of thumb for this PR: do **not** add new context fields for transient per-row state if a local
variable or an existing map's key set suffices.

## Known Limitations

- **Block boundaries:** `phoenix.replication.log.rotation.size.bytes` (256 MB) and the in-block size cap
  unchanged. A wide row with a very large pre-image inflates a single record but doesn't affect
  correctness.
- **WAL sidecar size:** `WALEdit.METAFAMILY` cells are WAL-only and never reach an HFile.
  `RSRpcServices.checkCellSizeLimit` does not inspect them (sidecar is peeled into the `PRE_IMAGE`
  attribute on the standby reader before `batchAll`, and attributes aren't subject to the cell-size
  check). The practical limit is heap and the RPC frame budget (~256 MB default).
- **Cross-cluster re-replication:** METAFAMILY cells are filtered out of HBase replication by
  `ScopeWALEntryFilter` because user tables don't define METAFAMILY in their CF descriptors →
  `hasGlobalScope(scopes, METAFAMILY) == false` → cell dropped. Same mechanism that protected
  `IndexedKeyValue` historically.
