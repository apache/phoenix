# Idempotent Standby Index Generation via Pre-Image-in-Attribute

## Context

The `eliminate-index-replication-v2` branch generates index mutations on the standby cluster by re-running `IndexRegionObserver` on replicated data mutations. It is blocked by a replay-ordering hazard documented in `docs/replay-ordering-analysis.md`: within a replication round, same-row mutations can land in different shard files and be processed in random order. Out-of-order application breaks IRO because `getCurrentRowStates` reads the *current* row from HBase, which may already reflect a later mutation — producing wrong index entries for partial updates.

This plan eliminates `getCurrentRowStates` on the standby by **shipping the primary-side pre-image (the row state IRO already reads on the primary) inside the replicated mutation as a serialized snapshot taken at read time**. With the pre-image carried alongside each mutation, the standby can compute the same index update regardless of replay order.

## Approach

Reuse the existing `REPLICATED_MUTATION` mutation attribute. Today it holds a sentinel `Bytes.toBytes(true)`. Change it to hold the **serialized pre-image `Put` for that mutation's row**, captured atomically inside `readDataTableRows` while the row lock is held — so the bytes are an immutable snapshot regardless of any later concurrent activity. When a mutation has no pre-image (no row on disk, or the IRO branch did not call `getCurrentRowStates`), the attribute value is `HConstants.EMPTY_BYTE_ARRAY`. The non-null check (`getAttribute(REPLICATED_MUTATION) != null`) used at `IndexRegionObserver.java:1033, 1712` keeps working because the value is still non-null.

For two-phase delivery this attribute must reach the standby through both routes the cherry-pick already supports:

- **Replication-log path** (`replicateMutations` → `LogFileCodec`): the attribute travels via `LogFileCodec`'s existing length-prefixed attribute encoding. No codec change needed; `REPLICATED_MUTATION` is already in `REPLICATION_ATTR_KEYS` (`ReplicationLogGroup.java:194-199`). This is the dominant path and the one the standby actually replays.
- **WAL-restore path** (`preWALRestore` → `replicateEditOnWALRestore`): used by Phoenix's primary-side recovery when an unflushed memstore is replayed during region open. Phoenix already persists `REPLICATION_ATTR_KEYS` into `WALKey.extendedAttributes` via `preWALAppend` (`IndexRegionObserver.java:1920-1932, 1962-1974`), but only the **first mutation's** attributes are copied into the (per-edit) WALKey. Pre-image is per-row, so the WALKey-attribute lane cannot carry it for multi-row edits. We therefore inject the pre-image into the WALEdit as **per-row sidecar cells** under `WALEdit.METAFAMILY` (mirroring the `IndexedKeyValue` pattern at `IndexedKeyValue.java:63-77`). On the standby, `replicateEditOnWALRestore` extracts those sidecar cells and re-applies them as the `REPLICATED_MUTATION` attribute on the reconstructed mutations before passing them down to `logGroup.append`.

Why this design:
- **Race-free**: the snapshot is serialized inside `readDataTableRows` while the row lock is held. Reaching back to `BatchMutateContext.dataRowStates` from `replicateMutations` would be unsafe because concurrent mutations could mutate the post-image `Put` in `Pair<Put,Put>.getSecond()`.
- **Mirrors an existing precedent**: gating on `this.shouldReplicate` and calling `setAttribute` on the mutation matches `IndexRegionObserver.java:858-860` (`IGNORE_REPLICATION_ATTRIB`).
- **Per-row WAL sidecar matches existing precedent**: `IndexedKeyValue` cells use `WALEdit.METAFAMILY` exactly for this purpose. Each WAL sidecar cell carries the row key it belongs to, so multi-row WAL edits work naturally.
- **No wire-format change** to the replication log codec.

## Implementation

### 1. New constant for the WAL sidecar qualifier

**File**: `phoenix-core-server/.../hbase/index/IndexRegionObserver.java`

Add a class-level constant near `IGNORE_REPLICATION_ATTRIB`:

```java
// Sidecar cell qualifier used under WALEdit.METAFAMILY to ship per-row
// pre-image bytes through the WAL-restore path on the primary.
private static final byte[] PRE_IMAGE_WAL_QUALIFIER = Bytes.toBytes("_PhoenixPreImage");
```

### 2. Capture and attach the pre-image at read time

**File**: same. Modify `readDataTableRows` (line 1349-1368).

After building the pre-image `Put` for a row (line 1360-1362), if `this.shouldReplicate`:

1. Serialize the pre-image once: `byte[] preImageBytes = ProtobufUtil.toMutation(MutationProto.MutationType.PUT, preImage).toByteArray();` (this is the same PB schema HBase uses everywhere).
2. Walk the current `MiniBatchOperationInProgress<Mutation>` (passed in via `BatchMutateContext`'s caller chain — `getCurrentRowStates` already has the `ObserverContext`; we need to thread `miniBatchOp` to `readDataTableRows`, which is a small refactor) and for each operation whose row equals `rowKey` and which is not `ignoreReplicationFilter.test(m)`, call `m.setAttribute(REPLICATED_MUTATION, preImageBytes)`.
3. Also inject a sidecar cell into the WAL via `miniBatchOp.getWalEdit(0)`:
   ```java
   Cell sidecar = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
       .setRow(rowKey)
       .setFamily(WALEdit.METAFAMILY)
       .setQualifier(PRE_IMAGE_WAL_QUALIFIER)
       .setTimestamp(HConstants.LATEST_TIMESTAMP)
       .setType(Cell.Type.Put)
       .setValue(preImageBytes)
       .build();
   miniBatchOp.getWalEdit(0).add(sidecar);
   ```
   (Use `addOperationsFromCoprocessors` semantics if `getWalEdit` is not available at this hook; verify exact API in `Indexer.java:480` precedent.)

For mutations whose row had no entry in the on-disk scan (line 1356 `cells.isEmpty()` continue), they reach `replicateMutations` without a pre-image attribute set. Apply a follow-up step in `replicateMutations` (below) to fill in `EMPTY_BYTE_ARRAY` so the standby can distinguish "primary intentionally had no pre-image" from "missing".

### 3. Default `REPLICATED_MUTATION` to empty when pre-image was not read

**File**: same. `replicateMutations` (line 2645).

Today (after the cherry-pick) we set `m.setAttribute(REPLICATED_MUTATION, Bytes.toBytes(true))` unconditionally on each outbound mutation. Change this to: if the attribute is **already set** (by step 2 during pre-image capture), leave it. Otherwise, set it to `HConstants.EMPTY_BYTE_ARRAY`. Same logic for the split-mutation path.

```java
private static void markReplicated(Mutation m) {
    if (m.getAttribute(REPLICATED_MUTATION) == null) {
        m.setAttribute(REPLICATED_MUTATION, HConstants.EMPTY_BYTE_ARRAY);
    }
}
```

This preserves the "is replicated?" semantics across all three categories: pre-image captured (value = serialized Put), pre-image absent (value = empty), not replicated (attribute = null).

### 4. Standby: consume pre-image instead of reading row state

**File**: same. Modify the IRO branch that decides whether to call `getCurrentRowStates` (line 1781-1798) to first try populating `dataRowStates` from each mutation's `REPLICATED_MUTATION` attribute.

Add `populateRowStatesFromAttributes(MiniBatchOperationInProgress, BatchMutateContext)`:

- Iterate mutations. For each mutation with non-null `REPLICATED_MUTATION`:
  - If `attr.length == 0` → no pre-image; insert `new Pair<>(null, null)` into `context.dataRowStates` keyed by the mutation's row.
  - Else → `Put preImage = ProtobufUtil.toPut(MutationProto.parseFrom(attr));` and insert `new Pair<>(preImage, new Put(preImage))`. This mirrors `readDataTableRows` line 1364-1365 exactly so downstream consumers (`prepareIndexMutations`, `applyPendingPutMutations`, `applyPendingDeleteMutations`, `updateMutationsForConditionalTTL`, `addOnDupMutationsToBatch`) work unchanged.

In the `prepareDataRowStates` path: if every mutation in `rowsToLock` carries `REPLICATED_MUTATION`, call `populateRowStatesFromAttributes` and skip `getCurrentRowStates`. Otherwise (mixed batch — defensive) fall back to the existing scan path. This preserves correctness on non-replicated batches.

### 5. Standby WAL-restore path: pull sidecar cells

**File**: same. Modify `replicateEditOnWALRestore` (line 758).

Today it iterates `logEdit.getCells()` and uses `splitCellsIntoMutations` to rebuild `Mutation` objects. Before splitting:

1. Walk cells once, extract any cell whose family is `WALEdit.METAFAMILY` and qualifier is `PRE_IMAGE_WAL_QUALIFIER`. Build `Map<ImmutableBytesPtr, byte[]> preImageByRow`.
2. Pass the remaining cells (excluding sidecars) to `splitCellsIntoMutations`.
3. After reconstruction, for each `Mutation split`: `byte[] pre = preImageByRow.get(new ImmutableBytesPtr(split.getRow()));` and call `split.setAttribute(REPLICATED_MUTATION, pre != null ? pre : HConstants.EMPTY_BYTE_ARRAY);` **before** `copyWALKeyAttributesToMutation` (so the WAL key's old REPLICATED_MUTATION value, if any, doesn't overwrite the per-row one).

This keeps the existing WAL-restore semantics intact while ensuring per-row pre-image bytes survive the WAL round-trip.

### 6. Comprehensive scope: branches that read row state on the primary

`getCurrentRowStates` is called when any of `hasAtomic`, `returnResult`, `hasGlobalIndex`, `hasUncoveredIndex` (with partial check), `hasTransform`, `hasConditionalTTL`, `hasRowDelete` are set (`IndexRegionObserver.java:1781-1798`). All of them populate the same `dataRowStates` and consume it via the same downstream methods. The pre-image-in-attribute mechanism handles all uniformly — no per-branch code changes beyond steps 2, 4, 5.

Local indexes are unaffected — they don't use `dataRowStates` (`prepareIndexMutations` line 1388-1390 skips local indexes).

## Key Files

| File | Change |
|------|--------|
| `phoenix-core-server/.../hbase/index/IndexRegionObserver.java` | New constant `PRE_IMAGE_WAL_QUALIFIER`. Modify `readDataTableRows` to serialize pre-image, set attribute on matching mutations, inject WAL sidecar cell. Modify `replicateMutations` to default attribute to empty when not set. New `populateRowStatesFromAttributes` and call site in `prepareDataRowStates`. Modify `replicateEditOnWALRestore` to extract sidecar cells and reattach as attribute. |
| `phoenix-core-server/.../replication/log/LogFileCodec.java` | No change. |
| `phoenix-core-server/.../replication/ReplicationLogGroup.java` | No change. |
| `phoenix-core-client/.../coprocessorclient/BaseScannerRegionObserverConstants.java` | No change. |

## Verification

1. **Build**: `mvn package -pl phoenix-core-server -am -DskipTests`.
2. **Spotless**: `mvn spotless:apply` before commit.
3. **Unit — codec round-trip**: extend `LogFileCodecTest.testMutationAttributesRoundTrip` so `REPLICATED_MUTATION` carries a serialized non-empty `Put`; assert the deserialized attribute bytes match exactly.
4. **Unit — empty pre-image**: assert that `REPLICATED_MUTATION = EMPTY_BYTE_ARRAY` round-trips and is treated as "no pre-image" by `populateRowStatesFromAttributes` (yields `Pair<null,null>`).
5. **Integration — happy path**: `ReplicationLogGroupIT.testIndexRegenerationOnStandby` continues to pass; `assertTablesEqualAcrossClusters` verifies index entries match cell-for-cell.
6. **Integration — out-of-order replay (regression test for the original blocker)**: new test in `ReplicationLogProcessorTestIT` writes M1 (full insert, ts=100) and M2 (partial update, ts=200) for the same row to separate log files, replays file-B-first then file-A. Assert: data table correct AND index table on the standby has the correct value for the column not touched by M2 (the original blocker symptom — must now pass because each mutation carries its own pre-image).
7. **Integration — WAL-restore path**: extend a test that simulates an unflushed memstore replay (or use HBase's WAL-replay test harness if available) to confirm sidecar cells survive the WAL round-trip and produce the same standby index state as the log-file path.
8. **Integration — non-indexed table**: confirm a table with no indexes replicates normally, with `REPLICATED_MUTATION` set to an empty byte array, no sidecar cells in WAL.
9. **Existing IT suite**: `mvn verify -pl phoenix-core -Dit.test=ReplicationLogGroupIT,ReplicationLogProcessorTestIT,IndexRegionObserverIT`.

## Out of Scope

- Optimization for batches where the pre-image is identical for many mutations (rare — would require dedup keyed by row).
- Conditional-TTL evaluation on the standby that depends on row metadata not contained in the pre-image cells (verify during testing; if a gap surfaces, ship the additional metadata in the same attribute or via a sibling attribute).

## Known Limitations

Neither delivery path enforces a hard size cap on the pre-image payload, but each has practical considerations:

- **Replication-log path**: `LogFileCodec.java:154-163` encodes attributes with vint-prefixed length — no per-attribute or per-mutation cap is checked. The relevant ceiling is `phoenix.replication.logfile.block.size` (default 1MB) at `LogFileFormatWriter.java:85`, which **rolls a block** when exceeded rather than rejecting the write. A row whose serialized pre-image exceeds the block size simply produces an oversize block; throughput suffers, correctness does not.
- **WAL sidecar path**: `hbase.client.keyvalue.maxsize` (~10MB) is checked only at `RSRpcServices.checkCellSizeLimit` on the client RPC entry, **not** on cells appended by coprocessors via `WALEdit.add`. The server-side `hbase.server.keyvalue.maxsize` is enforced during HFile flush (`HRegion.java:4021-4025`), but `WALEdit.METAFAMILY` cells are WAL-only and never reach an HFile. The existing `IndexedKeyValue` precedent under METAFAMILY has no cap either. **No effective HBase-level cap applies on this path.**

Residual concern: very wide rows bloat replication-log blocks (one oversize block per such mutation) and add per-mutation serialization cost. We accept this as-is and will revisit (e.g., compression, size-cap with scan fallback, or chunking) only if production telemetry shows it materially affects replication backlog or block-roll cadence.
