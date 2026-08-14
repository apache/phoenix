# Idempotent Standby Index Generation via Pre-Image-in-Attribute

## Context

This plan supersedes `standby-index-preimage-in-attribute.md` for one section: the
"Known Limitations" subsection that asserted HBase's `hbase.client.keyvalue.maxsize`
(default ~10MB) constrains the WAL sidecar payload. That assertion does not match
the code paths the design actually uses. This plan corrects the limitation in light
of code-path evidence and otherwise leaves the original implementation plan
unchanged.

The branch `eliminate-index-replication-v2` regenerates index mutations on the
standby by re-running `IndexRegionObserver`, but is blocked by replay ordering: a
mutation can be replayed before another mutation against the same row, causing
`getCurrentRowStates` on the standby to read a row that already reflects a later
write. The fix is to ship the primary-side pre-image alongside each mutation so
the standby can reconstruct the same index update without reading row state.

## Approach (unchanged from original plan)

Reuse the `REPLICATED_MUTATION` mutation attribute. Today it holds a sentinel; change
it to hold the **serialized pre-image `Put` bytes** captured atomically inside
`readDataTableRows` while the row lock is held. When no pre-image was read
(empty row, or branch did not call `getCurrentRowStates`) the value is
`HConstants.EMPTY_BYTE_ARRAY` so the existing non-null check still works.

Two delivery routes:
- **Replication-log path** (`replicateMutations` → `LogFileCodec`): the attribute
  travels via `LogFileCodec`'s existing length-prefixed attribute encoding. No
  codec change required — `REPLICATED_MUTATION` is already in
  `REPLICATION_ATTR_KEYS`.
- **WAL-restore path** (`preWALRestore` → `replicateEditOnWALRestore`): inject a
  per-row sidecar cell into the WALEdit under `WALEdit.METAFAMILY` (mirrors
  `IndexedKeyValue`); on standby, extract sidecars and reattach as the attribute
  on each reconstructed mutation before `logGroup.append`.

Implementation steps (unchanged from the original plan, summarized):

1. Add class-level constant `PRE_IMAGE_WAL_QUALIFIER` near
   `IGNORE_REPLICATION_ATTRIB` in `IndexRegionObserver.java`.
2. In `readDataTableRows` (and its caller chain — thread `MiniBatchOperationInProgress`
   through), serialize each row's pre-image `Put` once via
   `ProtobufUtil.toMutation(MutationProto.MutationType.PUT, preImage).toByteArray()`,
   call `setAttribute(REPLICATED_MUTATION, bytes)` on every matching mutation in
   the batch (skipping `ignoreReplicationFilter`-marked ones), and append a sidecar
   cell to `miniBatchOp.getWalEdit(0)`.
3. In `replicateMutations` (and split path), default the attribute to
   `HConstants.EMPTY_BYTE_ARRAY` only when not already set.
4. On the standby, add `populateRowStatesFromAttributes` and call it from
   `prepareDataRowStates` to skip `getCurrentRowStates` when every mutation in
   `rowsToLock` carries `REPLICATED_MUTATION`.
5. In `replicateEditOnWALRestore`, peel sidecar cells, reattach as the attribute on
   reconstructed mutations **before** `copyWALKeyAttributesToMutation` so per-row
   pre-images outrank the per-edit WALKey value.

## Key Files

- `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/IndexRegionObserver.java` (all changes)
- No changes to `LogFileCodec.java`, `ReplicationLogGroup.java`, or
  `BaseScannerRegionObserverConstants.java`.

## Verification

1. Build: `mvn package -pl phoenix-core-server -am -DskipTests`.
2. Spotless: `mvn spotless:apply` before commit.
3. Unit — codec round-trip: extend `LogFileCodecTest.testMutationAttributesRoundTrip`
   with a non-empty serialized `Put` as the attribute value; assert byte-for-byte
   equality.
4. Unit — empty pre-image: `EMPTY_BYTE_ARRAY` round-trips and produces
   `Pair<null,null>` from `populateRowStatesFromAttributes`.
5. Integration — happy path: `ReplicationLogGroupIT.testIndexRegenerationOnStandby`
   continues to pass via `assertTablesEqualAcrossClusters`.
6. Integration — out-of-order replay regression: write M1 (full insert ts=100) and
   M2 (partial update ts=200) for the same row to separate log files; replay
   file-B-first. Assert standby index columns untouched by M2 carry the M1 value.
7. Integration — WAL-restore: simulate unflushed memstore replay; sidecar-bearing
   WAL round-trip yields the same standby state as the log-file path.
8. Integration — non-indexed table: replicates normally with empty attribute, no
   sidecar cells.
9. Existing IT suite:
   `mvn verify -pl phoenix-core -Dit.test=ReplicationLogGroupIT,ReplicationLogProcessorTestIT,IndexRegionObserverIT`.

## Out of Scope

- Dedup of identical pre-images across mutations of the same row (rare; only
  matters if multiple mutations target one row in a single batch).
- Conditional-TTL metadata on the standby that the pre-image cells may not
  capture (verify during testing; revisit only if a gap surfaces).

## Known Limitations (corrected)

The original plan claimed HBase's `hbase.client.keyvalue.maxsize` (default ~10MB)
caps the sidecar payload. Code-path evidence does not support that:

- **Replication-log path** (`LogFileCodec.java:154-163`): attribute length is
  vint-prefixed; **no per-attribute or per-mutation size cap is enforced**. The
  only relevant ceiling is `phoenix.replication.logfile.block.size` (default 1MB)
  at `LogFileFormatWriter.java:85`, which **rolls a new block** when exceeded —
  it does not reject the write. A pre-image larger than the block size simply
  triggers a rollover and produces a single oversize block.
- **WAL sidecar path**: `hbase.client.keyvalue.maxsize` is checked at
  `RSRpcServices.checkCellSizeLimit` only on the **client RPC entry**, not on
  cells appended by coprocessors via `WALEdit.add`. The 10MB
  `hbase.server.keyvalue.maxsize` is checked during HFile flush
  (`HRegion.java:4021-4025`), but `WALEdit.METAFAMILY` cells are WAL-only and
  never reach an HFile. The existing `IndexedKeyValue` precedent (`IndexedKeyValue.java:63-83`) under
  the same family has no size cap either. **No effective HBase-level cap applies on this path.**

Practical residual concern: very wide rows bloat replication-log blocks (one
oversize block per such mutation on the log path) and increase per-mutation
serialization cost. Throughput, not correctness. We accept this as-is and will
revisit only if production telemetry shows it materially affects replication
backlog or block-roll cadence.
