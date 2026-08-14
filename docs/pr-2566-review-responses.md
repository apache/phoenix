# PR #2566 — Response to review

Thanks for the thorough pass. I worked through every finding against the Phoenix
and HBase source. Summary up front: **of the 7 blockers, none is a correctness
bug** — six are misreadings or already-correct behavior, one (region-split test)
is a valid coverage gap deferred to a separate change. The genuinely useful
lower-severity items (error-message table context, doc notes) are addressed in
commit `ab02c410f2`; observability and API-freeze items are tracked as follow-ups
appropriate to a pre-release feature branch.

Dispositions below cite the source I traced so the reasoning is checkable.

---

## Blockers

### 1. Replication wire format has no version marker — **Won't fix (out of scope)**
This is a pre-release feature on `PHOENIX-7562-feature-new`; no standby running an
older format exists, so there is no active→old-standby compatibility surface to
protect yet. `LogFileHeader` already carries `VERSION_MAJOR/MINOR`
(`LogFileHeader.java:32-34`) to gate a format change before GA. Deferring the
compat-matrix work to pre-release hardening.

### 2. DoNotRetryIOException leaks transient failures → rolling-restart stall — **Invalid**
`isNonRetryable` matches `DoNotRetryIOException` specifically, walking the cause
chain (`ReplicationLogProcessor.java:446-453`). HBase does **not** wrap transient
region conditions as DNRIOE: region-not-online is `NotServingRegionException`, a
moved region is `RegionMovedException`, RS-stopping is
`ServerNotRunningYetException` — none extend `DoNotRetryIOException`, so all remain
retryable and self-heal across a rolling restart exactly as before. Only genuine
deterministic contract violations (missing PRE_IMAGE, decode failure) hit the
non-retry branch, where burning `batchRetryCount` backoff sleeps would be pure
waste. This mirrors HBase's own retrying caller's contract.

### 3. Batching splits "record is a batch" atomicity — **Invalid**
The size check runs at the **record boundary**, not mid-record. The inner loop
adds every mutation of a record to the batch (`ReplicationLogProcessor.java:258-262`);
only after the record is fully added is `currentBatchSize >= getBatchSize()`
evaluated (`:263`). A record — and every `(row, ts)` group within it — is never
split across two batches. The invariant is stated in the comment at `:250-257`.

### 4. Local-index rowkey rebuild vs standby region assignment/split — **Valid coverage gap, out of scope for this PR**
The rebuild is correct by design: the standby regenerates the local-index rowkey
with its **own** `encodedRegionName` (`IndexRegionObserver.java:1512`), which is
exactly why regeneration (not replication) is required — a replicated index row
would carry the active's `partition_id`. There is genuinely no IT replaying across
a region split/move; that exercises HBase region mechanics more than this PR's
logic, so it is tracked as a separate follow-up test rather than expanding this
diff.

### 5. capturePreImageCells writes all pre-images to WAL slot 0 — **Invalid**
Slot assignment does not survive to the persisted WAL. HBase's `buildWALEdits`
merges **every** per-operation coprocessor WALEdit into a single `WALEdit` per
nonce group (`HRegion.java:3583-3589`). Each pre-image cell is row-keyed
(`buildPreImageCell` sets `.setRow(row)`, `IndexRegionObserver.java:1554`), so the
WAL-restore path re-associates pre-images to rows by row key, not slot index.
Concentrating them in slot 0 is functionally identical to spreading them.

### 6. Pre-image capture ordering vs prepareDataRowStates — **Already correct**
The ordering is already `prepareDataRowStates` (`:2598`) →
`capturePreImageCells` (`:2602`). Capture serializes the pre-image to bytes
immediately via `encodePreImage` → `toByteArray()` (`:1702`, `:1543`), so the
snapshot is taken at capture time; any later in-place mutation of `dataRowStates`
cannot affect the already-serialized bytes. This is the behavior the finding asks
for.

### 7. (row, ts) collision clobbers the first pre-image — **Invalid**
`buildReplicatedRowGroups` accumulates same-key mutations into a `List` via
`computeIfAbsent` (`:1408-1411`) — nothing is overwritten. The pre-image is read
once from the group's first mutation (`groupMutations.get(0)`, `:1415`) because the
active writes exactly one pre-image cell per row per batch, so every member of a
`(row, ts)` group shares it. A split mutation's cells land in the same list, not a
competing map entry.

---

## Also raised as a blocker: Missing PRE_IMAGE = poison batch — **Intentional; doc note added (`ab02c410f2`)**
The throw fires only on a genuine wire-contract violation, never on the happy path.
On the standby `decodePreImage` is reached only for `builder.isEnabled(m)` mutations
(the `enabledMutations` filter at `:1430`); on the active every enabled row is
guaranteed a `dataRowStates` entry (`applyPendingPutMutations:1337-1341`,
`applyPendingDeleteMutations:1294-1298`) and therefore a pre-image cell — real, or
the empty-row sentinel (`capturePreImageCells:1698-1704`). So an index-enabled
mutation always ships with a pre-image; `decodePreImage` returns Put-or-null and
does not throw.

The absence of a fallback is deliberate and load-bearing: the standby must not scan
the region for prior row state — that out-of-order-unsafe read is precisely what the
per-`(row, ts)` pre-image replaces. Regenerating an index against a scanned state
would silently corrupt it. Halting the batch (DNRIOE, no retry) is the safe response
to a contract violation.

The one legitimate path to a missing pre-image is **schema skew** — the standby
carrying an index the active lacked when it shipped the batch. That already violates
the feature's foundational assumption (regeneration requires matching index
maintainers on both clusters), independent of this code. Documented that assumption
in the `decodePreImage` javadoc.

---

## Must-fix

- **Error messages missing table context** — **Fixed (`ab02c410f2`)**. `decodePreImage`
  and `PreImageLocalTable.getCurrentRowState` now name the table. Threaded via
  instance methods reading the `dataTableName` field (added a `@VisibleForTesting`
  constructor alongside the public no-arg one HBase loads reflectively).
- **PreImageLocalTable up-front population validation** — the `getCurrentRowState`
  guard (`PreImageLocalTable.java:86-91`) already fails loud on a `(row, ts)` miss.
  A constructor-time full-population check isn't feasible: the populating side keys
  by `(row, ts)` derived per-mutation, so "complete" is only defined relative to the
  batch being replayed.
- **Put+Delete same (row, ts) ordering in deriveNextState** — **Not reachable**.
  A null-column UPSERT (the only plain-index same-row Put+Delete source) emits the
  Put before the Delete (`PTableImpl.toRowMutations():1511,1513`), the cell stream
  preserves that order end-to-end, and the two touch disjoint columns anyway. The
  overlapping cases (atomic / conditional-TTL / returnResult) are excluded on
  replication by the `checkState` at `IndexRegionObserver.java:2508-2511`.
- **PRE_IMAGE cell ts = LATEST_TIMESTAMP vs finalized ts** — **Invalid**. The
  pre-image cell's ts never participates in grouping or matching. The reader peels it
  into the PRE_IMAGE attribute and matches to its row **by row key**
  (`reconstructMutations:152,165`); the `(row, ts)` grouping key comes from
  `IndexUtil.getMaxTimestamp(m)` on the data mutation (`:1409`). The ts on the peeled
  METAFAMILY marker is inert.
- **Public attribute keys now protocol** (`REPLICATED_MUTATION` / `PRE_IMAGE` /
  `PRE_IMAGE_WAL_QUALIFIER`) — reasonable to freeze once the feature approaches GA;
  tracking with the versioning item (#1) as pre-release hardening.
- **LogFile.Writer.append old overload omits attributes / getIndexUpdates javadoc /
  ReplicationLogGroup.append IllegalArgumentException undocumented** — javadoc-level
  items; will add.
- **Removed `phoenix.ha.group.store.peer.cache.retry.interval.seconds`** — the key
  was never released, so no deprecation cycle is owed.

---

## Performance & memory

- **Per-row protobuf serialization under lock / N× standby CPU** — inherent to
  regenerating rather than replicating index entries; the deliberate tradeoff of the
  feature (eliminates index-replication traffic and its ordering hazards). Not a
  regression. Will size with capture-path metrics.
- **splitCellsIntoMutations unbounded allocation** (`MutationCellGrouper.java:174`) —
  **No change**. Mutation count is O(cells), bounded by a single record's stream =
  one active-side batch already capped by the writer's batch-size limits. A soft cap
  would add a drop-or-warn failure mode to guard input the write side already bounds.
- **Per-batch LinkedHashMap on the active** (`captureReplicationCells:1647`) —
  **No change**. One short-lived map per batch, hanging off the already-per-batch
  `BatchMutateContext`; young-gen, collected cheaply. Grouping cells by row is
  intrinsic to producing the log stream.
- **RowTsKey.hashCode collisions** (`:395`) — **No change**. `31 * row.hashCode() +
  Long.hashCode(ts)` is the standard `Objects.hash`-style mix; `ImmutableBytesPtr`
  already distributes across the row bytes.

---

## Docs & Ops

- **Zero metrics on new hot paths** — valid observability gap; tracked as a
  follow-up (doesn't gate correctness of this PR).
- **Error messages missing table context** — **Fixed (`ab02c410f2`)** (see must-fix).
- **Class-level thread-safety notes** — **Added (`ab02c410f2`)** for
  `PreImageLocalTable` (per-batch, single-threaded, map immutable after construction)
  and `MutationCellGrouper` (stateless static-only holder).
- **Release note for batch-atomicity semantic change** — moot; the underlying
  claim (#3) was a misreading, no semantic change exists.

---

## Landed in this PR (commit `ab02c410f2`)
- Table context in `decodePreImage` and `PreImageLocalTable` error messages.
- `decodePreImage` schema-skew javadoc note.
- Thread-safety class notes on `PreImageLocalTable` and `MutationCellGrouper`.

## Tracked as follow-ups (out of scope for this branch)
- Wire-format version bump + public-attribute freeze (pre-GA hardening).
- Capture-path metrics (`capturePreImageCells`, `prepareReplicatedIndexMutations`,
  `buildReplicatedRowGroups`).
- Region-split / assignment replay IT.
- Remaining API javadoc nits (append overload, getIndexUpdates, ReplicationLogGroup.append).
