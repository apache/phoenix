# V2 Scan Construction

> Status: **default path**. `WHERE_OPTIMIZER_V2_ENABLED=true` (default) routes WHERE
> optimization through V2. `WHERE_OPTIMIZER_V2_ENABLED=false` selects the legacy V1
> `WhereOptimizer` and is kept as a regression-comparison escape hatch. Companion
> to `where-optimizer-v2.md`.

## Pipeline

```
WhereOptimizerV2.run
  → ExpressionNormalizer                 (rewrite RVC inequalities, IN lists, BETWEEN)
  → KeySpaceExpressionVisitor            (produces KeySpaceList + consumed-nodes set)
  → V2ScanBuilder.build                  (classifies shape; produces ScanRanges)
  → CompoundByteEncoderEmitter           (overrides scan.startRow/stopRow for
                                          in-envelope shapes)
  → context.setScanRanges
  → context.setV2ScanArtifact            (logical KeySpaceList for explain-plan)
  → RemoveExtractedNodesVisitorV2        (residual filter)
```

The `KeySpaceList` is the load-bearing intermediate representation. Everything above
`V2ScanBuilder.build` runs over the algebra; everything below runs over byte-level
scan construction.

## Package layout

`phoenix-core-client/src/main/java/org/apache/phoenix/compile/keyspace/`
- `KeySpace`, `KeySpaceList` — N-dimensional algebra (unchanged from `where-optimizer-v2.md`).
- `ExpressionNormalizer` — rewrites non-atomic predicates (RVC inequality →
  lexicographic OR-of-ANDs, `col IN (v1, v2, ...)` → OR of equalities, BETWEEN →
  AND of range comparisons).
- `KeySpaceExpressionVisitor` — walks the normalized Expression tree, producing a
  final `KeySpaceList` and the set of fully-consumed nodes.
- `KeyRangeExtractor` — projects a `KeySpaceList` onto V1's per-slot CNF shape
  (`List<List<KeyRange>> ranges`, `int[] slotSpan`, `boolean useSkipScan`) that
  `ScanRanges.create` + `SkipScanFilter` consume. Used by `V2ScanBuilder` for shape
  classes where native emission isn't implemented yet (see classification tree
  below).
- `WhereOptimizerV2` — entry point; orchestrates the pipeline.
- `RemoveExtractedNodesVisitorV2` — strips consumed nodes from the normalized tree
  to produce the residual filter.

`phoenix-core-client/src/main/java/org/apache/phoenix/compile/keyspace/scan/`
- `V2ScanBuilder` — classification dispatch (see §"Classification tree" below).
- `CompoundByteEncoder` — emits start/stop row bytes from a `KeySpace` (per-dim
  encoding, separator rules, DESC inversion, inclusive→exclusive `nextKey` bump).
  No dependency on `ScanUtil`.
- `CompoundByteEncoderEmitter` — overrides `scan.startRow`/`stopRow` with
  `CompoundByteEncoder` output, prepending prefix bytes (salt / viewIndexId /
  tenantId). Gated by an envelope check (`isInScope`) that excludes salted tables
  and IS_NULL/IS_NOT_NULL sentinels.
- `V2ScanArtifact` — logical-form handle attached to `StatementContext` so the
  explain-plan formatter can read the pre-encoding `KeySpaceList` instead of
  decoding post-encoding bytes.
- `V2ExplainFormatter` — produces `ExplainPlanAttributes.getKeyRanges()` from the
  `V2ScanArtifact`.

## What V2 owns, what V2 reuses

| Concern | Owned by V2 | Reused from V1 |
|---|---|---|
| Algebra (AND/OR/NOT intersection + merge) | `KeySpace`, `KeySpaceList` | — |
| Expression normalization | `ExpressionNormalizer` | — |
| Per-dim range construction | `KeySpaceExpressionVisitor` | `KeyRange` |
| Shape classification | `V2ScanBuilder.classify` | — |
| Start/stop row bytes | `CompoundByteEncoder` (classes 3, 4a/b/c, 5 in envelope) | `ScanUtil.setKey` (classes 4d, 4e, out-of-envelope) |
| `useSkipScan` decision | `V2ScanBuilder` | — |
| CNF / `slotSpan` shape | `V2ScanBuilder` (class 3) | `KeyRangeExtractor.emitV1Projection` (classes 4, 5) |
| SkipScanFilter class + region-server behavior | — | `SkipScanFilter` unchanged |
| Explain-plan string (key ranges) | `V2ExplainFormatter` (falls back to V1 when shape not handled) | `ExplainTable.appendScanRow` |
| ScanRanges wrapper type | — | `ScanRanges` (populated by V2, consumed by `ScanPlan` unchanged) |

## Residual filter

The residual `Expression` is what's left after `RemoveExtractedNodesVisitorV2` strips
nodes that the scan bytes + SkipScanFilter fully enforce. It's computed from the
normalized tree (not the caller's original tree) because rewrites like
RVC-inequality expansion change node identity.

`/*+ RANGE_SCAN */` overrides `useSkipScan` to `false` after extraction; the
original WHERE is preserved as residual in that case, because predicates previously
consumed under the assumption that SkipScanFilter would enforce them can't rely on
it anymore. Matches V1's [`WhereOptimizer.java:395`](../phoenix-core-client/src/main/java/org/apache/phoenix/compile/WhereOptimizer.java#L395) behavior.

## Byte emission envelope

`CompoundByteEncoderEmitter.isInScope` decides whether the encoder's bytes override
`ScanRanges.create`'s output. In scope:
- Single-space or multi-space `KeySpaceList`.
- No IS_NULL / IS_NOT_NULL sentinels.
- Not salted (salt bytes are per-row hashes; static prefix would miss buckets 1+).

Out-of-scope shapes keep whatever `ScanRanges.create` produced via `ScanUtil.setKey`.

RVC OFFSET (`minOffset` present) is additionally excluded at the emitter call site:
`RVCOffsetCompiler` reads `scan.startRow` to build the paging cursor and is
sensitive to the classical byte layout. See
`QueryMoreIT.testRVCOnDescWithLeadingPKEquality`.

## Classification tree: KeySpaceList → Scan + Filter

The authoritative decision tree `V2ScanBuilder.build` follows. Classes are applied
in order; the first match wins. Every shape has a single emission function — no
per-test heuristics, no ad-hoc fall-through.

### Inputs

A `KeySpaceList` produced by `KeySpaceExpressionVisitor` (post-normalization,
post-merge-to-fixpoint), plus:
- `RowKeySchema`, `PTable`
- Prefix slots (salt byte / viewIndexId / tenantId — auto-populated, not in `KeySpaceList`)
- Hints (`SKIP_SCAN` / `RANGE_SCAN`)
- `minOffset` (RVC OFFSET paging cursor)

### Outputs

- `Scan.startRow` / `stopRow` / `includeStartRow` / `includeStopRow`
- `useSkipScan` — whether `SkipScanFilter` is attached (independent of residual filters)
- CNF `List<List<KeyRange>>` + `slotSpan` + `RowKeySchema` — consumed by `SkipScanFilter`, `ScanRanges.isPointLookup`, explain-plan, local-index pruning
- Residual `Expression` — WHERE predicates not fully characterized by scan bytes + SkipScanFilter

### Classes

#### 1. DEGENERATE

`list.isUnsatisfiable()`.

- `context.setScanRanges(ScanRanges.NOTHING)`, no scan, no filter, residual = `null`.

#### 2. EVERYTHING

`list.isEverything()` AND `prefixSlots == 0` AND `!minOffset.isPresent()`.

- `context.setScanRanges(ScanRanges.EVERYTHING)`, residual = original WHERE.

#### 3. POINT_LOOKUP_LIST

Every space is all-single-key on every productive dim past prefix, no IS_NULL/IS_NOT_NULL sentinels, no middle gaps within a space. `list.size() ≥ 1`. (When `list.size() == 1` and only one productive dim exists, also classified here — but current implementation routes single-dim single-tuple through the classical path to preserve DESC var-width byte shape; see `V2ScanBuilder.isPointLookupList`.)

- **Bytes:** one point key per space (prefix || encoded tail via `CompoundByteEncoder`). `Scan.startRow = min(point keys)`, `Scan.stopRow = nextKey(max(point keys))`.
- **useSkipScan:** `N > 1` (single point key is a true point lookup; multiple point keys use SkipScanFilter to seek between them).
- **CNF:** one slot with N point ranges, `VAR_BINARY_SCHEMA`, `slotSpan = SINGLE_COLUMN_SLOT_SPAN`. Downstream doesn't need per-column metadata — every byte is pinned.
- **Residual:** visitor-consumed nodes removed.

#### 4. RANGE_SCAN

`list.size() == 1`, single space, no IS_NULL sentinels, at least one productive dim past prefix. Subcase by the shape of the one space:

##### 4a. ALL_PINNED

Every productive dim past prefix is single-key. Effectively a single-row point lookup that didn't qualify as POINT_LOOKUP_LIST because only one tuple exists or because the shape benefits from schema-preserving emission.

- **Bytes:** compound lower = compound upper = encoded row via `CompoundByteEncoder`. `Scan.startRow = lower`, `Scan.stopRow = nextKey(upper)`.
- **useSkipScan:** `false`. Scan bytes fully pin one row.
- **CNF:** per-dim slots (schema-preserving for explain-plan + local-index pruning) OR one compound slot with `slotSpan = N-1` — the choice is a byte-emission tuning, not a correctness distinction (both produce the same `Scan.startRow/stopRow` when bytes come from `CompoundByteEncoder`).
- **Residual:** visitor-consumed nodes removed.

##### 4b. LEADING_PINS_THEN_TRAILING_RANGE

`K` leading productive dims are single-key, followed by exactly one range dim, nothing productive after. E.g. `PK1='a' AND PK2 BETWEEN 10 AND 20`.

- **Bytes:** compound interval `[pin_1·pin_2·...·pin_K·range.lower, pin_1·pin_2·...·pin_K·range.upper]` via `CompoundByteEncoder`.
- **useSkipScan:** `false`. Scan bytes fully characterize the predicate.
- **CNF:** per-dim slots preserving schema metadata.
- **Residual:** visitor-consumed nodes removed.

##### 4c. LEADING_RANGE_WITH_TRAILING_CONSTRAINTS

First productive dim past prefix is a range, followed by more productive dims. E.g. `PK1 >= 'x' AND PK2 = 'y'`.

- **Bytes:** compound interval narrows on leading range only; trailing dims contribute only to their own bytes within the compound encoding (e.g., `PK1 >= 'x' AND PK2 = 'y'` → `startRow = 'x'·'y'`, `stopRow = ByteUtil.EMPTY_END_ROW`).
- **useSkipScan:** `true`. Trailing-dim predicates past the leading range can't be enforced by scan bytes; SkipScanFilter seeks per-row to rows satisfying them.
- **CNF:** per-dim slots. SkipScanFilter reads these to enforce trailing constraints.
- **Residual:** trailing-dim predicates stay in the residual (V1's `hasUnboundedRange → stopExtracting` rule — SkipScanFilter alone can be defeated by data patterns, so residual is a correctness backstop). Leading-dim predicates may still be extracted if fully captured by scan bytes.

##### 4d. LEADING_EVERYTHING

First dim past prefix is `EVERYTHING_RANGE`. E.g. `substr(non_leading_pk)='x'`.

- **Bytes:** `startRow` = empty, `stopRow` = empty.
- **useSkipScan:** `false`. Nothing to narrow via scan bytes; no per-slot discrimination possible.
- **CNF:** adapter path (`KeyRangeExtractor`) — it handles the extraction correctly and produces the empty-bytes shape plus per-slot CNF for any trailing constraints.
- **Residual:** full predicate goes to residual (visitor must not consume anything that can't be enforced by bytes/filter).

##### 4e. MIDDLE_GAP

Productive dims on both sides of an `EVERYTHING_RANGE` dim past prefix. E.g. `PK1='a' AND PK3='c'` with PK2 unconstrained.

- **Bytes:** compound stops at the gap on the lower side; upper side depends on whether the post-gap dim contributes. Adapter's stop-at-gap behavior handles this.
- **useSkipScan:** `true`. SkipScanFilter seeks across the middle-EVERYTHING to values of trailing dims.
- **CNF:** adapter path. SkipScanFilter uses the per-slot disjunctions to drive seeks.
- **Residual:** predicates not captured by the per-slot CNF stay in residual.

#### 5. SKIP_SCAN_LIST

`list.size() > 1` with at least one non-point-key range somewhere (otherwise would be POINT_LOOKUP_LIST).

- **Bytes:** compound interval `[lex-min(encoded lowers), lex-max(encoded uppers))` — the hull covering every space. Via `CompoundByteEncoder`.
- **useSkipScan:** `true`. SkipScanFilter enforces per-space per-dim discrimination inside the hull.
- **CNF:** adapter's `emitV1Projection` — projects each space onto each PK column, coalesces per-column. This projection is what `SkipScanFilter` consumes; cross-dim correlation is lost at this boundary (documented limitation — the residual filter re-evaluates the cross-dim correlation).
- **Residual:** visitor-consumed nodes that the CNF projection fully captures are removed; cross-dim-correlated predicates stay.

### Hint overrides

Applied after classification, before emission:

- `/*+ SKIP_SCAN */` → force `useSkipScan = true`. If the CNF has per-slot disjunctions, SkipScanFilter attaches and runs. If it doesn't, the flag is honored but the filter sees a single-range-per-slot shape and degrades to near-no-op. Cost, not correctness.
- `/*+ RANGE_SCAN */` → force `useSkipScan = false`. **Critical correctness consequence:** any predicate marked consumed under the assumption SkipScanFilter would enforce it must move back to the residual. `WhereOptimizerV2.run` checks the hint after extraction and preserves the original WHERE as residual when `RANGE_SCAN` was hinted (matches V1's [WhereOptimizer.java:395](../phoenix-core-client/src/main/java/org/apache/phoenix/compile/WhereOptimizer.java#L395) behavior).

### Byte emission: CompoundByteEncoder authoritative

For all non-DEGENERATE, non-EVERYTHING classes in the encoder's envelope,
`CompoundByteEncoder` is the source of `Scan.startRow/stopRow` bytes. The CNF shape
(per-dim slots vs. compound slot with `slotSpan > 0`) is irrelevant to scan bytes
under this model — it matters only for what downstream consumers (SkipScanFilter,
ScanRanges.isPointLookup, explain-plan, local-index pruning) see.

**Exception — SkipScanFilter per-region cursor stepping.** `SkipScanFilter.intersect()`
reads `slotSpan[0]` to step the schema cursor when intersecting per-region
`[scanStartRow, scanStopRow)` with the filter's slots. Under-counting `slotSpan`
makes the cursor step through fewer bytes per row than the scan actually covers,
which re-orders the rows delivered to `UncoveredGlobalIndexRegionScanner` /
`CDCGlobalIndexRegionScanner`. The result set stays correct (same rows), but
per-PK iteration order can diverge from V1 — breaks CDC callers that rely on
timestamp-ascending delivery. Two fixes in V2 preserve parity:

1. `KeyRangeExtractor` routes `IN-list-on-leading + range-on-trailing` through
   `emitV1Projection` (see `where-optimizer-v2.md` §7.2 Gate 4) so the CNF has the
   V1-compatible two-slot shape `[[points], [range]]`.
2. `emitV1Projection` extends `slotSpan[lastSlot]` to span trailing unconstrained
   PK columns when the last slot contains a non-point range (see §7.7).

See `CDCQueryIT.testSelectCDC` on salted data tables for the motivating shape.

Out-of-envelope shapes (salted tables, IS_NULL / IS_NOT_NULL sentinels, RVC OFFSET)
fall back to `ScanUtil.setKey` via `ScanRanges.create`.

### Responsibility map

| Class | Classifier | Emission | CNF shape | useSkipScan |
|-------|------------|----------|-----------|-------------|
| 1 DEGENERATE | `list.isUnsatisfiable()` | `ScanRanges.NOTHING` | — | — |
| 2 EVERYTHING | `list.isEverything() && noPrefix && !minOffset` | `ScanRanges.EVERYTHING` | — | — |
| 3 POINT_LOOKUP_LIST | `isPointLookupList` | `CompoundByteEncoder` per space | 1 slot × N point keys, VAR_BINARY | `N > 1` |
| 4a ALL_PINNED | `list.size()==1 && allDimsSingleKey` | `CompoundByteEncoder` | per-dim, real schema | `false` |
| 4b LEADING_PINS_THEN_RANGE | `list.size()==1 && K pins + 1 trailing range` | `CompoundByteEncoder` | per-dim, real schema | `false` |
| 4c LEADING_RANGE_WITH_TRAILING | `list.size()==1 && leading range + productive trailing` | `CompoundByteEncoder` | per-dim, real schema | `true` |
| 4d LEADING_EVERYTHING | `list.size()==1 && leadingDim==EVERYTHING` | adapter | adapter | adapter |
| 4e MIDDLE_GAP | `list.size()==1 && productive dims on both sides of EVERYTHING` | adapter | adapter | adapter |
| 5 SKIP_SCAN_LIST | `list.size() > 1 && !isPointLookupList` | adapter | adapter (`emitV1Projection`) | adapter |

Classes 4d, 4e, and 5 will migrate to native emission as SKIP_SCAN native path and compound-byte `MIDDLE_GAP` handling land. Until then, the adapter is the correct path for them — its per-slot projection is what `SkipScanFilter` consumes.

## Known limitations

- **Adapter dependency for classes 4d / 4e / 5.** `V2ScanBuilder.build` calls
  `KeyRangeExtractor` to produce the V1-shaped CNF these classes consume. Native
  emission for these classes is tracked in PHOENIX-6791 follow-up work.
- **V1 path preserved.** `WhereOptimizer.pushKeyExpressionsToScan` still implements
  the legacy key-slot enumerator, reachable via `WHERE_OPTIMIZER_V2_ENABLED=false`.
  Deleting it requires dropping the V2=off configuration as a supported mode.
- **Differential byte-encoding protection.** `CompoundByteEncoder` diverged from
  V1's `ScanUtil.setKey` in known ways (trailing-separator rules, inclusive-upper
  bump timing). The 22-shape `CompoundByteEncoderDifferentialTest` covers the
  envelope; any new shape admitted to the envelope needs an entry there.
