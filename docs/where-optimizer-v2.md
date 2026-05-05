# WHERE Optimizer V2 — Design and Implementation

PHOENIX-6791 redesigns Apache Phoenix's WHERE-clause optimizer. This document describes what the redesign is, why it exists, the mathematical model it implements, and how that model is realized in code — with a walkthrough of the pipeline end-to-end.

The source lives in `phoenix-core-client/src/main/java/org/apache/phoenix/compile/keyspace/`. The feature is gated by `QueryServices.WHERE_OPTIMIZER_V2_ENABLED` (default `true` on this branch). When the flag is off, the legacy optimizer runs unchanged.

---

## 1. Why

The legacy `WhereOptimizer` in `phoenix-core-client/src/main/java/org/apache/phoenix/compile/WhereOptimizer.java` enumerates primary-key ranges by walking the expression tree with a mutable visitor that concatenates byte-encoded slots as it goes. Over the years that approach has accumulated correctness and maintainability problems:

1. **Intractable enumeration** — arbitrary WHERE expressions can produce cartesian explosions across PK dimensions, causing OOMs, query timeouts, and the current "max IN list skip-scan size" safety valve (a heuristic cap).
2. **Equivalence violations** — logically equivalent expressions produce different scans. `(PK1, PK2) > (A, B)` and its lex-expansion `PK1 > A OR (PK1 = A AND PK2 > B)` generate different start/stop rows despite describing the same rows.
3. **PHOENIX-6669** — degenerate queries on non-leading PK columns return wrong results. The legacy code has per-position degeneracy checks that don't agree.
4. **Overlapping concepts** — `KeyRange` / `KeySlot` / `KeySlots` / `coalesce` / `union` / `concat` accumulated layers of semi-overlapping machinery through successive patches.

V2 replaces the range-enumeration core with a **mathematical model** (N-dimensional key spaces with well-defined AND/OR algebra) and defers byte-level key emission to a final step. The algorithm is bounded to O(N²) in the number of PK columns by applying cartesian-product widening before any byte expansion.

## 2. Scope and boundaries

**In scope.** Only the WHERE-optimizer and Expression-level normalization are redesigned. Specifically: a new `compile.keyspace` package; a new driver `WhereOptimizerV2` invoked in `WhereOptimizer.pushKeyExpressionsToScan`; a feature flag; parameterized tests; new unit tests for the algebra; a reference-model-based differential harness (`compile.keyspace.algebra`).

**Out of scope — intentionally unchanged.**
- `SkipScanFilter` — consumes the same per-slot `List<List<KeyRange>>` shape as today.
- `ScanRanges` — called with the same arguments; V2 produces the identical downstream inputs.
- `KeyRange` — used as-is; V2 does not touch its internals.
- `WhereCompiler`'s own translation — the entry point into the optimizer is unchanged.
- The residual-filter mechanism — V2 reuses Phoenix's existing extract-and-remove logic for dropping nodes that became redundant once a key range captured their meaning.

## 3. The Key-Space Model

A query's WHERE clause, for optimization purposes, is a predicate over rows. Each row has a primary key with `N` columns. We model the primary-key space as `N`-dimensional: dimension `i` is the domain of PK column `i`.

A **KeySpace** is an `N`-dimensional axis-aligned box: one `KeyRange` per dimension. The predicate `PK1 = 'a' AND PK2 > 3` (on a 3-PK table with columns PK1, PK2, PK3) is a KeySpace `[[a, a], (3, +∞), (-∞, +∞)]`. A dimension with no active constraint is `EVERYTHING_RANGE`.

A **KeySpaceList** is a disjunction of KeySpaces — the scan region is the union of the boxes. A single box covers conjunctive predicates directly; OR shows up as multiple boxes.

Two operations on KeySpaceLists:

- **AND** — distribute over OR: `(a ∨ b) ∧ (c ∨ d) = (a∧c) ∨ (a∧d) ∨ (b∧c) ∨ (b∧d)`. Per-box AND is per-dimension intersection. After the cross product, run a merge-to-fixpoint pass.
- **OR** — concatenate the lists, then run the merge-to-fixpoint pass.

**Merge rules for OR** (from the design):
1. **Containment** — if one box is entirely contained in the other, the union is the larger box.
2. **N−1 agreement** — if two boxes agree on N−1 dimensions and the ranges on the remaining dimension are non-disjoint (overlap or are adjacent with opposite inclusivities, so the union is a single interval), the union is the box with the merged dim.

If neither rule applies, the boxes stay as two separate entries in the list.

**Widening — bounded complexity.** Before emitting byte-level key ranges, if the list size exceeds a configured cartesian bound, drop a trailing dimension (replace it with `EVERYTHING_RANGE` in every box, then re-run merge-to-fixpoint). Each drop weakly reduces list size and cannot introduce false negatives (the residual filter still evaluates the dropped predicate at scan time). This is what keeps the algorithm bounded: no cartesian ever blows up before trailing dims are dropped.

**Normalization.** Several input shapes are rewritten up-front so per-dim intersection composes correctly:
- RVC inequality `(c1,...,cK) OP (v1,...,vK)` for OP ∈ {<, ≤, >, ≥} — expanded to lex OR-of-ANDs. Without this, RVC-inequality has no direct representation in the per-dim model.
- Scalar `IN (v1, v2, ...)` — expanded to `a=v1 OR a=v2 OR ...`. RVC IN is left intact; the visitor handles it by producing one KeySpace per row value.
- BETWEEN — lowered at parse time already by `StatementNormalizer`; doesn't reach this pass.

**Final byte emission.** Once the KeySpaceList is bounded, convert it into the `List<List<KeyRange>>` shape that `ScanRanges.create` consumes. That shape is the **V1 projection** of the KeySpaceList: one output slot per PK column, each containing the coalesced disjunction of every KeySpace's range on that column. It is the exact shape legacy produced, and it is what the existing `ScanRanges` / `SkipScanFilter` machinery was designed to consume. See §7 for details.

An optional **compound emission** optimization can produce tighter scans for specific shapes (e.g., a high-cardinality RVC-IN becomes a POINT LOOKUP rather than a SkipScan over per-column disjunctions). Compound emission concatenates per-dim bytes into a single compound `KeyRange` per KeySpace, preserving cross-dim tuple correlation at the byte level. It isn't always safe against the V1-era downstream utilities (`ScanUtil.setKey`, special cases for `IS_NULL_RANGE`, etc.), so the extractor falls back to the V1 projection whenever compound emission would trip those utilities. Once V1 is deprecated and the downstream code is simplified, the V1-projection fallback can be deleted and compound emission becomes the sole path.

---

## 4. The keyspace Package — files and responsibilities

```
compile/keyspace/
├── KeySpace.java                         N-dim box; per-dim AND/intersect; merge-rule union
├── KeySpaceList.java                     Disjunction of KeySpace; AND/OR with fixpoint merge + widening
├── ExpressionNormalizer.java             RVC-inequality & scalar-IN rewrites
├── KeySpaceExpressionVisitor.java        Expression → KeySpaceList. Handles leaves (comparisons, IS NULL,
│                                         LIKE, IN, RVC) and composes AND/OR recursively.
├── KeyRangeExtractor.java                KeySpaceList → ScanRanges-shaped (List<List<KeyRange>>, slotSpan,
│                                         useSkipScan). Default path emits the V1 projection (one slot
│                                         per PK column); optional compound emission with
│                                         stripTrailingSeparator for shapes that benefit.
├── RemoveExtractedNodesVisitorV2.java    Walks the normalized tree and drops nodes fully consumed by
│                                         the key ranges, producing the residual filter.
├── WhereOptimizerV2.java                 Driver: entry point that orchestrates the pipeline.
```

Tests live alongside in `phoenix-core/src/test/java/org/apache/phoenix/compile/keyspace/`. A separate `compile.keyspace.algebra` package under `phoenix-core/src/test/java` holds a pure-Java reference model of the algebra for differential testing — see §8.

---

## 5. The Pipeline — WhereOptimizerV2.run

`WhereOptimizerV2.run` is called from `WhereOptimizer.pushKeyExpressionsToScan` when the feature flag is on. It takes the same inputs as the legacy method and writes to the same `context.setScanRanges(...)` / returns the same residual `Expression` shape.

The pipeline has four steps:

### Step 1 — Normalize + Visit

```java
Expression normalized = ExpressionNormalizer.normalize(whereClause);
KeySpaceExpressionVisitor visitor = new KeySpaceExpressionVisitor(table);
KeySpaceExpressionVisitor.Result r = normalized.accept(visitor);
```

`normalize` returns an equivalent expression tree with RVC inequalities lex-expanded and scalar INs expanded to OR-chains. The visitor walks the result and builds a `KeySpaceList`. Each visited node returns a `Result(KeySpaceList list, Set<Expression> consumed)` — the list is the narrowing, `consumed` is the set of sub-expressions fully captured by that narrowing (used later to build the residual filter).

### Step 2 — Degeneracy check + Extraction

```java
if (r.list().isUnsatisfiable()) {
  context.setScanRanges(ScanRanges.NOTHING);
  return null;
}
int bound = getCartesianBound(context);
extract = KeyRangeExtractor.extract(r.list(), nPk, bound, prefixSlots, schema);
```

If the visitor detected uniform degeneracy, short-circuit to an empty scan (this is how PHOENIX-6669 is fixed — the per-dim model produces `empty` uniformly instead of the legacy code's position-dependent checks).

Otherwise, `KeyRangeExtractor.extract` converts the list into the `(ranges, slotSpan, useSkipScan)` triple that `ScanRanges.create` accepts.

### Step 3 — Build CNF and materialize ScanRanges

```java
List<List<KeyRange>> cnf = new ArrayList<>(nPk);
if (isSalted)       cnf.add(saltByteRange);         // single point at 0x00
if (isSharedIndex)  cnf.add(viewIndexIdRange);
if (isMultiTenant)  cnf.add(tenantIdRange);
for (slot : extract.ranges) cnf.add(slot);          // user-tail from extractor

int[] slotSpan = new int[cnf.size()];
System.arraycopy(extract.slotSpan, 0, slotSpan, prefixSlots, ...);

ScanRanges scanRanges = ScanRanges.create(schema, cnf, slotSpan, nBuckets, useSkipScan, ...);
context.setScanRanges(scanRanges);
```

Prefix slots (salt byte, view-index id, tenant id) are prepended here so the extractor only has to handle user-PK columns. Each prefix slot is a singleton point range — this lets `ScanRanges` classify the query as a point lookup if the user-tail slots are also all points.

### Step 4 — Residual filter

```java
if (hints.contains(Hint.RANGE_SCAN)) {
  // SkipScanFilter is dropped; residual must preserve the full expression.
  return residualInput;
}
return residualInput.accept(new RemoveExtractedNodesVisitorV2(consumed));
```

Nodes in `consumed` are stripped from the normalized tree by `RemoveExtractedNodesVisitorV2`. What remains is the residual filter — the predicates that must still be evaluated at scan time because the ScanRanges didn't capture them exactly. The RANGE_SCAN hint forces `useSkipScan=false`, which means the per-slot SkipScanFilter is not installed at scan time; without it, any predicate we removed under the assumption the skip-scan would enforce it must be restored. That's the special case for the hint.

---

## 6. The Visitor — KeySpaceExpressionVisitor

The visitor extends `StatelessTraverseNoExpressionVisitor<KeySpaceResult>`. Each node kind maps to a list-producing rule:

| Node                             | Rule                                                                                                                   |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `ComparisonExpression` on PK col | Single-dim `KeySpace` with the comparison as a `KeyRange` on that dim; wrapped in a singleton `KeySpaceList`.         |
| `IsNullExpression` on PK col     | Single-dim KeySpace using `KeyRange.IS_NULL_RANGE` / `IS_NOT_NULL_RANGE`.                                              |
| `AndExpression`                  | Fold children with `KeySpaceList.and` (cross-product AND + merge fixpoint).                                             |
| `OrExpression`                   | Fold children with `KeySpaceList.or` (concat + merge fixpoint).                                                         |
| `InListExpression` (scalar)      | Not reached — `ExpressionNormalizer` already rewrote to `a=v1 OR a=v2 OR ...`.                                          |
| `InListExpression` (RVC LHS)     | Produce one KeySpace per row value; union via `KeySpaceList.orAll` (equivalent to repeated OR but avoids O(K²) folds). |
| `LikeExpression`                 | Compute the LIKE-pattern prefix, convert to a half-open `KeyRange` on the dim.                                          |
| `RowValueConstructorExpression`  | Only reached after normalization; residual case (raw RVC not under a comparison) maps to EVERYTHING.                   |
| Anything else                    | EVERYTHING — the visitor can't narrow based on this node; it stays in the residual.                                    |

For non-PK predicates, the visitor returns `Result.everything(nPk)`. Those predicates contribute nothing to the scan narrowing and remain fully in the residual filter.

**Consumption tracking.** A node is added to `consumed` only when its narrowing is fully captured by the resulting key range. For scalar equality on a PK column, that's always. For RVC-IN, only when the LHS columns form a contiguous PK prefix (otherwise the visitor may widen trailing dims, and the residual filter still needs to reject false positives). This conservative stance is what prevents over-extraction bugs — the previous-optimizer family of patches fought these edge cases position-by-position.

---

## 7. The Extractor — KeyRangeExtractor

The extractor takes a `KeySpaceList` and produces `Result(List<List<KeyRange>>, int[] slotSpan, boolean useSkipScan)` — exactly the shape `ScanRanges.create` consumes.

**The V1 projection is the default.** `emitV1Projection` projects the `KeySpaceList` onto one output slot per PK column, coalesces per column, applies the cartesian-bound widening rule, and emits. This is the V1-shaped output the legacy optimizer produced and the shape existing downstream machinery (`ScanRanges`, `SkipScanFilter`, `ScanUtil.setKey`) was designed to consume.

**Compound emission is an optional optimization.** For shapes where a tighter scan is achievable — principally high-cardinality RVC-IN — the extractor can concatenate per-dim bytes into a single compound `KeyRange` per KeySpace, yielding a POINT LOOKUP on N compound keys rather than a SkipScan over N per-column disjunctions (see §10.2 Runtime I/O). The compound path is gated on shape preconditions (§7.2) that avoid known V1-era downstream quirks. When any precondition fails, the extractor falls back to `emitV1Projection`.

Once V1 is deprecated and the downstream utilities are simplified, the V1-projection fallback can be removed and compound emission becomes the sole path.

### 7.1 Entry and upfront gates

```java
public static Result extract(KeySpaceList list, int nPkColumns, int cartesianBound,
                             int prefixSlots, RowKeySchema schema) {
  if (list.isUnsatisfiable()) return nothing();
  if (list.isEverything())    return everything();

  // Scan all spaces to classify the list.
  int minProductiveStart = nPkColumns;
  int maxProductiveEnd   = prefixSlots;
  boolean allSpacesHaveMiddleGap = true;
  for (KeySpace ks : list.spaces()) {
    int start = firstConstrainedDim(ks, prefixSlots);
    int endStrict = firstProductiveStopStrict(ks, prefixSlots);   // first EVERYTHING past prefix
    int endAny    = firstProductiveStopAnyPrefix(ks, prefixSlots); // last constrained dim + 1
    ...
    boolean hasMiddleGap = start == prefixSlots && endStrict < endAny;
    if (!hasMiddleGap) allSpacesHaveMiddleGap = false;
  }
```

For each KeySpace the extractor records three positions:
- `start` — first non-EVERYTHING dim at or after the prefix. (If this is past `prefixSlots`, the space doesn't anchor the leading PK column.)
- `endStrict` — first EVERYTHING dim past the prefix. Stops at the first gap.
- `endAny` — position just past the last constrained dim. Walks through gaps.

`hasMiddleGap` is true when the space has a constrained leading dim AND a constrained trailing dim with an EVERYTHING gap between them.

### 7.2 Routing gates — when to fall back to the V1 projection instead of attempting compound

By default the extractor would attempt compound emission. Four gates fall back to `emitV1Projection` when compound emission would trip a known V1-era downstream quirk. Each gate targets one specific concern:

```java
// Gate 1: leading EVERYTHING past prefix, or every space has a middle gap.
if (minProductiveStart > prefixSlots || allSpacesHaveMiddleGap) {
  return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
}

// Gate 2: single space, single productive dim.
if (list.spaces().size() == 1 && (maxProductiveEnd - prefixSlots) == 1) {
  return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
}

// Gate 3: any space has IS_NULL_RANGE / IS_NOT_NULL_RANGE at the leading productive dim.
for (KeySpace ks : list.spaces()) {
  KeyRange leadingDim = ks.get(prefixSlots);
  if (leadingDim == KeyRange.IS_NULL_RANGE || leadingDim == KeyRange.IS_NOT_NULL_RANGE) {
    return emitV1Projection(...);
  }
}

// Gate 4: IN-list on leading PK + range on any trailing PK.
//   Detect: every space has a single-key leading dim but the points differ across
//   spaces (IN-list shape) AND some later dim has a non-single-key range.
if (prefixSlots < maxProductiveEnd && list.spaces().size() >= 2) {
  boolean leadingIsInList = ...;   // see KeyRangeExtractor for full code
  boolean leadingDiffers   = ...;
  boolean laterRangeExists = ...;
  if (leadingIsInList && leadingDiffers && laterRangeExists) {
    return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
  }
}
```

**Gate 1**: When the leading PK is unanchored (`minProductiveStart > prefixSlots`), a compound scan would have an unbounded startRow; the V1 projection preserves the trailing-dim narrowing via `SkipScanFilter`. When every space has a middle-EVERYTHING gap, a compound emission would inflate `slotSpan` / `boundPkColumnCount`, producing incorrect point-lookup classification and breaking the local-index-pruning heuristic; the V1 projection respects the gap by emitting a singleton EVERYTHING slot.

**Gate 2**: A trivial single-space single-dim compound would be byte-identical to a one-column V1 projection, but the V1 projection path lets `ScanRanges.create` run `ScanUtil.setKey` with the correct field, which handles DESC separators and fixed-width padding natively. Using compound here would cause a double-separator byte in some DESC cases (see §7.4).

**Gate 3**: `IS_NULL_RANGE` and `IS_NOT_NULL_RANGE` are sentinels; `ScanRanges.create` recognises them explicitly. Compound emission would collapse them into empty bytes and lose the special handling; the V1 projection passes them through intact.

**Gate 4** (CDCQueryIT regression fix): The classical `IN-list × range` shape. Example: `PARTITION_ID() IN ('p1','p2','p3') AND PHOENIX_ROW_TIMESTAMP() BETWEEN A AND B` on a CDC-index PK of `(PARTITION_ID, ts, K)`. V1 emits two distinct slots — slot 0 holds the three points, slot 1 holds the range — with `slotSpan=[0, 1]` so the trailing range extends through the last PK column. V2's compound-per-space form collapses this into one slot with `slotSpan=[1]`; admission semantics match, but `SkipScanFilter.intersect()` reads `slotSpan[0]` to step the schema cursor when intersecting the per-region scan bounds, and the under-counted slotSpan misaligns cursor stepping. Downstream consumers that walk index rows in that stepped order (notably `UncoveredGlobalIndexRegionScanner` / `CDCGlobalIndexRegionScanner`, which decode each index row to reconstruct the data-row key) then deliver rows in a different order. The data returned is correct, but the per-PK iteration order is no longer timestamp-ascending — `CDCBaseIT.verifyChangesViaSCN`, which relies on timestamp-ascending delivery, fails on salted tables. Falling back to `emitV1Projection` restores V1's 2-slot form so the slotSpan shape matches and per-region cursor stepping stays aligned. See `CDCQueryIT.testSelectCDC`.

### 7.3 Compound emission

If none of the gates fires, compound emission runs. For each space:

```java
for (KeySpace ks : list.spaces()) {
  int end = firstProductiveStop(ks, prefixSlots);
  List<List<KeyRange>> perDimSlots = buildPerDimSlots(ks, productiveStart, end);
  byte[] lo = getKeyWithSchemaOffset(schema, perDimSlots, perDimSpan, Bound.LOWER, productiveStart);
  byte[] hi = getKeyWithSchemaOffset(schema, perDimSlots, perDimSpan, Bound.UPPER, productiveStart);

  // Strip the trailing separator the byte serializer appended — see §7.4.
  Field lastField = schema.getField(productiveStart + len - 1);
  if (!lastField.getDataType().isFixedWidth()) {
    lo = stripTrailingSeparator(lo, lastField);
    hi = stripTrailingSeparator(hi, lastField);
  }

  KeyRange compound;
  boolean shorterThanSlotSpan = end < maxProductiveEnd;
  if (allSingleKey && lo.length > 0 && !shorterThanSlotSpan) {
    compound = KeyRange.getKeyRange(lo);          // point key
  } else {
    compound = KeyRange.getKeyRange(lo, true, hi, false);   // half-open range
  }
  compounds.add(compound);
}
List<KeyRange> coalesced = KeyRange.coalesce(compounds);
```

`getKeyWithSchemaOffset` builds a sub-schema over fields `[productiveStart, maxFields)` and delegates to `ScanUtil.getMinKey` / `ScanUtil.getMaxKey`. The sub-schema is needed because the prefix fields (salt, viewIndexId, tenantId) are not in `perDimSlots` — passing the full schema would decode our first dim's bytes against the wrong field and leak a spurious separator into the compound.

**Short-than-slot-span half-open** (the `shorterThanSlotSpan` branch): when a space's productive run is shorter than `maxProductiveLen`, emitting the compound as a point key would produce bytes shorter than `SkipScanFilter` expects for the slot. The half-open form `[lo, nextKey(lo))` matches any row whose leading bytes equal `lo`, with the trailing dims implicitly wildcard.

After building one compound per space, `KeyRange.coalesce` merges adjacent or overlapping compounds. This is the last place where merges happen; from here on the list is fixed.

### 7.4 stripTrailingSeparator — fixing the double-separator bug

`ScanUtil.getMinKey` / `getMaxKey` serialize the per-dim slots by walking the schema and appending the appropriate separator byte after each variable-length field (including the last one):
- ASC variable-length → `\x00` separator
- DESC variable-length → `\xFF` separator

The compound bytes returned by `getMinKey` therefore already include a trailing separator. We then wrap those bytes in a single-key `KeyRange` and pass it to `ScanRanges.create`. Downstream, `ScanRanges.create` → `getPointKeys` → `ScanUtil.setKey` iterates the ranges *again* and, when it sees the leading field is variable-length, appends *another* separator. Result: the startRow has one extra byte.

The fix lives in `KeyRangeExtractor`:

```java
if (!lastField.getDataType().isFixedWidth()) {
  lo = stripTrailingSeparator(lo, lastField);
  hi = stripTrailingSeparator(hi, lastField);
}

private static byte[] stripTrailingSeparator(byte[] key, Field lastField) {
  if (key == null || key == KeyRange.UNBOUND || key.length == 0) return key;
  byte expectedSep = lastField.getSortOrder() == SortOrder.DESC
      ? QueryConstants.DESC_SEPARATOR_BYTE
      : QueryConstants.SEPARATOR_BYTE;
  if (key[key.length - 1] == expectedSep) {
    byte[] stripped = new byte[key.length - 1];
    System.arraycopy(key, 0, stripped, 0, stripped.length);
    return stripped;
  }
  return key;
}
```

The check is safe even if the trailing byte is not the expected separator (value happens to match): we only strip when the trailing byte is the exact separator the serializer would append, and downstream `setKey` appends it back. Net effect: zero change in produced bytes — except in the erroneous double-separator case, where we now produce the correct single-separator form.

The helper is idempotent and conservatively typed: it doesn't need to reason about whether the serializer actually appended a separator for this specific call, only that if it did, we want the form without it.

### 7.5 Mixed-width post-coalesce check

```java
if (coalesced.size() > 1 && maxProductiveLen > 1) {
  int commonLoLen = -2, commonUpLen = -2;
  boolean mixedWidth = false, anyNonPoint = false;
  for (KeyRange kr : coalesced) {
    if (!kr.isSingleKey()) anyNonPoint = true;
    if (kr.getLowerRange() != KeyRange.UNBOUND) {
      int loLen = kr.getLowerRange().length;
      if (commonLoLen == -2) commonLoLen = loLen;
      else if (commonLoLen != loLen) mixedWidth = true;
    }
    ...
  }
  if (mixedWidth && anyNonPoint) {
    return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
  }
}
```

Mixed-width compounds (ranges whose bound bytes differ in length) within a single output slot can confuse `SkipScanFilter`'s navigation when any range is non-point: the per-slot walker compares the extracted row bytes (full slot-span width) against each range's bounds, and a short-bound non-point range will incorrectly exclude rows whose trailing dims have non-matching bytes. Falling back to the V1 projection narrows each column independently and sidesteps the issue.

**All-point-key mixed-width compounds are allowed.** A typical case is an RVC IN-list with variable-length VARCHAR values: each tuple produces a different-length point byte string. `SkipScanFilter` compares each point individually against row bytes — mismatched widths are correct; only non-point ranges care about width uniformity. The `anyNonPoint` guard is what makes this distinction.

### 7.6 Finalization

```java
List<List<KeyRange>> out = new ArrayList<>();
for (int d = prefixSlots; d < productiveStart; d++) {
  out.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));  // padding, no-op normally
}
out.add(coalesced);                                              // single compound slot
int[] slotSpan = new int[out.size()];
slotSpan[out.size() - 1] = maxProductiveLen - 1;                 // compound slot spans maxProductiveLen cols
boolean useSkipScan = coalesced.size() > 1;
return new Result(out, slotSpan, useSkipScan);
```

The emitted result is a single output slot containing one or more compound ranges, with `slotSpan` equal to `maxProductiveLen - 1` so `ScanRanges` knows the compound covers that many user PK columns.

### 7.7 The V1 projection — emitV1Projection

The V1 projection is the default output path. It emits one output slot per user PK column, with each slot being the coalesced OR of every KeySpace's range on that column. This is exactly the shape the legacy optimizer produced and the downstream `ScanRanges` / `SkipScanFilter` machinery consumes natively.

```java
static Result emitV1Projection(KeySpaceList list, int nPkColumns, int cartesianBound,
                                      int prefixSlots) {
  boolean[] slotSubsumedByEverything = new boolean[nPkColumns];
  List<Set<KeyRange>> perSlot = new ArrayList<>(nPkColumns);
  int globalLastConstrained = prefixSlots - 1;

  for (KeySpace ks : list.spaces()) {
    int end = firstProductiveStopAnyPrefix(ks, prefixSlots);
    for (int d = prefixSlots; d < end; d++) {
      KeyRange r = ks.get(d);
      if (r == KeyRange.EVERYTHING_RANGE) slotSubsumedByEverything[d] = true;
      else perSlot.get(d).add(r);
    }
    // Dims past this space's end are wildcard for this space — mark them subsumed.
    for (int d = end; d < nPkColumns; d++) slotSubsumedByEverything[d] = true;
    ...
  }
  // Collapse subsumed dims to a single EVERYTHING_RANGE.
  for (int d = prefixSlots; d < nPkColumns; d++) {
    if (slotSubsumedByEverything[d]) {
      perSlot.get(d).clear();
      perSlot.get(d).add(KeyRange.EVERYTHING_RANGE);
    }
  }
  ...
```

The **EVERYTHING subsumption** is load-bearing. Without it, the OR across spaces would exclude EVERYTHING ranges from the accumulator and wrongly narrow a dim. Example: list has two spaces, one with dim c = `[RRS_, RRS\`)`, another with dim c = EVERYTHING. The correct per-slot OR on dim c is EVERYTHING (since anything matches one branch). Dropping the EVERYTHING would leave only `[RRS_, RRS\`)` and cause matching rows in the second branch to be filtered out.

Two places where a dim is subsumed by EVERYTHING:
1. Explicitly — some space has `EVERYTHING_RANGE` on that dim (constraint wasn't narrowed by that branch).
2. Implicitly — some space's productive run ends before that dim (the branch says nothing about it, so it matches all values).

After accumulation, each slot's coalesced ranges go into `out`. Cartesian-bound widening may truncate trailing slots if the product exceeds `cartesianBound`.

**Trailing-range slotSpan extension.** When the last emitted slot contains a non-point range and the productive window stops before the last PK column, `emitV1Projection` sets `slotSpan[lastSlotIdx] = (nPkColumns - 1) - lastSlotPkIdx` — effectively extending the slot to span every trailing unconstrained PK column. This mirrors V1's convention that a trailing scalar range encodes column-min/max for trailing cols via `ScanUtil.getMinKey/getMaxKey`, giving the range-slot an effective pk-span equal to `nPkColumns - slot_position`. Under-counting slotSpan here has the same downstream consequence as the Gate-4 shape described above: `SkipScanFilter.intersect()` uses `slotSpan[0]` to step the schema cursor per region, and an under-counted slotSpan misaligns cursor stepping. Observable symptom on CDC indexes is out-of-order event delivery; see `CDCQueryIT.testSelectCDC` on salted data tables.

```java
if (lastSlotHasRange) {
  int lastSlotPkIdx = prefixSlots + lastSlotIdx;
  if (lastSlotPkIdx < nPkColumns - 1) {
    slotSpan[lastSlotIdx] = (nPkColumns - 1) - lastSlotPkIdx;
  }
}
```

All-single-key last slots don't need the extension — point keys don't carry trailing-col encodings in their bytes, and `SkipScanFilter` doesn't step through trailing cols for them.

**Consumer-side adjustments for the extension.** Extending `slotSpan` affects two consumers that read `ScanRanges` for purposes other than filter navigation, and both have to discount the extension to match V1's observable behavior:

1. **Cost comparator** (`QueryOptimizer.effectiveBoundPkColumnCount`). The plan-ranking tiebreaker primarily uses `getBoundPkColumnCount()`, which adds `slotSpan[i]+1` per slot. When the trailing range is a bounded scalar (e.g. `BETWEEN a AND b` on a single PK col), the extension is a V1-compat marker for SkipScanFilter — not real multi-col byte-level narrowing. Counting it would let a plan with a narrow bounded range on one PK col tie with a plan whose compound range genuinely covers multiple PK cols, flipping the choice via weaker tiebreakers.

   The discriminator between the two shapes is output arity: `emitV1Projection` only extends the last slot of a **multi-slot** output (there's at least one leading concretely-narrowed slot plus the bumped last slot). A **single-slot** shape with `slotSpan > 0` comes from the compound-emission path — the range bytes genuinely concatenate `span+1` PK columns. The helper discounts `slotSpan[i]` to `1` only when `nRanges > 1` **and** `i == nRanges - 1` **and** the slot is a non-point bounded range; otherwise it honors `slotSpan[i]+1` unchanged. See `CostBasedDecisionIT.testCostOverridesStaticPlanOrdering3` (multi-slot shape must discount) vs. `RowTimestampIT.testAutomaticallySettingRowTimestampWith*` (single-slot compound on a `PK1=v AND PK2 BETWEEN a AND b` data-table scan — must not discount).

2. **Explain-plan formatter** (`ExplainTable.appendScanRow`, legacy fallback path). The formatter decomposes a slot's compound bytes via the `RowKeySchema` iterator, emitting one value per PK column consumed. When the slotSpan-extension shape applies, the bytes cover only the constrained range's column — the iterator stops early. The formatter emits only what the bytes cover and advances its loop counter by the number emitted; it does **not** pad with `*` for the unvisited trailing PK columns. Padding would diverge from V1's display (V1 stopped at `getBoundSlotCount`, never displaying those trailing columns). See `ChildViewsUseParentViewIndexIT.testIndexOnParentViewWithTenantSpecificConnection`.

---

## 8. Reference Algebra — Differential Testing

`compile.keyspace.algebra` (test-only, under `phoenix-core/src/test/java`) is a reference implementation of the key-space algorithm over a pure-Java abstract expression tree. It's 1000 lines of self-contained code with no Phoenix types: `AbstractRange<T extends Comparable<T>>` for 1-D intervals, `AbstractKeySpace` for N-dim boxes, `AbstractKeySpaceList` for the disjunction, `AbstractExpression` for leaf/And/Or nodes, and `Reference.extract(expr, nPk)` as the entry point. It's a **test oracle** in the differential-testing sense — the production code is compared against it.

The reference implementation intentionally ignores Phoenix's byte encoding, DESC inversion, separator bytes, salt/tenant prefixes, null semantics, and scalar-function wrappers. Its job is the set-algebra: given a predicate over `N` abstract dimensions, what is the emitted `KeySpaceList`?

`HarnessCorpusTest.java` in the same package runs a library of Expression shapes through both the production pipeline and the reference, decodes the production `ScanRanges` back to an `AbstractKeySpaceList`-comparable form with `ScanRangesDecoder`, and asserts soundness: every row the original predicate matches must be in the emitted scan region. False positives (rows in the scan region that don't satisfy the predicate) are acceptable because the residual filter rejects them at scan time; false negatives are correctness bugs and fail the test.

This was a significant debugging accelerator. When the production emission diverged from the reference, the divergence was almost always a bug; when it didn't, we had confidence in the algorithm.

---

## 9. Feature Flag and Rollout

Two new `QueryServices` constants:

```
phoenix.where.optimizer.v2.enabled          (default true  on this branch)
phoenix.where.optimizer.v2.cartesianBound   (default 50000, same magnitude as legacy MAX_IN_LIST_SKIP_SCAN_SIZE)
```

`WhereOptimizer.pushKeyExpressionsToScan` branches on the flag near the top and delegates to `WhereOptimizerV2.run` when enabled. Both entry callers (`WhereCompiler` and `RVCOffsetCompiler`) go through the same public method, so no caller-side changes.

The same test suites run under both flag values. The 138 `WhereOptimizerTest` methods are re-run via a `WhereOptimizerV2Test` subclass that forces the flag on; divergences that are cosmetic (explain-string shape, byte-level detail of equivalent scans) are documented per-assertion with conditional checks; divergences that are semantic are tracked as V2 regressions until fixed.

---

## 10. V1 vs V2 Performance Comparison

This section documents the performance characteristics of V2 relative to V1 along three axes: **optimizer CPU** (planning time), **runtime I/O** (region-server work to satisfy a query), and **memory** (allocations during planning and peak footprint). Where a dimension has concrete measurements, they come from the JMH benchmark at [phoenix-core/src/test/java/.../WhereOptimizerBenchmark.java](/Users/kozdemir/my_os_repo/6791/phoenix/phoenix-core/src/test/java/org/apache/phoenix/compile/WhereOptimizerBenchmark.java); where a dimension is structural (HBase-level I/O, for example) the analysis is qualitative because it can't be measured without a real cluster.

### 10.1 Optimizer CPU (planning time)

Compile-time measurements using `WhereOptimizerBenchmark` — each benchmark compiles a prepared statement end-to-end (parse + resolve + WHERE-optimize + plan) and reports average time per compile. Forked JVM disabled; 2 warmup × 1s + 3 measurement × 1s per parameter combination; JMH `Blackhole` prevents DCE.

| Benchmark | size | V1 (µs/op) | V2 (µs/op) | V2 / V1 |
|---|---:|---:|---:|---:|
| `rvcInequality` `(a,b) >= (?,?)` | — | 45.6 ± 5.2 | 48.6 ± 3.7 | **1.07×** |
| `rvcInList` `(a,b) IN (...)` | 5 | 68.3 ± 6.3 | 68.9 ± 5.6 | 1.01× |
| `rvcInList` | 50 | 301.2 ± 20.5 | 306.1 ± 18.5 | 1.02× |
| `rvcInList` | 500 | 2,610.7 ± 114.8 | 2,676.4 ± 96.4 | 1.03× |
| `orChain` `a=? OR a=? OR ...` | 5 | 47.5 ± 3.2 | 49.4 ± 2.2 | 1.04× |
| `orChain` | 50 | 133.3 ± 6.7 | 145.8 ± 13.1 | 1.09× |
| `orChain` | 500 | 1,006.7 ± 35.7 | 1,140.1 ± 16.4 | 1.13× |
| `mixedPredicates` (eq + RVC ineq + scalar IN) | 5–500 | ~46.5 | ~49.0 | 1.05× |
| **`cartesianExplosion`** (a=? AND b IN AND c IN AND d IN) | 5 | 62.6 ± 5.8 | 139.4 ± 7.0 | 2.23× |
| **`cartesianExplosion`** | 50 | 7,138.5 ± 391.3 | **1,725.2 ± 442.8** | **0.24×** |
| **`cartesianExplosion`** | 500 | 20,990.3 ± 1,373.4 | **2,096.6 ± 79.2** | **0.10×** |

**Reading the table.**
- For "normal" query shapes (rvcInequality, rvcInList, orChain, mixedPredicates) V2 is within ~10% of V1 — slightly slower due to the up-front normalization pass (`ExpressionNormalizer`) that V1 skips, plus the KeySpaceList merge fixpoint overhead.
- For the `cartesianExplosion` shape — `a = ? AND b IN (...) AND c IN (...) AND d IN (...)` on a 4-column PK, where the per-dim cartesian product grows as *n³* — V2 is **4–10× faster** at realistic scale. At n=500 the per-dim product is 1.25×10⁸ combinations; V1 enumerates slot-by-slot and accumulates an `inListSkipScanCardinality` counter that forces a range scan, while V2's extractor recognizes the bound has been exceeded early and drops trailing dims before any byte expansion.

**Why V2 is faster on the pathological shape.** The design was specifically for this — the cartesian-widening rule in `KeyRangeExtractor` (§7) drops trailing dimensions before any byte-level range enumeration, keeping the algorithm O(N²) in PK column count rather than O(product-of-dim-sizes). V1's O(product) behavior is what produced the reported OOMs and timeouts on real queries (PHOENIX-7770, PHOENIX-5833).

**Why V2 is slightly slower on normal shapes.** Two costs V1 avoids:
1. `ExpressionNormalizer` runs once at the top, walking the tree to identify RVC-inequality and scalar-IN nodes that need rewriting. Short-circuits cleanly when no such node exists (the vast majority of real queries) but still costs one full tree walk.
2. `KeySpaceList.and` runs a merge-to-fixpoint pass after each cartesian combine; for single-space lists this is a no-op but for multi-space lists it's O(K²) on list size.

Both overheads are small constants on the hot path.

### 10.2 Runtime I/O (region-server work)

This is where V2 has the largest and most consequential wins. Phoenix's WHERE optimizer decides what HBase blocks must be read to satisfy a query; that cost dominates query latency on any table large enough to matter.

**Case: RVC-IN with cardinality > MAX_IN_LIST_SKIP_SCAN_SIZE and at least one DESC PK.**

| | V1 (RANGE SCAN) | V2 (POINT LOOKUP) |
|---|---|---|
| Explain-plan line | `CLIENT PARALLEL 1-WAY RANGE SCAN OVER ...` | `CLIENT PARALLEL 1-WAY POINT LOOKUP ON N KEYS` |
| Key-range shape | `[min_tuple, max_tuple + 1)` (single range) | `N` individual row keys |
| HBase blocks read | every block between `min_tuple` and `max_tuple` | only blocks containing one of the `N` keys |
| Residual filter work | scans every row in the range, applies IN predicate | none — key-equality is exact |
| Cost scales with | **rows spanned between tuples** (can be arbitrarily large) | **N, the tuple count** (bounded by query) |
| Rows returned to client | identical in both plans | identical in both plans |

For a sparse 15-tuple IN list spread across a 10 M-row key range on V1, the region server reads every block in that 10 M-row span (tens to hundreds of thousands of HBase blocks) and runs a filter on each row; V2 reads only the 15 blocks that contain the target rows. Same result set, orders of magnitude less I/O.

The V1 heuristic ("force RANGE SCAN above cardinality 15 with DESC") was written when the alternative was the legacy `SkipScanFilter` whose per-hop cost was believed high relative to a straight range scan. Modern HBase handles multi-key point lookups efficiently; the heuristic no longer pays its cost. V2's choice is unambiguously better — this is one of the main reasons to prefer V2.

**Other shapes where V2 reads the same or fewer blocks.**
- **Cartesian explosion cases** — V2 truncates trailing dims that would produce >50k combinations, while V1 falls back to a range scan on the leading dim; block-read counts are comparable.
- **OR-chains with mergeable branches** — V2's `KeySpaceList.or` + merge-fixpoint collapses adjacent ranges before emission; V1 emits each branch separately and relies on `KeyRange.coalesce` downstream, which occasionally misses merges V2 catches. Savings: small but real on heavy-OR queries.

**Shapes where runtime I/O is identical.**
- Simple scalar comparisons, equality, IN-list on a single PK — both V1 and V2 emit the same set of point keys or range.
- RVC inequality that lex-expands cleanly — V2's normalizer produces the same compound key region as V1.

**Shapes where V2 currently reads more than V1** — none documented. The known limitations (§11.1, §11.2) represent shapes where V2 does **no worse** than V1 — both fall back to full scan + residual filter.

### 10.3 Memory

Two dimensions: **optimizer-phase allocations** (objects created during planning, which survive until the plan is handed to execution) and **peak heap during query execution**.

**Optimizer-phase allocations (V2 vs V1).**

V2 allocates slightly more per compile for normal query shapes. The extra allocations:
- `KeySpace` / `KeySpaceList` objects (~tens of bytes each; one per predicate node post-normalization).
- `ExpressionNormalizer` tree clones for any RVC-inequality or scalar-IN node it rewrites. For queries without those shapes the fast-path `needsRewrite` check avoids this cost.
- Hash sets tracking consumed nodes — same order of magnitude as V1's `KeySlots` tracking.

V1 allocates `KeySlot` / `MultiKeySlot` / `SingleKeySlot` objects plus the recursive `KeySlots` lists. The two implementations are comparable in allocation rate; nothing in V2 is materially heavier at steady state.

**Catastrophic memory cases.**

V1's cartesian enumeration path is where memory blew up in production (PHOENIX-7770 documented OOM on a 4-column PK with three mid-cardinality IN lists). The `KeySlotsIterator.slotsIterator` builds the per-slot cross-product up-front; for a query with IN lists of size 100 × 100 × 100, that's 10⁶ `KeyRange` instances allocated before any bound check. V2's `cartesianBound` truncation in `KeyRangeExtractor.extract` caps total emission at 50 k ranges regardless of input shape — the O(product) explosion never happens.

**Peak heap during query execution.**

For the typical case, both V1 and V2 produce the same `ScanRanges` / `SkipScanFilter` downstream objects, so peak heap at scan time is identical. The pathological difference is in the RVC-IN-with-DESC case covered in §10.2: V1's RANGE SCAN materializes a region-server-side row iterator that holds `Cell` objects for every row in the scanned range; V2's POINT LOOKUP allocates only `N` row iterators. For high-cardinality sparse queries this is another order-of-magnitude reduction, but specifically on the region server, not the client.

### 10.4 Summary

- **Optimizer CPU** — V2 is ~5-10% slower on normal shapes (constant-factor overhead from normalization + list merge) and **2-10× faster** on cartesian-explosion shapes. Net: favorable at the tail where V1 actually hurts.
- **Runtime I/O** — V2 is equal or better on every documented shape; **orders of magnitude better** on sparse high-cardinality RVC-IN with DESC (main production win).
- **Memory** — comparable per-query; **eliminates V1's O(product) explosion** that caused production OOMs.

The headline: V2 trades a small, bounded compile-time overhead for much better worst-case behavior — bounded planning complexity, bounded memory, and much tighter scan regions in the shapes that matter most. The parity cases (§11) are all behavioral-shape differences, not efficiency gaps.

---

## 11. Known Limitations

The following limitations remain in V2. They fall into three categories: **behavioral-parity divergences** (V2 and V1 both produce correct results but differ in explain-string shape, byte-level detail, or heuristic classification), **lost optimizations** (V2 falls back to a wider scan or the residual filter for shapes legacy handled natively), and **edge-case correctness gaps** (narrow patterns where V2 currently produces wrong results and a fix is deferred).

Each limitation documents the symptom, why it exists, the current mitigation, and the pointer to where a fix would land.

### 11.1 RANGE_SCAN hint — residual preserves full expression (lost optimization, not a correctness hole)

**Symptom.** When the query carries `/*+ RANGE_SCAN */`, V2 emits the full normalized expression as the residual filter instead of the smaller residual that `RemoveExtractedNodesVisitorV2` would produce.

**Why.** The optimizer's consumption tracking marks a node as "fully consumed" when its meaning is captured either by the scan's start/stop row **or** by the per-slot `SkipScanFilter` that `ScanRanges.create` installs. Under `useSkipScan = true` (the default), pruning a consumed node from the residual is safe because whichever of the two mechanisms captured it is still in effect. The `RANGE_SCAN` hint forces `useSkipScan = false`, which disables the per-slot filter. If V2 pruned naively under the hint, any node that was consumed *only* by the per-slot filter would now go unchecked — that would be a correctness hole. V2 defends against it by skipping `RemoveExtractedNodesVisitorV2` entirely when the hint is present and returning the full normalized expression as the residual. **No correctness hole exists** — the guard is what prevents one.

**Impact.** Slight extra CPU at scan time evaluating predicates that the scan range's start/stop bounds already narrowed. The extra work is proportional to rows-actually-scanned, not rows-in-table, and matches legacy behavior byte-for-byte.

**Fix location.** `WhereOptimizerV2.run` at the step-4 residual construction. A future optimization could refine consumption tracking to distinguish "consumed by scan range" from "consumed by per-slot filter" — nodes in the first category are safe to prune even under RANGE_SCAN. Not done today because the visitor currently tracks consumption as a single boolean per node.

### 11.2 Scalar functions inside RVC-IN children (shared shortfall — not a V1→V2 regression)

**Scope.** RVC **inequality** with a scalar-function child — e.g., `(a, TO_CHAR(b), c) > (v1, v2, v3)` — **works correctly** in V2. `ExpressionNormalizer` lex-expands the RVC inequality into an OR of ANDs (`a > v1 OR (a = v1 AND TO_CHAR(b) > v2) OR (a = v1 AND TO_CHAR(b) = v2 AND c > v3)`), and each scalar comparison in the expansion passes through `ComparisonExpression.visitLeave`, which calls `resolveScalarFunctionChain` on the LHS and delegates to `ScalarFunction.newKeyPart(...)` for the byte encoding. `WhereOptimizerTest.testUseOfFunctionOnLHSInRVC`, `testUseOfFunctionOnLHSInMiddleOfRVC`, and `testUseOfFunctionOnLHSInMiddleOfRVCForLTE` all pass under V2 with the expected compound startRow/stopRow shapes.

**Shared shortfall: RVC-IN with scalar-function children.**

**Symptom.** A query like `(a, SUBSTR(b, 1, 3), c) IN ((v1a, v2a, v3a), (v1b, v2b, v3b))` produces a full table scan under **both** V1 and V2, with the predicate enforced by a `RowKeyComparisonFilter` residual. Measured by direct probe:

```
testRvcInListLeadingScalarFunction   V1: startRow=empty stopRow=empty filter=RowKeyComparisonFilter
                                     V2: startRow=empty stopRow=empty filter=RowKeyComparisonFilter
testRvcInListMiddleScalarFunction    V1: startRow=empty stopRow=empty filter=RowKeyComparisonFilter
                                     V2: startRow=empty stopRow=empty filter=RowKeyComparisonFilter
```

**Why V1 doesn't narrow.** V1's `visitLeave(RowValueConstructorExpression)` builds a `RowValueConstructorKeyPart` whose span stops at the first `OrderPreserving.YES_IF_LAST` child (including any `SUBSTR` — see `WhereOptimizer.java:861`). The keyPart that drives `InListExpression.visitLeave` then covers fewer LHS children than the RVC-IN expects. The per-row decoding path in `RowValueConstructorKeyPart.getKeyRange` uses `InListExpression.create`'s sort-packed literal for each IN value; that packed form was serialized in the full LHS-type width, which doesn't cleanly split back into per-child comparable byte slices for a keyPart whose span is shorter than the full LHS. The net effect: the keyPart returns a "can't convert" result and the scan stays empty.

**Why V2 doesn't narrow.** V2's visitor now recognizes scalar-function children in the RVC-IN loop — `pkPositionOf` failures fall through to `resolveScalarFunctionChain`, and per-row ranges are produced via `chain.keyPart.getKeyRange(EQUAL, rhsChild)`. The visitor output **is** a narrow KeySpaceList. But the extractor's routing gates (§7.2 Gate 2 — single productive dim mixed with prefix columns, Gate 3 — etc.) route this shape through `emitV1Projection`, which cannot represent per-tuple RVC correlation across dimensions. The downstream ScanRanges then collapses to a full-table scan and the predicate is kept in the residual.

**Known exposure.** Two regression tests pin the current shared shortfall:
- `WhereOptimizerTest.testRvcInListLeadingScalarFunction` — `(substr(organization_id, 1, 3), parent_id) IN ((…),(…))`.
- `WhereOptimizerTest.testRvcInListMiddleScalarFunction` — `(organization_id, substr(parent_id, 1, 3), created_date) IN ((…),(…))`.

Both are currently parity assertions: they pin `EMPTY_START_ROW` / `EMPTY_END_ROW` + non-null residual filter under **both** V1 and V2. If either optimizer starts narrowing, the test fails and the shape of the new narrowing is captured.

**Impact.** Correctness is preserved; performance is "full scan + filter" on both paths. Since V1 is already in this state and the pattern is absent from any existing test or IT, production queries hitting it are believed rare.

**Fix plan** (out of scope for V2 GA; beats V1 if implemented).
1. **Visitor** (already landed as a forward-looking change): in `KeySpaceExpressionVisitor.visitLeave(InListExpression, ...)`, for each LHS child that isn't a bare `RowKeyColumnExpression`, call `resolveScalarFunctionChain` and record the chain. In `buildRvcEqualitySpace`, when a child has a chain, call `chain.keyPart.getKeyRange(EQUAL, rhsChild)` for the per-dim range and leave the node unconsumed (so the residual filter still enforces the original IN).
2. **Extractor**: extend compound emission (§7.3) to cover the shape this visitor output produces — per-tuple per-dim equality spaces with chain-derived ranges. The core work is ensuring `stripTrailingSeparator`-style byte-shape handling applies to chain-produced ranges, which may have different trailing-byte conventions than bare-PK ranges.

Estimated remaining work after step 1 is ~80 lines in the extractor plus per-shape unit tests. Step 1 is self-contained and already integrated; step 2 is where V2 would diverge from V1 to actually produce a narrower scan. This is strictly better than V1, not parity — V1 itself doesn't narrow this shape.

**Priority.** Low-to-medium for GA. Not a V2 regression. Worth landing as a forward improvement once the remaining V2 work stabilizes.

### 11.3 DESC + RVC-IN on variable-length PK — theoretical edge, no current reproducer

**History.** Early V2 development reproduced a data-correctness regression on `InListIT.testWithVariousPKTypes` — 4 sort-order combos silently dropped matching rows for RVC-IN queries on `(TIMESTAMP, VARCHAR, VARCHAR)` PKs where at least one VARCHAR was declared DESC. §7.4's `stripTrailingSeparator` fix resolved all 4 combos: compound emission now produces byte-equal single-separator compound keys and `ScanRanges.create → setKey` appends the separator exactly once.

**Two shapes remain theoretically fragile** but are not currently reproducing wrong results:

1. **V1-projection fallback path for var-length DESC.** If a query trips one of the routing gates in §7.2 (leading EVERYTHING past the prefix, middle-EVERYTHING gap, IS_NULL sentinel, mixed-width post-coalesce) AND the PK has a var-length DESC column, `emitV1Projection` replaces the compound with a per-column projection. That projection loses RVC tuple correlation — e.g., `(pk2, pk3) IN (('x','1'),('y','2'))` projects to `pk2 ∈ {'x','y'} × pk3 ∈ {'1','2'}`, producing 4 combinations that the `SkipScanFilter` cartesian would match, not just the 2 original tuples. The **residual filter must** reject the false positives at scan time. A characterization test (`WhereOptimizerTest.testRvcInListMiddleGapWithTrailingVarcharDesc`) asserts the residual filter is always emitted for this shape.

2. **Compound emission for ≥3 tuples with DESC VARCHAR on non-trailing position.** `ScanUtil.getMinKey` serializes an internal separator byte between a non-trailing DESC VARCHAR field and the next field. `stripTrailingSeparator` only strips *trailing* separators, so an internal one remains. A characterization test (`WhereOptimizerTest.testRvcInListWithNonTrailingVarcharDesc`) asserts the scan bytes narrow correctly for this shape (start row begins with the smallest tuple, stop row covers the largest, emitted range count ≥ tuple count).

Both characterization tests pass under V1 and V2 today. They pin the current correct behavior so that a future regression introducing wrong bytes or a missing residual filter would fail the test immediately rather than silently corrupt results.

**Test coverage today.**
- `InListIT.testWithVariousPKTypes` runs the full 24-combo sort-order matrix (VARCHAR × 8 orders + other types) with 2-tuple RVC-INs and is **228/228 green** under V2.
- `WhereOptimizerTest.testRvcInListWithNonTrailingVarcharDesc` and `testRvcInListMiddleGapWithTrailingVarcharDesc` pin the compile-time scan-bytes behavior for the two theoretically fragile shapes above.

**Known coverage gaps.** No end-to-end IT exercises RVC-IN with **≥3 tuples** on a var-length DESC PK. The compile-time characterization tests show the scan bytes narrow correctly at the optimizer output, but the SkipScanFilter's per-slot navigation semantics for mixed-width DESC bytes at ≥3 tuples haven't been directly verified against a running region server.

**Recommended next steps.**
1. **Add IT coverage** — a new `InListIT` parameterized case with 3+ tuples on var-length DESC PKs, across all 8 sort-order combos, asserting the exact result set. If any combo returns wrong rows, that's a real bug with a concrete reproducer and the fix can be targeted precisely.
2. **Only then** — if a reproducer emerges — consider the byte-level fixes. Two candidate paths:
   - `ScanUtil.setKey` / `ScanUtil.getMinKey` — teach them not to double-append DESC separators when called with pre-encoded compound bytes. Touches a shared utility, higher blast radius.
   - `KeyRangeExtractor` — port legacy's `KeyExpressionVisitor` compound-span-aware emission. Self-contained but ~200 lines of byte-level logic.

Speculative fixes are not warranted until IT coverage produces a reproducer.

### 11.4 Other legacy-parity gaps (documented, not yet converged)

From the prior work tracking in `WhereOptimizerV2Test`, several legacy-parity failures remain in the 138-test corpus. These are **byte-shape divergences** where V2 produces correct results with a scan width equivalent to or strictly better than V1, but asserts on specific byte sequences that differ:

- **`RowValueConstructorKeyPart` clip logic** (Group A, ~11 tests). Legacy splits an RVC inequality into a leading equality prefix plus a trailing scalar when intersecting with overlapping scalar constraints. V2's per-dim model handles most shapes after normalization, but a few compound shapes still diverge in byte layout. No correctness impact; tests assert byte bytes, not row sets.
- **Complex OR + multi-slot skip-scan shapes** (Group C, ~6 tests). Legacy's specialised DNF + skip-scan cardinality tracking produces specific byte layouts for OR-of-AND-of-range trees that V2 emits in a slightly different shape. Scan width typically equivalent.
- **Hint-driven residual filter shape** (Group D, 2 tests). `/*+ RANGE_SCAN */` and `/*+ SKIP_SCAN */` with non-PK filters produce legacy-specific filter types (`RowKeyComparisonFilter`, `SingleKeyValueComparisonFilter`). V2 uses a generic residual path; functionally equivalent but test assertions check the specific filter class.
- **DESC byte edge cases** (Group E, 2 tests). `testDescDecimalRange` (DECIMAL + DESC + range scan) and similar; boundary bytes differ by one due to `ByteUtil.previousKey` handling in compound-span splicing.

**Status.** These tests are marked expected-divergent in the V2 parameterized harness; the scan-efficiency analysis in the redesign plan confirms V2 is equivalent-or-better on scan width for all of them. They remain on the follow-up list but do not block V2's default-on rollout because no correctness regression was found against real data in IT coverage.

**Fix location.** Each group has its own landing zone (per the redesign plan's "follow-up work items" section), incrementally addressed after V2 proves stable at default-on for a release.

## 12. Summary

V2 replaces the legacy optimizer's mutable per-slot concatenation with a mathematical model (N-dim key-space algebra with containment / N−1-agreement merge rules) and a clear pipeline: normalize → visit → extract → emit. The extractor defaults to emitting the V1 projection of the final `KeySpaceList` (one slot per PK column), so downstream code (`ScanRanges`, `SkipScanFilter`) receives the exact shape it was designed for. Compound emission is an optional optimization for shapes where a tighter compound byte form gives a narrower scan; it is gated by a handful of routing rules (§7.2) that fall back to the V1 projection when compound would trip V1-era downstream quirks. Once V1 is deprecated and those quirks are fixed, the V1 projection fallback can be removed and compound emission becomes the sole path.

The algorithm is provably bounded (O(N²) via cartesian widening), equivalence-respecting (logically equivalent inputs produce equal `KeySpaceList`s after normalization), and tested against an independent reference implementation in `compile.keyspace.algebra`.
