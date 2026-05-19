/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile.keyspace;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.util.ScanUtil;

/**
 * Converts a final {@link KeySpaceList} into the shape
 * {@link org.apache.phoenix.compile.ScanRanges#create} consumes:
 * {@code List<List<KeyRange>> ranges}, {@code int[] slotSpan}, {@code boolean useSkipScan}.
 * <p>
 * <b>The V1 projection (default output shape).</b> The legacy optimizer produced one
 * {@link KeyRange} list per PK column ("slot"): the disjunction of every narrowing the
 * WHERE clause places on that column. {@link org.apache.phoenix.compile.ScanRanges}
 * and {@link org.apache.phoenix.filter.SkipScanFilter} are built against that shape.
 * V2 computes its narrowing as a {@link KeySpaceList} (disjunction of N-dim boxes)
 * and, by projecting each {@link KeySpace} onto each PK column and coalescing per column,
 * produces the same V1-compatible shape. This is the role of
 * {@link #emitV1Projection}. The method name reflects its job: it's the boundary layer
 * where V2's N-dim key-space algebra is converted into V1's per-slot disjunctions so
 * the existing downstream machinery can consume it unchanged.
 * <p>
 * <b>Compound emission (optional optimization).</b> For some shapes a tighter scan is
 * possible by concatenating per-dim bytes into a single compound {@link KeyRange} per
 * {@link KeySpace} — preserving cross-dim tuple correlation at the byte level. Compound
 * emission uses one output slot with {@code slotSpan = maxProductiveLen - 1}; the start
 * and stop rows then narrow to the exact compound interval (e.g. a 15-tuple RVC-IN
 * becomes a POINT LOOKUP on 15 compound keys rather than a SkipScan over 15 per-column
 * disjunctions). When compound emission is unsafe — single productive dim, IS_NULL
 * sentinels, middle-EVERYTHING gap, mixed-width coalesced ranges with non-point values
 * — the extractor falls back to {@link #emitV1Projection}.
 * <p>
 * Once the legacy V1 optimizer is removed and downstream utilities ({@code ScanUtil.setKey},
 * {@code ScanRanges.create}'s special cases for {@code IS_NULL_RANGE}, etc.) are
 * simplified to match the compound shape natively, the per-slot fallback can be
 * deleted and compound emission becomes the sole path.
 * <p>
 * Correctness guarantee (for both paths): for every row the original predicate matches,
 * the emitted scan contains it. False positives (rows in the emitted scan that don't
 * satisfy the predicate) are handled by the residual filter. The cartesian-bound
 * widening rule (drop trailing dims when the list size would exceed a threshold) is
 * applied inside the extractor for the per-slot path and upstream in
 * {@link KeySpaceList} for the compound path.
 * <p>
 * Prefix slots (salt byte, view-index id, tenant id) are prepended by {@link WhereOptimizerV2}
 * at CNF-build time; this class emits only the user tail.
 */
public final class KeyRangeExtractor {

  /** Result of an extraction pass, shaped exactly like the inputs to {@code ScanRanges.create}. */
  public static final class Result {
    public final List<List<KeyRange>> ranges;
    public final int[] slotSpan;
    public final boolean useSkipScan;

    public Result(List<List<KeyRange>> ranges, int[] slotSpan, boolean useSkipScan) {
      this.ranges = ranges;
      this.slotSpan = slotSpan;
      this.useSkipScan = useSkipScan;
    }

    public boolean isNothing() {
      return ranges.size() == 1 && ranges.get(0).size() == 1
        && ranges.get(0).get(0) == KeyRange.EMPTY_RANGE;
    }

    public boolean isEverything() {
      return ranges.isEmpty();
    }
  }

  public static Result everything() {
    return new Result(Collections.<List<KeyRange>>emptyList(), new int[0], false);
  }

  public static Result nothing() {
    return new Result(
      Collections.<List<KeyRange>>singletonList(Collections.singletonList(KeyRange.EMPTY_RANGE)),
      ScanUtil.SINGLE_COLUMN_SLOT_SPAN, false);
  }

  private KeyRangeExtractor() {
  }

  /**
   * Legacy entry point used by tests that don't have a schema handy. Emits per-slot output
   * (pre-compound-emission behavior). Kept for test compatibility.
   */
  public static Result extract(KeySpaceList list, int nPkColumns, int cartesianBound) {
    return emitV1ProjectionStopAtGap(list, nPkColumns, cartesianBound, 0);
  }

  /**
   * Legacy entry point for schema-less tests with prefix slots.
   */
  public static Result extract(KeySpaceList list, int nPkColumns, int cartesianBound,
    int prefixSlots) {
    return emitV1ProjectionStopAtGap(list, nPkColumns, cartesianBound, prefixSlots);
  }

  /**
   * Compound-emission entry point: emits one compound {@link KeyRange} per {@link KeySpace}
   * in the list, into a single output slot with {@code slotSpan = maxProductiveLen - 1}.
   * Requires a schema to concatenate per-dim bytes with correct separator handling.
   */
  public static Result extract(KeySpaceList list, int nPkColumns, int cartesianBound,
    int prefixSlots, RowKeySchema schema) {
    if (schema == null) {
      return emitV1ProjectionStopAtGap(list, nPkColumns, cartesianBound, prefixSlots);
    }
    if (list.isUnsatisfiable()) {
      return nothing();
    }
    if (list.isEverything()) {
      return everything();
    }

    // Scan spaces to find the widest productive extent and whether every space has a
    // middle-EVERYTHING gap. When every space has a middle gap past the prefix, emit
    // per-slot so SkipScanFilter can narrow the trailing dim independently. When only
    // some spaces have middle gaps, compound-emit each space independently so the
    // non-middle-gap spaces can anchor a tight compound startRow.
    int minProductiveStart = nPkColumns;
    int maxProductiveEnd = prefixSlots;
    boolean allSpacesHaveMiddleGap = true;
    for (KeySpace ks : list.spaces()) {
      int start = firstConstrainedDim(ks, prefixSlots);
      if (start < 0) {
        // Space is EVERYTHING past the prefix — whole list is EVERYTHING from our view.
        return everything();
      }
      if (start < minProductiveStart) minProductiveStart = start;
      int endStrict = firstProductiveStopStrict(ks, prefixSlots);
      int endAny = firstProductiveStopAnyPrefix(ks, prefixSlots);
      if (endAny > maxProductiveEnd) maxProductiveEnd = endAny;
      // Detect a middle gap by comparing the strict stop (first EVERYTHING past prefix)
      // against the any-prefix stop (last constrained dim past prefix). They diverge
      // iff there's an EVERYTHING dim BEFORE the last constrained dim, i.e. a gap.
      boolean hasMiddleGap = start == prefixSlots && endStrict < endAny;
      if (!hasMiddleGap) {
        allSpacesHaveMiddleGap = false;
      }
    }

    // Leading EVERYTHING past the prefix, or EVERY space has a middle gap: emit per-slot.
    // The per-slot SkipScanFilter handles narrowing past the gap and ScanRanges reports
    // boundPkColumnCount correctly for the local-index-pruning heuristic.
    if (minProductiveStart > prefixSlots || allSpacesHaveMiddleGap) {
      return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
    }


    // Single-space, single-productive-dim: trivial case with no compound benefit.
    // Using compound emission would pre-build bytes with separators, then ScanRanges.create
    // (via getPointKeys -> ScanUtil.setKey) re-appends separator bytes for DESC fields,
    // producing wider-than-correct scan. Per-slot emission lets ScanRanges process the
    // range once with the real schema, matching V1's byte output exactly.
    if (list.spaces().size() == 1 && (maxProductiveEnd - prefixSlots) == 1) {
      return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
    }

    // If any space has IS_NULL_RANGE / IS_NOT_NULL_RANGE at ANY productive dim, route
    // to per-slot emission so ScanRanges receives the IS_NULL / IS_NOT_NULL sentinel
    // intact. ScanRanges.create has special-case handling for IS_NULL_RANGE (producing
    // the correct empty-bytes + separator boundary). Compound emission would collapse
    // the sentinel into either a degenerate half-open range or a zero-length
    // single-key at the wrong byte position, both of which produce scan rows that
    // skip actual null-value rows.
    for (KeySpace ks : list.spaces()) {
      for (int d = prefixSlots; d < ks.nDims(); d++) {
        KeyRange dim = ks.get(d);
        if (dim == KeyRange.IS_NULL_RANGE || dim == KeyRange.IS_NOT_NULL_RANGE) {
          return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
        }
      }
    }

    // Mixed comparator safety gate: SkipScanFilter uses a single BytesComparator per
    // compound slot, derived from schema.getField(rowKeyPosition) — i.e. the slot's
    // LEADING field. When the compound spans multiple fields that require different
    // comparators (e.g. ASC fixed-width BIGINT leading + DESC variable-width DECIMAL
    // trailing), the leading-field comparator produces wrong results for the trailing
    // field's bytes. Concretely: DESC var-width uses DescVarLengthFastByteComparisons
    // which handles the variable-length DESC sort correctly; plain lex comparison
    // (ASC fixed) does not. Fall back to per-slot emission so each slot gets its own
    // comparator. See SortOrderIT.testSkipScanCompare.
    if (prefixSlots < maxProductiveEnd) {
      org.apache.phoenix.schema.ValueSchema.Field leadingField = schema.getField(prefixSlots);
      org.apache.phoenix.util.ScanUtil.BytesComparator leadingCmp =
        org.apache.phoenix.util.ScanUtil.getComparator(leadingField);
      for (int d = prefixSlots + 1; d < maxProductiveEnd; d++) {
        org.apache.phoenix.schema.ValueSchema.Field f = schema.getField(d);
        if (org.apache.phoenix.util.ScanUtil.getComparator(f) != leadingCmp) {
          return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
        }
      }
    }

    // (Compound-too-wide safety gate moved below, after compound window is computed.)

    // IN-list-on-leading + range-on-trailing gate: when the leading productive dim is a
    // single-key per space but points differ across spaces (classical IN-list shape)
    // AND some later dim has a non-single-key range, route to per-slot emission. V2's
    // compound-per-space form collapses the two-dimensional shape `[IN-list] × [range]`
    // into one compound slot with {@code slotSpan = compoundLen - 1}, which under-
    // counts PK columns covered (trailing unconstrained cols aren't accounted for).
    // Server-side consumers that walk scans per-slot (e.g. {@link org.apache.phoenix
    // .coprocessor.UncoveredGlobalIndexRegionScanner} /
    // {@link org.apache.phoenix.coprocessor.CDCGlobalIndexRegionScanner} decode
    // index-row keys to reconstruct data-row keys; ordering depends on how
    // {@link org.apache.phoenix.filter.SkipScanFilter#intersect} steps the schema
    // cursor, which reads {@code slotSpan[0]}) rely on the per-slot shape. Per-slot
    // emission produces `[IN-list]` on slot 0 and `[range]` on slot 1 with the correct
    // trailing-col coverage in slotSpan. See CDCQueryIT.testSelectCDC with salted
    // data tables.
    if (prefixSlots < maxProductiveEnd && list.spaces().size() >= 2) {
      boolean leadingIsInList = true;
      KeyRange firstLeading = null;
      boolean leadingDiffers = false;
      for (KeySpace ks : list.spaces()) {
        KeyRange leading = ks.get(prefixSlots);
        if (!leading.isSingleKey() || leading == KeyRange.IS_NULL_RANGE
          || leading == KeyRange.IS_NOT_NULL_RANGE) {
          leadingIsInList = false;
          break;
        }
        if (firstLeading == null) {
          firstLeading = leading;
        } else if (!firstLeading.equals(leading)) {
          leadingDiffers = true;
        }
      }
      if (leadingIsInList && leadingDiffers) {
        boolean laterRangeExists = false;
        outer:
        for (KeySpace ks : list.spaces()) {
          for (int d = prefixSlots + 1; d < maxProductiveEnd; d++) {
            KeyRange dim = ks.get(d);
            if (dim == KeyRange.EVERYTHING_RANGE) continue;
            if (!dim.isSingleKey()) {
              laterRangeExists = true;
              break outer;
            }
          }
        }
        if (laterRangeExists) {
          return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
        }
      }
    }

    int productiveStart = prefixSlots;
    int maxProductiveLen = maxProductiveEnd - productiveStart;
    if (maxProductiveLen <= 0) {
      return everything();
    }

    // Classify each dim in [productiveStart, maxProductiveEnd) as "pinned" or not.
    // A dim is pinned iff every space has the same single-key value on that dim.
    //
    // Only trailing-pinned dims are split out into their own slots; leading-pinned
    // dims are folded into the compound. V1's shape works this way: when all spaces
    // agree on an equality on some leading dim(s), the compound bytes anchor the
    // scan tightly with that prefix. But when pinned equalities appear after an
    // unbounded-range slot (i.e., trailing-pinned), they don't narrow the scan's
    // start/stop bounds any further — V1 emits them as separate slots past the
    // unbounded-compound slot, and ScanRanges.getBoundPkColumnCount() correctly
    // stops counting at the first unbounded slot.
    //
    // Example: (pk1, pk2) > ('0','0') AND pk3 = '...' AND pk4 = '...' →
    //   compound spans pk1+pk2 with unbounded upper; pk3 and pk4 become trailing
    //   pinned slots. Bound count stops at the compound (3 cols total with tenantId).
    KeyRange[] pinnedValue = new KeyRange[maxProductiveEnd];
    for (int d = productiveStart; d < maxProductiveEnd; d++) {
      KeyRange shared = null;
      boolean allAgree = true;
      for (KeySpace ks : list.spaces()) {
        KeyRange r = ks.get(d);
        if (
          !r.isSingleKey() || r == KeyRange.IS_NULL_RANGE || r == KeyRange.IS_NOT_NULL_RANGE
        ) {
          allAgree = false;
          break;
        }
        if (shared == null) {
          shared = r;
        } else if (!shared.equals(r)) {
          allAgree = false;
          break;
        }
      }
      pinnedValue[d] = allAgree ? shared : null;
    }
    // Compound window: [compoundStart, compoundEnd). Leading-pinned dims stay in
    // the compound (compoundStart = productiveStart); trailing-pinned dims are
    // split out only when the compound has at least one non-pinned range dim AND
    // the compound would end up with an unbounded side. Splitting otherwise would
    // break scans where the compound captures all narrowing in a single fully-
    // bounded range (e.g., `id LIKE 'xy%' AND type = 1` → one 2-col compound
    // with both bounds fully specified).
    int compoundStart = productiveStart;
    int compoundEnd = maxProductiveEnd;
    // Check whether splitting is warranted: find the first non-pinned dim. If all
    // dims are pinned or there's no non-pinned dim before the trailing pinned
    // run, don't split.
    int firstNonPinned = -1;
    for (int d = productiveStart; d < maxProductiveEnd; d++) {
      if (pinnedValue[d] == null) {
        firstNonPinned = d;
        break;
      }
    }
    if (firstNonPinned >= 0) {
      // Check whether the non-pinned dim(s) would produce a compound with an
      // unbounded side across any space. Only then is trailing-split beneficial.
      boolean anyUnbound = false;
      for (KeySpace ks : list.spaces()) {
        for (int d = firstNonPinned; d < maxProductiveEnd && pinnedValue[d] == null; d++) {
          KeyRange r = ks.get(d);
          if (r == KeyRange.EVERYTHING_RANGE || r.isUnbound()) {
            anyUnbound = true;
            break;
          }
        }
        if (anyUnbound) break;
      }
      if (anyUnbound) {
        while (compoundEnd > compoundStart && pinnedValue[compoundEnd - 1] != null) {
          compoundEnd--;
        }
      }
    }
    int compoundLen = compoundEnd - compoundStart;

    // Safety gate: compound emission is UNSAFE when any space has a non-single-key dim
    // followed by ANY further constraint (pinned or range) on a later dim WITHIN THE
    // COMPOUND WINDOW. The compound byte range [lo1+lo2, hi1+hi2) is lex-wider than the
    // conjunction, so rows with col1 strictly between lo1 and hi1 pass regardless of
    // col2's value — and V2 doesn't emit a residual SkipScanFilter to reject them. V1
    // falls back to per-column projection with a SkipScanFilter for this shape.
    //
    // Example broken shapes:
    //   key_1 in [000,200) AND key_2 in [aabb,aadd) → rows with key_1='100', key_2='aaaa'
    //     are in the compound [000aabb, 200) but shouldn't match (key_2 out of range).
    //   CREATETIME in [A,B] AND ACCOUNTID='v' → rows with any ACCOUNTID value in the
    //     middle CREATETIME band are in the compound but shouldn't match.
    //
    // Checked within compound window: trailing pinned dims outside the window are split
    // into separate slots and don't participate in this check.
    //
    // Rule: if any space has a non-single-key dim followed by any further constrained
    // dim (single-key or range) in the compound window, fall back. Trailing non-single-
    // key within the window is safe (last dim of the compound range, bound correctly).
    for (KeySpace ks : list.spaces()) {
      boolean sawNonSingleKey = false;
      for (int d = compoundStart; d < compoundEnd; d++) {
        KeyRange dim = ks.get(d);
        if (dim == KeyRange.EVERYTHING_RANGE) continue;
        if (!dim.isSingleKey()) {
          if (sawNonSingleKey) {
            return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
          }
          sawNonSingleKey = true;
        } else if (sawNonSingleKey) {
          return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
        }
      }
    }

    // Build one compound KeyRange per space, only over the [compoundStart, compoundEnd)
    // window. Pinned prefix/suffix dims are emitted as individual slots outside the loop.
    List<KeyRange> compounds = new ArrayList<>(list.size());
    // Skip the compound build entirely when every productive dim is pinned: no range
    // part to compound. The pinned slots below carry all the narrowing.
    if (compoundLen > 0) {
    for (KeySpace ks : list.spaces()) {
      int end = firstProductiveStop(ks, prefixSlots);
      // Clamp end to the compound window: trailing pinned dims are emitted separately.
      if (end > compoundEnd) end = compoundEnd;
      // Per-dim view: dims [compoundStart, end) as individual slots with slotSpan 0.
      int len = end - compoundStart;
      if (len <= 0) {
        // Space is all-EVERYTHING past the prefix — contributes EVERYTHING. The whole
        // list's emission becomes EVERYTHING.
        return everything();
      }
      List<List<KeyRange>> perDimSlots = new ArrayList<>(len);
      int[] perDimSpan = new int[len];
      boolean allSingleKey = true;
      // IS_NULL_RANGE has empty bounds and KeyRange.isSingleKey() returns true. For
      // a non-leading IS NULL with trailing unconstrained PK columns AND leading
      // single-key equality prefix, the compound must be half-open to exclude rows with
      // non-null values on the null-dim. For leading IS NULL (no single-key prefix),
      // keeping the IS_NULL_RANGE sentinel lets ScanRanges.create handle it specially
      // (it has separate codepaths for IS_NULL_RANGE that set the right scan bounds).
      boolean hasTrailingUnconstrained = end < ks.nDims();
      // Count the leading single-key equality prefix within this space's productive run.
      int leadingSingleKeyCount = 0;
      for (int d = compoundStart; d < end; d++) {
        KeyRange dim = ks.get(d);
        if (dim.isSingleKey() && dim != KeyRange.IS_NULL_RANGE
          && dim != KeyRange.IS_NOT_NULL_RANGE) {
          leadingSingleKeyCount++;
        } else {
          break;
        }
      }
      for (int d = compoundStart; d < end; d++) {
        KeyRange dim = ks.get(d);
        perDimSlots.add(Collections.singletonList(dim));
        if (!dim.isSingleKey()) {
          allSingleKey = false;
        } else if ((dim == KeyRange.IS_NULL_RANGE || dim == KeyRange.IS_NOT_NULL_RANGE)
          && hasTrailingUnconstrained && leadingSingleKeyCount > 0) {
          // Non-leading IS NULL with leading equality prefix: convert to half-open so
          // trailing non-null rows don't sneak in via the nextKey-bumped upper.
          allSingleKey = false;
        }
      }
      // Use setKey variant with schemaStartIndex so the schema is walked starting from
      // the user-tail fields (after prefix columns like salt, viewIndexId, tenantId).
      // Without this, the first user-tail slot's bytes get decoded against the schema's
      // leading field (e.g. the VARCHAR tenantId slot), which appends a spurious `\x00`
      // separator for non-fixed-width leading fields.
      byte[] lo = getKeyWithSchemaOffset(schema, perDimSlots, perDimSpan,
        KeyRange.Bound.LOWER, compoundStart);
      byte[] hi = getKeyWithSchemaOffset(schema, perDimSlots, perDimSpan,
        KeyRange.Bound.UPPER, compoundStart);
      // Strip the trailing separator byte for the last productive field if it's
      // variable-length AND that field is the last field in the full PK schema.
      // ScanUtil.getMinKey/getMaxKey append a trailing separator for variable-length
      // fields (both ASC `\x00` and DESC `\xFF`). Downstream ScanRanges.create ->
      // ScanUtil.setKey walks our compound bytes again and re-appends another separator
      // when it finishes the same field, producing a double-separator bug (extra
      // trailing `\xFF` for DESC, extra `\x00` for ASC). Stripping here lets the
      // downstream setKey re-add it correctly.
      //
      // IMPORTANT: only strip when the last productive field is actually the last field
      // in the PK. If there are unconstrained PK fields after the productive run, the
      // trailing separator is an internal boundary marker between the last-productive
      // dim and the (wildcard) next dim — downstream setKey needs it to know where the
      // constrained prefix ends. Stripping in that case produces a startRow that's too
      // short and misses the dim boundary (see QueryCompilerTest.testRVCScanBoundaries1).
      org.apache.phoenix.schema.ValueSchema.Field lastField =
        schema.getField(compoundStart + len - 1);
      boolean lastIsVarLength = !lastField.getDataType().isFixedWidth();
      boolean lastIsLastPkField = (compoundStart + len) == schema.getMaxFields();
      // Strip when:
      // (a) this field is the last PK field (no trailing unconstrained dims), OR
      // (b) all productive dims are single-key (we'll emit as a point key, and the
      //     trailing separator is redundant — downstream SkipScanFilter works with
      //     raw point bytes).
      // When neither condition holds (range with trailing EVERYTHING dims), keep the
      // separator as a boundary marker for downstream setKey (see testRVCScanBoundaries1).
      if (lastIsVarLength && (lastIsLastPkField || allSingleKey)) {
        lo = stripTrailingSeparator(lo, lastField);
        hi = stripTrailingSeparator(hi, lastField);
      }
      // Wrap into a compound KeyRange. getMinKey/getMaxKey already apply exclusive-bound
      // bumping internally.
      //
      // For all-single-key compounds (every productive dim is a point equality), emit as
      // KeyRange.getKeyRange(bytes) — a single-key range. Downstream
      // ScanRanges.isPointLookup() needs isSingleKey()=true on the range to promote the
      // scan to a proper GET-style point lookup; a half-open [lo, hi) range never
      // qualifies even when lo and hi are nextKey-adjacent.
      //
      // EXCEPTION: when this space's productive dims end before maxProductiveEnd (i.e. the
      // slot-span covers more dims than this space constrains), a single-key compound would
      // have fewer bytes than the SkipScanFilter expects for this slot. Emit a half-open
      // range [lo, nextKey(lo)) in that case so the range matches any row whose leading
      // bytes equal lo — the trailing unconstrained dims are implicitly wild.
      KeyRange compound;
      boolean shorterThanSlotSpan = end < compoundEnd;
      if (allSingleKey && lo != null && lo.length > 0 && !shorterThanSlotSpan) {
        compound = KeyRange.getKeyRange(lo);
      } else {
        compound = KeyRange.getKeyRange(lo == null ? KeyRange.UNBOUND : lo, true,
          hi == null ? KeyRange.UNBOUND : hi, false);
      }
      if (compound == KeyRange.EMPTY_RANGE) {
        continue;
      }
      compounds.add(compound);
    }
    if (compounds.isEmpty()) {
      return nothing();
    }

    // Cartesian bound: if the number of compound ranges exceeds the bound, we need to
    // widen. That widening happens upstream in KeySpaceList; by the time we reach here
    // the list is already bounded. Still, apply a defensive cap.
    BigInteger bound = BigInteger.valueOf(Math.max(1, cartesianBound));
    if (BigInteger.valueOf(compounds.size()).compareTo(bound) > 0) {
      // Over budget — drop everything past the bound (sound widening: truncation admits
      // more rows but never fewer; residual filter handles any extras).
      compounds = compounds.subList(0, cartesianBound);
    }
    } // end if (compoundLen > 0)

    // Coalesce adjacent/overlapping compound ranges. Since the bytes are lex-ordered the
    // standard KeyRange.coalesce is applicable. If the compound window is empty, this
    // yields an empty list (no compound slot will be emitted).
    List<KeyRange> coalesced = compounds.isEmpty()
      ? java.util.Collections.<KeyRange>emptyList()
      : KeyRange.coalesce(compounds);
    if (!coalesced.isEmpty() && coalesced.size() == 1 && coalesced.get(0) == KeyRange.EMPTY_RANGE) {
      return nothing();
    }

    // Mixed-width ranges within a single compound slot: if the coalesced ranges have
    // different bound widths AND any of them is non-point (not a single-key), SkipScanFilter
    // can't navigate them correctly — its per-slot walker compares the extracted row bytes
    // (full slot-span width) against each range's bounds, and a short-bound range will
    // incorrectly exclude rows whose trailing dims have non-matching bytes. Fall back to
    // per-slot emission in that case.
    //
    // Exempted: all-point-key compounds with mixed widths (e.g. RVC IN-list with
    // variable-length VARCHAR tuples producing different byte widths per tuple). Each
    // point compound is a single-key range; SkipScanFilter compares each row's bytes
    // against each point individually, which works correctly regardless of width.
    //
    // UNBOUND bounds are excluded from the width comparison — they don't participate in
    // byte comparison, so a range with UNBOUND lower and bounded upper can coexist with
    // a fully-bounded range of different width.
    if (coalesced.size() > 1 && compoundLen > 1) {
      int commonLoLen = -2;
      int commonUpLen = -2;
      boolean mixedWidth = false;
      boolean anyNonPoint = false;
      for (KeyRange kr : coalesced) {
        if (!kr.isSingleKey()) anyNonPoint = true;
        if (kr.getLowerRange() != KeyRange.UNBOUND) {
          int loLen = kr.getLowerRange().length;
          if (commonLoLen == -2) commonLoLen = loLen;
          else if (commonLoLen != loLen) { mixedWidth = true; }
        }
        if (kr.getUpperRange() != KeyRange.UNBOUND) {
          int upLen = kr.getUpperRange().length;
          if (commonUpLen == -2) commonUpLen = upLen;
          else if (commonUpLen != upLen) { mixedWidth = true; }
        }
      }
      if (mixedWidth && anyNonPoint) {
        // Mixed-width non-point compound ranges can't be navigated by SkipScanFilter
        // correctly when packed into a single compound slot — its per-slot walker
        // compares row bytes (slot-span width) against each range's bounds, and a
        // short-bound range incorrectly excludes rows whose trailing dims have non-
        // matching bytes.
        //
        // Previous implementation collapsed the set into a single `[min-lower, max-upper]`
        // bounding range and relied on the residual filter to reject extras. That was
        // unsafe: V2's consumed-set logic may strip OR nodes from the residual when the
        // OR is fully extractable in isolation (e.g. `(pk2='a' OR pk2='b')` is single-
        // dim OR, consumed). After stripping, the wider scan region leaks rows that
        // match neither original compound. See RowValueConstructorIT
        // .testComparisonAgainstRVCCombinedWithOrAnd_2.
        //
        // Safe fix: fall back to per-column projection so each PK column gets its own
        // slot in the SkipScanFilter and the downstream filter enforces the predicates
        // per-row. This matches V1's behavior for IN-list + RVC-inequality shapes.
        return emitV1Projection(list, nPkColumns, cartesianBound, prefixSlots);
      }
    }

    // Emit slots in order: [pre-productive EVERYTHING gaps] [leading pinned slots]
    // [compound slot] [trailing pinned slots]. Leading pinned dims come from
    // [productiveStart, compoundStart); trailing pinned dims come from
    // [compoundEnd, maxProductiveEnd). The compound slot itself spans
    // [compoundStart, compoundEnd).
    List<List<KeyRange>> out = new ArrayList<>();
    List<Integer> slotSpanList = new ArrayList<>();
    // Pre-productive EVERYTHING padding (for the rare case where productiveStart >
    // prefixSlots due to leading EVERYTHING dims; the gate above usually prevents this
    // by routing to emitV1Projection, but kept for safety).
    for (int d = prefixSlots; d < productiveStart; d++) {
      out.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));
      slotSpanList.add(0);
    }
    // Compound slot (only if non-empty window).
    if (compoundLen > 0) {
      out.add(coalesced);
      // slotSpan for the compound: normally (compoundLen - 1) physical cols beyond
      // the first. But if coalesce collapsed multiple per-space compounds with
      // UNBOUND upper into a single range whose lower bytes cover fewer cols than
      // compoundLen (e.g., 4-tuple RVC lex-expansion on an index whose common
      // leading prefix is only 1 col after coalesce), adjust down to the actual
      // byte span so ScanRanges.getBoundPkColumnCount() doesn't over-count.
      //
      // Only trim the span in this specific shape: a single coalesced range with
      // UNBOUND upper (the post-coalesce compound is a half-open interval). For
      // multi-range compounds, single-key point lookups (e.g., trailing IS_NULL),
      // or ranges with both sides bounded, keep compoundLen — those shapes don't
      // collapse widths in a way that requires trimming.
      int span = compoundLen;
      if (coalesced.size() == 1) {
        KeyRange only = coalesced.get(0);
        if (only.getUpperRange() == KeyRange.UNBOUND && !only.isSingleKey()) {
          int actualLowerCols =
            countColsInKey(schema, only.getLowerRange(), compoundStart, compoundLen);
          if (actualLowerCols > 0 && actualLowerCols < span) {
            span = actualLowerCols;
          }
        } else if (only.getLowerRange() == KeyRange.UNBOUND && !only.isSingleKey()) {
          int actualUpperCols =
            countColsInKey(schema, only.getUpperRange(), compoundStart, compoundLen);
          if (actualUpperCols > 0 && actualUpperCols < span) {
            span = actualUpperCols;
          }
        }
      }
      slotSpanList.add(span - 1);
    }
    // Trailing pinned slots: one per pinned dim after the compound window. These
    // aren't counted by getBoundPkSpan because the compound slot has hasUnbound, but
    // they still narrow the scan's skip-scan filter beyond what the compound alone does.
    boolean emittedTrailingPinned = false;
    for (int d = compoundEnd; d < maxProductiveEnd; d++) {
      out.add(Collections.singletonList(pinnedValue[d]));
      slotSpanList.add(0);
      emittedTrailingPinned = true;
    }
    int[] slotSpan = new int[slotSpanList.size()];
    for (int i = 0; i < slotSpan.length; i++) slotSpan[i] = slotSpanList.get(i);
    // useSkipScan is true when the scan region contains rows that don't satisfy the
    // predicate AND downstream SkipScanFilter is required to reject them per-row.
    //   (a) Multiple coalesced compound ranges → SkipScanFilter navigates the gaps.
    //   (b) Trailing pinned slots were split off the compound window because the
    //       compound has an unbounded side (anyUnbound branch above). The compound
    //       byte interval is lex-wider than the conjunction (e.g. `a='aaa' AND b>='bbb'
    //       AND c='ccc' AND d='ddd'` produces compound [aaabbb, ∞) with trailing slots
    //       c='ccc', d='ddd'). Without SkipScanFilter, rows whose leading bytes fall
    //       inside the compound but whose trailing dims don't equal the pinned values
    //       slip through. Force useSkipScan so the filter enforces per-row equality.
    boolean useSkipScan = coalesced.size() > 1 || emittedTrailingPinned;
    return new Result(out, slotSpan, useSkipScan);
  }

  /**
   * Variant of {@link ScanUtil#getMinKey}/{@link ScanUtil#getMaxKey} that walks a subset
   * of the schema starting at {@code schemaStartIndex}. The public
   * {@link ScanUtil#getMinKey} starts schema iteration at field 0, which is wrong when
   * the slots correspond to user PK columns after prefix columns (salt, viewIndexId,
   * tenantId). We construct a sub-schema from fields [schemaStartIndex, maxFields) so
   * the first slot's bytes are decoded against the correct schema field, avoiding
   * spurious separator bytes from non-fixed-width prefix fields leaking into the compound.
   */
  private static byte[] getKeyWithSchemaOffset(RowKeySchema schema, List<List<KeyRange>> slots,
    int[] slotSpan, KeyRange.Bound bound, int schemaStartIndex) {
    if (slots.isEmpty()) {
      return KeyRange.UNBOUND;
    }
    // Build a sub-schema over fields [schemaStartIndex, maxFields).
    RowKeySchema subSchema;
    if (schemaStartIndex == 0) {
      subSchema = schema;
    } else {
      RowKeySchema.RowKeySchemaBuilder b =
        new RowKeySchema.RowKeySchemaBuilder(schema.getMaxFields() - schemaStartIndex);
      for (int d = schemaStartIndex; d < schema.getMaxFields(); d++) {
        org.apache.phoenix.schema.ValueSchema.Field f = schema.getField(d);
        b.addField(f, f.isNullable(), f.getSortOrder());
      }
      b.rowKeyOrderOptimizable(schema.rowKeyOrderOptimizable());
      subSchema = b.build();
    }
    return bound == KeyRange.Bound.LOWER
      ? ScanUtil.getMinKey(subSchema, slots, slotSpan)
      : ScanUtil.getMaxKey(subSchema, slots, slotSpan);
  }

  /**
   * Count the number of schema fields consumed when decoding {@code key} starting
   * at {@code startField}, stopping after {@code maxFields} fields or end of key.
   */
  private static int countColsInKey(RowKeySchema schema, byte[] key, int startField,
    int maxFields) {
    if (key == null || key == KeyRange.UNBOUND || key.length == 0) return 0;
    int offset = 0;
    int cols = 0;
    for (int f = startField; f < startField + maxFields && f < schema.getMaxFields(); f++) {
      if (offset >= key.length) break;
      org.apache.phoenix.schema.ValueSchema.Field field = schema.getField(f);
      int fieldLen;
      if (field.getDataType().isFixedWidth()) {
        Integer maxCol = field.getMaxLength();
        fieldLen = (maxCol != null) ? maxCol : field.getDataType().getByteSize();
      } else {
        int end = offset;
        while (end < key.length
          && key[end] != org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE
          && key[end] != org.apache.phoenix.query.QueryConstants.DESC_SEPARATOR_BYTE) {
          end++;
        }
        fieldLen = end - offset;
      }
      offset += fieldLen;
      cols++;
      if (offset >= key.length) break;
      if (!field.getDataType().isFixedWidth() && offset < key.length) {
        offset++;
      }
    }
    return cols;
  }

  /**
   * Collapse a list of {@link KeyRange}s into a single bounding range with
   * lex-minimum lower bound and lex-maximum upper bound across the inputs. Used when
   * downstream {@link org.apache.phoenix.filter.SkipScanFilter} can't navigate
   * mixed-width non-point compound ranges; the residual filter enforces the
   * original predicate at scan time, so over-approximating here is sound.
   */
  private static KeyRange collapseToSingleBoundingRange(List<KeyRange> ranges) {
    byte[] minLower = null;
    boolean minLowerInclusive = true;
    byte[] maxUpper = null;
    boolean maxUpperInclusive = false;
    boolean anyLowerUnbound = false;
    boolean anyUpperUnbound = false;
    for (KeyRange r : ranges) {
      if (r.getLowerRange() == KeyRange.UNBOUND) {
        anyLowerUnbound = true;
      } else if (!anyLowerUnbound) {
        if (minLower == null
          || org.apache.hadoop.hbase.util.Bytes.compareTo(r.getLowerRange(), minLower) < 0) {
          minLower = r.getLowerRange();
          minLowerInclusive = r.isLowerInclusive();
        } else if (
          org.apache.hadoop.hbase.util.Bytes.compareTo(r.getLowerRange(), minLower) == 0
            && r.isLowerInclusive()
        ) {
          // same lex bytes but this one is inclusive — widen to inclusive
          minLowerInclusive = true;
        }
      }
      if (r.getUpperRange() == KeyRange.UNBOUND) {
        anyUpperUnbound = true;
      } else if (!anyUpperUnbound) {
        if (maxUpper == null
          || org.apache.hadoop.hbase.util.Bytes.compareTo(r.getUpperRange(), maxUpper) > 0) {
          maxUpper = r.getUpperRange();
          maxUpperInclusive = r.isUpperInclusive();
        } else if (
          org.apache.hadoop.hbase.util.Bytes.compareTo(r.getUpperRange(), maxUpper) == 0
            && r.isUpperInclusive()
        ) {
          maxUpperInclusive = true;
        }
      }
    }
    byte[] lo = anyLowerUnbound ? KeyRange.UNBOUND : minLower;
    byte[] hi = anyUpperUnbound ? KeyRange.UNBOUND : maxUpper;
    boolean loInc = anyLowerUnbound ? false : minLowerInclusive;
    boolean hiInc = anyUpperUnbound ? false : maxUpperInclusive;
    return KeyRange.getKeyRange(lo, loInc, hi, hiInc);
  }

  /**
   * Strip the trailing separator byte appended by {@link ScanUtil#getMinKey}/getMaxKey
   * for a variable-length last field. Expects the compound bytes to end with the
   * appropriate separator byte for the field's sort order (`\x00` for ASC,
   * `\xFF` for DESC). Safe to call even if the byte isn't a separator — we only strip
   * when the trailing byte matches the expected separator.
   */
  private static byte[] stripTrailingSeparator(byte[] key,
    org.apache.phoenix.schema.ValueSchema.Field lastField) {
    if (key == null || key == KeyRange.UNBOUND || key.length == 0) {
      return key;
    }
    byte expectedSep =
      lastField.getSortOrder() == org.apache.phoenix.schema.SortOrder.DESC
        ? org.apache.phoenix.query.QueryConstants.DESC_SEPARATOR_BYTE
        : org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE;
    if (key[key.length - 1] == expectedSep) {
      byte[] stripped = new byte[key.length - 1];
      System.arraycopy(key, 0, stripped, 0, stripped.length);
      return stripped;
    }
    return key;
  }

  /**
   * V1-shaped per-column projection that stops at the first EVERYTHING past the prefix.
   * Kept as the legacy entry point used by tests without a schema; less general than
   * {@link #emitV1Projection} which walks past EVERYTHING gaps so trailing constraints
   * can still narrow via {@link org.apache.phoenix.filter.SkipScanFilter}.
   */
  static Result emitV1ProjectionStopAtGap(KeySpaceList list, int nPkColumns, int cartesianBound,
    int prefixSlots) {
    if (list.isUnsatisfiable()) {
      return nothing();
    }
    if (list.isEverything()) {
      return everything();
    }
    List<java.util.Set<KeyRange>> perSlot = new ArrayList<>(nPkColumns);
    for (int i = 0; i < nPkColumns; i++) {
      perSlot.add(new java.util.LinkedHashSet<KeyRange>());
    }
    int globalLeadingStop = nPkColumns;
    for (KeySpace ks : list.spaces()) {
      int stop = firstProductiveStop(ks, prefixSlots);
      globalLeadingStop = Math.min(globalLeadingStop, stop);
      for (int d = 0; d < stop; d++) {
        perSlot.get(d).add(ks.get(d));
      }
    }
    if (globalLeadingStop <= prefixSlots) {
      boolean anyBeyondPrefix = false;
      for (int d = prefixSlots; d < globalLeadingStop; d++) {
        if (!perSlot.get(d).isEmpty()) {
          anyBeyondPrefix = true;
          break;
        }
      }
      if (!anyBeyondPrefix) {
        return everything();
      }
    }

    int kept = globalLeadingStop;
    BigInteger running = BigInteger.ONE;
    BigInteger bound = BigInteger.valueOf(Math.max(1, cartesianBound));
    int allowed = kept;
    for (int d = 0; d < kept; d++) {
      int slotSize = perSlot.get(d).isEmpty() ? 1 : perSlot.get(d).size();
      running = running.multiply(BigInteger.valueOf(slotSize));
      if (running.compareTo(bound) > 0) {
        allowed = d + 1;
        break;
      }
    }

    List<List<KeyRange>> out = new ArrayList<>(allowed);
    boolean useSkipScan = false;
    for (int d = 0; d < allowed; d++) {
      if (perSlot.get(d).isEmpty()) {
        break;
      }
      List<KeyRange> coalesced = KeyRange.coalesce(new ArrayList<>(perSlot.get(d)));
      if (coalesced.size() == 1 && coalesced.get(0) == KeyRange.EMPTY_RANGE) {
        return nothing();
      }
      out.add(coalesced);
      if (coalesced.size() > 1) {
        useSkipScan = true;
      }
    }
    if (out.isEmpty()) {
      return everything();
    }
    int[] slotSpan = new int[out.size()];
    return new Result(out, slotSpan, useSkipScan);
  }

  /** First dim at or after {@code from} with a non-EVERYTHING range, or {@code -1}. */
  private static int firstConstrainedDim(KeySpace ks, int from) {
    for (int d = from; d < ks.nDims(); d++) {
      if (ks.get(d) != KeyRange.EVERYTHING_RANGE) {
        return d;
      }
    }
    return -1;
  }

  /**
   * Productive-run end for {@code ks}: one past the highest constrained dim at or after
   * {@code prefixSlots}. Unlike {@link #firstProductiveStop} this version walks through
   * middle EVERYTHING gaps regardless of {@code prefixSlots}, returning the largest
   * meaningful dim the space constrains. Used for compound-extent discovery.
   */
  private static int firstProductiveStopAnyPrefix(KeySpace ks, int prefixSlots) {
    int lastConstrained = prefixSlots - 1;
    for (int i = prefixSlots; i < ks.nDims(); i++) {
      if (ks.get(i) != KeyRange.EVERYTHING_RANGE) {
        lastConstrained = i;
      }
    }
    return lastConstrained + 1;
  }

  /**
   * V1-shaped per-column projection of the {@link KeySpaceList}. For each PK column
   * past the prefix, emits the coalesced disjunction of every KeySpace's range on that
   * column; gaps (EVERYTHING dims) are emitted as singleton EVERYTHING slots so trailing
   * constraints still drive {@link org.apache.phoenix.filter.SkipScanFilter} narrowing.
   * This is the shape {@link org.apache.phoenix.compile.ScanRanges} was designed to
   * consume and is the default fallback whenever compound emission is unsafe.
   * <p>
   * Applies the cartesian-bound widening rule: if the running product of per-column
   * range counts exceeds {@code cartesianBound}, trailing columns are dropped. The
   * residual filter re-evaluates dropped constraints at scan time, so correctness is
   * preserved.
   * <p>
   * Output invariants per column: (a) every column from {@code prefixSlots} up to the
   * last constrained column emits a non-empty list; (b) a column where some KeySpace
   * has EVERYTHING is collapsed to the singleton EVERYTHING so the per-column OR
   * respects space-level disjunctions.
   */
  static Result emitV1Projection(KeySpaceList list, int nPkColumns, int cartesianBound,
    int prefixSlots) {
    if (list.isUnsatisfiable()) {
      return nothing();
    }
    if (list.isEverything()) {
      return everything();
    }
    // Per-slot accumulation, walking all dims regardless of leading EVERYTHING.
    // For each dim, the union across spaces is the union of every space's contribution
    // on that dim. A space that ends before dim d (its last-constrained dim is before d)
    // contributes EVERYTHING on d — the OR over spaces is then EVERYTHING on d, no matter
    // what the other spaces contribute. Any space's dim whose range IS EVERYTHING also
    // subsumes the per-dim union to EVERYTHING. Track per-dim subsumption explicitly so
    // the final per-dim set collapses to EVERYTHING when subsumed.
    List<java.util.Set<KeyRange>> perSlot = new ArrayList<>(nPkColumns);
    boolean[] slotSubsumedByEverything = new boolean[nPkColumns];
    for (int i = 0; i < nPkColumns; i++) {
      perSlot.add(new java.util.LinkedHashSet<KeyRange>());
    }
    int globalLastConstrained = prefixSlots - 1;
    for (KeySpace ks : list.spaces()) {
      int end = firstProductiveStopAnyPrefix(ks, prefixSlots);
      for (int d = prefixSlots; d < end; d++) {
        KeyRange r = ks.get(d);
        if (r == KeyRange.EVERYTHING_RANGE) {
          slotSubsumedByEverything[d] = true;
        } else {
          perSlot.get(d).add(r);
        }
      }
      // Dims past this space's productive end are unconstrained by this space — their
      // per-dim OR includes EVERYTHING due to this branch.
      for (int d = end; d < nPkColumns; d++) {
        slotSubsumedByEverything[d] = true;
      }
      if (end - 1 > globalLastConstrained) globalLastConstrained = end - 1;
    }
    // Collapse subsumed dims to a single EVERYTHING entry.
    for (int d = prefixSlots; d < nPkColumns; d++) {
      if (slotSubsumedByEverything[d]) {
        perSlot.get(d).clear();
        perSlot.get(d).add(KeyRange.EVERYTHING_RANGE);
      }
    }
    if (globalLastConstrained < prefixSlots) {
      return everything();
    }

    // Cartesian bound: same semantics as the legacy path — drop trailing dims when the
    // running product exceeds the bound.
    int kept = globalLastConstrained + 1;
    BigInteger running = BigInteger.ONE;
    BigInteger bound = BigInteger.valueOf(Math.max(1, cartesianBound));
    int allowed = kept;
    for (int d = prefixSlots; d < kept; d++) {
      int slotSize = perSlot.get(d).isEmpty() ? 1 : perSlot.get(d).size();
      running = running.multiply(BigInteger.valueOf(slotSize));
      if (running.compareTo(bound) > 0) {
        allowed = d + 1;
        break;
      }
    }

    // Emit per-slot, starting at prefixSlots. Fill EVERYTHING for any gap-slots between
    // prefixSlots and the first constrained dim.
    List<List<KeyRange>> out = new ArrayList<>(allowed - prefixSlots);
    boolean useSkipScan = false;
    for (int d = prefixSlots; d < allowed; d++) {
      if (perSlot.get(d).isEmpty()) {
        out.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));
        continue;
      }
      List<KeyRange> coalesced = KeyRange.coalesce(new ArrayList<>(perSlot.get(d)));
      if (coalesced.size() == 1 && coalesced.get(0) == KeyRange.EMPTY_RANGE) {
        return nothing();
      }
      out.add(coalesced);
      if (coalesced.size() > 1) {
        useSkipScan = true;
      }
    }
    if (out.isEmpty()) {
      return everything();
    }
    // SkipScanFilter has an invariant: the leading productive slot must have a concrete
    // lower bound. {@link SkipScanFilter#setNextCellHint} builds the seek-hint startKey by
    // concatenating each slot's {@code getRange(LOWER)}. {@code EVERYTHING_RANGE}'s lower
    // is {@code UNBOUND} (empty bytes), so a leading EVERYTHING contributes zero bytes to
    // the hint. If the next slot is a point (e.g. {@code {2}}), the full hint is only the
    // trailing slot's bytes — shorter than any already-scanned row and lex-less than rows
    // whose leading column exceeds the trailing slot's value. HBase rejects such backward
    // seeks with {@code "next hint must come after previous hint"}. V1 sidesteps this by
    // leaving the trailing predicate to the residual BooleanExpressionFilter when the
    // leading slot is unbounded (see WhereOptimizer's {@code stopExtracting} at line 398
    // when {@code hasUnboundedRange} is true). V2 must respect the same invariant: when
    // the leading productive slot is EVERYTHING we cannot install a SkipScanFilter. The
    // WhereOptimizerV2 driver, on observing this shape, preserves the predicate in the
    // residual by not consuming the visitor's extract nodes.
    // SkipScanFilter is needed when the scan byte interval is wider than the
    // conjunction of per-slot constraints. This happens whenever there are two or
    // more constrained slots AND any slot except the last constrained has a
    // non-point range — rows where that slot is mid-range with later slots out of
    // range would slip through the compound byte interval.
    //
    // Also needed when any slot EXCEPT the last is an IS_NOT_NULL_RANGE: that
    // sentinel's lower is {0x01} and upper UNBOUND, so the scan stretches past
    // valid rows. A trailing IS_NOT_NULL on a slot past the first gets missed by
    // start/stop row alone; the filter must enforce per-row non-null.
    //
    // V1 always installs a SkipScanFilter for multi-slot projections with any
    // non-point slot; V2 matches that here.
    if (!useSkipScan) {
      int firstConstrained = -1;
      int lastConstrained = -1;
      for (int i = 0; i < out.size(); i++) {
        List<KeyRange> slot = out.get(i);
        boolean slotIsEverything = slot.size() == 1 && slot.get(0) == KeyRange.EVERYTHING_RANGE;
        if (!slotIsEverything) {
          if (firstConstrained < 0) firstConstrained = i;
          lastConstrained = i;
        }
      }
      if (firstConstrained >= 0 && lastConstrained > firstConstrained) {
        // Multiple constrained slots. If any slot EXCEPT the last has a non-point
        // range OR any slot past the first has any non-point constraint (range or
        // IS_NOT_NULL_RANGE), the compound byte interval is wider than the
        // conjunction and a SkipScanFilter is required.
        boolean needSkipScan = false;
        // Check slots [firstConstrained, lastConstrained] for any non-point in a
        // non-last position, or any non-point past the first.
        for (int i = firstConstrained; i <= lastConstrained && !needSkipScan; i++) {
          List<KeyRange> slot = out.get(i);
          for (KeyRange r : slot) {
            if (!r.isSingleKey()) {
              // Non-point at this slot. If it's NOT the last constrained, we need
              // a filter because the trailing slot(s) can't narrow the scan
              // within this range. If it IS the last constrained but there are
              // preceding constrained slots with >1 point (skip-scan cardinality),
              // useSkipScan is already true from the earlier coalesce check.
              if (i < lastConstrained) {
                needSkipScan = true;
                break;
              }
              // Non-point at the last constrained slot: only problematic if a
              // preceding slot was also non-point (handled above in a prior
              // iteration). Otherwise the compound byte range captures it.
            }
          }
        }
        if (needSkipScan) {
          useSkipScan = true;
        }
      }
    }
    // Final invariant gate: if the leading emitted slot (the first user-PK slot) is
    // EVERYTHING, suppress useSkipScan regardless of what downstream slots contain.
    // SkipScanFilter's setNextCellHint builds the hint from slot lowers — an EVERYTHING
    // slot contributes zero bytes, producing a seek shorter than any already-scanned
    // row (see the block comment above). Prefix bytes (salt / viewIndex / tenant) are
    // concrete but only anchor start/stop; the hint construction still requires a
    // non-empty leading user slot. The caller (WhereOptimizerV2) will leave the trailing
    // predicate in the residual BooleanExpressionFilter when this gate fires.
    if (!out.isEmpty()) {
      List<KeyRange> leading = out.get(0);
      boolean leadingIsEverything =
          leading.size() == 1 && leading.get(0) == KeyRange.EVERYTHING_RANGE;
      if (leadingIsEverything) {
        useSkipScan = false;
      }
    }
    int[] slotSpan = new int[out.size()];
    // Extend slotSpan on the last emitted slot when it is a range (non-point) and the
    // productive window stops before the last PK column. V1's {@code WhereOptimizer}
    // treats a trailing scalar range on col i as spanning through the final PK col —
    // the range bytes encode column-min/max via {@code ScanUtil.getMinKey/getMaxKey},
    // effectively covering trailing cols — and sets {@code slotSpan[last] = nPkColumns
    // - i - 1}. V2's per-slot emission must match this convention so that
    // {@link org.apache.phoenix.compile.ScanRanges#getBoundPkColumnCount} and
    // {@link org.apache.phoenix.filter.SkipScanFilter#intersect} (which reads
    // {@code slotSpan[0]} to step the schema cursor per region) see a PK-terminating
    // slot. Under-counting slotSpan misaligns per-region cursor stepping and causes
    // out-of-order delivery of index rows — see CDCQueryIT.testSelectCDC with salted
    // data tables.
    //
    // Constraints:
    // 1. Last slot has any non-point range. All-single-key last slots don't need
    //    extension (point keys don't carry trailing col encodings).
    // 2. No EVERYTHING slot exists between prefixSlots and the last slot. V1 only
    //    extends slotSpan when the range is consecutive with leading productive slots.
    //    If there's an intervening EVERYTHING gap (e.g. org_id=x AND EVERYTHING AND
    //    feature <= 'B' on a 4-PK table), V1 keeps slotSpan=0 and uses a
    //    BooleanExpressionFilter rather than a SkipScanFilter with an extended
    //    slotSpan. Extending here would produce a SkipScanFilter whose range bytes
    //    cover fewer cols than slotSpan claims, and the filter would wrongly reject
    //    rows whose trailing-col bytes exceed the range upper bound. See
    //    ProductMetricsIT.testFeatureLTEAggregation.
    if (out.size() > 0) {
      int lastSlotIdx = out.size() - 1;
      List<KeyRange> lastSlot = out.get(lastSlotIdx);
      boolean lastSlotHasRange = false;
      for (KeyRange kr : lastSlot) {
        if (!kr.isSingleKey() && kr != KeyRange.EVERYTHING_RANGE) {
          lastSlotHasRange = true;
          break;
        }
      }
      boolean hasInteriorEverythingGap = false;
      for (int i = 0; i < lastSlotIdx; i++) {
        List<KeyRange> slot = out.get(i);
        if (slot.size() == 1 && slot.get(0) == KeyRange.EVERYTHING_RANGE) {
          hasInteriorEverythingGap = true;
          break;
        }
      }
      if (lastSlotHasRange && !hasInteriorEverythingGap) {
        int lastSlotPkIdx = prefixSlots + lastSlotIdx;
        if (lastSlotPkIdx < nPkColumns - 1) {
          slotSpan[lastSlotIdx] = (nPkColumns - 1) - lastSlotPkIdx;
        }
      }
    }
    return new Result(out, slotSpan, useSkipScan);
  }

  /**
   * First productive stop for {@code ks}: the first dim at or after {@code prefixSlots}
   * whose range is EVERYTHING. Dims in {@code [0, prefixSlots)} may be EVERYTHING
   * without stopping the scan — the driver fills them in from table metadata (salt /
   * view-index / tenant).
   * <p>
   * With a prefix ({@code prefixSlots > 0}, e.g. salted tables), gaps past the prefix
   * are safe because the prefix provides a compound starting point — walk through them.
   * Without a prefix, stop at the first EVERYTHING to preserve the invariant that
   * trailing dims past a gap can't contribute to start/stop rows.
   */
  private static int firstProductiveStop(KeySpace ks, int prefixSlots) {
    if (prefixSlots == 0) {
      for (int i = 0; i < ks.nDims(); i++) {
        if (ks.get(i) == KeyRange.EVERYTHING_RANGE) {
          return i;
        }
      }
      return ks.nDims();
    }
    int lastConstrained = prefixSlots - 1;
    for (int i = prefixSlots; i < ks.nDims(); i++) {
      if (ks.get(i) != KeyRange.EVERYTHING_RANGE) {
        lastConstrained = i;
      }
    }
    return lastConstrained + 1;
  }

  /**
   * Like {@link #firstProductiveStop} but always stops at the first EVERYTHING past the
   * prefix regardless of whether prefix slots exist. Used for middle-gap detection where
   * the presence of a gap matters even on salted tables.
   */
  private static int firstProductiveStopStrict(KeySpace ks, int prefixSlots) {
    for (int i = prefixSlots; i < ks.nDims(); i++) {
      if (ks.get(i) == KeyRange.EVERYTHING_RANGE) {
        return i;
      }
    }
    return ks.nDims();
  }
}
