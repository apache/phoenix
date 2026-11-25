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
package org.apache.phoenix.compile.keyspace.scan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.keyspace.KeyRangeExtractor;
import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.compile.keyspace.KeySpaceList;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * Scan-construction entry point for the V2 WHERE optimizer.
 * <p>
 * Pipeline:
 *
 * <pre>
 *   WhereOptimizerV2.run
 *     → ExpressionNormalizer → KeySpaceExpressionVisitor  (produces KeySpaceList)
 *     → V2ScanBuilder.build                                (this class)
 *     → CompoundByteEncoderEmitter.overrideScanRows        (in-envelope shapes)
 *     → context.setScanRanges / context.setV2ScanArtifact
 * </pre>
 *
 * Dispatches on a classification of the {@link KeySpaceList} (see
 * {@code docs/where-optimizer-v2-scan-construction.md} §"Classification tree"):
 * <ul>
 * <li>Class 1 DEGENERATE → {@link ScanRanges#NOTHING}</li>
 * <li>Class 2 EVERYTHING → {@link ScanRanges#EVERYTHING}</li>
 * <li>Class 3 POINT_LOOKUP_LIST → natively emitted via {@link CompoundByteEncoder}</li>
 * <li>Classes 4a–4e (RANGE_SCAN subcases) and 5 (SKIP_SCAN_LIST) → route through the
 * {@link KeyRangeExtractor} adapter to produce V1-shaped CNF; scan start/stop bytes are then
 * sourced from {@link CompoundByteEncoder} (via {@link CompoundByteEncoderEmitter} in
 * {@code WhereOptimizerV2.run}) for shapes in the encoder's proven envelope.</li>
 * </ul>
 * Downstream consumers (SkipScanFilter, ScanRanges.isPointLookup, explain-plan formatter,
 * local-index pruning) read from the ScanRanges this builder produces. V2-owned metadata is
 * attached via {@link V2ScanArtifact} so the explain-plan formatter renders from the pre-encoding
 * {@link KeySpaceList} rather than the post-encoding bytes.
 */
public final class V2ScanBuilder {

  private V2ScanBuilder() {
  }

  /**
   * Inputs gathered at {@code WhereOptimizerV2.run} and passed to the scan builder. All fields are
   * read-only.
   */
  public static final class Inputs {
    public final KeySpaceList list;
    public final PTable table;
    public final RowKeySchema schema;
    /**
     * The nullability of each key column by row key position. The row key schema merges equal
     * adjacent fields and loses their nullability. The PK columns of the table keep it.
     */
    public final boolean[] pkNullable;
    public final int nPkColumns;
    public final int prefixSlots;
    public final Integer nBuckets;
    public final boolean isSalted;
    public final boolean isMultiTenant;
    public final boolean isSharedIndex;
    public final byte[] tenantIdBytes;
    public final Set<Hint> hints;
    public final int cartesianBound;
    public final Optional<byte[]> minOffset;

    public Inputs(KeySpaceList list, PTable table, RowKeySchema schema, int nPkColumns,
      int prefixSlots, Integer nBuckets, boolean isSalted, boolean isMultiTenant,
      boolean isSharedIndex, byte[] tenantIdBytes, Set<Hint> hints, int cartesianBound,
      Optional<byte[]> minOffset) {
      this.list = list;
      this.table = table;
      this.schema = schema;
      this.pkNullable = pkNullable(table);
      this.nPkColumns = nPkColumns;
      this.prefixSlots = prefixSlots;
      this.nBuckets = nBuckets;
      this.isSalted = isSalted;
      this.isMultiTenant = isMultiTenant;
      this.isSharedIndex = isSharedIndex;
      this.tenantIdBytes = tenantIdBytes;
      this.hints = hints;
      this.cartesianBound = cartesianBound;
      this.minOffset = minOffset;
    }

    private static boolean[] pkNullable(PTable table) {
      if (table == null) {
        return null;
      }
      List<PColumn> pkColumns = table.getPKColumns();
      boolean[] nullable = new boolean[pkColumns.size()];
      for (int i = 0; i < nullable.length; i++) {
        nullable[i] = pkColumns.get(i).isNullable();
      }
      return nullable;
    }
  }

  /**
   * Output of the scan builder. For now this is a thin wrapper around {@link ScanRanges} (the
   * existing type), leaving room to grow into a richer V2-owned adapter as more responsibilities
   * move into this class.
   */
  public static final class Result {
    public final ScanRanges scanRanges;
    /**
     * {@code true} iff the builder's classification of the emitted key space is "matches nothing" —
     * the caller short-circuits the residual and returns {@code null}. Distinct from
     * {@code scanRanges.isDegenerate()} only insofar as it's set by the builder's own
     * classification path (not always derivable from {@code scanRanges}).
     */
    public final boolean isNothing;
    /**
     * True when the emitted scan is a sound over-approximation of the {@link KeySpaceList} (algebra
     * widening and/or extractor cartesian truncation). Callers must retain visitor-consumed
     * predicates in the residual filter.
     */
    public final boolean approximated;

    public Result(ScanRanges scanRanges, boolean isNothing) {
      this(scanRanges, isNothing, false);
    }

    public Result(ScanRanges scanRanges, boolean isNothing, boolean approximated) {
      this.scanRanges = scanRanges;
      this.isNothing = isNothing;
      this.approximated = approximated;
    }

    public static Result nothing() {
      return new Result(ScanRanges.NOTHING, true, false);
    }

    public static Result everything() {
      return new Result(ScanRanges.EVERYTHING, false, false);
    }
  }

  /**
   * Build a {@link ScanRanges} from the given {@link KeySpaceList} and context.
   * <p>
   * Follows the classification tree in {@code docs/where-optimizer-v2-scan-construction.md}
   * §"Classification tree". Shapes with a native V2 emission path are handled directly; shapes
   * routed through the {@link KeyRangeExtractor} adapter produce the V1-projected per-slot CNF
   * shape that {@link org.apache.phoenix.compile.ScanRanges#create} + the downstream
   * {@code ScanUtil.setKey} consume.
   * <p>
   * Currently native classes:
   * <ul>
   * <li><b>1 DEGENERATE</b> — {@code list.isUnsatisfiable()} → {@link ScanRanges#NOTHING}.</li>
   * <li><b>2 EVERYTHING</b> — {@code list.isEverything() && !prefixSlots && !minOffset} →
   * {@link ScanRanges#EVERYTHING}.</li>
   * <li><b>3 POINT_LOOKUP_LIST</b> — every space all-single-key across every productive dim past
   * prefix, {@code list.size() ≥ 2} (single-space single-tuple routes through adapter to preserve
   * DESC var-width byte shape). Emitted directly via {@link CompoundByteEncoder}, preserving
   * cross-dim tuple correlation.</li>
   * </ul>
   * Classes 4 (RANGE_SCAN subcases) and 5 (SKIP_SCAN_LIST) currently route through the
   * {@link KeyRangeExtractor} adapter to produce the V1-shaped CNF that {@link SkipScanFilter}
   * consumes. {@link CompoundByteEncoderEmitter} then overrides
   * {@code scan.startRow}/{@code stopRow} with encoder-sourced bytes for in-envelope shapes (see
   * {@code docs/where-optimizer-v2-scan-construction.md} §"Byte emission envelope"). Native
   * emission for classes 4 and 5 is PHOENIX-6791 follow-up work.
   */
  public static Result build(Inputs in) {
    // Class 1: DEGENERATE.
    if (in.list.isUnsatisfiable()) {
      return Result.nothing();
    }
    // Class 2: EVERYTHING.
    if (in.list.isEverything()) {
      if (in.prefixSlots == 0 && !in.minOffset.isPresent()) {
        return Result.everything();
      }
    }

    // Class 3: POINT_LOOKUP_LIST.
    if (isPointLookupList(in)) {
      Result pl = buildPointLookupList(in);
      if (pl != null) {
        return pl;
      }
      // Native path opted out (encoder refused a space, e.g., IS_NULL sentinel).
      // Fall through to the classical adapter.
    }

    // Classes 4 (RANGE_SCAN subcases) and 5 (SKIP_SCAN_LIST): adapter.

    KeyRangeExtractor.Result extract = KeyRangeExtractor.extract(in.list, in.nPkColumns,
      in.cartesianBound, in.prefixSlots, in.schema, in.pkNullable);
    if (extract.isNothing()) {
      return Result.nothing();
    }

    // Build CNF exactly the way WhereOptimizerV2.run does today: prefix slots (salt /
    // viewIndexId / tenantId) + extractor-emitted user tail.
    List<List<KeyRange>> cnf = new ArrayList<>(in.nPkColumns);
    if (in.isSalted) {
      // Salt byte placeholder. ScanRanges.isPointLookup requires a singleton point range
      // (not EVERYTHING) for the whole query to classify as a point lookup when the user
      // slots also carry single keys.
      cnf.add(Collections.singletonList(
        PChar.INSTANCE.getKeyRange(QueryConstants.SEPARATOR_BYTE_ARRAY, SortOrder.ASC)));
    }
    if (in.isSharedIndex) {
      byte[] viewIndexBytes = in.table.getviewIndexIdType().toBytes(in.table.getViewIndexId());
      cnf.add(Collections.singletonList(KeyRange.getKeyRange(viewIndexBytes)));
    }
    if (in.isMultiTenant) {
      cnf.add(Collections.singletonList(KeyRange.getKeyRange(in.tenantIdBytes)));
    }
    boolean useSkipScan = extract.useSkipScan;
    if (in.hints != null) {
      if (in.hints.contains(Hint.SKIP_SCAN)) {
        useSkipScan = true;
      } else if (in.hints.contains(Hint.RANGE_SCAN)) {
        useSkipScan = false;
      }
    }
    for (int i = 0; i < extract.ranges.size(); i++) {
      cnf.add(extract.ranges.get(i));
    }
    int[] slotSpan = new int[cnf.size()];
    if (extract.slotSpan.length > 0) {
      int len = Math.min(extract.slotSpan.length, slotSpan.length - in.prefixSlots);
      if (len > 0) {
        System.arraycopy(extract.slotSpan, 0, slotSpan, in.prefixSlots, len);
      }
    }

    boolean approximated = extract.approximated || in.list.isApproximated();
    // A slot that loses its null rows ends the key slots. The residual filter then applies the
    // full predicate.
    int nullSlot = firstSlotThatSkipsNull(cnf, in.prefixSlots);
    nullSlot = earlierSlot(nullSlot, firstSlotAfterNullableDescGap(in, cnf, slotSpan));
    if (nullSlot >= 0) {
      cnf = new ArrayList<>(cnf.subList(0, nullSlot));
      slotSpan = Arrays.copyOf(slotSpan, nullSlot);
      useSkipScan &= nullSlot > in.prefixSlots;
      approximated = true;
    }
    // The region server gets the skip-scan filter in serialized form. KeyRange serialization
    // drops the inverted flag. A DESC range whose raw bounds cross then reads as empty there, and
    // the scan loses its rows. For such a slot, use a range scan and keep the residual filter.
    // A DESC range before the last slot also loses rows in the filter, so it gets the same scan.
    // A range that reaches the all-0xFF key before the last slot also loses rows in the filter.
    // A null check after an unbound range also loses rows in the filter.
    if (
      useSkipScan && (hasCrossedRawBounds(extract.ranges)
        || hasDescUpperBoundBeforeLastSlot(in.schema, cnf, slotSpan)
        || (extract.droppedTrailingNullSlots
          && hasDescUpperBound(in.schema, cnf, slotSpan, cnf.size()))
        || hasAllOnesRangeBeforeLastSlot(in, cnf, slotSpan)
        || hasNullCheckAfterUnboundRange(in, cnf, slotSpan))
    ) {
      useSkipScan = false;
      approximated = true;
    }
    // An empty bound after a range slot does not separate null rows in the filter.
    if (useSkipScan && hasNullableEmptyLowerAfterRange(in.table, cnf, slotSpan)) {
      approximated = true;
    }
    ScanRanges scanRanges = ScanRanges.create(in.schema, cnf, slotSpan, in.nBuckets, useSkipScan,
      in.table.getRowTimestampColPos(), in.minOffset);
    return new Result(scanRanges, false, approximated);
  }

  /**
   * Returns the first user slot that holds IS_NULL and a range with no lower bound, or -1. A null
   * key value sorts first in the row key, for ASC and DESC columns. ScanRanges sorts the range with
   * no lower bound before IS_NULL. That range then gives the start key and the seek hints, and it
   * starts after null. Thus the scan skips the null rows of IS_NULL.
   */
  private static int firstSlotThatSkipsNull(List<List<KeyRange>> cnf, int prefixSlots) {
    for (int i = prefixSlots; i < cnf.size(); i++) {
      List<KeyRange> slot = cnf.get(i);
      if (!slot.contains(KeyRange.IS_NULL_RANGE)) {
        continue;
      }
      for (KeyRange r : slot) {
        if (r.lowerUnbound() && !KeyRange.IS_NULL_RANGE.equals(r)) {
          return i;
        }
      }
    }
    return -1;
  }

  /** Returns the lower of two slot indexes, where -1 means no slot. */
  private static int earlierSlot(int a, int b) {
    return a < 0 || (b >= 0 && b < a) ? b : a;
  }

  /**
   * True when the skip-scan filter can lose rows of a null check or a nullable range. After a slot
   * with an unbound range or a gap, the filter can step back over the key fields. It finds the end
   * of a field by its separator byte. A null DESC field and a DESC field next to a null field break
   * this search, so the filter can seek back, fail, or skip rows. A trailing IS_NULL on a DESC
   * column keeps its separator bytes in a seek hint after an inclusive lower bound. The hint then
   * sorts after the row at that bound. After an unbound DESC fixed-width column, a null check can
   * cause a seek back. A range with no lower bound on a nullable column admits null rows. The
   * filter can step back over these null fields after an unbound range that is not a gap. This step
   * fails when an earlier slot has a null check or an unbound DESC fixed-width column.
   */
  private static boolean hasNullCheckAfterUnboundRange(Inputs in, List<List<KeyRange>> cnf,
    int[] slotSpan) {
    // The row key schema merges equal adjacent fields and can lose their nullability. The PK
    // columns of the table keep it.
    List<PColumn> pkColumns = in.table.getPKColumns();
    boolean afterUnbound = false;
    boolean afterOpenRange = false;
    boolean afterNullCheck = false;
    boolean afterDescFixedUnbound = false;
    int field = -1;
    for (int i = 0; i < cnf.size(); i++) {
      field += slotSpan[i] + 1;
      if (i < in.prefixSlots) {
        continue;
      }
      List<KeyRange> slot = cnf.get(i);
      int first = field - slotSpan[i];
      boolean nullable = first >= pkColumns.size() || pkColumns.get(first).isNullable();
      if (
        slot.contains(KeyRange.IS_NULL_RANGE) && (afterDescFixedUnbound
          || (afterUnbound && isNullSeparatorHazard(in.schema, cnf, i, first)))
      ) {
        return true;
      }
      boolean unbound = false;
      boolean openRange = false;
      for (KeyRange r : slot) {
        if (
          afterOpenRange && (afterNullCheck || afterDescFixedUnbound) && nullable
            && r.lowerUnbound() && r != KeyRange.EVERYTHING_RANGE
        ) {
          return true;
        }
        unbound |= r.isUnbound();
        openRange |= r.isUnbound() && r != KeyRange.EVERYTHING_RANGE;
      }
      afterUnbound |= unbound;
      afterOpenRange |= openRange;
      afterNullCheck |= slot.contains(KeyRange.IS_NULL_RANGE);
      afterDescFixedUnbound |= unbound && in.schema.getField(field).getSortOrder() == SortOrder.DESC
        && in.schema.getField(field).getDataType().isFixedWidth();
    }
    return false;
  }

  /**
   * True when the IS_NULL in slot {@code i} breaks the separator search of the skip-scan filter.
   * The search fails on the DESC variable-length field before the null field when a later slot
   * restricts the key. It also fails on a null DESC variable-length field when the filter steps
   * back over it. This occurs when the next slot holds a single key and a later slot restricts the
   * key. As the last slot, a null DESC variable-length field breaks the seek hint after an
   * inclusive lower bound in the slot before it.
   */
  private static boolean isNullSeparatorHazard(RowKeySchema schema, List<List<KeyRange>> cnf, int i,
    int field) {
    boolean descVarLength = isDescVarLength(schema, field);
    if (i == cnf.size() - 1) {
      if (!descVarLength) {
        return false;
      }
      for (KeyRange r : cnf.get(i - 1)) {
        if (!r.lowerUnbound() && r.isLowerInclusive()) {
          return true;
        }
      }
      return false;
    }
    if (isDescVarLength(schema, field - 1) && !isGap(cnf.get(i + 1))) {
      return true;
    }
    if (!descVarLength || !hasSingleKey(cnf.get(i + 1))) {
      return false;
    }
    for (int j = i + 2; j < cnf.size(); j++) {
      if (!isGap(cnf.get(j))) {
        return true;
      }
    }
    return false;
  }

  /** True when the row key field is DESC and has a variable length. */
  private static boolean isDescVarLength(RowKeySchema schema, int field) {
    return schema.getField(field).getSortOrder() == SortOrder.DESC
      && !schema.getField(field).getDataType().isFixedWidth();
  }

  /** True when the slot does not restrict its key column. */
  private static boolean isGap(List<KeyRange> slot) {
    return slot.size() == 1 && slot.get(0) == KeyRange.EVERYTHING_RANGE;
  }

  /** True when the slot holds a single key, which includes IS_NULL. */
  private static boolean hasSingleKey(List<KeyRange> slot) {
    for (KeyRange r : slot) {
      if (r.isSingleKey()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the slot count to keep after a gap on a nullable DESC variable-length column, or -1.
   * The rule applies when a slot with a single key follows the gap and another slot follows it.
   * When the last of these slots rejects a row, the skip-scan filter steps back over the gap value.
   * It finds the end of a field by its separator byte. A null DESC value has a different separator
   * byte, so the filter can seek back or skip rows. The filter keeps the gap and the next slot,
   * because it then does not step back over the gap value.
   */
  private static int firstSlotAfterNullableDescGap(Inputs in, List<List<KeyRange>> cnf,
    int[] slotSpan) {
    // The row key schema merges equal adjacent fields and can lose their nullability. The PK
    // columns of the table keep it.
    List<PColumn> pkColumns = in.table.getPKColumns();
    int field = -1;
    for (int i = 0; i < cnf.size() - 2; i++) {
      field += slotSpan[i] + 1;
      int first = field - slotSpan[i];
      if (
        i >= in.prefixSlots && isGap(cnf.get(i)) && isDescVarLength(in.schema, field)
          && (first >= pkColumns.size() || pkColumns.get(first).isNullable())
          && hasSingleKey(cnf.get(i + 1))
      ) {
        return i + 2;
      }
    }
    return -1;
  }

  /**
   * True when a range in {@code slots} has raw bounds that read as empty without the inverted flag.
   * The raw lower bound is above the raw upper bound, or they are equal with an exclusive end. Only
   * an inverted range on a DESC variable-length column is valid in that form.
   */
  private static boolean hasCrossedRawBounds(List<List<KeyRange>> slots) {
    for (List<KeyRange> slot : slots) {
      for (KeyRange r : slot) {
        byte[] lower = r.getLowerRange();
        byte[] upper = r.getUpperRange();
        if (lower.length == 0 || upper.length == 0) {
          continue;
        }
        int cmp = Bytes.compareTo(lower, upper);
        if (cmp > 0 || (cmp == 0 && !(r.isLowerInclusive() && r.isUpperInclusive()))) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * True when a slot before the last slot ends on a DESC variable-length column and has a range
   * with a raw upper bound. When a later slot rejects a row, the skip-scan filter increments the
   * bytes of this column. On a DESC column, the incremented value can be above the upper bound. The
   * longer values that start with it can still be in the range. The filter then skips their rows.
   * For example, k2 > '1' has the raw upper bound \xCE. After '2' (\xCD), the filter tries \xCE and
   * stops, so it skips '10' (\xCE\xCF). A range scan with the residual filter keeps these rows.
   */
  private static boolean hasDescUpperBoundBeforeLastSlot(RowKeySchema schema,
    List<List<KeyRange>> cnf, int[] slotSpan) {
    return hasDescUpperBound(schema, cnf, slotSpan, cnf.size() - 1);
  }

  /**
   * True when one of the first {@code end} slots has the DESC upper bound that
   * {@link #hasDescUpperBoundBeforeLastSlot} describes. The extractor can drop the slots of a
   * trailing null run. The last slot then had later slots, so the check also applies to it.
   */
  private static boolean hasDescUpperBound(RowKeySchema schema, List<List<KeyRange>> cnf,
    int[] slotSpan, int end) {
    int field = -1;
    for (int i = 0; i < end; i++) {
      field += slotSpan[i] + 1;
      if (
        schema.getField(field).getSortOrder() != SortOrder.DESC
          || schema.getField(field).getDataType().isFixedWidth()
      ) {
        continue;
      }
      for (KeyRange r : cnf.get(i)) {
        if (!r.isSingleKey() && !r.upperUnbound()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * True when a range before the last slot can reach the key value with all bytes 0xFF, and the
   * skip-scan filter can then lose rows. When the next slot rejects a row, the filter increments
   * the key bytes up to the end of the range slot. For the all-0xFF value, this carries into an
   * earlier slot, but the filter keeps its position in the range slot. If that position is not the
   * first range, the seek hint skips the lower ranges for the new key prefix. A carry into a point
   * that is not next to another point is safe, because no row with the new prefix matches. A carry
   * into the salt byte is also safe, because each bucket has its own scan. If all the earlier key
   * bytes can also be 0xFF, the increment fails and the seek hint does not move forward. An
   * inclusive bound at the all-0xFF value has the same problem as an unbound range. The scan stop
   * row does not prevent it. That row can be past the first rejected key, or it can be empty. On
   * INTEGER keys, the skip scan fails at k1 = 2147483647 (\xFF\xFF\xFF\xFF) for
   * {@code k1 >= 0 AND k2 = 2} and for {@code k1 <= 2147483647 AND k2 < 6}. V1 uses a range scan
   * after an unbound range, so it does not have this problem there.
   */
  private static boolean hasAllOnesRangeBeforeLastSlot(Inputs in, List<List<KeyRange>> cnf,
    int[] slotSpan) {
    boolean[] canBeAllOnes = new boolean[cnf.size()];
    boolean[] safeCarry = new boolean[cnf.size()];
    int field = -1;
    for (int j = 0; j < cnf.size() - 1; j++) {
      field += slotSpan[j] + 1;
      List<KeyRange> slot = cnf.get(j);
      if (in.isSalted && j == 0) {
        // Only the last of 256 buckets has the salt byte 0xFF.
        canBeAllOnes[j] = SaltingUtil.MAX_BUCKET_NUM.equals(in.nBuckets);
        safeCarry[j] = true;
        continue;
      }
      boolean allOnesType = true;
      boolean fixedWidth = true;
      for (int f = field - slotSpan[j]; f <= field; f++) {
        allOnesType &= hasAllOnesValue(in.schema.getField(f));
        fixedWidth &= in.schema.getField(f).getDataType().isFixedWidth();
      }
      boolean allOnesRange = false;
      for (KeyRange r : slot) {
        boolean reaches = allOnesType
          && (r.upperUnbound() || (r.isUpperInclusive() && isAllOnes(r.getUpperRange())));
        canBeAllOnes[j] |= reaches;
        allOnesRange |= reaches && !r.isSingleKey();
      }
      safeCarry[j] = hasNoAdjacentPoints(slot, fixedWidth);
      // The filter increments the key at this slot only when the next slot can reject a row
      // above its last range.
      boolean nextCanOverflow = true;
      for (KeyRange r : cnf.get(j + 1)) {
        nextCanOverflow &= !r.upperUnbound();
      }
      if (!allOnesRange || !nextCanOverflow) {
        continue;
      }
      // Find the slot that takes the carry. Stop at a slot that cannot be all 0xFF.
      int m = j - 1;
      for (; m >= 0; m--) {
        if (slot.size() > 1 && !safeCarry[m]) {
          return true;
        }
        if (!canBeAllOnes[m]) {
          break;
        }
      }
      if (m < 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when the slot holds only points and no point is the next key value of another point. A
   * fixed-width type is necessary for more than one point, because the increment of a
   * variable-width key goes into its separator byte.
   */
  private static boolean hasNoAdjacentPoints(List<KeyRange> slot, boolean fixedWidth) {
    if (slot.size() == 1) {
      return slot.get(0).isSingleKey();
    }
    if (!fixedWidth) {
      return false;
    }
    TreeSet<byte[]> points = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (KeyRange r : slot) {
      if (!r.isSingleKey()) {
        return false;
      }
      points.add(r.getLowerRange());
    }
    for (byte[] point : points) {
      byte[] next = point.clone();
      if (ByteUtil.nextKey(next, next.length) && points.contains(next)) {
        return false;
      }
    }
    return true;
  }

  /** True when a value of the fixed-width field has key bytes that are all 0xFF. */
  private static boolean hasAllOnesValue(Field field) {
    PDataType type = field.getDataType();
    if (!type.isFixedWidth()) {
      return false;
    }
    byte[] ones = new byte[field.getByteSize()];
    Arrays.fill(ones, (byte) -1);
    try {
      Object value = type.toObject(ones, 0, ones.length, type, field.getSortOrder(),
        field.getMaxLength(), field.getScale());
      return Arrays.equals(ones, type.toBytes(value, field.getSortOrder()));
    } catch (RuntimeException e) {
      // The bytes are not a valid value of the type.
      return false;
    }
  }

  private static boolean isAllOnes(byte[] bytes) {
    for (byte b : bytes) {
      if (b != -1) {
        return false;
      }
    }
    return bytes.length > 0;
  }

  /**
   * True when a slot after a slot that is not a point has a range with an empty raw lower bound,
   * and the slot starts on a key column that can be null. A null value has empty bytes, and these
   * bytes sort first for both sort orders. Such a range thus admits null, or it is IS NULL. The
   * skip-scan filter compares these rows in place, without a seek to the bound. It does not keep
   * null apart from the other values, so the residual filter must keep the predicate. The PK
   * columns give the nullable flag, because the condensed row key schema can lose it.
   */
  private static boolean hasNullableEmptyLowerAfterRange(PTable table, List<List<KeyRange>> cnf,
    int[] slotSpan) {
    boolean afterRange = false;
    int field = 0;
    for (int i = 0; i < cnf.size(); field += slotSpan[i] + 1, i++) {
      for (KeyRange r : cnf.get(i)) {
        if (
          afterRange && r.getLowerRange().length == 0
            && (r.getUpperRange().length > 0 || r.isSingleKey())
            && table.getPKColumns().get(field).isNullable()
        ) {
          return true;
        }
      }
      for (KeyRange r : cnf.get(i)) {
        afterRange |= !r.isSingleKey();
      }
    }
    return false;
  }

  /**
   * Classifier: is every space in the list all-single-key across every productive dim, with no
   * IS_NULL / IS_NOT_NULL sentinels? This is the RVC-IN / RVC-equality OR shape.
   * <p>
   * Restricted to multi-space lists (size ≥ 2). Single-space all-pinned shapes flow through the
   * classical path which is already byte-identical to V1 (proven by parity harness across 142
   * tests); routing them through the native path would change byte output unnecessarily and break
   * byte-shape assertions on point lookups.
   */
  private static boolean isPointLookupList(Inputs in) {
    if (in.list.isUnsatisfiable() || in.list.isEverything()) {
      return false;
    }
    if (in.list.size() < 2) {
      return false;
    }
    if (in.isSalted) {
      // Salted tables: each row's salt byte is hash(row_key_no_salt) % nBuckets; the
      // native path can't replicate that hashing here. ScanRanges.create does it
      // correctly for point-lookup shapes via getPointKeys; defer to the adapter.
      return false;
    }
    if (in.minOffset.isPresent()) {
      // RVC-OFFSET uses getScanRange().getLowerRange() downstream; the classical path's
      // byte layout is what that consumer expects. Stay on the adapter.
      return false;
    }
    // Every space must be all-single-key past prefix, every dim must be constrained
    // (no middle gaps), and no IS_NULL / IS_NOT_NULL sentinels.
    int nPk = in.nPkColumns;
    int productiveDims = 0;
    for (KeySpace s : in.list.spaces()) {
      int thisProductive = 0;
      for (int d = in.prefixSlots; d < nPk; d++) {
        KeyRange r = s.get(d);
        if (r == KeyRange.EVERYTHING_RANGE) {
          // Any unconstrained user dimension (leading or middle) means this is not a
          // full-row point key. Leading EVERYTHING is especially dangerous: the encoder
          // emits a separator for the wildcard and buildPointLookupList would treat the
          // resulting bytes as an exact point, missing valid rows.
          return false;
        }
        if (r == KeyRange.IS_NULL_RANGE || r == KeyRange.IS_NOT_NULL_RANGE) return false;
        if (!r.isSingleKey()) return false;
        thisProductive++;
      }
      productiveDims = Math.max(productiveDims, thisProductive);
    }
    // Native path is targeted at multi-PK-column RVC-IN shapes where per-slot cartesian
    // would lose tuple correlation. Single-PK-column IN-lists (e.g., `pk IN (a,b,c)`)
    // flow through the classical path; it produces correct bytes for them, and the
    // encoder's per-dim output doesn't include the trailing terminator that HBase stored
    // rows have on DESC var-width columns, producing off-by-one startRow comparisons
    // (see WhereOptimizerTest.testLastPkColumnIsVariableLengthAndDescBug5307's first
    // assertion for the 5-byte DESC-VARCHAR single-col single-tuple shape).
    if (productiveDims < 2) {
      return false;
    }
    return true;
  }

  /**
   * Build a {@link ScanRanges} for a POINT_LOOKUP_LIST shape directly via
   * {@link CompoundByteEncoder}. Each space becomes one full-rowkey byte[] (including prefix
   * bytes); these are fed to {@code ScanRanges.create} with VAR_BINARY_SCHEMA so downstream
   * {@code isPointLookup} classification succeeds and the scan is dispatched as a SkipScan of point
   * keys.
   * <p>
   * Returns {@code null} if any space's encoded lower bytes are UNBOUND (would collapse the list) —
   * the caller falls back to the adapter.
   */
  private static Result buildPointLookupList(Inputs in) {
    byte[] prefixBytes = buildPrefixBytes(in);
    java.util.List<KeyRange> pointKeys = new java.util.ArrayList<>(in.list.spaces().size());
    for (KeySpace s : in.list.spaces()) {
      byte[] tail = CompoundByteEncoder.encodeLower(in.schema, in.pkNullable, s, in.prefixSlots);
      if (tail == null || tail.length == 0) {
        // Encoder refused this space (e.g. all-EVERYTHING past prefix). Fall back.
        return null;
      }
      byte[] full;
      if (prefixBytes.length == 0) {
        full = tail;
      } else {
        full = new byte[prefixBytes.length + tail.length];
        System.arraycopy(prefixBytes, 0, full, 0, prefixBytes.length);
        System.arraycopy(tail, 0, full, prefixBytes.length, tail.length);
      }
      pointKeys.add(KeyRange.getKeyRange(full));
    }
    if (pointKeys.isEmpty()) {
      return Result.nothing();
    }
    java.util.List<java.util.List<KeyRange>> cnf = java.util.Collections.singletonList(pointKeys);
    int[] slotSpan = org.apache.phoenix.util.ScanUtil.SINGLE_COLUMN_SLOT_SPAN;
    // Use VAR_BINARY_SCHEMA so ScanRanges.create treats this as raw bytes — isPointLookup
    // succeeds, and SkipScanFilter navigates the N point keys individually without trying
    // to decode them against the original schema's per-field comparators.
    ScanRanges scanRanges =
      ScanRanges.create(org.apache.phoenix.util.SchemaUtil.VAR_BINARY_SCHEMA, cnf, slotSpan,
        in.nBuckets, pointKeys.size() > 1, in.table.getRowTimestampColPos(), in.minOffset);
    return new Result(scanRanges, false, in.list.isApproximated());
  }

  /**
   * Prefix bytes for salt / viewIndexId / tenantId — mirror of {@code WhereOptimizerV2
   * .buildPrefixBytes}. Duplicated here to keep {@link V2ScanBuilder} self-contained on the native
   * emission path.
   */
  private static byte[] buildPrefixBytes(Inputs in) {
    java.util.List<byte[]> parts = new java.util.ArrayList<>(3);
    if (in.isSalted) {
      parts.add(new byte[] { 0 });
    }
    if (in.isSharedIndex) {
      parts.add(in.table.getviewIndexIdType().toBytes(in.table.getViewIndexId()));
    }
    if (in.isMultiTenant) {
      parts.add(in.tenantIdBytes);
      org.apache.phoenix.schema.ValueSchema.Field f =
        in.table.getRowKeySchema().getField((in.isSalted ? 1 : 0) + (in.isSharedIndex ? 1 : 0));
      if (!f.getDataType().isFixedWidth()) {
        parts.add(new byte[] { QueryConstants.SEPARATOR_BYTE });
      }
    }
    int total = 0;
    for (byte[] p : parts)
      total += p.length;
    byte[] out = new byte[total];
    int off = 0;
    for (byte[] p : parts) {
      System.arraycopy(p, 0, out, off, p.length);
      off += p.length;
    }
    return out;
  }
}
