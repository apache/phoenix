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
package org.apache.phoenix.compile.keyspace.oracle;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;

/**
 * Converts V2's {@link ScanRanges} output (byte-level {@link KeyRange}s per slot) back into
 * an {@link AbstractKeySpaceList} with typed values, so we can diff it against the oracle's
 * output in the same domain.
 * <p>
 * Limitations:
 * <ul>
 * <li>Assumes no salted / multi-tenant / view-index prefix (harness skips those tables).</li>
 * <li>Assumes {@code slotSpan[i] == 0} for every slot (V2's current emission).
 * {@code slotSpan[i] > 0} would mean a single range spans multiple PK cols in byte-space
 * and cannot be decoded per-column without reversing the concatenation.</li>
 * <li>Ignores the {@code scanRange} (the pre-computed start/stop row) and works from the
 * per-slot {@code ranges} instead — that's the representation the oracle compares against.</li>
 * </ul>
 * If any of these preconditions are violated the decoder throws
 * {@link UnsupportedEncodingShape}.
 */
public final class ScanRangesDecoder {

  public static final class UnsupportedEncodingShape extends RuntimeException {
    public UnsupportedEncodingShape(String reason) {
      super(reason);
    }
  }

  private ScanRangesDecoder() {}

  /**
   * Decode {@code sr} against {@code table}'s PK columns and return one
   * {@link AbstractKeySpaceList} describing the same rows.
   * <p>
   * Mapping rule:
   * <ul>
   * <li>{@code ScanRanges.EVERYTHING} → {@link AbstractKeySpaceList#everything(int)}.</li>
   * <li>{@code ScanRanges.NOTHING} → {@link AbstractKeySpaceList#unsatisfiable(int)}.</li>
   * <li>Otherwise: for each slot {@code i}, decode each {@link KeyRange} to an
   * {@link AbstractRange} over the column's Java type; collapse to one
   * {@link AbstractKeySpace} per combination (cartesian product across slots — what the
   * per-slot emission shape describes semantically).</li>
   * </ul>
   */
  public static AbstractKeySpaceList decode(ScanRanges sr, PTable table) {
    int nPk = table.getPKColumns().size();
    if (sr == ScanRanges.EVERYTHING) {
      return AbstractKeySpaceList.everything(nPk);
    }
    if (sr == ScanRanges.NOTHING) {
      return AbstractKeySpaceList.unsatisfiable(nPk);
    }
    List<List<KeyRange>> slots = sr.getRanges();
    int[] slotSpan = sr.getSlotSpans();
    // Always use the TABLE'S RowKeySchema for per-column byte splitting — not sr.getSchema().
    // When ScanRanges.create detects a multi-key point lookup it rewrites the schema to
    // VAR_BINARY_SCHEMA and collapses slotSpan to [0], treating each compound as a single
    // varbinary field. That's an internal optimization; the semantically correct per-column
    // decomposition is still available via the table's PK schema.
    RowKeySchema schema = table.getRowKeySchema();

    // Special case: point-lookup mode. `sr.isPointLookup()` means `ranges` is a single
    // slot of one or more full-PK compound byte keys. Split each compound into per-column
    // point values to recover the AbstractKeySpace shape; each compound key contributes
    // one KeySpace with per-column points.
    if (sr.isPointLookup() && slots.size() == 1) {
      return decodePointLookup(slots.get(0), schema, table, nPk);
    }

    // Walk slots; each slot contributes one or more KeyRanges. `slotSpan[i]` is the extra
    // PK columns packed into slot i (0 = one column, k > 0 = k+1 columns in the bytes).
    //
    // Output is a per-PK-col list of AbstractRange<?>. For a compound slot with
    // slotSpan[i] = k spanning cols [pkCursor..pkCursor+k], we split its single range into
    // k+1 per-column entries via splitCompoundRange; multi-range compound slots aren't
    // supported here and bail to UnsupportedEncodingShape.
    List<List<AbstractRange<?>>> perCol = new ArrayList<>(nPk);
    for (int i = 0; i < nPk; i++) {
      perCol.add(null);
    }
    int pkCursor = 0;
    for (int i = 0; i < slots.size(); i++) {
      List<KeyRange> ranges = slots.get(i);
      int span = slotSpan[i]; // extra-cols packed; 0 → slot is 1 column
      int firstCol = pkCursor;
      int lastCol = pkCursor + span;
      if (lastCol >= nPk) {
        throw new UnsupportedEncodingShape(
          "slot " + i + " span " + span + " exceeds PK arity");
      }
      if (ranges.size() == 1 && ranges.get(0) == KeyRange.EMPTY_RANGE) {
        return AbstractKeySpaceList.unsatisfiable(nPk);
      }

      if (span == 0) {
        // Simple per-column slot.
        List<AbstractRange<?>> decoded = new ArrayList<>(ranges.size());
        PColumn column = table.getPKColumns().get(firstCol);
        for (KeyRange r : ranges) {
          decoded.add(decodeRange(r, column));
        }
        perCol.set(firstCol, decoded);
      } else {
        // Compound slot: each KeyRange is a lex-ordered byte interval over (span + 1)
        // PK cols. Lex intervals cannot be faithfully represented as N-dim boxes in
        // general; the decoder expands each compound range into a disjunction of
        // lex-step boxes (the inverse of ExpressionNormalizer.rewriteRvcInequality).
        // With multiple KeyRanges in the slot we union the expansions.
        return decodeWithCompoundSlots(slots, slotSpan, schema, table, nPk);
      }
      pkCursor = lastCol + 1;
    }
    // Trailing PK cols past the last slot get EVERYTHING.
    for (int i = pkCursor; i < nPk; i++) {
      perCol.set(i, java.util.Collections.<AbstractRange<?>>singletonList(
        AbstractRange.everything()));
    }
    // Defensive: any slot we didn't fill (shouldn't happen, but handle anyway).
    for (int i = 0; i < nPk; i++) {
      if (perCol.get(i) == null) {
        perCol.set(i, java.util.Collections.<AbstractRange<?>>singletonList(
          AbstractRange.everything()));
      }
    }

    // Cartesian product across cols: one AbstractKeySpace per combination. For typical
    // queries each col has 1 range. For IN-list on one PK col the product equals the IN
    // list size.
    List<AbstractKeySpace> cross = cartesian(perCol, nPk);
    if (cross.isEmpty()) return AbstractKeySpaceList.unsatisfiable(nPk);
    return AbstractKeySpaceList.of(nPk, cross.toArray(new AbstractKeySpace[0]));
  }

  /**
   * Decode a general ScanRanges shape that mixes per-column slots and compound slots
   * (slotSpan &gt; 0 with multiple ranges). Each slot contributes a disjunction of tuples
   * over its covered PK columns; the final KeySpaceList is the cartesian product of the
   * per-slot disjunctions.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static AbstractKeySpaceList decodeWithCompoundSlots(List<List<KeyRange>> slots,
    int[] slotSpan, RowKeySchema schema, PTable table, int nPk) {
    // For each slot, produce a list of "tuples" (AbstractRange[] of the slot's column span).
    // A span=0 slot with K ranges produces K length-1 tuples; a span=S slot with M ranges
    // produces M length-(S+1) tuples (each compound split into S+1 per-column ranges).
    List<List<AbstractRange<?>[]>> perSlotTuples = new ArrayList<>(slots.size());
    int[] perSlotSpan = new int[slots.size()];
    int pkCursor = 0;
    for (int i = 0; i < slots.size(); i++) {
      List<KeyRange> ranges = slots.get(i);
      int span = slotSpan[i];
      int firstCol = pkCursor;
      int lastCol = pkCursor + span;
      perSlotSpan[i] = span + 1;
      List<AbstractRange<?>[]> tuples = new ArrayList<>(ranges.size());
      if (span == 0) {
        PColumn column = table.getPKColumns().get(firstCol);
        for (KeyRange r : ranges) {
          tuples.add(new AbstractRange<?>[] { decodeRange(r, column) });
        }
      } else {
        // Each compound KeyRange expands to a disjunction of lex-boxes (one per "step"
        // in the lex decomposition). Each resulting tuple is a separate choice in the
        // slot's disjunction.
        for (KeyRange compound : ranges) {
          tuples.addAll(expandCompoundRange(compound, schema, table, firstCol, span + 1));
        }
      }
      perSlotTuples.add(tuples);
      pkCursor = lastCol + 1;
    }
    // Fill trailing PK cols with EVERYTHING (as a single slot with span+1 = remaining cols).
    int trailingCols = nPk - pkCursor;
    if (trailingCols > 0) {
      AbstractRange<?>[] trailing = new AbstractRange<?>[trailingCols];
      java.util.Arrays.fill(trailing, AbstractRange.everything());
      List<AbstractRange<?>[]> trailingTuples = new ArrayList<>(1);
      trailingTuples.add(trailing);
      perSlotTuples.add(trailingTuples);
      perSlotSpan = java.util.Arrays.copyOf(perSlotSpan, perSlotSpan.length + 1);
      perSlotSpan[perSlotSpan.length - 1] = trailingCols;
    }

    // Cartesian product across slots: each combination becomes one AbstractKeySpace.
    List<AbstractKeySpace> spaces = new ArrayList<>();
    AbstractRange<?>[] current = new AbstractRange<?>[nPk];
    cartesianCompound(perSlotTuples, perSlotSpan, 0, 0, current, spaces, nPk);
    if (spaces.isEmpty()) return AbstractKeySpaceList.unsatisfiable(nPk);
    return AbstractKeySpaceList.of(nPk, spaces.toArray(new AbstractKeySpace[0]));
  }

  private static void cartesianCompound(List<List<AbstractRange<?>[]>> perSlotTuples,
    int[] perSlotSpan, int slotIdx, int colCursor, AbstractRange<?>[] current,
    List<AbstractKeySpace> out, int nPk) {
    if (slotIdx == perSlotTuples.size()) {
      // Fill any remaining columns (shouldn't happen if perSlotSpan sums to nPk, but be safe).
      for (int i = colCursor; i < nPk; i++) {
        current[i] = AbstractRange.everything();
      }
      out.add(AbstractKeySpace.of(current));
      return;
    }
    int span = perSlotSpan[slotIdx];
    for (AbstractRange<?>[] tuple : perSlotTuples.get(slotIdx)) {
      for (int i = 0; i < span; i++) {
        current[colCursor + i] = tuple[i];
      }
      cartesianCompound(perSlotTuples, perSlotSpan, slotIdx + 1, colCursor + span, current, out,
        nPk);
    }
  }

  /**
   * Decode a point-lookup {@link ScanRanges}: one slot containing one or more compound
   * point-key byte ranges, each of which represents a full N-column PK tuple. Each
   * compound key becomes an {@link AbstractKeySpace} with per-column point values.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static AbstractKeySpaceList decodePointLookup(List<KeyRange> keys, RowKeySchema schema,
    PTable table, int nPk) {
    List<AbstractKeySpace> spaces = new ArrayList<>(keys.size());
    for (KeyRange r : keys) {
      if (r == KeyRange.EMPTY_RANGE) continue;
      if (!r.isSingleKey()) {
        throw new UnsupportedEncodingShape("point-lookup slot contains non-singleton range");
      }
      byte[] bytes = r.getLowerRange();
      Object[] vals = new Object[nPk];
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      schema.iterator(bytes, 0, bytes.length, ptr, 0);
      int maxOffset = bytes.length;
      for (int i = 0; i < nPk; i++) {
        Boolean hasValue = schema.next(ptr, i, maxOffset);
        if (hasValue == null) break;
        if (ptr.getLength() > 0) {
          PColumn col = table.getPKColumns().get(i);
          vals[i] = col.getDataType().toObject(ptr.get(), ptr.getOffset(), ptr.getLength(),
            col.getDataType(), col.getSortOrder());
        }
      }
      AbstractRange<?>[] dims = new AbstractRange<?>[nPk];
      for (int i = 0; i < nPk; i++) {
        if (vals[i] == null) {
          dims[i] = AbstractRange.everything();
        } else if (vals[i] instanceof Comparable) {
          dims[i] = AbstractRange.point((Comparable) vals[i]);
        } else {
          throw new UnsupportedEncodingShape("point-lookup col not Comparable: " + vals[i]);
        }
      }
      spaces.add(AbstractKeySpace.of(dims));
    }
    if (spaces.isEmpty()) return AbstractKeySpaceList.unsatisfiable(nPk);
    return AbstractKeySpaceList.of(nPk, spaces.toArray(new AbstractKeySpace[0]));
  }

  /**
   * Expand a compound lex-interval {@link KeyRange} into one or more {@link AbstractRange}
   * tuples, each representing a box in the lex decomposition. The disjunction of the
   * returned tuples equals the original lex interval exactly.
   * <p>
   * For a compound range with lower {@code L = (l0, ..., l_{p-1})} and upper
   * {@code U = (u0, ..., u_{q-1})}, sharing a common prefix of length {@code k}:
   * <ul>
   * <li>Dims {@code [0, k)} are pinned to the prefix (point ranges).</li>
   * <li>At dim {@code k}: either L and U span the same column with a byte-range, or
   * we split into one "lower tail" step + one "open middle" box + one "upper tail" step.</li>
   * <li>Trailing dims depend on the step chosen at dim {@code k}.</li>
   * </ul>
   * Returns a list of {@code AbstractRange[colCount]} tuples.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static List<AbstractRange<?>[]> expandCompoundRange(KeyRange compound,
    RowKeySchema schema, PTable table, int firstCol, int colCount) {
    Object[] lo = decodeCompoundTuple(compound, Bound.LOWER, schema, table, firstCol, colCount);
    Object[] hi = decodeCompoundTuple(compound, Bound.UPPER, schema, table, firstCol, colCount);
    boolean loInc = compound.isLowerInclusive();
    boolean hiInc = compound.isUpperInclusive();
    boolean loUnbound = compound.lowerUnbound();
    boolean hiUnbound = compound.upperUnbound();

    List<AbstractRange<?>[]> result = new ArrayList<>();

    // Trivial shortcuts.
    if (loUnbound && hiUnbound) {
      AbstractRange<?>[] tuple = new AbstractRange<?>[colCount];
      java.util.Arrays.fill(tuple, AbstractRange.everything());
      result.add(tuple);
      return result;
    }

    // Find common prefix length.
    int k = 0;
    while (k < colCount && lo[k] != null && hi[k] != null
      && java.util.Objects.equals(lo[k], hi[k])) {
      k++;
    }

    if (k == colCount) {
      // L == U fully — equivalent to a single point if both inclusive, else empty.
      if (!loInc || !hiInc) return result; // empty
      AbstractRange<?>[] tuple = new AbstractRange<?>[colCount];
      for (int i = 0; i < colCount; i++) {
        tuple[i] = lo[i] != null ? AbstractRange.point((Comparable) lo[i])
          : AbstractRange.everything();
      }
      result.add(tuple);
      return result;
    }

    // Common prefix dims [0, k) are points. From dim k onward we build the lex steps.
    AbstractRange<?>[] prefix = new AbstractRange<?>[k];
    for (int i = 0; i < k; i++) {
      prefix[i] = lo[i] != null ? AbstractRange.point((Comparable) lo[i])
        : AbstractRange.everything();
    }

    Object loK = k < lo.length ? lo[k] : null;
    Object hiK = k < hi.length ? hi[k] : null;
    boolean loHasTailBelow = hasNonNullTail(lo, k + 1);
    boolean hiHasTailBelow = hasNonNullTail(hi, k + 1);

    // Step 1 (LOWER TAIL): when dim k equals lo[k], dim [k+1..] must be >= lo[k+1..].
    // Only relevant if lo has a tail beyond k.
    if (loK != null && loHasTailBelow) {
      result.addAll(expandLowerTail(prefix, loK, lo, loInc, k, colCount));
    } else if (loK != null) {
      // No tail — add a tuple with dim k starting at lo[k] (with loInc), rest EVERYTHING,
      // bounded above by hi[k] (see middle/upper steps below).
    }

    // Step 2 (OPEN MIDDLE): dim k ∈ (lo[k], hi[k]), dims [k+1..] unconstrained.
    // If lo[k] has no tail, this becomes [lo[k], hi[k]) with appropriate inclusivity.
    AbstractRange<?>[] middleTuple = new AbstractRange<?>[colCount];
    System.arraycopy(prefix, 0, middleTuple, 0, k);
    boolean middleLoInc = (loK != null && !loHasTailBelow) ? loInc : false;
    boolean middleHiInc = (hiK != null && !hiHasTailBelow) ? hiInc : false;
    if (loK == null && hiK == null) {
      middleTuple[k] = AbstractRange.everything();
    } else {
      middleTuple[k] = AbstractRange.of((Comparable) loK, middleLoInc, (Comparable) hiK,
        middleHiInc);
    }
    for (int i = k + 1; i < colCount; i++) {
      middleTuple[i] = AbstractRange.everything();
    }
    // Only add middle if dim-k range is non-empty.
    if (!middleTuple[k].isEmpty()) {
      result.add(middleTuple);
    }

    // Step 3 (UPPER TAIL): when dim k equals hi[k], dim [k+1..] must be < hi[k+1..].
    if (hiK != null && hiHasTailBelow) {
      result.addAll(expandUpperTail(prefix, hiK, hi, hiInc, k, colCount));
    }

    return result;
  }

  private static boolean hasNonNullTail(Object[] arr, int fromIdx) {
    for (int i = fromIdx; i < arr.length; i++) {
      if (arr[i] != null) return true;
    }
    return false;
  }

  /**
   * Expand the "dim k == lo[k] AND dims [k+1..] >= lo-tail" step into a disjunction of
   * lex-boxes. Recurses on the tail.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static List<AbstractRange<?>[]> expandLowerTail(AbstractRange<?>[] prefix,
    Object loK, Object[] lo, boolean loInc, int k, int colCount) {
    // Pin dim k to lo[k].
    AbstractRange<?>[] withPin = new AbstractRange<?>[colCount];
    System.arraycopy(prefix, 0, withPin, 0, k);
    withPin[k] = AbstractRange.point((Comparable) loK);
    // For the remaining dims, we need lex >= lo[k+1..]. This recursively expands.
    List<AbstractRange<?>[]> tail = expandOneSidedLower(lo, k + 1, colCount, loInc);
    List<AbstractRange<?>[]> result = new ArrayList<>(tail.size());
    for (AbstractRange<?>[] tailTuple : tail) {
      AbstractRange<?>[] combined = withPin.clone();
      for (int i = k + 1; i < colCount; i++) {
        combined[i] = tailTuple[i - (k + 1)];
      }
      result.add(combined);
    }
    return result;
  }

  /**
   * Expand the "dim k == hi[k] AND dims [k+1..] < hi-tail" step into a disjunction.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static List<AbstractRange<?>[]> expandUpperTail(AbstractRange<?>[] prefix,
    Object hiK, Object[] hi, boolean hiInc, int k, int colCount) {
    AbstractRange<?>[] withPin = new AbstractRange<?>[colCount];
    System.arraycopy(prefix, 0, withPin, 0, k);
    withPin[k] = AbstractRange.point((Comparable) hiK);
    List<AbstractRange<?>[]> tail = expandOneSidedUpper(hi, k + 1, colCount, hiInc);
    List<AbstractRange<?>[]> result = new ArrayList<>(tail.size());
    for (AbstractRange<?>[] tailTuple : tail) {
      AbstractRange<?>[] combined = withPin.clone();
      for (int i = k + 1; i < colCount; i++) {
        combined[i] = tailTuple[i - (k + 1)];
      }
      result.add(combined);
    }
    return result;
  }

  /**
   * One-sided lex expansion: rows whose dim tuple is {@code >= tail[from..end)} (strict
   * or inclusive based on {@code inclusive}), expressed over a dim array of length
   * {@code colCount - from}. Returns a list of box-tuples.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static List<AbstractRange<?>[]> expandOneSidedLower(Object[] tail, int from,
    int colCount, boolean inclusive) {
    int tailLen = colCount - from;
    List<AbstractRange<?>[]> out = new ArrayList<>();
    if (tailLen <= 0) return out;
    // Step i (i in [from, colCount)): dims [from..i-1] = tail[..], dim i > tail[i]
    //   (or for the last step, if inclusive, dim i >= tail[i]).
    for (int i = from; i < colCount; i++) {
      AbstractRange<?>[] tuple = new AbstractRange<?>[tailLen];
      // Pin [from..i-1] to tail[from..i-1].
      for (int j = from; j < i; j++) {
        if (tail[j] == null) {
          tuple[j - from] = AbstractRange.everything();
        } else {
          tuple[j - from] = AbstractRange.point((Comparable) tail[j]);
        }
      }
      // Dim i: > tail[i] (strict), or for the last dim with inclusive, >= tail[i].
      boolean isLast = (i == colCount - 1);
      Object v = tail[i];
      if (v == null) {
        tuple[i - from] = AbstractRange.everything();
      } else if (isLast && inclusive) {
        tuple[i - from] = AbstractRange.atLeast((Comparable) v);
      } else {
        tuple[i - from] = AbstractRange.greaterThan((Comparable) v);
      }
      // Dims (i, colCount): unconstrained.
      for (int j = i + 1; j < colCount; j++) {
        tuple[j - from] = AbstractRange.everything();
      }
      out.add(tuple);
    }
    return out;
  }

  /** Mirror of {@link #expandOneSidedLower} for upper-bounded lex. */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static List<AbstractRange<?>[]> expandOneSidedUpper(Object[] tail, int from,
    int colCount, boolean inclusive) {
    int tailLen = colCount - from;
    List<AbstractRange<?>[]> out = new ArrayList<>();
    if (tailLen <= 0) return out;
    for (int i = from; i < colCount; i++) {
      AbstractRange<?>[] tuple = new AbstractRange<?>[tailLen];
      for (int j = from; j < i; j++) {
        if (tail[j] == null) {
          tuple[j - from] = AbstractRange.everything();
        } else {
          tuple[j - from] = AbstractRange.point((Comparable) tail[j]);
        }
      }
      boolean isLast = (i == colCount - 1);
      Object v = tail[i];
      if (v == null) {
        tuple[i - from] = AbstractRange.everything();
      } else if (isLast && inclusive) {
        tuple[i - from] = AbstractRange.atMost((Comparable) v);
      } else {
        tuple[i - from] = AbstractRange.lessThan((Comparable) v);
      }
      for (int j = i + 1; j < colCount; j++) {
        tuple[j - from] = AbstractRange.everything();
      }
      out.add(tuple);
    }
    return out;
  }

  /** Kept for backward compatibility with earlier callers. */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static AbstractRange<?>[] splitCompoundRange(KeyRange compound, RowKeySchema schema,
    PTable table, int firstCol, int colCount) {
    List<AbstractRange<?>[]> expansion =
      expandCompoundRange(compound, schema, table, firstCol, colCount);
    // This method preserves the old contract (single tuple output). When multi-step
    // expansion is needed the caller should use expandCompoundRange directly.
    if (expansion.size() == 1) return expansion.get(0);
    AbstractRange<?>[] widened = new AbstractRange<?>[colCount];
    java.util.Arrays.fill(widened, AbstractRange.everything());
    return widened;
  }

  private enum Bound { LOWER, UPPER }

  /**
   * Decode the lower or upper bound of a compound KeyRange into an {@code Object[]} of
   * per-column typed values, one entry per PK column in the compound span.
   * {@code null} at a position means "unbounded" at that column (the bound bytes ran out
   * before reaching this column).
   */
  private static Object[] decodeCompoundTuple(KeyRange compound, Bound bound, RowKeySchema schema,
    PTable table, int firstCol, int colCount) {
    boolean unbound = (bound == Bound.LOWER) ? compound.lowerUnbound() : compound.upperUnbound();
    byte[] bytes = (bound == Bound.LOWER) ? compound.getLowerRange() : compound.getUpperRange();
    Object[] out = new Object[colCount];
    if (unbound || bytes.length == 0) {
      return out;
    }
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    // iterator(..., position=0) sets ptr to (src, 0, 0) but doesn't advance into field 0.
    // We then use next(ptr, i, maxOffset) which reads field i by advancing past its bytes
    // AND handles the leading separator skip for variable-width previous fields.
    schema.iterator(bytes, 0, bytes.length, ptr, firstCol);
    int maxOffset = bytes.length;
    for (int i = 0; i < colCount; i++) {
      int colIdx = firstCol + i;
      Boolean hasValue = schema.next(ptr, colIdx, maxOffset);
      if (hasValue == null) break;
      PColumn column = table.getPKColumns().get(colIdx);
      PDataType type = column.getDataType();
      SortOrder sortOrder = column.getSortOrder();
      if (ptr.getLength() > 0) {
        Object v = type.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), type, sortOrder);
        out[i] = v;
      }
    }
    return out;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static AbstractRange<?> decodeRange(KeyRange r, PColumn column) {
    if (r == KeyRange.EVERYTHING_RANGE) return AbstractRange.everything();
    if (r == KeyRange.EMPTY_RANGE) return AbstractRange.empty();
    PDataType type = column.getDataType();
    SortOrder sortOrder = column.getSortOrder();
    Object lo = null;
    Object hi = null;
    if (!r.lowerUnbound() && r.getLowerRange().length > 0) {
      lo = type.toObject(r.getLowerRange(), 0, r.getLowerRange().length, type, sortOrder);
    }
    if (!r.upperUnbound() && r.getUpperRange().length > 0) {
      hi = type.toObject(r.getUpperRange(), 0, r.getUpperRange().length, type, sortOrder);
    }
    boolean loInc = r.isLowerInclusive();
    boolean hiInc = r.isUpperInclusive();
    if (lo != null && !(lo instanceof Comparable)) {
      throw new UnsupportedEncodingShape("decoded lo not Comparable: " + lo);
    }
    if (hi != null && !(hi instanceof Comparable)) {
      throw new UnsupportedEncodingShape("decoded hi not Comparable: " + hi);
    }
    return AbstractRange.of((Comparable) lo, loInc, (Comparable) hi, hiInc);
  }

  /**
   * Cartesian product of per-slot ranges → list of {@link AbstractKeySpace}. Each element of
   * the result combines one range from each slot into an N-dim tuple. For typical queries
   * the product is small (each slot has 1 range); for IN-list-on-single-dim queries the
   * product equals the IN list size.
   */
  private static List<AbstractKeySpace> cartesian(List<List<AbstractRange<?>>> perSlot, int nPk) {
    List<AbstractKeySpace> out = new ArrayList<>();
    AbstractRange<?>[] current = new AbstractRange<?>[nPk];
    buildCartesian(perSlot, 0, current, out, nPk);
    return out;
  }

  private static void buildCartesian(List<List<AbstractRange<?>>> perSlot, int slotIdx,
    AbstractRange<?>[] current, List<AbstractKeySpace> out, int nPk) {
    if (slotIdx == nPk) {
      out.add(AbstractKeySpace.of(current));
      return;
    }
    for (AbstractRange<?> r : perSlot.get(slotIdx)) {
      current[slotIdx] = r;
      buildCartesian(perSlot, slotIdx + 1, current, out, nPk);
    }
  }
}
