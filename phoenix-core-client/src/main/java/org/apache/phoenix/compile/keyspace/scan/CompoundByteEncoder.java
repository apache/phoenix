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

import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.compile.keyspace.KeySpaceList;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;

/**
 * V2-owned byte encoder for a compound primary-key scan bound.
 * <p>
 * Converts a single {@link KeySpace} (one N-dim box) into the start-row/stop-row byte
 * sequences that HBase consumes. Owns the separator-insertion rules, DESC inversion,
 * inclusive/exclusive bound bumping — the same responsibilities as
 * {@code ScanUtil.setKey}, but with V2-specific choices about slot spans and tail-strip
 * behavior that the V1-shaped entry point can't easily accommodate.
 * <p>
 * <b>Scope of this commit (infrastructure).</b> The encoder is standalone and golden-
 * tested against hand-constructed {@link KeySpace} inputs with expected V1-equivalent
 * byte outputs. It is NOT yet wired into the scan path; V2ScanBuilder still delegates
 * to {@code ScanRanges.create} for actual scan construction. Wiring happens in a
 * follow-up commit once the encoder has enough shape coverage to back the RVC-boundary
 * class of test failures.
 * <p>
 * <b>Rules.</b> For each PK column between {@code prefixSlots} and {@code lastConstrained}:
 * <ol>
 * <li>Write the column's lower (for {@link org.apache.phoenix.query.KeyRange.Bound#LOWER}) or
 *     upper (for {@link org.apache.phoenix.query.KeyRange.Bound#UPPER}) bytes.</li>
 * <li>If the column is variable-width and not the last PK column, append a separator byte
 *     (ASC {@code \x00} / DESC {@code \xFF}).</li>
 * <li>If setting the lower bound with exclusive-lower: {@code nextKey} the whole key so
 *     far (bump), then continue.</li>
 * <li>If setting the upper bound with exclusive-upper: stop iterating — nothing trailing
 *     can match the bound.</li>
 * <li>After all columns processed, if the upper is inclusive (either a single-key or a
 *     range-inclusive-upper), {@code nextKey} the whole key to convert to the
 *     byte-exclusive form HBase expects for {@code setStopRow}.</li>
 * </ol>
 * <p>
 * These rules are deliberately a strict subset of what {@code ScanUtil.setKey} handles —
 * they cover single-space {@link KeySpace}s with point or range ranges per dim.
 * <p>
 * <b>Multi-space lists.</b> {@link #encodeListLower} and {@link #encodeListUpper} extend
 * the single-space encoding to a {@link org.apache.phoenix.compile.keyspace.KeySpaceList}:
 * the list's scan lower is the byte-lex-min of per-space lower encodings; the scan upper
 * is the byte-lex-max of per-space upper encodings. This preserves within-space tuple
 * correlation (the single-space encoder already gets that right) and widens to the
 * bounding envelope of the union — the residual filter handles rows in the envelope
 * gap. Unbounded sides ({@link KeyRange#UNBOUND}) short-circuit to UNBOUND for LOWER/UPPER
 * respectively, matching HBase's semantics for {@code scan.withStartRow} / {@code withStopRow}.
 */
public final class CompoundByteEncoder {

  private CompoundByteEncoder() {
  }

  /**
   * Encode the lower-row bytes for the given {@link KeySpace} against the given schema.
   * Returns {@link KeyRange#UNBOUND} (empty byte array) when the result is unbounded.
   *
   * @param schema      full row-key schema
   * @param space       the N-dim box; {@code space.nDims()} must equal {@code schema.getMaxFields()}
   * @param startField  first PK column to include in the encoding (0 for user queries,
   *                    {@code prefixSlots} when the caller prepends salt/viewIndexId/tenantId)
   * @return lower-row bytes suitable for {@code scan.withStartRow(...)}
   */
  public static byte[] encodeLower(RowKeySchema schema, KeySpace space, int startField) {
    return encode(schema, space, startField, KeyRange.Bound.LOWER);
  }

  /**
   * Encode the upper-row bytes for the given {@link KeySpace} against the given schema.
   * Returns {@link KeyRange#UNBOUND} (empty byte array) when the result is unbounded.
   */
  public static byte[] encodeUpper(RowKeySchema schema, KeySpace space, int startField) {
    return encode(schema, space, startField, KeyRange.Bound.UPPER);
  }

  /**
   * Encode the lower-row bytes for a {@link KeySpaceList}: the byte-lex-min of per-space
   * lower encodings. Any space that encodes to {@link KeyRange#UNBOUND} (empty bytes)
   * collapses the whole list's lower to UNBOUND.
   */
  public static byte[] encodeListLower(RowKeySchema schema, KeySpaceList list, int startField) {
    if (list.isUnsatisfiable() || list.isEverything()) {
      return KeyRange.UNBOUND;
    }
    byte[] min = null;
    for (KeySpace s : list.spaces()) {
      byte[] b = encodeLower(schema, s, startField);
      if (b == KeyRange.UNBOUND || b.length == 0) {
        return KeyRange.UNBOUND;
      }
      if (min == null || org.apache.hadoop.hbase.util.Bytes.compareTo(b, min) < 0) {
        min = b;
      }
    }
    return min == null ? KeyRange.UNBOUND : min;
  }

  /**
   * Encode the upper-row bytes for a {@link KeySpaceList}: the byte-lex-max of per-space
   * upper encodings. Any space that encodes to {@link KeyRange#UNBOUND} (empty bytes)
   * collapses the whole list's upper to UNBOUND.
   */
  public static byte[] encodeListUpper(RowKeySchema schema, KeySpaceList list, int startField) {
    if (list.isUnsatisfiable() || list.isEverything()) {
      return KeyRange.UNBOUND;
    }
    byte[] max = null;
    for (KeySpace s : list.spaces()) {
      byte[] b = encodeUpper(schema, s, startField);
      if (b == KeyRange.UNBOUND || b.length == 0) {
        return KeyRange.UNBOUND;
      }
      if (max == null || org.apache.hadoop.hbase.util.Bytes.compareTo(b, max) > 0) {
        max = b;
      }
    }
    return max == null ? KeyRange.UNBOUND : max;
  }

  private static byte[] encode(RowKeySchema schema, KeySpace space, int startField,
    KeyRange.Bound bound) {
    final int nFields = schema.getMaxFields();
    if (space.nDims() != nFields) {
      throw new IllegalArgumentException(
        "KeySpace arity (" + space.nDims() + ") must equal schema maxFields (" + nFields + ")");
    }
    // Find last constrained field. Trailing EVERYTHING fields don't contribute bytes.
    int lastConstrained = startField - 1;
    for (int d = startField; d < nFields; d++) {
      if (space.get(d) != KeyRange.EVERYTHING_RANGE) {
        lastConstrained = d;
      }
    }
    if (lastConstrained < startField) {
      return KeyRange.UNBOUND;
    }

    // Pre-size: worst case is sum of per-field byte widths + a separator per field.
    int maxLength = 0;
    for (int d = startField; d <= lastConstrained; d++) {
      KeyRange kr = space.get(d);
      byte[] b = kr.getRange(bound);
      maxLength += (b != null ? b.length : 0) + 2;
    }
    byte[] buf = new byte[maxLength];
    int offset = 0;
    boolean anyInclusiveUpperRangeKey = false;
    boolean lastInclusiveUpperSingleKey = false;

    for (int d = startField; d <= lastConstrained; d++) {
      KeyRange kr = space.get(d);
      Field field = schema.getField(d);
      boolean isFixedWidth = field.getDataType().isFixedWidth();
      // Unbound-for-this-bound on a fixed-width field: can't encode past here.
      // For UPPER: stop entirely (nothing more narrows the scan).
      // For LOWER on fixed-width UNBOUND: stop (SEP-only terminator doesn't filter).
      // For LOWER on var-width UNBOUND: keep going (empty bytes + SEP still filter nulls).
      if (kr.isUnbound(bound) && (bound == KeyRange.Bound.UPPER || isFixedWidth)) {
        break;
      }
      byte[] bytes = kr.getRange(bound);
      if (bytes == null) {
        bytes = ByteUtil.EMPTY_BYTE_ARRAY;
      }
      System.arraycopy(bytes, 0, buf, offset, bytes.length);
      offset += bytes.length;

      boolean inclusiveUpper = kr.isUpperInclusive() && bound == KeyRange.Bound.UPPER;
      boolean exclusiveLower =
        !kr.isLowerInclusive() && bound == KeyRange.Bound.LOWER && kr != KeyRange.EVERYTHING_RANGE;
      boolean exclusiveUpper = !kr.isUpperInclusive() && bound == KeyRange.Bound.UPPER;
      lastInclusiveUpperSingleKey = kr.isSingleKey() && inclusiveUpper;
      anyInclusiveUpperRangeKey |= !kr.isSingleKey() && inclusiveUpper;

      // Separator rules. For var-width fields: append SEP when
      //   - SEP is DESC (always append — DESC-var-width terminator must be there), OR
      //   - not exclusive upper AND (there are trailing fields to separate OR the bound
      //     needs the SEP to be bumped correctly for inclusive/exclusive semantics).
      //
      // LOWER-bound + inclusive-lower-single-key special case: suppress the SEP. For a
      // row with the constrained value and trailing-null PK columns (stored with no
      // trailing bytes), the row-key is just the value bytes — shorter than "value·SEP".
      // Appending SEP would make startRow > such rows and exclude them.  A simple value
      // like `N000001` (SYSTEM.STATS metadata row) has row-key `N000001` with no
      // trailing bytes; a scan startRow of `N000001·\x00` skips it. Leaving the raw
      // value bytes as startRow correctly includes it.  V1's setKey appends SEP then
      // tail-strips on LOWER; the encoder achieves the same by not appending in the
      // first place.
      // Only suppress on the LAST processed dim. Mid-compound dims still need the SEP
      // as a structural boundary between dim N's bytes and dim N+1's bytes — without it,
      // the scan startRow would confuse multi-dim prefix matching.
      boolean isLastProcessedDim = (d == lastConstrained);
      boolean lowerSingleKeyInclusive = bound == KeyRange.Bound.LOWER && kr.isSingleKey()
        && kr.isLowerInclusive() && isLastProcessedDim;
      if (field.getDataType() != PVarbinaryEncoded.INSTANCE) {
        byte sepByte = SchemaUtil.getSeparatorByte(schema.rowKeyOrderOptimizable(),
          bytes.length == 0, field);
        boolean forceDesc = sepByte == QueryConstants.DESC_SEPARATOR_BYTE;
        boolean appendForBoundSemantics = !exclusiveUpper
          && ((d + 1) < nFields || inclusiveUpper || exclusiveLower);
        // DESC separators must always be appended — DESC-var-width terminator is load-
        // bearing at scan time. Suppress only ASC SEPs on the last-processed-dim when
        // the bound is inclusive-lower + single-key.
        boolean suppress = !forceDesc && lowerSingleKeyInclusive;
        boolean shouldAppend = !isFixedWidth && (forceDesc || appendForBoundSemantics)
          && !suppress;
        if (shouldAppend) {
          buf[offset++] = sepByte;
          if (sepByte != QueryConstants.DESC_SEPARATOR_BYTE) {
            lastInclusiveUpperSingleKey &= (d + 1) < nFields;
          }
        }
      } else {
        byte[] sepBytes = SchemaUtil.getSeparatorBytesForVarBinaryEncoded(
          schema.rowKeyOrderOptimizable(), bytes.length == 0, field.getSortOrder());
        boolean forceDesc = sepBytes == QueryConstants.DESC_VARBINARY_ENCODED_SEPARATOR_BYTES;
        boolean appendForBoundSemantics = !exclusiveUpper
          && ((d + 1) < nFields || inclusiveUpper || exclusiveLower);
        boolean suppress = !forceDesc && lowerSingleKeyInclusive;
        boolean shouldAppend = !isFixedWidth && (forceDesc || appendForBoundSemantics)
          && !suppress;
        if (shouldAppend) {
          buf[offset++] = sepBytes[0];
          buf[offset++] = sepBytes[1];
          if (sepBytes != QueryConstants.DESC_VARBINARY_ENCODED_SEPARATOR_BYTES) {
            lastInclusiveUpperSingleKey &= (d + 1) < nFields;
          }
        }
      }

      if (exclusiveUpper) {
        // Any bytes past here would admit rows matching the upper — stop.
        break;
      }
      // Exclusive lower: bump the whole key so far, continuing with more slots after.
      if (exclusiveLower) {
        if (!ByteUtil.nextKey(buf, offset)) {
          // Overflow: caller should treat as unbounded.
          return KeyRange.UNBOUND;
        }
        // DESC var-width filter-non-null terminator. Mirrors ScanUtil.setKey lines
        // 619-630: when we've just bumped past a var-width DESC field with empty bytes,
        // DESC keys ignore the last byte as the terminator — without this explicit
        // DESC_SEPARATOR_BYTE, the bumped separator byte would be interpreted as the
        // terminator and the filter would mis-match non-null values.
        if (field.getDataType() != PVarbinaryEncoded.INSTANCE) {
          if (!isFixedWidth && bytes.length == 0
            && SchemaUtil.getSeparatorByte(schema.rowKeyOrderOptimizable(), false, field)
                == QueryConstants.DESC_SEPARATOR_BYTE) {
            buf[offset++] = QueryConstants.DESC_SEPARATOR_BYTE;
          }
        } else {
          if (!isFixedWidth && bytes.length == 0
            && SchemaUtil.getSeparatorBytesForVarBinaryEncoded(
              schema.rowKeyOrderOptimizable(), false, field.getSortOrder())
                == QueryConstants.DESC_VARBINARY_ENCODED_SEPARATOR_BYTES) {
            buf[offset++] = QueryConstants.DESC_VARBINARY_ENCODED_SEPARATOR_BYTES[0];
            buf[offset++] = QueryConstants.DESC_VARBINARY_ENCODED_SEPARATOR_BYTES[1];
          }
        }
      }
    }

    // Post-loop bump: inclusive-upper single-key or any-inclusive-upper-range triggers a
    // nextKey on the whole key. For an inclusive upper `col <= N`, the HBase stopRow is
    // the byte-exclusive form `nextKey(N-bytes)`, which is what this produces.
    if (lastInclusiveUpperSingleKey || anyInclusiveUpperRangeKey) {
      if (!ByteUtil.nextKey(buf, offset)) {
        return KeyRange.UNBOUND;
      }
    }

    if (offset == 0) {
      return KeyRange.UNBOUND;
    }
    byte[] out = new byte[offset];
    System.arraycopy(buf, 0, out, 0, offset);
    return out;
  }
}
