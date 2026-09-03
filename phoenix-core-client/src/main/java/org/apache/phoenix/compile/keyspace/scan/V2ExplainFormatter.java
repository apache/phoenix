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

import java.text.Format;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.compile.keyspace.KeySpaceList;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.StringUtil;

/**
 * V2-owned explain-plan keyRanges formatter.
 * <p>
 * Reads from {@link V2ScanArtifact#list()} directly rather than re-decoding the byte-
 * encoded {@code ScanRanges} that drives actual scan execution. The artifact carries the
 * pre-encoding, mathematical form of the scan, so inclusive-upper displays as
 * {@code [*, 1]} (matching V1) instead of {@code [*, 2)} (what V2's compound byte
 * emission produces after {@code nextKey(1) = 2}).
 * <p>
 * Current scope: single-space KeySpaceList with point or range ranges per dim. For
 * multi-space lists the caller falls back to the legacy byte-decoding path in
 * {@code ExplainTable.appendKeyRanges}, which produces different but semantically
 * equivalent output. A future extension will handle the multi-space case by computing
 * per-dim unions and emitting SkipScanFilter-style displays.
 */
public final class V2ExplainFormatter {

  private V2ExplainFormatter() {
  }

  /**
   * Build the keyRanges display string for a scan whose V2 artifact is {@code artifact}.
   * Returns {@code null} if this formatter does not handle the input shape — the caller
   * should fall back to the legacy formatter.
   */
  public static String appendKeyRanges(StatementContext context, TableRef tableRef,
    V2ScanArtifact artifact) {
    KeySpaceList list = artifact.list();
    if (list.isUnsatisfiable()) {
      return "";
    }
    PTable table = tableRef.getTable();
    int nPk = artifact.nPkColumns();
    int prefixSlots = artifact.prefixSlots();

    // KeySpaceList.isEverything() means no user-dim constraint, but when prefix slots
    // (salt / viewIndexId / tenantId) are present the scan is still narrowed to the
    // tenant partition — render those prefix values. Without this, tenant-only queries
    // display as empty brackets while V1 shows {@code ['tenantId']}.
    if (list.isEverything()) {
      return renderPrefixOnly(context, table, prefixSlots);
    }
    if (list.size() != 1) {
      // Multi-space path not implemented yet; fall back.
      return null;
    }
    KeySpace space = list.spaces().get(0);
    boolean isLocalIndex = org.apache.phoenix.util.ScanUtil.isLocalIndex(context.getScan());
    // On a local index the viewIndexId slot is the leading user-prefix slot, which is
    // slot 0 on an unsalted table and slot 1 on a salted table (the salt byte sits in
    // slot 0). Using a hardcoded d==0 check mis-attributes the salt byte as a viewIndexId
    // and leaves the real viewIndexId unshifted.
    boolean scanIsSalted = context.getScanRanges() != null && context.getScanRanges().isSalted();
    int viewIndexIdSlot = scanIsSalted ? 1 : 0;

    // KeySpace indexing is by absolute PK position ({@link KeySpaceExpressionVisitor}
    // emits ranges via {@code KeySpace.single(pkPos, ...)} with {@code pkPos} being the
    // column's absolute index in the PK). So {@code space.get(d)} — where d is an
    // absolute PK index — returns the dim for PK column d. Prefix dims (viewIndexId /
    // tenantId / salt) are always EVERYTHING inside the KeySpaceList because the visitor
    // doesn't know about them; prefix values are rendered from scanRanges below.
    //
    // Fall back when a dim's raw bytes don't match the PK column's full fixed-width size.
    // Happens when a scalar function (SUBSTR, FLOOR, ...) creates a KeyRange with truncated
    // bytes (e.g. 3-byte substr prefix on an 8-byte LONG column). V1's ExplainTable reads
    // post-processing ScanRanges bytes which have been zero-padded to the field width; V2's
    // artifact carries the raw (pre-processing) bytes. Rather than duplicating V1's padding
    // logic, defer to the legacy byte-decoding formatter.
    for (int d = prefixSlots; d < nPk && d < space.nDims(); d++) {
      KeyRange kr = space.get(d);
      if (kr == KeyRange.EVERYTHING_RANGE || kr == KeyRange.IS_NULL_RANGE
        || kr == KeyRange.IS_NOT_NULL_RANGE) {
        continue;
      }
      PColumn col = table.getPKColumns().get(d);
      PDataType type = col.getDataType();
      if (!type.isFixedWidth()) {
        continue;
      }
      Integer maxLen = col.getMaxLength();
      Integer typeSize = type.getByteSize();
      if (maxLen == null && typeSize == null) {
        continue;
      }
      int expected = maxLen != null ? maxLen : typeSize;
      byte[] lb = kr.getRange(KeyRange.Bound.LOWER);
      byte[] ub = kr.getRange(KeyRange.Bound.UPPER);
      if ((lb != null && lb.length != 0 && lb.length != expected)
        || (ub != null && ub.length != 0 && ub.length != expected)) {
        return null;
      }
    }

    // Last-dim-to-display index: highest dim with a non-EVERYTHING range. V1 truncates
    // at the first EVERYTHING past the prefix so the display doesn't end in `*,*,*`.
    int lastConstrained = prefixSlots - 1;
    for (int d = prefixSlots; d < nPk && d < space.nDims(); d++) {
      KeyRange kr = space.get(d);
      if (kr != KeyRange.EVERYTHING_RANGE) {
        lastConstrained = d;
      }
    }
    if (lastConstrained < prefixSlots) {
      return "";
    }

    StringBuilder lower = new StringBuilder();
    StringBuilder upper = new StringBuilder();
    // Prefix columns (salt byte / viewIndexId / tenantId) aren't in the KeySpaceList —
    // they're auto-populated slots. Read them from the ScanRanges's per-slot structure,
    // which the caller already built. This keeps prefix display identical to V1's.
    int prefixEmitted = 0;
    if (context.getScanRanges() != null && !context.getScanRanges().getRanges().isEmpty()) {
      java.util.List<java.util.List<KeyRange>> ranges = context.getScanRanges().getRanges();
      for (int d = 0; d < prefixSlots && d < ranges.size(); d++) {
        // Slot 0 on a salted table holds the full [0..nBuckets-1] salt-bucket range
        // list after {@link ScanRanges}'s constructor populates it via
        // {@link SaltingUtil#generateAllSaltingRanges}. Lower bound reads the first
        // range, upper bound reads the last — so the scan displays as
        // {@code [X'00', ...] - [X'03', ...]} for nBuckets=4. Reading the same entry
        // for both bounds would display {@code [X'00', ...]} as both lower and upper,
        // losing the span across salt buckets. Other prefix slots (viewIndexId, tenantId)
        // have a single singleton range, so the choice doesn't matter there.
        java.util.List<KeyRange> slot = ranges.get(d);
        KeyRange lowerRange = slot.get(0);
        KeyRange upperRange = slot.get(slot.size() - 1);
        byte[] lb = lowerRange.getRange(KeyRange.Bound.LOWER);
        byte[] ub = upperRange.getRange(KeyRange.Bound.UPPER);
        // On a local-index scan, slot 0 holds the viewIndexId. Render it via V1's
        // shifted-value format ({@code Short.MIN_VALUE + 1 → "1"}) so the explain-plan
        // matches V1's output. Without this flag, the raw stored bytes display as
        // {@code -32768}, diverging from V1 on every local-index explain assertion.
        boolean changeViewIndexId = isLocalIndex && d == viewIndexIdSlot;
        appendPKColumnValue(lower, context, table, lb, null, d, changeViewIndexId);
        lower.append(',');
        appendPKColumnValue(upper, context, table, ub, null, d, changeViewIndexId);
        upper.append(',');
        prefixEmitted++;
      }
    }

    // Now walk the KeySpaceList dims aligned with PK columns [prefixSlots, lastConstrained].
    for (int d = prefixSlots; d <= lastConstrained; d++) {
      KeyRange kr = space.get(d);
      Boolean isNull =
        kr == KeyRange.IS_NULL_RANGE ? Boolean.TRUE
          : kr == KeyRange.IS_NOT_NULL_RANGE ? Boolean.FALSE : null;
      byte[] lb = kr.getRange(KeyRange.Bound.LOWER);
      byte[] ub = kr.getRange(KeyRange.Bound.UPPER);
      boolean changeViewIndexId = isLocalIndex && d == viewIndexIdSlot;
      appendPKColumnValue(lower, context, table, lb, isNull, d, changeViewIndexId);
      lower.append(',');
      appendPKColumnValue(upper, context, table, ub, isNull, d, changeViewIndexId);
      upper.append(',');
    }

    // Trim trailing commas, wrap in brackets. Emit `[L] - [U]` when they differ, else
    // `[LU]` — the same shape as ExplainTable.appendKeyRanges.
    trimLastComma(lower);
    trimLastComma(upper);
    StringBuilder out = new StringBuilder();
    out.append(" [");
    out.append(lower);
    out.append(']');
    if (!StringUtil.equals(lower, upper)) {
      out.append(" - [");
      out.append(upper);
      out.append(']');
    }
    return out.toString();
  }

  private static void trimLastComma(StringBuilder buf) {
    int n = buf.length();
    if (n > 0 && buf.charAt(n - 1) == ',') {
      buf.setLength(n - 1);
    }
  }

  /**
   * Render {@code [prefix_1, ..., prefix_k]} from the per-slot ranges in the attached
   * {@link org.apache.phoenix.compile.ScanRanges}. Used when the user-dim KeySpaceList
   * is EVERYTHING but the scan is still narrowed by salt / viewIndexId / tenantId
   * prefix slots — e.g. a tenant-specific full-view scan.
   */
  private static String renderPrefixOnly(StatementContext context, PTable table,
    int prefixSlots) {
    if (prefixSlots == 0 || context.getScanRanges() == null
      || context.getScanRanges().getRanges().isEmpty()) {
      return "";
    }
    boolean isLocalIndex = org.apache.phoenix.util.ScanUtil.isLocalIndex(context.getScan());
    // On a local index the viewIndexId slot is the leading user-prefix slot, which is
    // slot 0 on an unsalted table and slot 1 on a salted table (the salt byte sits in
    // slot 0). Using a hardcoded d==0 check mis-attributes the salt byte as a viewIndexId
    // and leaves the real viewIndexId unshifted.
    boolean scanIsSalted = context.getScanRanges() != null && context.getScanRanges().isSalted();
    int viewIndexIdSlot = scanIsSalted ? 1 : 0;
    java.util.List<java.util.List<KeyRange>> ranges = context.getScanRanges().getRanges();
    StringBuilder lower = new StringBuilder();
    StringBuilder upper = new StringBuilder();
    for (int d = 0; d < prefixSlots && d < ranges.size(); d++) {
      // See comment in appendKeyRanges' prefix loop: salt slot 0 has nBuckets entries;
      // lower uses the first range, upper uses the last.
      java.util.List<KeyRange> slot = ranges.get(d);
      byte[] lb = slot.get(0).getRange(KeyRange.Bound.LOWER);
      byte[] ub = slot.get(slot.size() - 1).getRange(KeyRange.Bound.UPPER);
      boolean changeViewIndexId = isLocalIndex && d == viewIndexIdSlot;
      appendPKColumnValue(lower, context, table, lb, null, d, changeViewIndexId);
      lower.append(',');
      appendPKColumnValue(upper, context, table, ub, null, d, changeViewIndexId);
      upper.append(',');
    }
    trimLastComma(lower);
    trimLastComma(upper);
    StringBuilder out = new StringBuilder();
    out.append(" [");
    out.append(lower);
    out.append(']');
    if (!StringUtil.equals(lower, upper)) {
      out.append(" - [");
      out.append(upper);
      out.append(']');
    }
    return out.toString();
  }

  /**
   * Mirrors {@code ExplainTable.appendPKColumnValue} for V2. Consolidated here so the
   * formatter can render each column value without depending on ExplainTable internals.
   */
  private static void appendPKColumnValue(StringBuilder buf, StatementContext context,
    PTable table, byte[] range, Boolean isNull, int slotIndex, boolean changeViewIndexId) {
    if (Boolean.TRUE.equals(isNull)) {
      buf.append("null");
      return;
    }
    if (Boolean.FALSE.equals(isNull)) {
      buf.append("not null");
      return;
    }
    if (range == null || range.length == 0) {
      buf.append('*');
      return;
    }
    PDataType type = context.getScanRanges().getSchema().getField(slotIndex).getDataType();
    PColumn column = table.getPKColumns().get(slotIndex);
    SortOrder sortOrder = column.getSortOrder();
    if (sortOrder == SortOrder.DESC) {
      buf.append('~');
      ImmutableBytesWritable ptr = new ImmutableBytesWritable(range);
      type.coerceBytes(ptr, type, sortOrder, SortOrder.getDefault());
      range = ptr.get();
    }
    if (changeViewIndexId) {
      buf.append(getViewIndexValue(type, range).toString());
    } else {
      Format formatter = context.getConnection().getFormatter(type);
      buf.append(type.toStringLiteral(range, formatter));
    }
  }

  private static Long getViewIndexValue(PDataType type, byte[] range) {
    boolean useLongViewIndex =
      org.apache.phoenix.util.MetaDataUtil.getViewIndexIdDataType().equals(type);
    Object s = type.toObject(range);
    return (useLongViewIndex ? (Long) s : (Short) s) + Short.MAX_VALUE + 2;
  }
}
