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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.keyspace.KeyRangeExtractor;
import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.compile.keyspace.KeySpaceList;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;

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
 *     {@link KeyRangeExtractor} adapter to produce V1-shaped CNF; scan start/stop bytes
 *     are then sourced from {@link CompoundByteEncoder} (via
 *     {@link CompoundByteEncoderEmitter} in {@code WhereOptimizerV2.run}) for shapes in
 *     the encoder's proven envelope.</li>
 * </ul>
 * Downstream consumers (SkipScanFilter, ScanRanges.isPointLookup, explain-plan
 * formatter, local-index pruning) read from the ScanRanges this builder produces.
 * V2-owned metadata is attached via {@link V2ScanArtifact} so the explain-plan formatter
 * renders from the pre-encoding {@link KeySpaceList} rather than the post-encoding bytes.
 */
public final class V2ScanBuilder {

  private V2ScanBuilder() {
  }

  /**
   * Inputs gathered at {@code WhereOptimizerV2.run} and passed to the scan builder.
   * All fields are read-only.
   */
  public static final class Inputs {
    public final KeySpaceList list;
    public final PTable table;
    public final RowKeySchema schema;
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
  }

  /**
   * Output of the scan builder. For now this is a thin wrapper around
   * {@link ScanRanges} (the existing type), leaving room to grow into a richer V2-owned
   * adapter as more responsibilities move into this class.
   */
  public static final class Result {
    public final ScanRanges scanRanges;
    /**
     * {@code true} iff the builder's classification of the emitted key space is "matches
     * nothing" — the caller short-circuits the residual and returns {@code null}. Distinct
     * from {@code scanRanges.isDegenerate()} only insofar as it's set by the builder's
     * own classification path (not always derivable from {@code scanRanges}).
     */
    public final boolean isNothing;

    public Result(ScanRanges scanRanges, boolean isNothing) {
      this.scanRanges = scanRanges;
      this.isNothing = isNothing;
    }

    public static Result nothing() {
      return new Result(ScanRanges.NOTHING, true);
    }

    public static Result everything() {
      return new Result(ScanRanges.EVERYTHING, false);
    }
  }

  /**
   * Build a {@link ScanRanges} from the given {@link KeySpaceList} and context.
   * <p>
   * Follows the classification tree in {@code docs/where-optimizer-v2-scan-construction.md}
   * §"Classification tree". Shapes with a native V2 emission path are handled directly;
   * shapes routed through the {@link KeyRangeExtractor} adapter produce the V1-projected
   * per-slot CNF shape that {@link org.apache.phoenix.compile.ScanRanges#create} + the
   * downstream {@code ScanUtil.setKey} consume.
   * <p>
   * Currently native classes:
   * <ul>
   * <li><b>1 DEGENERATE</b> — {@code list.isUnsatisfiable()} → {@link ScanRanges#NOTHING}.</li>
   * <li><b>2 EVERYTHING</b> — {@code list.isEverything() && !prefixSlots && !minOffset}
   *     → {@link ScanRanges#EVERYTHING}.</li>
   * <li><b>3 POINT_LOOKUP_LIST</b> — every space all-single-key across every productive
   *     dim past prefix, {@code list.size() ≥ 2} (single-space single-tuple routes through
   *     adapter to preserve DESC var-width byte shape). Emitted directly via
   *     {@link CompoundByteEncoder}, preserving cross-dim tuple correlation.</li>
   * </ul>
   * Classes 4 (RANGE_SCAN subcases) and 5 (SKIP_SCAN_LIST) currently route through the
   * {@link KeyRangeExtractor} adapter to produce the V1-shaped CNF that
   * {@link SkipScanFilter} consumes. {@link CompoundByteEncoderEmitter} then overrides
   * {@code scan.startRow}/{@code stopRow} with encoder-sourced bytes for in-envelope
   * shapes (see {@code docs/where-optimizer-v2-scan-construction.md} §"Byte emission
   * envelope"). Native emission for classes 4 and 5 is PHOENIX-6791 follow-up work.
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

    KeyRangeExtractor.Result extract = KeyRangeExtractor.extract(
      in.list, in.nPkColumns, in.cartesianBound, in.prefixSlots, in.schema);
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

    ScanRanges scanRanges = ScanRanges.create(in.schema, cnf, slotSpan, in.nBuckets, useSkipScan,
      in.table.getRowTimestampColPos(), in.minOffset);
    return new Result(scanRanges, false);
  }

  /**
   * Classifier: is every space in the list all-single-key across every productive dim,
   * with no IS_NULL / IS_NOT_NULL sentinels? This is the RVC-IN / RVC-equality OR shape.
   * <p>
   * Restricted to multi-space lists (size ≥ 2). Single-space all-pinned shapes flow
   * through the classical path which is already byte-identical to V1 (proven by parity
   * harness across 142 tests); routing them through the native path would change byte
   * output unnecessarily and break byte-shape assertions on point lookups.
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
          if (thisProductive > 0) return false;  // middle gap
          continue;
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
   * {@link CompoundByteEncoder}. Each space becomes one full-rowkey byte[] (including
   * prefix bytes); these are fed to {@code ScanRanges.create} with VAR_BINARY_SCHEMA so
   * downstream {@code isPointLookup} classification succeeds and the scan is dispatched
   * as a SkipScan of point keys.
   * <p>
   * Returns {@code null} if any space's encoded lower bytes are UNBOUND (would collapse
   * the list) — the caller falls back to the adapter.
   */
  private static Result buildPointLookupList(Inputs in) {
    byte[] prefixBytes = buildPrefixBytes(in);
    java.util.List<KeyRange> pointKeys =
      new java.util.ArrayList<>(in.list.spaces().size());
    for (KeySpace s : in.list.spaces()) {
      byte[] tail = CompoundByteEncoder.encodeLower(in.schema, s, in.prefixSlots);
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
    java.util.List<java.util.List<KeyRange>> cnf =
      java.util.Collections.singletonList(pointKeys);
    int[] slotSpan = org.apache.phoenix.util.ScanUtil.SINGLE_COLUMN_SLOT_SPAN;
    // Use VAR_BINARY_SCHEMA so ScanRanges.create treats this as raw bytes — isPointLookup
    // succeeds, and SkipScanFilter navigates the N point keys individually without trying
    // to decode them against the original schema's per-field comparators.
    ScanRanges scanRanges = ScanRanges.create(
      org.apache.phoenix.util.SchemaUtil.VAR_BINARY_SCHEMA,
      cnf, slotSpan, in.nBuckets, pointKeys.size() > 1,
      in.table.getRowTimestampColPos(), in.minOffset);
    return new Result(scanRanges, false);
  }

  /**
   * Prefix bytes for salt / viewIndexId / tenantId — mirror of {@code WhereOptimizerV2
   * .buildPrefixBytes}. Duplicated here to keep {@link V2ScanBuilder} self-contained on
   * the native emission path.
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
    for (byte[] p : parts) total += p.length;
    byte[] out = new byte[total];
    int off = 0;
    for (byte[] p : parts) {
      System.arraycopy(p, 0, out, off, p.length);
      off += p.length;
    }
    return out;
  }
}
