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

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.compile.keyspace.scan.V2ScanBuilder;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.util.ScanUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * Entry point for the N-dimensional key-space WHERE optimizer. Pipes an expression through
 * {@link ExpressionNormalizer}, {@link KeySpaceExpressionVisitor}, {@link KeyRangeExtractor},
 * and finally {@link ScanRanges#create}, then strips fully-consumed nodes via
 * {@link WhereOptimizer.RemoveExtractedNodesVisitor}.
 * <p>
 * The driver is invoked in place of the legacy {@link WhereOptimizer} visitor when the
 * {@link QueryServices#WHERE_OPTIMIZER_V2_ENABLED} flag is set. Both the legacy path and this
 * one write the same shape to {@code context.setScanRanges(...)} and return an Expression
 * representing the residual filter.
 */
public final class WhereOptimizerV2 {

  private WhereOptimizerV2() {
  }

  public static Expression run(StatementContext context, Set<Hint> hints, Expression whereClause,
    Set<Expression> extractNodes, Optional<byte[]> minOffset) throws SQLException {

    PTable table = context.getCurrentTable().getTable();
    RowKeySchema schema = table.getRowKeySchema();
    Integer nBuckets = table.getBucketNum();
    boolean isSalted = nBuckets != null;
    PName tenantId = context.getConnection().getTenantId();
    boolean isMultiTenant = tenantId != null && table.isMultiTenant();
    boolean isSharedIndex = table.getViewIndexId() != null;
    byte[] tenantIdBytes = isMultiTenant
      ? ScanUtil.getTenantIdBytes(schema, isSalted, tenantId, isSharedIndex)
      : null;

    // Short-circuits matching WhereOptimizer.pushKeyExpressionsToScan.
    if (whereClause == null && !isMultiTenant && !isSharedIndex && !minOffset.isPresent()) {
      context.setScanRanges(ScanRanges.EVERYTHING);
      return whereClause;
    }
    if (LiteralExpression.isBooleanFalseOrNull(whereClause)) {
      context.setScanRanges(ScanRanges.NOTHING);
      return null;
    }

    // FROM-less SELECT (e.g., `SELECT 1` or expression-only queries) resolves to a
    // synthetic PTable with no PK columns. Phoenix represents this by returning null
    // from getPKColumns(). There's nothing the optimizer can narrow in that case — leave
    // the scan as EVERYTHING and return the residual expression unchanged so the
    // executor evaluates it at scan time.
    List<PColumn> pkColumns = table.getPKColumns();
    if (pkColumns == null || pkColumns.isEmpty()) {
      context.setScanRanges(ScanRanges.EVERYTHING);
      return whereClause;
    }
    int nPk = pkColumns.size();
    int prefixSlots = (isSalted ? 1 : 0) + (isSharedIndex ? 1 : 0) + (isMultiTenant ? 1 : 0);

    // Step 1: normalize + visit. The normalized tree is what the residual filter is built
    // from — extracted Expression nodes come from the normalized tree, not the caller's
    // original tree, so applying {@link WhereOptimizer.RemoveExtractedNodesVisitor} against
    // the original would find nothing to strip for RVC-inequality and IN rewrites.
    KeySpaceList keySpaceList;
    Set<Expression> consumed = (extractNodes == null) ? new HashSet<Expression>() : extractNodes;
    Expression residualInput = whereClause;
    Set<Expression> visitorConsumed = Collections.emptySet();
    if (whereClause == null) {
      keySpaceList = KeySpaceList.everything(nPk);
    } else {
      Expression normalized = ExpressionNormalizer.normalize(whereClause);
      residualInput = normalized;
      KeySpaceExpressionVisitor visitor = new KeySpaceExpressionVisitor(table);
      KeySpaceExpressionVisitor.Result r = normalized.accept(visitor);
      if (r == null || r.list().isEverything()) {
        keySpaceList = KeySpaceList.everything(nPk);
      } else if (r.list().isUnsatisfiable()) {
        // PHOENIX-6669 short-circuit: degeneracy detected uniformly across all PK positions.
        context.setScanRanges(ScanRanges.NOTHING);
        return null;
      } else {
        keySpaceList = r.list();
        visitorConsumed = r.consumed();
      }
    }

    int bound = context.getConnection().getQueryServices().getConfiguration()
      .getInt(QueryServices.WHERE_OPTIMIZER_V2_CARTESIAN_BOUND,
        QueryServicesOptions.DEFAULT_WHERE_OPTIMIZER_V2_CARTESIAN_BOUND);

    V2ScanBuilder.Inputs inputs = new V2ScanBuilder.Inputs(keySpaceList, table, schema, nPk,
      prefixSlots, nBuckets, isSalted, isMultiTenant, isSharedIndex, tenantIdBytes, hints,
      bound, minOffset);
    V2ScanBuilder.Result r = V2ScanBuilder.build(inputs);
    if (r.isNothing) {
      context.setScanRanges(ScanRanges.NOTHING);
      return null;
    }
    boolean emittedEverything = r.scanRanges == ScanRanges.EVERYTHING;
    context.setScanRanges(r.scanRanges);
    // Attach the V2 artifact so downstream consumers (explain-plan formatter) can read
    // the logical KeySpaceList rather than the byte-encoded ScanRanges. Skipped for
    // EVERYTHING since there's nothing to display anyway.
    if (!emittedEverything) {
      context.setV2ScanArtifact(new org.apache.phoenix.compile.keyspace.scan.V2ScanArtifact(
        keySpaceList, nPk, prefixSlots));
    }
    // Override scan start/stop rows with CompoundByteEncoder output for shapes in the
    // encoder's proven envelope. The encoder's bytes preserve trailing separators that
    // ScanUtil.setKey's tail-strip would drop, and its multi-space list envelope preserves
    // cross-dim tuple correlation that per-slot projection loses — see
    // docs/where-optimizer-v2-scan-construction.md. RVC OFFSET is skipped because
    // RVCOffsetCompiler reads scan.startRow to build the paging cursor and is sensitive
    // to the classical path's exact byte layout. See QueryMoreIT.testRVCOnDescWithLeadingPKEquality.
    if (!emittedEverything && !minOffset.isPresent()
      && org.apache.phoenix.compile.keyspace.scan.CompoundByteEncoderEmitter.isInScope(
        keySpaceList, schema, prefixSlots, isSalted)) {
      org.apache.phoenix.compile.keyspace.scan.CompoundByteEncoderEmitter.overrideScanRows(
        context.getScan(), keySpaceList, schema, prefixSlots,
        buildPrefixBytes(isSalted, isSharedIndex, isMultiTenant, table, tenantIdBytes));
    }

    // If the emitted scan range is "everything" (no leading-PK narrowing survived the
    // extract pass, e.g. a predicate on a non-leading PK column with no leading
    // constraint), the visitor may still have populated consumed nodes for those
    // predicates, but since they didn't influence the scan range, the residual filter
    // must retain them for correctness. Match v1 semantics by clearing consumed in that
    // case.
    //
    // Additionally, when the leading productive slot (the first user-PK dim at
    // {@code prefixSlots}) is EVERYTHING in every space but a trailing slot is
    // constrained, we cannot safely install a SkipScanFilter. Its setNextCellHint path
    // builds startKey by concatenating each slot's lower bound — an EVERYTHING slot
    // contributes zero bytes, producing a seek hint shorter than (and lex-less than)
    // any row already past the trailing-slot boundary. HBase rejects the backward
    // seek with {@code "next hint must come after previous hint"}. Prefix bytes (salt
    // bucket, viewIndexId, tenantId) from {@code [0, prefixSlots)} are concrete, but
    // they only anchor the scan's start/stop — the hint construction still uses the
    // user slots and the same invariant applies. The predicate must stay in the
    // residual BooleanExpressionFilter.
    // Exception: when the caller forces SKIP_SCAN via hint, honor the hint —
    // V2ScanBuilder installs the SkipScanFilter anyway and the caller accepts the
    // associated risk (V1 has the same behavior); consume so the shape matches V1's
    // compile-plan output.
    boolean forcedSkipScan = hints != null && hints.contains(Hint.SKIP_SCAN);
    boolean leadingEverythingPastPrefix = !emittedEverything && !forcedSkipScan
        && hasLeadingEverythingAt(keySpaceList, prefixSlots);
    if (!emittedEverything && !leadingEverythingPastPrefix) {
      consumed.addAll(visitorConsumed);
    }

    // Step 4: residual filter — drop nodes the key ranges fully captured.
    if (residualInput == null) {
      return null;
    }
    // With a RANGE_SCAN hint, the SkipScanFilter is dropped (useSkipScan is forced false),
    // so the per-slot narrowing we emitted doesn't apply at scan time. Any node consumed
    // under the assumption that it was captured by the skip-scan slots must stay in the
    // residual filter, otherwise rows that fail those predicates leak through. Preserve
    // the original whereClause as residual in that case — matches V1's behavior.
    boolean rangeScanHint = hints != null && hints.contains(Hint.RANGE_SCAN);
    if (rangeScanHint) {
      return residualInput;
    }
    // Honor the caller-supplied extractNodes if one was provided (used by tests and
    // RVCOffsetCompiler to observe which nodes were extracted); otherwise, a local collection
    // was used and we apply it as a one-shot removal.
    Set<Expression> toRemove = (extractNodes == null) ? consumed : extractNodes;
    if (toRemove.isEmpty()) {
      return residualInput;
    }
    Expression residual = residualInput.accept(new RemoveExtractedNodesVisitorV2(toRemove));
    // If the removal visitor collapsed everything, it returns null → no residual filter.
    return residual;
  }

  /**
   * True when every space in {@code list} has {@link org.apache.phoenix.query.KeyRange
   * #EVERYTHING_RANGE} at dimension {@code prefixSlots} — i.e., the first user-PK slot is
   * unbounded in every branch. When this holds, SkipScanFilter cannot be installed (its
   * seek-hint construction requires a concrete lower bound on the leading slot).
   */
  private static boolean hasLeadingEverythingAt(KeySpaceList list, int prefixSlots) {
    if (list.spaces().isEmpty()) {
      return false;
    }
    for (KeySpace ks : list.spaces()) {
      if (prefixSlots >= ks.nDims()) {
        return false;
      }
      if (ks.get(prefixSlots) != org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE) {
        return false;
      }
    }
    return true;
  }

  /**
   * Concatenate the prefix bytes the scan carries before the user-PK columns: salt byte
   * (0x00 placeholder), viewIndexId, tenantId — each a concrete point-key. Matches the
   * byte layout {@code ScanUtil.setKey} produces for the same prefix slots so encoder
   * output can be prepended with these bytes and equal the scan-path's full row.
   */
  private static byte[] buildPrefixBytes(boolean isSalted, boolean isSharedIndex,
    boolean isMultiTenant, PTable table, byte[] tenantIdBytes) {
    java.util.List<byte[]> parts = new java.util.ArrayList<>(3);
    if (isSalted) {
      parts.add(new byte[] { 0 });
    }
    if (isSharedIndex) {
      parts.add(table.getviewIndexIdType().toBytes(table.getViewIndexId()));
    }
    if (isMultiTenant) {
      parts.add(tenantIdBytes);
      // Variable-width tenantId columns carry a trailing separator byte in the row layout;
      // ScanUtil appends it. The encoder's output starts after the prefix, so we must
      // include the separator here when the tenant column is variable-width.
      org.apache.phoenix.schema.ValueSchema.Field f =
        table.getRowKeySchema().getField((isSalted ? 1 : 0) + (isSharedIndex ? 1 : 0));
      if (!f.getDataType().isFixedWidth()) {
        parts.add(new byte[] { org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE });
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
