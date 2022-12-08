/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.BaseExpression;
import org.apache.phoenix.expression.BaseExpression.ExpressionComparabilityWrapper;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.function.FunctionExpression.OrderPreserving;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterators;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.NonNull;


/**
 *
 * Class that pushes row key expressions from the where clause to form the start/stop
 * key of the scan and removes the expressions from the where clause when possible.
 *
 * 
 * @since 0.1
 */
public class WhereOptimizer {
    private static final List<KeyRange> EVERYTHING_RANGES = Collections.<KeyRange>singletonList(KeyRange.EVERYTHING_RANGE);
    private static final List<KeyRange> SALT_PLACEHOLDER = Collections.singletonList(PChar.INSTANCE.getKeyRange(QueryConstants.SEPARATOR_BYTE_ARRAY));
    
    private WhereOptimizer() {
    }

    /**
     * Pushes row key expressions from the where clause into the start/stop key of the scan.
     * @param context the shared context during query compilation
     * @param hints the set, possibly empty, of hints in this statement
     * @param whereClause the where clause expression
     * @return the new where clause with the key expressions removed
     */
    public static Expression pushKeyExpressionsToScan(StatementContext context, Set<Hint> hints, Expression whereClause)
            throws SQLException{
        return pushKeyExpressionsToScan(context, hints, whereClause, null, Optional.<byte[]>absent());
    }

    // For testing so that the extractedNodes can be verified
    public static Expression pushKeyExpressionsToScan(StatementContext context, Set<Hint> hints,
            Expression whereClause, Set<Expression> extractNodes, Optional<byte[]> minOffset) throws SQLException {
        PName tenantId = context.getConnection().getTenantId();
        byte[] tenantIdBytes = null;
        PTable table = context.getCurrentTable().getTable();
        Integer nBuckets = table.getBucketNum();
        boolean isSalted = nBuckets != null;
        RowKeySchema schema = table.getRowKeySchema();
        boolean isMultiTenant = tenantId != null && table.isMultiTenant();
        boolean isSharedIndex = table.getViewIndexId() != null;
        ImmutableBytesWritable ptr = context.getTempPtr();
        int maxInListSkipScanSize = context.getConnection().getQueryServices().getConfiguration()
                .getInt(QueryServices.MAX_IN_LIST_SKIP_SCAN_SIZE,
                        QueryServicesOptions.DEFAULT_MAX_IN_LIST_SKIP_SCAN_SIZE);

        if (isMultiTenant) {
            tenantIdBytes = ScanUtil.getTenantIdBytes(schema, isSalted, tenantId, isSharedIndex);
        }

        if (whereClause == null && (tenantId == null || !table.isMultiTenant()) && table.getViewIndexId() == null && !minOffset.isPresent()) {
            context.setScanRanges(ScanRanges.EVERYTHING);
            return whereClause;
        }
        if (LiteralExpression.isBooleanFalseOrNull(whereClause)) {
            context.setScanRanges(ScanRanges.NOTHING);
            return null;
        }
        KeyExpressionVisitor visitor = new KeyExpressionVisitor(context, table);
        KeyExpressionVisitor.KeySlots keySlots = null;
        if (whereClause != null) {
            // TODO:: When we only have one where clause, the keySlots returns as a single slot object,
            // instead of an array of slots for the corresponding column. Change the behavior so it
            // becomes consistent.
            keySlots = whereClause.accept(visitor);
    
            if (keySlots == null && (tenantId == null || !table.isMultiTenant()) && table.getViewIndexId() == null && !minOffset.isPresent()) {
                context.setScanRanges(ScanRanges.EVERYTHING);
                return whereClause;
            }
            // If a parameter is bound to null (as will be the case for calculating ResultSetMetaData and
            // ParameterMetaData), this will be the case. It can also happen for an equality comparison
            // for unequal lengths.
            if (keySlots == KeyExpressionVisitor.EMPTY_KEY_SLOTS) {
                context.setScanRanges(ScanRanges.NOTHING);
                return null;
            }
        }
        if (keySlots == null) {
            keySlots = KeyExpressionVisitor.EMPTY_KEY_SLOTS;
        }
        
        if (extractNodes == null) {
            extractNodes = new HashSet<Expression>(table.getPKColumns().size());
        }

        int pkPos = 0;
        int nPKColumns = table.getPKColumns().size();
        int[] slotSpanArray = new int[nPKColumns];
        List<List<KeyRange>> cnf = Lists.newArrayListWithExpectedSize(schema.getMaxFields());
        boolean hasViewIndex = table.getViewIndexId() != null;
        Iterator<KeyExpressionVisitor.KeySlot> iterator = keySlots.getSlots().iterator();
        // Add placeholder for salt byte ranges
        if (isSalted) {
            cnf.add(SALT_PLACEHOLDER);
            // Increment the pkPos, as the salt column is in the row schema
            // Do not increment the iterator, though, as there will never be
            // an expression in the keySlots for the salt column
            pkPos++;
        }
        
        // Add unique index ID for shared indexes on views. This ensures
        // that different indexes don't interleave.
        if (hasViewIndex) {
            byte[] viewIndexBytes = table.getviewIndexIdType().toBytes(table.getViewIndexId());
            KeyRange indexIdKeyRange = KeyRange.getKeyRange(viewIndexBytes);
            cnf.add(Collections.singletonList(indexIdKeyRange));
            pkPos++;
        }
        
        // Add tenant data isolation for tenant-specific tables
        if (isMultiTenant) {
            KeyRange tenantIdKeyRange = KeyRange.getKeyRange(tenantIdBytes);
            cnf.add(Collections.singletonList(tenantIdKeyRange));
            pkPos++;
        }
        
        boolean forcedSkipScan = hints.contains(Hint.SKIP_SCAN);
        boolean forcedRangeScan = hints.contains(Hint.RANGE_SCAN);
        boolean hasUnboundedRange = false;
        boolean hasMultiRanges = false;
        boolean hasRangeKey = false;
        boolean useSkipScan = false;
        boolean checkMaxSkipScanCardinality = false;
        BigInteger inListSkipScanCardinality = BigInteger.ONE; // using BigInteger to avoid overflow issues


        // Concat byte arrays of literals to form scan start key
        while (iterator.hasNext()) {
            KeyExpressionVisitor.KeySlot slot = iterator.next();
            // If the position of the pk columns in the query skips any part of the row k
            // then we have to handle in the next phase through a key filter.
            // If the slot is null this means we have no entry for this pk position.
            if (slot == null || slot.getKeyRanges().isEmpty())  {
                continue;
            }
            if(slot.getPKPosition() < pkPos) {
                continue;
            }
            if (slot.getPKPosition() != pkPos) {
                hasUnboundedRange = hasRangeKey = true;
                for (int i= pkPos; i < slot.getPKPosition(); i++) {
                    cnf.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));
                }
            }
            KeyPart keyPart = slot.getKeyPart();
            List<KeyRange> keyRanges = slot.getKeyRanges();
            SortOrder prevSortOrder = null;
            int slotOffset = 0;
            int clipLeftSpan = 0;
            boolean onlySplittedRVCLeftValid = false;
            boolean stopExtracting = false;
            // Iterate through all spans of this slot
            boolean areAllSingleKey = KeyRange.areAllSingleKey(keyRanges);
            boolean isInList = false;
            int cnfStartPos = cnf.size();

            // TODO:
            //  Using keyPart.getExtractNodes() to determine whether the keyPart has a IN List
            //  is not guaranteed, since the IN LIST slot may not have any extracted nodes.
            if (keyPart.getExtractNodes() != null && keyPart.getExtractNodes().size() > 0
                    && keyPart.getExtractNodes().iterator().next() instanceof InListExpression){
                isInList = true;
            }
            while (true) {
                SortOrder sortOrder =
                        schema.getField(slot.getPKPosition() + slotOffset).getSortOrder();
                if (prevSortOrder == null)  {
                    prevSortOrder = sortOrder;
                } else if (prevSortOrder != sortOrder || (prevSortOrder == SortOrder.DESC && isInList)) {
                    //Consider the Universe of keys to be [0,7]+ on the leading column A
                    // and [0,7]+ on trailing column B, with a padbyte of 0 for ASC and 7 for DESC
                    //if our key range for ASC keys is leading [2,*] and trailing [3,*],
                    //   → [x203 - x777]
                    //for this particular plan the leading key is descending (ie index desc)
                    // consider the data
                    // (3,2) ORDER BY A,B→ x302 → ORDER BY A DESC,B → x472
                    // (3,3) ORDER BY A,B→ x303 → ORDER BY A DESC,B → x473
                    // (3,4) ORDER BY A,B→ x304 → ORDER BY A DESC,B → x474
                    // (2,3) ORDER BY A,B→ x203 → ORDER BY A DESC,B → x573
                    // (2,7) ORDER BY A,B→ x207 → ORDER BY A DESC,B → x577
                    // And the logical expression (A,B) > (2,3)
                    // In the DESC A order the selected values are not contiguous,
                    // (2,7),(3,2),(3,3),(3,4)
                    // In the normal ASC order by the values are all contiguous
                    // Therefore the key cannot be extracted out and a full filter must be applied
                    // In addition, the boundary of the scan is tricky as the values are not bound
                    // by (2,3) it is instead bound by (2,7), this should map to, [x000,x577]
                    // FUTURE: May be able to perform a type of skip scan for this case.

                    // If the sort order changes, we must clip the portion with the same sort order
                    // and invert the key ranges and swap the upper and lower bounds.
                    List<KeyRange> leftRanges = clipLeft(schema, slot.getPKPosition()
                            + slotOffset - clipLeftSpan, clipLeftSpan, keyRanges, ptr);
                    keyRanges =
                            clipRight(schema, slot.getPKPosition() + slotOffset - 1, keyRanges,
                                    leftRanges, ptr);
                    leftRanges = KeyRange.coalesce(leftRanges);
                    keyRanges = KeyRange.coalesce(keyRanges);
                    if (prevSortOrder == SortOrder.DESC) {
                        leftRanges = invertKeyRanges(leftRanges);
                    }
                    slotSpanArray[cnf.size()] = clipLeftSpan-1;
                    cnf.add(leftRanges);
                    pkPos = slot.getPKPosition() + slotOffset;
                    clipLeftSpan = 0;
                    prevSortOrder = sortOrder;
                    // If we had an IN clause with mixed sort ordering then we need to check the possibility of
                    // skip scan key generation explosion.
                    checkMaxSkipScanCardinality |= isInList;
                    // since we have to clip the portion with the same sort order, we can no longer
                    // extract the nodes from the where clause
                    // for eg. for the schema A VARCHAR DESC, B VARCHAR ASC and query
                    //   WHERE (A,B) < ('a','b')
                    // the range (* - a\xFFb) is converted to [~a-*)(*-b)
                    // so we still need to filter on A,B
                    stopExtracting = true;
                    if(!areAllSingleKey) {
                        //for cnf, we only add [~a-*) to it, (*-b) is skipped.
                        //but for all single key, we can continue.
                        onlySplittedRVCLeftValid = true;
                        break;
                    }
                }
                clipLeftSpan++;
                slotOffset++;
                if (slotOffset >= slot.getPKSpan()) {
                    break;
                }
            }

            if(onlySplittedRVCLeftValid) {
                keyRanges = cnf.get(cnf.size()-1);
            } else {
                if (schema.getField(
                       slot.getPKPosition() + slotOffset - 1).getSortOrder() == SortOrder.DESC) {
                   keyRanges = invertKeyRanges(keyRanges);
                }
                pkPos = slot.getPKPosition() + slotOffset;
                slotSpanArray[cnf.size()] = clipLeftSpan-1;
                cnf.add(keyRanges);
            }

            // Do not use the skipScanFilter when there is a large IN clause (for e.g > 50k elements)
            // Since the generation of point keys for skip scan filter will blow up the memory usage.
            // See ScanRanges.getPointKeys(...) where using the various slot key ranges
            // to generate point keys will lead to combinatorial explosion.
            // The following check will ensure the cardinality of generated point keys
            // is below the configured max (maxInListSkipScanSize).
            // We shall force a range scan if the configured max is exceeded.
            // cnfStartPos => is the start slot of this IN list
            if (checkMaxSkipScanCardinality) {
                for (int i = cnfStartPos; i < cnf.size(); i++) {
                    // using int can result in overflow
                    inListSkipScanCardinality =
                        inListSkipScanCardinality.multiply(BigInteger.valueOf(cnf.get(i).size()));
                }
                // If the maxInListSkipScanSize <= 0 then the feature (to force range scan) is turned off
                if (maxInListSkipScanSize > 0) {
                    forcedRangeScan =
                        inListSkipScanCardinality.compareTo(BigInteger.valueOf(maxInListSkipScanSize)) == 1 ? true : false;
                }
                // Reset the check flag for the next IN list clause
                checkMaxSkipScanCardinality = false;
            }

            // TODO: when stats are available, we may want to use a skip scan if the
            // cardinality of this slot is low.
            /**
             * We use skip scan when:
             * 1.previous slot has unbound and force skip scan and
             * 2.not force Range Scan and
             * 3.previous rowkey slot has range or current rowkey slot have multiple ranges.
             *
             * Once we can not use skip scan and we have a non-contiguous range, we can not remove
             * the whereExpressions of current rowkey slot from the current {@link SelectStatement#where},
             * because the {@link Scan#startRow} and {@link Scan#endRow} could not exactly represent
             * currentRowKeySlotRanges.
             * So we should stop extracting whereExpressions of current rowkey slot once we encounter:
             * 1. we now use range scan and
             * 2. previous rowkey slot has unbound or
             *    previous rowkey slot has range or
             *    current rowkey slot have multiple ranges.
             */
            hasMultiRanges |= keyRanges.size() > 1;
            useSkipScan |=
                    (!hasUnboundedRange || forcedSkipScan) &&
                    !forcedRangeScan &&
                    (hasRangeKey || hasMultiRanges);

            stopExtracting |=
                     !useSkipScan &&
                     (hasUnboundedRange || hasRangeKey || hasMultiRanges);

            for (int i = 0; (!hasUnboundedRange || !hasRangeKey) && i < keyRanges.size(); i++) {
                KeyRange range  = keyRanges.get(i);
                if (range.isUnbound()) {
                    hasUnboundedRange = hasRangeKey = true;
                } else if (!range.isSingleKey()) {
                    hasRangeKey = true;
                }
            }
            // Will be null in cases for which only part of the expression was factored out here
            // to set the start/end key. An example would be <column> LIKE 'foo%bar' where we can
            // set the start key to 'foo' but still need to match the regex at filter time.
            // Don't extract expressions if we're forcing a range scan and we've already come
            // across a multi-range for a prior slot. The reason is that we have an inexact range after
            // that, so must filter on the remaining conditions (see issue #467).
            if (!stopExtracting) {
                Set<Expression> nodesToExtract = keyPart.getExtractNodes();
                extractNodes.addAll(nodesToExtract);
            }
        }
        // If we have fully qualified point keys with multi-column spans (i.e. RVC),
        // we can still use our skip scan. The ScanRanges.create() call will explode
        // out the keys.
        slotSpanArray = Arrays.copyOf(slotSpanArray, cnf.size());
        ScanRanges scanRanges = ScanRanges.create(schema, cnf, slotSpanArray, nBuckets, useSkipScan, table.getRowTimestampColPos(), minOffset);
        context.setScanRanges(scanRanges);
        if (whereClause == null) {
            return null;
        } else {
            return whereClause.accept(new RemoveExtractedNodesVisitor(extractNodes));
        }
    }
    
    private static KeyRange getTrailingRange(RowKeySchema rowKeySchema, int clippedPkPos, KeyRange range, KeyRange clippedResult, ImmutableBytesWritable ptr) {
        // We are interested in the clipped part's Seperator. Since we combined first part, we need to
        // remove its separator from the trailing parts' start
        int clippedSepLength= rowKeySchema.getField(clippedPkPos).getDataType().isFixedWidth() ? 0 : 1;
        byte[] lowerRange = KeyRange.UNBOUND;
        boolean lowerInclusive = false;
        // Lower range of trailing part of RVC must be true, so we can form a new range to intersect going forward
        if (!range.lowerUnbound()
                && range.getLowerRange().length > clippedResult.getLowerRange().length
                && Bytes.startsWith(range.getLowerRange(), clippedResult.getLowerRange())) {
            lowerRange = range.getLowerRange();
            int offset = clippedResult.getLowerRange().length + clippedSepLength;
            ptr.set(lowerRange, offset, lowerRange.length - offset);
            lowerRange = ptr.copyBytes();
            lowerInclusive = range.isLowerInclusive();
        }
        byte[] upperRange = KeyRange.UNBOUND;
        boolean upperInclusive = false;
        if (!range.upperUnbound()
                && range.getUpperRange().length > clippedResult.getUpperRange().length
                && Bytes.startsWith(range.getUpperRange(), clippedResult.getUpperRange())) {
            upperRange = range.getUpperRange();
            int offset = clippedResult.getUpperRange().length + clippedSepLength;
            ptr.set(upperRange, offset, upperRange.length - offset);
            upperRange = ptr.copyBytes();
            upperInclusive = range.isUpperInclusive();
        }
        return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
    }

    private static List<KeyRange> clipRight(RowKeySchema schema, int pkPos, List<KeyRange> keyRanges,
            List<KeyRange> leftRanges, ImmutableBytesWritable ptr) {
        List<KeyRange> clippedKeyRanges = Lists.newArrayListWithExpectedSize(keyRanges.size());
        for (int i = 0; i < leftRanges.size(); i++) {
            KeyRange leftRange = leftRanges.get(i);
            KeyRange range = keyRanges.get(i);
            KeyRange clippedKeyRange = getTrailingRange(schema, pkPos, range, leftRange, ptr);
            clippedKeyRanges.add(clippedKeyRange);
        }
        return clippedKeyRanges;
    }

    private static List<KeyRange> clipLeft(RowKeySchema schema, int pkPos, int clipLeftSpan, List<KeyRange> keyRanges, ImmutableBytesWritable ptr) {
        List<KeyRange> clippedKeyRanges = Lists.newArrayListWithExpectedSize(keyRanges.size());
        for (KeyRange keyRange : keyRanges) {
            KeyRange clippedKeyRange = schema.clipLeft(pkPos, keyRange, clipLeftSpan, ptr);
            clippedKeyRanges.add(clippedKeyRange);
        }
        return clippedKeyRanges;
    }

    private static List<KeyRange> invertKeyRanges(List<KeyRange> keyRanges) {
        keyRanges = new ArrayList<KeyRange>(keyRanges);
        for (int i = 0; i < keyRanges.size(); i++) {
            KeyRange range = keyRanges.get(i);
            range = range.invert();
            keyRanges.set(i, range);
        }
        return keyRanges;
    }

    /**
     * Get an optimal combination of key expressions for hash join key range optimization.
     * @return returns true if the entire combined expression is covered by key range optimization
     * @param result the optimal combination of key expressions
     * @param context the temporary context to get scan ranges set by pushKeyExpressionsToScan()
     * @param statement the statement being compiled
     * @param expressions the join key expressions
     * @return the optimal list of key expressions
     */
    public static boolean getKeyExpressionCombination(List<Expression> result, StatementContext context, FilterableStatement statement, List<Expression> expressions) throws SQLException {
        List<Integer> candidateIndexes = Lists.newArrayList();
        final List<Integer> pkPositions = Lists.newArrayList();
        PTable table = context.getCurrentTable().getTable();
        for (int i = 0; i < expressions.size(); i++) {
            Expression expression = expressions.get(i);
            KeyExpressionVisitor visitor = new KeyExpressionVisitor(context, table);
            KeyExpressionVisitor.KeySlots keySlots = expression.accept(visitor);
            int minPkPos = Integer.MAX_VALUE; 
            if (keySlots != null) {
                Iterator<KeyExpressionVisitor.KeySlot> iterator = keySlots.getSlots().iterator();
                while (iterator.hasNext()) {
                    KeyExpressionVisitor.KeySlot slot = iterator.next();
                    if (slot.getPKPosition() < minPkPos) {
                        minPkPos = slot.getPKPosition();
                    }
                }
                if (minPkPos != Integer.MAX_VALUE) {
                    candidateIndexes.add(i);
                    pkPositions.add(minPkPos);
                }
            }
        }
        
        if (candidateIndexes.isEmpty())
            return false;
        
        Collections.sort(candidateIndexes, new Comparator<Integer>() {
            @Override
            public int compare(Integer left, Integer right) {
                return pkPositions.get(left) - pkPositions.get(right);
            }
        });
        
        List<Expression> candidates = Lists.newArrayList();
        List<List<Expression>> sampleValues = Lists.newArrayList();
        for (Integer index : candidateIndexes) {
            candidates.add(expressions.get(index));
        }        
        for (int i = 0; i < 2; i++) {
            List<Expression> group = Lists.newArrayList();
            for (Expression expression : candidates) {
                PDataType type = expression.getDataType();
                group.add(LiteralExpression.newConstant(type.getSampleValue(), type));
            }
            sampleValues.add(group);
        }
        
        int count = 0;
        int offset = table.getBucketNum() == null ? 0 : SaltingUtil.NUM_SALTING_BYTES;
        int maxPkSpan = 0;
        Expression remaining = null;
        while (count < candidates.size()) {
            Expression lhs = count == 0 ? candidates.get(0) : new RowValueConstructorExpression(candidates.subList(0, count + 1), false);
            Expression firstRhs = count == 0 ? sampleValues.get(0).get(0) : new RowValueConstructorExpression(sampleValues.get(0).subList(0, count + 1), true);
            Expression secondRhs = count == 0 ? sampleValues.get(1).get(0) : new RowValueConstructorExpression(sampleValues.get(1).subList(0, count + 1), true);
            Expression testExpression = InListExpression.create(Lists.newArrayList(lhs, firstRhs, secondRhs), false, context.getTempPtr(), context.getCurrentTable().getTable().rowKeyOrderOptimizable());
            Set<Hint> hints = new HashSet<>();
            if(statement.getHint() != null){
                hints = statement.getHint().getHints();
            }

            remaining = pushKeyExpressionsToScan(context, hints, testExpression);
            if (context.getScanRanges().isPointLookup()) {
                count++;
                break; // found the best match
            }
            int pkSpan = context.getScanRanges().getBoundPkColumnCount() - offset;
            if (pkSpan <= maxPkSpan) {
                break;
            }
            maxPkSpan = pkSpan;
            count++;
        }
        
        result.addAll(candidates.subList(0, count));
        
        return count == candidates.size() 
                && (context.getScanRanges().isPointLookup() || context.getScanRanges().useSkipScanFilter())
                && (remaining == null || remaining.equals(LiteralExpression.newConstant(true, Determinism.ALWAYS)));
    }

    private static class RemoveExtractedNodesVisitor extends StatelessTraverseNoExpressionVisitor<Expression> {
        private final Set<Expression> nodesToRemove;

        private RemoveExtractedNodesVisitor(Set<Expression> nodesToRemove) {
            this.nodesToRemove = nodesToRemove;
        }

        @Override
        public Expression defaultReturn(Expression node, List<Expression> e) {
            return nodesToRemove.contains(node) ? null : node;
        }

        @Override
        public Iterator<Expression> visitEnter(OrExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public Iterator<Expression> visitEnter(AndExpression node) {
            return node.getChildren().iterator();
        }
        
        @Override
        public Expression visit(LiteralExpression node) {
            return nodesToRemove.contains(node) ? null : node;            
        }

        @Override
        public Expression visitLeave(AndExpression node, List<Expression> l) {
            if (!l.equals(node.getChildren())) {
                if (l.isEmpty()) {
                    // Don't return null here, because then our defaultReturn will kick in
                    return LiteralExpression.newConstant(true, Determinism.ALWAYS);
                }
                if (l.size() == 1) {
                    return l.get(0);
                }
                try {
                    return AndExpression.create(l);
                } catch (SQLException e) {
                    //shouldn't happen
                    throw new RuntimeException(e);
                }
            }
            return node;
        }
    }

    /*
     * TODO: We could potentially rewrite simple expressions to move constants to the RHS
     * such that we can form a start/stop key for a scan. For example, rewrite this:
     *     WHEREH a + 1 < 5
     * to this instead:
     *     WHERE a < 5 - 1
     * Currently the first case would not be optimized. This includes other arithmetic
     * operators, CASE statements, and string concatenation.
     */
    public static class KeyExpressionVisitor extends StatelessTraverseNoExpressionVisitor<KeyExpressionVisitor.KeySlots> {
        private static final KeySlots EMPTY_KEY_SLOTS = new KeySlots() {
            @Override
            public boolean isPartialExtraction() {
                return false;
            }

            @Override
            public List<KeySlot> getSlots() {
                return Collections.emptyList();
            }
        };

        private static boolean isDegenerate(List<KeyRange> keyRanges) {
            return keyRanges == null || keyRanges.isEmpty() || (keyRanges.size() == 1 && keyRanges.get(0) == KeyRange.EMPTY_RANGE);
        }
        
        private KeySlots newKeyParts(KeySlot slot, Expression extractNode, KeyRange keyRange) {
            if (keyRange == null) {
                return EMPTY_KEY_SLOTS;
            }
            
            List<KeyRange> keyRanges = Collections.<KeyRange>singletonList(keyRange);
            return newKeyParts(slot, extractNode, keyRanges);
        }

        private KeySlots newKeyParts(KeySlot slot, Expression extractNode, List<KeyRange> keyRanges) {
            if (isDegenerate(keyRanges)) {
                return EMPTY_KEY_SLOTS;
            }

            Set<Expression> extractNodes = extractNode == null || slot.getKeyPart().getExtractNodes().isEmpty()
                  ? Collections.emptySet()
                  : new LinkedHashSet<>(Collections.<Expression>singleton(extractNode));
            return new SingleKeySlot(new BaseKeyPart(table, slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), slot.getPKSpan(), keyRanges, slot.getOrderPreserving());
        }

        private KeySlots newKeyParts(KeySlot slot, Set<Expression> extractNodes, List<KeyRange> keyRanges) {
            if (isDegenerate(keyRanges)) {
                return EMPTY_KEY_SLOTS;
            }
            
            return new SingleKeySlot(new BaseKeyPart(table, slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), slot.getPKSpan(), keyRanges, slot.getOrderPreserving());
        }

        private KeySlots newRowValueConstructorKeyParts(RowValueConstructorExpression rvc, List<KeySlots> childSlots) {
            if (childSlots.isEmpty() || rvc.isStateless()) {
                return null;
            }
            
            int position = -1;
            int initialPosition = -1;
            for (int i = 0; i < childSlots.size(); i++) {
                KeySlots slots = childSlots.get(i);
                KeySlot keySlot = slots.getSlots().iterator().next();
                Set<Expression> childExtractNodes = keySlot.getKeyPart().getExtractNodes();
                // Stop if there was a gap in extraction of RVC elements. This is required if the leading
                // RVC has not row key columns, as we'll still get childSlots if the RVC has trailing row
                // key columns. We can't rule the RVC out completely when the childSlots is less the the
                // RVC length, as a partial, *leading* match is optimizable.
                if (childExtractNodes.size() != 1 || !childExtractNodes.contains(rvc.getChildren().get(i))) {
                    break;
                }
                int pkPosition = keySlot.getPKPosition();
                if (pkPosition < 0) { // break for non PK columns
                    break;
                }
                // Continue while we have consecutive pk columns
                if (position == -1) {
                    position = initialPosition = pkPosition;
                } else if (pkPosition != position) {
                    break;
                }
                position++;
                
                // If we come to a point where we're not preserving order completely
                // then stop. We will never get a NO here, but we might get a YES_IF_LAST
                // if the child expression is only using part of the underlying pk column.
                // (for example, in the case of SUBSTR). In this case, we must stop building
                // the row key constructor at that point.
                assert(keySlot.getOrderPreserving() != OrderPreserving.NO);
                if (keySlot.getOrderPreserving() == OrderPreserving.YES_IF_LAST) {
                    break;
                }
            }
            if (position > 0) {
                int span = position - initialPosition;
                return new SingleKeySlot(new RowValueConstructorKeyPart(table.getPKColumns().get(initialPosition), rvc, span, childSlots), initialPosition, span, EVERYTHING_RANGES);
            }
            return null;
        }

        private KeySlots newScalarFunctionKeyPart(KeySlot slot, ScalarFunction node) {
            if (isDegenerate(slot.getKeyRanges())) {
                return EMPTY_KEY_SLOTS;
            }
            KeyPart part = node.newKeyPart(slot.getKeyPart());
            if (part == null) {
                return null;
            }
            
            // Scalar function always returns primitive and never a row value constructor, so span is always 1
            return new SingleKeySlot(part, slot.getPKPosition(), slot.getKeyRanges(), node.preservesOrder());
        }

        private KeySlots newCoerceKeyPart(KeySlot slot, final CoerceExpression node) {
            if (isDegenerate(slot.getKeyRanges())) {
                return EMPTY_KEY_SLOTS;
            }
            final Set<Expression> extractNodes = new LinkedHashSet<>(Collections.<Expression>singletonList(node));
            final KeyPart childPart = slot.getKeyPart();
            final ImmutableBytesWritable ptr = context.getTempPtr();
            return new SingleKeySlot(new CoerceKeySlot(
                    childPart, ptr, node, extractNodes), slot.getPKPosition(), slot.getKeyRanges());
        }

        /**
         * 
         * Iterates through all combinations of KeyRanges for a given
         * PK column (based on its slot position). Useful when expressions
         * are ORed together and subsequently ANDed. For example:
         *     WHERE (pk1 = 1 OR pk1 = 2) AND (pk2 = 3 OR pk2 = 4)
         * would iterate through and produce [1,3],[1,4],[2,3],[2,4].
         * 
         */
        static class SlotsIterator {
            public final int pkPos;
            private List<KeySlots> childSlots;
            private List<SlotRangesIterator> slotRangesIterator;
            private boolean firstCall = true;
            
            SlotsIterator(List<KeySlots> childSlots, int pkPos) {
                this.childSlots = childSlots;
                this.pkPos = pkPos;
                this.slotRangesIterator = Lists.newArrayListWithExpectedSize(childSlots.size() * 3 / 2);
                for (int i = 0; i < childSlots.size(); i++) {
                    SlotRangesIterator iterator = new SlotRangesIterator(i);
                    slotRangesIterator.add(iterator);
                    iterator.initialize();
                }
            }
            
            public KeySlot getSlot(int index) {
                SlotRangesIterator slotRanges = slotRangesIterator.get(index);
                return slotRanges.getSlot();
            }
            
            public KeyRange getRange(int index) {
                SlotRangesIterator slotRanges = slotRangesIterator.get(index);
                return slotRanges.getRange();
            }
            
            public boolean next() {
                if (firstCall) {
                    boolean hasAny = false;
                    for (int i = 0; i < childSlots.size(); i++) {
                        hasAny |= this.slotRangesIterator.get(i).initialize();
                    }
                    firstCall = false;
                    return hasAny;
                }
                int i = 0;
                while (i < childSlots.size() && !slotRangesIterator.get(i).next()) {
                    i++;
                }
                for (i = 0; i < childSlots.size(); i++) {
                    if (!this.slotRangesIterator.get(i).isWrapped()) {
                        return true;
                    }
                }
                return false;
            }
            
            private class SlotRangesIterator {
                public int slotIndex;
                public int rangeIndex;
                public final KeySlots slots;
                public boolean wrapped;
                
                public SlotRangesIterator(int slotsIndex) {
                    this.slots = childSlots.get(slotsIndex);
                }
                
                public boolean isWrapped() {
                    return wrapped || !hasAny();
                }
                
                private boolean initialize() {
                    slotIndex = 0;
                    rangeIndex = 0;
                    while (slotIndex < slots.getSlots().size() 
                            && (slots.getSlots().get(slotIndex) == null
                                || slots.getSlots().get(slotIndex).getKeyRanges().isEmpty()
                                || slots.getSlots().get(slotIndex).getPKPosition() != pkPos)) {
                        slotIndex++;
                    }
                    return hasAny();
                }
                
                private boolean hasAny() {
                    return slotIndex < slots.getSlots().size();
                }
                
                public KeySlot getSlot() {
                    if (!hasAny()) return null;
                    return slots.getSlots().get(slotIndex);
                }
                
                public KeyRange getRange() {
                    if (!hasAny()) return null;
                    return getSlot().getKeyRanges().get(rangeIndex);
                }
                
                public boolean next() {
                    if (!hasAny()) {
                        return false;
                    }
                    List<KeyRange> ranges = getSlot().getKeyRanges();
                    if ((rangeIndex = (rangeIndex + 1) % ranges.size()) == 0) {
                        do {
                            if (((slotIndex = (slotIndex + 1) % slots.getSlots().size()) == 0)) {
                                initialize();
                                wrapped = true;
                                return false;
                            }
                        } while (getSlot() == null || getSlot().getKeyRanges().isEmpty() || getSlot().getPKPosition() != pkPos);
                    }
                
                    return true;
                }
            }
        }
        
        /**
         * Ands together an arbitrary set of compiled expressions (represented as a list of KeySlots)
         * by intersecting each unique combination among the childSlots.
         * @param andExpression expressions being anded together
         * @param childSlots compiled form of child expressions being anded together.
         * @return
         */
        private KeySlots andKeySlots(AndExpression andExpression, List<KeySlots> childSlots) {

            if(childSlots.isEmpty()) {
                return null;
            }
            // Exit early if it's already been determined that one of the child slots cannot
            // possibly be true.
            boolean partialExtraction = andExpression.getChildren().size() != childSlots.size();

            int nChildSlots = childSlots.size();
            for (int i = 0; i < nChildSlots; i++) {
                KeySlots childSlot = childSlots.get(i);
                if (childSlot == EMPTY_KEY_SLOTS) {
                    return EMPTY_KEY_SLOTS;
                }
                // If any child slots represent partially extracted expressions, then carry
                // that forward. An example of a partially extracted expression would be a
                // RVC of (K1, K2, NonK3) in which only leading PK columns are extracted
                // from the RVC.
                partialExtraction |= childSlot.isPartialExtraction();
            }
            boolean mayExtractNodes = true;
            ImmutableBytesWritable ptr = context.getTempPtr();
            RowKeySchema rowKeySchema = table.getRowKeySchema();
            int nPkColumns = table.getPKColumns().size();
            KeySlot[] keySlotArray = new KeySlot[nPkColumns];
            int initPkPos = (table.getBucketNum() ==null ? 0 : 1) + (this.context.getConnection().getTenantId() != null && table.isMultiTenant() ? 1 : 0) + (table.getViewIndexId() == null ? 0 : 1);
            
            List<List<List<KeyRange[]>>> slotsTrailingRanges = Lists.newArrayListWithExpectedSize(nPkColumns);
            // Process all columns being ANDed in position order to guarantee
            // we have all information for leading PK columns before we attempt
            // to intersect them. For example:
            //     (A, B, C) >= (1, 2, 3) AND (B, C) < (4, 5) AND  A = 1
            // will processing slot 0 (i.e PK column A) across all children first,
            // followed by slot 1 (PK column B), and finally slot 2 (C). This is
            // done because we carry forward any constraints from preceding PK
            // columns which may impact following PK columns. In the above example
            // we'd carry forward that (B,C) >= (2,3) since we know that A is 1.
            for (int pkPos = initPkPos; pkPos < nPkColumns; pkPos++) {
                SlotsIterator iterator = new SlotsIterator(childSlots, pkPos);
                OrderPreserving orderPreserving = null;
                Set<KeyPart> visitedKeyParts = Sets.newHashSet();
                Set<Expression> extractNodes = new LinkedHashSet<>();
                List<KeyRange> keyRanges = Lists.newArrayList();
                // This is the information carried forward as we process in PK order.
                // It's parallel with the list of keyRanges.
                List<KeyRange[]> trailingRangesList = Lists.<KeyRange[]>newArrayList();
                KeyRange result = null;
                TrailingRangeIterator trailingRangeIterator = new TrailingRangeIterator(initPkPos, pkPos, slotsTrailingRanges);
                // Iterate through all combinations (i.e. constraints) for the PK slot being processed.
                // For example, with (A = 1 OR A = 2) AND (A,B) > (1,2) AND C = 3, we'd process the
                // following two combinations:
                //     A=1,(A,B) > (1,2)
                //     A=2,(A,B) > (1,2)
                // If we have no constraints for a PK, then we still must iterate through the information
                // that may have been rolled up based on the processing of previous PK slots. For example,
                // in the above ANDed expressions, we have no constraint on B, but we would end up with
                // rolled up information based on the B part of the (A,B) constraint.
                while (iterator.next() || (trailingRangeIterator.hasNext() && result != KeyRange.EMPTY_RANGE)) {
                    result = null;
                    KeyRange[] trailingRanges = newTrailingRange();
                    for (int i = 0; i < nChildSlots && result != KeyRange.EMPTY_RANGE; i++) {
                        KeySlot slot = iterator.getSlot(i);
                        // Rollup the order preserving and concatenate the extracted expressions.
                        // Extracted expressions end up being removed from the AND expression at
                        // the top level call (pushKeyExpressionsToScan) with anything remaining
                        // ending up as a Filter (rather than contributing to the start/stop row
                        // of the scan.
                        if (slot != null) {
                            KeyRange otherRange = iterator.getRange(i);
                            KeyRange range = result;
                            if (slot.getOrderPreserving() != null) {
                                orderPreserving = slot.getOrderPreserving().combine(orderPreserving);
                            }
                            // Extract once per iteration, when there are large number
                            // of OR clauses (for e.g N > 100k).
                            // The extractNodes.addAll method can get called N times.
                            if (visitedKeyParts.add(slot.getKeyPart()) && slot.getKeyPart().getExtractNodes() != null) {
                                extractNodes.addAll(slot.getKeyPart().getExtractNodes());
                            }
                            // Keep a running intersection of the ranges we see. Note that the
                            // ranges are derived from constants appearing on the RHS of a comparison
                            // expression. For example, the expression A > 5 would produce a keyRange
                            // of (5, *) for slot 0 (assuming A is the leading PK column) If the result
                            // ends up as an empty key, that combination is ruled out. This is essentially
                            // doing constant reduction.
                            result = intersectRanges(pkPos, range, otherRange, trailingRanges);
                        }
                    }
                    
                    if (result != KeyRange.EMPTY_RANGE) {
                        Map<KeyRange,List<KeyRange[]>> results = Maps.newHashMap();
                        trailingRangeIterator.init();
                        // Process all constraints that have been rolled up from previous
                        // processing of PK slots. This occurs for RVCs which span PK slots
                        // in which the leading part of the RVC is determined to be equal
                        // to a constant on the RHS.
                        while (trailingRangeIterator.hasNext()) {
                            // Loop through all combinations of values for all previously
                            // calculated slots.
                            do {
                                // Loop through all combinations of range constraints for the
                                // current combinations of values. If no valid combinations
                                // are found, we can rule out the result. We can also end up
                                // modifying the result if it has an intersection with the
                                // range constraints.
                                do {
                                    KeyRange priorTrailingRange = trailingRangeIterator.getRange();
                                    if (priorTrailingRange != KeyRange.EVERYTHING_RANGE) {
                                        KeyRange[] intTrailingRanges = Arrays.copyOf(trailingRanges, trailingRanges.length);
                                        // Intersect the current result with each range constraint. We essentially
                                        // rule out the result when we find a constraint that has no intersection
                                        KeyRange intResult = intersectRanges(pkPos, result, priorTrailingRange, intTrailingRanges);
                                        if (intResult != KeyRange.EMPTY_RANGE) {
                                            addResult(intResult, intTrailingRanges, results);
                                        }
                                    }
                                } while (trailingRangeIterator.nextTrailingRange());
                            } while (trailingRangeIterator.nextRange());
                        }
                        if (results.isEmpty() && result != null) { // No trailing range constraints
                            keyRanges.add(result);
                            trailingRangesList.add(trailingRanges);
                        } else {
                            mayExtractNodes &= results.size() <= 1;
                            for (Map.Entry<KeyRange,List<KeyRange[]>> entry : results.entrySet()) {
                                // Add same KeyRange with each KeyRange[] since the two lists are parallel
                                for (KeyRange[] trailingRange : entry.getValue()) {
                                    keyRanges.add(entry.getKey());
                                    trailingRangesList.add(trailingRange);
                                }
                            }
                        }
                    }
                }

                if (result == null && keyRanges.isEmpty()) {
                    slotsTrailingRanges.add(Collections.<List<KeyRange[]>>emptyList());
                } else {
                    // If we encountered a result for this slot and
                    // there are no ranges, this is the degenerate case.
                    if (keyRanges.isEmpty()) {
                        return EMPTY_KEY_SLOTS;
                    }
                    // Similar to KeyRange.coalesce(), except we must combine together
                    // any rolled up constraints (as a list of KeyRanges) for a
                    // particular value (as they're coalesced together). We maintain
                    // these KeyRange constraints as a parallel list between keyRanges
                    // and trailingRangesList.
                    keyRanges = coalesceKeyRangesAndTrailingRanges(keyRanges, trailingRangesList, slotsTrailingRanges);
                    int maxSpan = 1;
                    for (KeyRange aRange : keyRanges) {
                        int span = rowKeySchema.computeMaxSpan(pkPos, aRange, context.getTempPtr());
                        if (span > maxSpan) {
                            maxSpan = span;
                        }
                    }
                    keySlotArray[pkPos] = new KeySlot(
                            new BaseKeyPart(table, table.getPKColumns().get(pkPos), mayExtractNodes ? extractNodes : Collections.<Expression>emptySet()),
                            pkPos,
                            maxSpan,
                            keyRanges,
                            orderPreserving);
                }
            }
            
            // Filters trailing part of RVC based on ranges from PK columns after the one we're
            // currently processing that may overlap with this range. For example, with a PK
            // columns A,B,C and a range of A from [(1,2,3) - (4,5,6)] and B from (6-*), we
            // can filter the trailing part of the RVC for A, because the trailing part of
            // the RVC (2,3)-(5,6) does not intersect with (6-*). By removing the trailing
            // part of the RVC, we end up with a range of A from [1-4] and B from (6-*) which
            // enables us to use a skip scan.
            for (int i = 0; i < keySlotArray.length; i++) {
                KeySlot keySlot = keySlotArray[i];
                if (keySlot == null) continue;
                int pkSpan = keySlot.getPKSpan();
                int pkPos = keySlot.getPKPosition();
                boolean slotWasIntersected = false;
                List<KeyRange> keyRanges = keySlot.getKeyRanges();
                List<KeyRange> slotTrimmedResults = Lists.newArrayListWithExpectedSize(keyRanges.size());
                for (KeyRange result : keyRanges) {
                    boolean resultWasIntersected = false;
                    Set<KeyRange> trimmedResults = Sets.newHashSetWithExpectedSize(keyRanges.size());
                    for (int trailingPkPos = pkPos+1; trailingPkPos < pkPos+pkSpan && trailingPkPos < nPkColumns; trailingPkPos++) {
                        KeySlot nextKeySlot = keySlotArray[trailingPkPos];
                        if (nextKeySlot == null) continue;
                        for (KeyRange trailingRange : nextKeySlot.getKeyRanges()) {
                            resultWasIntersected = true;
                            KeyRange intResult = intersectTrailing(result, pkPos, trailingRange, trailingPkPos);
                            if (intResult != KeyRange.EMPTY_RANGE) {
                                trimmedResults.add(intResult);
                            }
                        }
                    }
                    if (resultWasIntersected) {
                        slotWasIntersected = true;
                        slotTrimmedResults.addAll(trimmedResults);
                        mayExtractNodes &= trimmedResults.size() <= 1;
                    } else {
                        slotTrimmedResults.add(result);
                    }
                }
                if (slotTrimmedResults.isEmpty()) {
                    return EMPTY_KEY_SLOTS;
                }
                if (slotWasIntersected) {
                    // Re-coalesce the ranges and recalc the max span since the ranges may have changed
                    slotTrimmedResults = KeyRange.coalesce(slotTrimmedResults);
                    pkSpan = 1;
                    for (KeyRange trimmedResult : slotTrimmedResults) {
                        pkSpan = Math.max(pkSpan, rowKeySchema.computeMaxSpan(pkPos, trimmedResult, ptr));
                    }
                }

                Set<Expression> extractNodes = mayExtractNodes ?
                        keySlotArray[pkPos].getKeyPart().getExtractNodes() :  new LinkedHashSet<>();
                keySlotArray[pkPos] = new KeySlot(
                        new BaseKeyPart(table, table.getPKColumns().get(pkPos), extractNodes),
                        pkPos,
                        pkSpan,
                        slotTrimmedResults,
                        keySlotArray[pkPos].getOrderPreserving());
            }
            List<KeySlot> keySlots = Arrays.asList(keySlotArray);
            // If we have a salt column, skip that slot because
            // they'll never be an expression that uses it directly.
            keySlots = keySlots.subList(initPkPos, keySlots.size());
            return new MultiKeySlot(keySlots, partialExtraction);
        }

        private KeyRange[] newTrailingRange() {
            KeyRange[] trailingRanges = new KeyRange[table.getPKColumns().size()];
            for (int i = 0; i < trailingRanges.length; i++) {
                trailingRanges[i] = KeyRange.EVERYTHING_RANGE;
            }
            return trailingRanges;
        }
        
        private static void addResult(KeyRange result, KeyRange[] trailingRange, Map<KeyRange,List<KeyRange[]>> results) {
            List<KeyRange[]> trailingRanges = Lists.<KeyRange[]>newArrayList(trailingRange);
            List<KeyRange[]> priorTrailingRanges = results.put(result, trailingRanges);
            if (priorTrailingRanges != null) {
                // This is tricky case. We may have multiple possible values based on the rolled up range
                // constraints from previous slots. We track unique ranges and concatenate together the
                // trailing range data. If there's more than one element in the set (i.e. more than one
                // possible result), we'll end up have more combinations than there actually are because
                // the constraint only apply for a single value, not for *all* combinations (which is a
                // limitation of our representation derived from what can be handled by our SkipScanFilter).
                // For example, if we we've gathered these ranges so far in a three PK table: (1,2), (A,B)
                // and have X as a constraint for value A and Y as a constraint for value B, we have the 
                // following possible combinations: 1AX, 2AX, 1BY, 2BY. However, our SkipScanFilter only
                // supports identifying combinations for *all* combinations of (1,2),(A,B),(X,Y) or 
                // AX, 1AY, 1BX, 1BY, 2AX, 2AY, 2BX, 2BY. See WhereOptimizerTest.testNotRepresentableBySkipScan()
                // for an example.
                trailingRanges.addAll(priorTrailingRanges);
            }
        }

        private List<KeyRange> coalesceKeyRangesAndTrailingRanges(List<KeyRange> keyRanges,
                List<KeyRange[]> trailingRangesList, List<List<List<KeyRange[]>>> slotsTrailingRanges) {
            List<Pair<KeyRange,List<KeyRange[]>>> pairs = coalesce(keyRanges, trailingRangesList);
            List<List<KeyRange[]>> trailingRanges = Lists.newArrayListWithExpectedSize(pairs.size());
            List<KeyRange>coalescedKeyRanges = Lists.newArrayListWithExpectedSize(pairs.size());
            for (Pair<KeyRange,List<KeyRange[]>> pair : pairs) {
                coalescedKeyRanges.add(pair.getFirst());
                trailingRanges.add(pair.getSecond());
            }
            slotsTrailingRanges.add(trailingRanges);
            return coalescedKeyRanges;
        }

        public static final Comparator<Pair<KeyRange,List<KeyRange[]>>> KEY_RANGE_PAIR_COMPARATOR = new Comparator<Pair<KeyRange,List<KeyRange[]>>>() {
            @Override public int compare(Pair<KeyRange,List<KeyRange[]>> o1, Pair<KeyRange,List<KeyRange[]>> o2) {
                return KeyRange.COMPARATOR.compare(o1.getFirst(), o2.getFirst());
            }
        };
        
        private static boolean isEverythingRanges(KeyRange[] ranges) {
            for (KeyRange range : ranges) {
                if (range != KeyRange.EVERYTHING_RANGE) {
                    return false;
                }
            }
            return true;
        }
        
        private static List<KeyRange[]> concat(List<KeyRange[]> list1, List<KeyRange[]> list2) {
            if (list1.size() == 1 && isEverythingRanges(list1.get(0))) {
                if (list2.size() == 1 && isEverythingRanges(list1.get(0))) {
                    return Collections.emptyList();
                }
                return list2;
            }
            if (list2.size() == 1 && isEverythingRanges(list2.get(0))) {
                return list1;
            }
            
            List<KeyRange[]> newList = Lists.<KeyRange[]>newArrayListWithExpectedSize(list1.size()+list2.size());
            newList.addAll(list1);
            newList.addAll(list2);
            return newList;
        }
        
        /**
         * Similar to KeyRange.coelesce, but con
         */
        @NonNull
        public static List<Pair<KeyRange,List<KeyRange[]>>> coalesce(List<KeyRange> keyRanges, List<KeyRange[]> trailingRangesList) {
            List<Pair<KeyRange,List<KeyRange[]>>> tmp = Lists.newArrayListWithExpectedSize(keyRanges.size());
            int nKeyRanges = keyRanges.size();
            for (int i = 0; i < nKeyRanges; i++) {
                KeyRange keyRange = keyRanges.get(i);
                KeyRange[] trailingRange = trailingRangesList.get(i);
                Pair<KeyRange,List<KeyRange[]>> pair = new Pair<KeyRange,List<KeyRange[]>>(keyRange,Lists.<KeyRange[]>newArrayList(trailingRange));
                tmp.add(pair);
            }
            Collections.sort(tmp, KEY_RANGE_PAIR_COMPARATOR);
            List<Pair<KeyRange,List<KeyRange[]>>> tmp2 = Lists.<Pair<KeyRange,List<KeyRange[]>>>newArrayListWithExpectedSize(tmp.size());
            Pair<KeyRange,List<KeyRange[]>> range = tmp.get(0);
            for (int i=1; i<tmp.size(); i++) {
                Pair<KeyRange,List<KeyRange[]>> otherRange = tmp.get(i);
                KeyRange intersect = range.getFirst().intersect(otherRange.getFirst());
                if (KeyRange.EMPTY_RANGE == intersect) {
                    tmp2.add(range);
                    range = otherRange;
                } else {
                    KeyRange newRange = range.getFirst().union(otherRange.getFirst());
                    range = new Pair<KeyRange,List<KeyRange[]>>(newRange,concat(range.getSecond(),otherRange.getSecond()));
                }
            }
            tmp2.add(range);
            List<Pair<KeyRange,List<KeyRange[]>>> tmp3 = Lists.<Pair<KeyRange,List<KeyRange[]>>>newArrayListWithExpectedSize(tmp2.size());
            range = tmp2.get(0);
            for (int i=1; i<tmp2.size(); i++) {
                Pair<KeyRange,List<KeyRange[]>> otherRange = tmp2.get(i);
                assert !range.getFirst().upperUnbound();
                assert !otherRange.getFirst().lowerUnbound();
                if (range.getFirst().isUpperInclusive() != otherRange.getFirst().isLowerInclusive()
                        && Bytes.equals(range.getFirst().getUpperRange(), otherRange.getFirst().getLowerRange())) {
                    KeyRange newRange = KeyRange.getKeyRange(
                            range.getFirst().getLowerRange(), range.getFirst().isLowerInclusive(),
                            otherRange.getFirst().getUpperRange(), otherRange.getFirst().isUpperInclusive());
                    range = new Pair<KeyRange,List<KeyRange[]>>(newRange,concat(range.getSecond(),otherRange.getSecond()));
                } else {
                    tmp3.add(range);
                    range = otherRange;
                }
            }
            tmp3.add(range);
            
            return tmp3;
        }
        
        /**
         * 
         * Iterates over all unique combinations of the List<KeyRange[]> representing
         * the constraints from previous slot positions. For example, if we have
         * a RVC of (A,B) = (2,1), then if A=2, we know that B must be 1.
         *
         */
        static class TrailingRangeIterator {
            private final List<List<List<KeyRange[]>>> slotTrailingRangesList;
            private final int[] rangePos;
            private final int[] trailingRangePos;
            private final int initPkPos;
            private final int pkPos;
            private int trailingRangePosIndex;
            private int rangePosIndex;
            private boolean hasMore = true;
            
            TrailingRangeIterator (int initPkPos, int pkPos, List<List<List<KeyRange[]>>> slotsTrailingRangesList) {
                this.slotTrailingRangesList = slotsTrailingRangesList;
                int nSlots = pkPos - initPkPos;
                rangePos = new int[nSlots];
                trailingRangePos = new int[nSlots];
                this.initPkPos = initPkPos;
                this.pkPos = pkPos;
                init();
            }
            
            public void init() {
                Arrays.fill(rangePos, 0);
                Arrays.fill(trailingRangePos, 0);
                rangePosIndex = rangePos.length - 1;
                trailingRangePosIndex = trailingRangePos.length - 1;
                this.hasMore = pkPos > initPkPos && skipEmpty();
            }

            public boolean hasNext() {
                return hasMore && skipEmpty();
            }
            
            public KeyRange getRange() {
                if (!hasMore) {
                    throw new NoSuchElementException();
                }
                KeyRange priorTrailingRange = KeyRange.EVERYTHING_RANGE;
                for (int priorPkPos = initPkPos; priorPkPos < pkPos; priorPkPos++) {
                    List<List<KeyRange[]>>trailingKeyRangesList = slotTrailingRangesList.get(priorPkPos-initPkPos);
                    if (!trailingKeyRangesList.isEmpty()) {
                        List<KeyRange[]> slotTrailingRanges = trailingKeyRangesList.get(rangePos[priorPkPos-initPkPos]);
                        if (!slotTrailingRanges.isEmpty()) {
                            KeyRange[] slotTrailingRange = slotTrailingRanges.get(trailingRangePos[priorPkPos-initPkPos]);
                            priorTrailingRange = priorTrailingRange.intersect(slotTrailingRange[pkPos]);
                        }
                    }
                }
                
                return priorTrailingRange;
            }
            
            private boolean skipEmptyTrailingRanges() {
                while (trailingRangePosIndex >= 0 && 
                        (slotTrailingRangesList.get(trailingRangePosIndex).isEmpty() 
                                || slotTrailingRangesList.get(trailingRangePosIndex).get(rangePos[trailingRangePosIndex]).isEmpty())) {
                    trailingRangePosIndex--;
                }
                if (trailingRangePosIndex >= 0) {
                    return true;
                }
                return false;
           }
            
            private boolean skipEmptyRanges() {
                trailingRangePosIndex = trailingRangePos.length - 1;
                while (rangePosIndex >= 0 && 
                        (slotTrailingRangesList.get(rangePosIndex).isEmpty())) {
                    rangePosIndex--;
                }
                return rangePosIndex >= 0;
            }
            
            private boolean skipEmpty() {
                if (!hasMore || slotTrailingRangesList.isEmpty() || rangePosIndex < 0) {
                    return hasMore=false;
                }
                do {
                    if (skipEmptyTrailingRanges()) {
                        return true;
                    }
                } while (skipEmptyRanges());
                return hasMore = rangePosIndex >= 0;
            }
            
            public boolean nextRange() {
                trailingRangePosIndex = trailingRangePos.length - 1;
                while (rangePosIndex >= 0 && 
                        (slotTrailingRangesList.get(rangePosIndex).isEmpty() 
                                 || (rangePos[rangePosIndex] = (rangePos[rangePosIndex] + 1) 
                                    % slotTrailingRangesList.get(rangePosIndex).size()) == 0)) {
                    rangePosIndex--;
                }
                return rangePosIndex >= 0;
            }

            public boolean nextTrailingRange() {
                while (trailingRangePosIndex >= 0 && 
                        (slotTrailingRangesList.get(trailingRangePosIndex).isEmpty() 
                                || slotTrailingRangesList.get(trailingRangePosIndex).get(rangePos[trailingRangePosIndex]).isEmpty() 
                                || (trailingRangePos[trailingRangePosIndex] = (trailingRangePos[trailingRangePosIndex] + 1) 
                                    % slotTrailingRangesList.get(trailingRangePosIndex).get(rangePos[trailingRangePosIndex]).size()) == 0)) {
                    trailingRangePosIndex--;
                }
                if (trailingRangePosIndex >= 0) {
                    return true;
                }
                return false;
            }
        }
        
        private KeyRange intersectRanges(int pkPos, KeyRange range, KeyRange otherRange, KeyRange[] trailingRanges) {
            // We need to initialize result to the other range rather than
            // initializing it to EVERYTHING_RANGE to handle the IS NULL case.
            // Otherwise EVERYTHING_RANGE intersected below with NULL_RANGE
            // becomes an EMPTY_RANGE.
            if (range == null) {
                range = otherRange;
            }
            KeyRange result = range;
            ImmutableBytesWritable ptr = context.getTempPtr();
            RowKeySchema rowKeySchema = table.getRowKeySchema();
            int minSpan = rowKeySchema.computeMinSpan(pkPos, result, ptr);
            int otherMinSpan = rowKeySchema.computeMinSpan(pkPos, otherRange, ptr);
            KeyRange otherClippedRange = otherRange;
            KeyRange clippedRange = result;
            if (minSpan != otherMinSpan && result != KeyRange.EVERYTHING_RANGE && otherRange != KeyRange.EVERYTHING_RANGE) {
                if (otherMinSpan > minSpan) {
                    otherClippedRange = rowKeySchema.clipLeft(pkPos, otherRange, minSpan, ptr);
                } else if (minSpan > otherMinSpan) {
                    clippedRange = rowKeySchema.clipLeft(pkPos, result, otherMinSpan, ptr);
                }
            }
            
            // intersect result with otherRange
            result = clippedRange.intersect(otherClippedRange);
            if (result == KeyRange.EMPTY_RANGE) {
                return result;
            }
            if (minSpan != otherMinSpan) {
                // If trailing ranges are of different spans, intersect them at the common
                // span and add remaining part of range used to trailing ranges
                // Without the special case for single key values, the trailing ranges
                // code doesn't work correctly for WhereOptimizerTest.testMultiSlotTrailingIntersect()
                if (result.isSingleKey() && !(range.isSingleKey() && otherRange.isSingleKey())) {
                    int trailingPkPos = pkPos + Math.min(minSpan, otherMinSpan);
                    KeyRange trailingRange = getTrailingRange(rowKeySchema, pkPos, minSpan > otherMinSpan ? range : otherRange, result, ptr);
                    trailingRanges[trailingPkPos] = trailingRanges[trailingPkPos].intersect(trailingRange);
                } else {
                    // Add back clipped part of range 
                    if (otherMinSpan > minSpan) {
                        result = concatSuffix(result, otherRange);
                    } else if (minSpan > otherMinSpan) {
                        result = concatSuffix(result, range);
                    }
                }
            }
            return result;
        }

        private static KeyRange concatSuffix(KeyRange result, KeyRange otherRange) {
            byte[] lowerRange = result.getLowerRange();
            byte[] clippedLowerRange = lowerRange;
            byte[] fullLowerRange = otherRange.getLowerRange();
            if (!result.lowerUnbound() && Bytes.startsWith(fullLowerRange, clippedLowerRange)) {
                lowerRange = fullLowerRange;
            }
            byte[] upperRange = result.getUpperRange();
            byte[] clippedUpperRange = upperRange;
            byte[] fullUpperRange = otherRange.getUpperRange();
            if (!result.lowerUnbound() && Bytes.startsWith(fullUpperRange, clippedUpperRange)) {
                upperRange = fullUpperRange;
            }
            if (lowerRange == clippedLowerRange && upperRange == clippedUpperRange) {
                return result;
            }
            return KeyRange.getKeyRange(lowerRange, result.isLowerInclusive(), upperRange, result.isUpperInclusive());
        }

        /**
         * Intersects an RVC that starts at pkPos with an overlapping range that starts at otherPKPos.
         * For example, ((A, B) - (J, K)) intersected with (F - *) would return ((A,F) - (J, K))
         *     ((A, B) - (J, K)) intersected with (M - P) would return (A-J) since both of the trailing
         * part of the RVC, B and K, do not intersect with B and K.
         * @param result an RVC expression starting from pkPos and with length of at least otherPKPos - pkPos.
         * @param pkPos the PK position of the leading part of the RVC expression
         * @param otherRange the other range to intersect with the overlapping part of the RVC.
         * @param otherPKPos the PK position of the leading part of the other range
         * @return resulting KeyRange from the intersection, potentially an empty range if the result RVC
         *  is a single key and the trailing part of the key does not intersect with the RVC.
         */
        private KeyRange intersectTrailing(KeyRange result, int pkPos, KeyRange otherRange, int otherPKPos) {
            RowKeySchema rowKeySchema = table.getRowKeySchema();
            ImmutableBytesWritable ptr = context.getTempPtr();
            int separatorLength = table.getPKColumns().get(otherPKPos-1).getDataType().isFixedWidth() ? 0 : 1;
            boolean lowerInclusive = result.isLowerInclusive();
            byte[] lowerRange = result.getLowerRange();
            ptr.set(lowerRange);
            // Position ptr at the point at which the two ranges overlap
            if (rowKeySchema.position(ptr, pkPos, otherPKPos)) {
                int lowerOffset = ptr.getOffset();
                // Increase the length of the ptr to include the entire trailing bytes
                ptr.set(ptr.get(), lowerOffset, lowerRange.length - lowerOffset);
                byte[] trailingBytes = ptr.copyBytes();
                
                // Special case for single key since single keys of different span lengths
                // will never overlap. We do not need to process both the lower and upper
                // ranges since they are the same.
                if (result.isSingleKey() && otherRange.isSingleKey()) {
                    int minSpan = rowKeySchema.computeMinSpan(pkPos, result, ptr);
                    int otherMinSpan =
                        rowKeySchema.computeMinSpan(otherPKPos, otherRange, ptr);
                    byte[] otherLowerRange;
                    boolean isFixedWidthAtEnd;
                    if (pkPos + minSpan <= otherPKPos + otherMinSpan) {
                        otherLowerRange = otherRange.getLowerRange();
                        isFixedWidthAtEnd = table.getPKColumns().get(pkPos + minSpan -1).getDataType().isFixedWidth();
                    } else {
                        otherLowerRange = trailingBytes;
                        trailingBytes = otherRange.getLowerRange();
                        isFixedWidthAtEnd = table.getPKColumns().get(otherPKPos + otherMinSpan -1).getDataType().isFixedWidth();
                    }
                    // If the otherRange starts with the overlapping trailing byte *and* we're comparing
                    // the entire key (i.e. not just a leading subset), then we have an intersection.
                    if (Bytes.startsWith(otherLowerRange, trailingBytes) &&
                            (isFixedWidthAtEnd || 
                             otherLowerRange.length == trailingBytes.length || 
                             otherLowerRange[trailingBytes.length] == QueryConstants.SEPARATOR_BYTE)) {
                        return result;
                    }
                    // Otherwise, there is no overlap
                    return KeyRange.EMPTY_RANGE;
                }
                // If we're not dealing with single keys, then we can use our normal intersection
                if (otherRange.intersect(KeyRange.getKeyRange(trailingBytes)) == KeyRange.EMPTY_RANGE) {
                    // Exit early since the upper range is the same as the lower range
                    if (result.isSingleKey()) {
                        return KeyRange.EMPTY_RANGE;
                    }
                    ptr.set(result.getLowerRange(), 0, lowerOffset - separatorLength);
                    lowerRange = ptr.copyBytes();
                }
            }
            boolean upperInclusive = result.isUpperInclusive();
            byte[] upperRange = result.getUpperRange();
            ptr.set(upperRange);
            if (rowKeySchema.position(ptr, pkPos, otherPKPos)) {
                int upperOffset = ptr.getOffset();
                ptr.set(ptr.get(), upperOffset, upperRange.length - upperOffset);
                if (otherRange.intersect(KeyRange.getKeyRange(ptr.copyBytes())) == KeyRange.EMPTY_RANGE) {
                    ptr.set(ptr.get(), 0, upperOffset - separatorLength);
                    upperRange = ptr.copyBytes();
                }
            }
            if (lowerRange == result.getLowerRange() && upperRange == result.getUpperRange()) {
                return result;
            }
            KeyRange range = KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
            return range;
        }

        private KeySlots orKeySlots(OrExpression orExpression, List<KeySlots> childSlots) {
            // If any children were filtered out, filter out the entire
            // OR expression because we don't have enough information to
            // constraint the scan start/stop key. An example would be:
            // WHERE organization_id=? OR key_value_column = 'x'
            // In this case, we cannot simply filter the key_value_column,
            // because we end up bubbling up only the organization_id=?
            // expression to form the start/stop key which is obviously wrong.
            // For an OR expression, you need to be able to extract
            // everything or nothing.
            if (orExpression.getChildren().size() != childSlots.size()) {
                return null;
            }
            int initialPos = (table.getBucketNum() ==null ? 0 : 1) + (this.context.getConnection().getTenantId() != null && table.isMultiTenant() ? 1 : 0) + (table.getViewIndexId() == null ? 0 : 1);
            KeySlot theSlot = null;
            Set<Expression> slotExtractNodes = new LinkedHashSet<>();
            int thePosition = -1;
            boolean partialExtraction = false;
            // TODO: Have separate list for single span versus multi span
            // For multi-span, we only need to keep a single range.
            List<KeyRange> slotRanges = Lists.newArrayList();
            for (KeySlots childSlot : childSlots) {
                if (childSlot == EMPTY_KEY_SLOTS) {
                    // TODO: can this ever happen and can we safely filter the expression tree?
                    continue;
                }
                // When we OR together expressions, we can only extract the entire OR expression
                // if all sub-expressions have been completely extracted. Otherwise, we must
                // leave the OR as a post filter.
                partialExtraction |= childSlot.isPartialExtraction();
                // TODO: Do the same optimization that we do for IN if the childSlots specify a fully qualified row key
                for (KeySlot slot : childSlot.getSlots()) {
                    if (slot == null) {
                        continue;
                    }
                    /*
                     * If we see a different PK column than before, we can't
                     * optimize it because our SkipScanFilter only handles
                     * top level expressions that are ANDed together (where in
                     * the same column expressions may be ORed together).
                     * For example, WHERE a=1 OR b=2 cannot be handled, while
                     *  WHERE (a=1 OR a=2) AND (b=2 OR b=3) can be handled.
                     * TODO: We could potentially handle these cases through
                     * multiple, nested SkipScanFilters, where each OR expression
                     * is handled by its own SkipScanFilter and the outer one
                     * increments the child ones and picks the one with the smallest
                     * key.
                     */
                    if (thePosition == -1) {
                        theSlot = slot;
                        thePosition = slot.getPKPosition();
                    } else if (thePosition != slot.getPKPosition()) {
                        return null;
                    }
                    slotExtractNodes.addAll(slot.getKeyPart().getExtractNodes());
                    slotRanges.addAll(slot.getKeyRanges());
                }
            }

            if (thePosition == -1) {
                return null;
            }
            if (theSlot == null) {
                theSlot = new KeySlot(new BaseKeyPart(table, table.getPKColumns().get(initialPos), slotExtractNodes), initialPos, 1, EVERYTHING_RANGES, null);
            }
            return newKeyParts(
                    theSlot,
                    partialExtraction ? slotExtractNodes : new LinkedHashSet<>(Collections.<Expression>singletonList(orExpression)),
                    slotRanges.isEmpty() ? EVERYTHING_RANGES : KeyRange.coalesce(slotRanges));
        }

        private final PTable table;
        private final StatementContext context;

        public KeyExpressionVisitor(StatementContext context, PTable table) {
            this.context = context;
            this.table = table;
        }

        @Override
        public Iterator<Expression> visitEnter(CoerceExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public KeySlots visitLeave(CoerceExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            return newCoerceKeyPart(childParts.get(0).getSlots().get(0), node);
        }

        @Override
        public Iterator<Expression> visitEnter(AndExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public KeySlots visitLeave(AndExpression node, List<KeySlots> l) {
            KeySlots keyExpr = andKeySlots(node, l);
            return keyExpr;
        }

        @Override
        public Iterator<Expression> visitEnter(OrExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public KeySlots visitLeave(OrExpression node, List<KeySlots> l) {
            KeySlots keySlots = orKeySlots(node, l);
            if (keySlots == null) {
                return null;
            }
            return keySlots;
        }

        @Override
        public Iterator<Expression> visitEnter(RowValueConstructorExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public KeySlots visitLeave(RowValueConstructorExpression node, List<KeySlots> childSlots) {
            return newRowValueConstructorKeyParts(node, childSlots);
        }

        @Override
        public KeySlots visit(RowKeyColumnExpression node) {
            PColumn column = table.getPKColumns().get(node.getPosition());
            return new SingleKeySlot(new BaseKeyPart(table, column, new LinkedHashSet<>(Collections.<Expression>singletonList(node))), node.getPosition(), 1, EVERYTHING_RANGES);
        }

        @Override
        public Iterator<Expression> visitEnter(ComparisonExpression node) {
            Expression rhs = node.getChildren().get(1);
            if (!rhs.isStateless() || node.getFilterOp() == CompareOp.NOT_EQUAL) {
                return Collections.emptyIterator();
            }
            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeySlots visitLeave(ComparisonExpression node, List<KeySlots> childParts) {
            // Delay adding to extractedNodes, until we're done traversing,
            // since we can't yet tell whether or not the PK column references
            // are contiguous
            if (childParts.isEmpty()) {
                return null;
            }
            Expression rhs = node.getChildren().get(1);
            KeySlots childSlots = childParts.get(0);
            KeySlot childSlot = childSlots.getSlots().get(0);
            KeyPart childPart = childSlot.getKeyPart();
            //SortOrder sortOrder = childPart.getColumn().getSortOrder();
            CompareOp op = node.getFilterOp();
            //CompareOp op = sortOrder.transform(node.getFilterOp());
            KeyRange keyRange = childPart.getKeyRange(op, rhs);
            return newKeyParts(childSlot, node, keyRange);
        }

        // TODO: consider supporting expression substitution in the PK for pre-joined tables
        // You'd need to register the expression for a given PK and substitute with a column
        // reference for this during ExpressionBuilder.
        @Override
        public Iterator<Expression> visitEnter(ScalarFunction node) {
            int index = node.getKeyFormationTraversalIndex();
            if (index < 0) {
                return Collections.emptyIterator();
            }
            return Iterators.singletonIterator(node.getChildren().get(index));
        }

        @Override
        public KeySlots visitLeave(ScalarFunction node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            return newScalarFunctionKeyPart(childParts.get(0).getSlots().get(0), node);
        }

        @Override
        public Iterator<Expression> visitEnter(LikeExpression node) {
            // TODO: can we optimize something that starts with '_' like this: foo LIKE '_a%' ?
            if (node.getLikeType() == LikeType.CASE_INSENSITIVE || // TODO: remove this when we optimize ILIKE
                ! (node.getChildren().get(1) instanceof LiteralExpression) || node.startsWithWildcard()) {
                return Collections.emptyIterator();
            }

            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeySlots visitLeave(LikeExpression node, List<KeySlots> childParts) {
            // TODO: optimize ILIKE by creating two ranges for the literal prefix: one with lower case, one with upper case
            if (childParts.isEmpty()) {
                return null;
            }
            // for SUBSTR(<column>,1,3) LIKE 'foo%'
            KeySlots childSlots = childParts.get(0);
            KeySlot childSlot = childSlots.getSlots().get(0);
            final String startsWith = node.getLiteralPrefix();
            SortOrder sortOrder = node.getChildren().get(0).getSortOrder();
            byte[] key = PVarchar.INSTANCE.toBytes(startsWith, sortOrder);
            // If the expression is an equality expression against a fixed length column
            // and the key length doesn't match the column length, the expression can
            // never be true.
            // An zero length byte literal is null which can never be compared against as true
            Expression firstChild = node.getChildren().get(0);
            Integer childNodeFixedLength = firstChild.getDataType().isFixedWidth() ? firstChild.getMaxLength() : null;
            if (childNodeFixedLength != null && key.length > childNodeFixedLength) {
                return EMPTY_KEY_SLOTS;
            }
            // TODO: is there a case where we'd need to go through the childPart to calculate the key range?
            PColumn column = childSlot.getKeyPart().getColumn();
            PDataType type = column.getDataType();
            byte[] lowerRange = key;
            byte[] upperRange = ByteUtil.nextKey(key);
            Integer columnFixedLength = column.getMaxLength();
            if (type.isFixedWidth()) {
                if (columnFixedLength != null) { // Sanity check - should always be non null
                    // Always use minimum byte to fill as otherwise our key is bigger
                    // that it should be when the sort order is descending.
                    lowerRange = type.pad(lowerRange, columnFixedLength, SortOrder.ASC);
                    upperRange = type.pad(upperRange, columnFixedLength, SortOrder.ASC);
                }
            } else if (column.getSortOrder() == SortOrder.DESC && table.rowKeyOrderOptimizable()) {
                // Append a zero byte if descending since a \xFF byte will be appended to the lowerRange
                // causing rows to be skipped that should be included. For example, with rows 'ab', 'a',
                // a lowerRange of 'a\xFF' would skip 'ab', while 'a\x00\xFF' would not.
                lowerRange = Arrays.copyOf(lowerRange, lowerRange.length+1);
                lowerRange[lowerRange.length-1] = QueryConstants.SEPARATOR_BYTE;
            }
            KeyRange range = type.getKeyRange(lowerRange, true, upperRange, false);
            if (column.getSortOrder() == SortOrder.DESC) {
                range = range.invert();
            }
            // Only extract LIKE expression if pattern ends with a wildcard and everything else was extracted
            return newKeyParts(childSlot, node.endsWithOnlyWildcard() ? node : null, range);
        }

        @Override
        public Iterator<Expression> visitEnter(InListExpression node) {
            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeySlots visitLeave(InListExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }

            List<Expression> keyExpressions = node.getKeyExpressions();
            Set<KeyRange> ranges = Sets.newHashSetWithExpectedSize(keyExpressions.size());
            KeySlot childSlot = childParts.get(0).getSlots().get(0);
            KeyPart childPart = childSlot.getKeyPart();
            // Handles cases like WHERE substr(foo,1,3) IN ('aaa','bbb')
            for (Expression key : keyExpressions) {
                KeyRange range = childPart.getKeyRange(CompareOp.EQUAL, key);
                if (range == null) {
                    return null;
                }
                if (range != KeyRange.EMPTY_RANGE) { // null means it can't possibly be in range
                    ranges.add(range);
                }
            }
            return newKeyParts(childSlot, node, new ArrayList<KeyRange>(ranges));
        }

        @Override
        public Iterator<Expression> visitEnter(IsNullExpression node) {
            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeySlots visitLeave(IsNullExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            KeySlots childSlots = childParts.get(0);
            KeySlot childSlot = childSlots.getSlots().get(0);
            PColumn column = childSlot.getKeyPart().getColumn();
            PDataType type = column.getDataType();
            boolean isFixedWidth = type.isFixedWidth();
            // Nothing changes for IS NULL and IS NOT NULL when DESC since
            // we represent NULL the same way for ASC and DESC
            if (isFixedWidth) { // if column can't be null
                return node.isNegate() ? null : 
                    newKeyParts(childSlot, node, type.getKeyRange(new byte[SchemaUtil.getFixedByteSize(column)], true,
                                                                  KeyRange.UNBOUND, true));
            } else {
                KeyRange keyRange = node.isNegate() ? KeyRange.IS_NOT_NULL_RANGE : KeyRange.IS_NULL_RANGE;
                return newKeyParts(childSlot, node, keyRange);
            }
        }

        /**
         * 
         * Top level data structure used to drive the formation
         * of the start/stop row of scans, essentially taking the
         * expression tree of a WHERE clause and producing the
         * ScanRanges instance during query compilation.
         *
         */
        public static interface KeySlots {
            
            /**
             * List of slots that store binding of constant values
             * for primary key columns. For example:
             * WHERE pk1 = 'foo' and pk2 = 'bar'
             * would produce two KeySlot instances that store that
             * pk1 = 'foo' and pk2 = 'bar'. 
             * @return
             */
            public List<KeySlot> getSlots();
            /**
             * Tracks whether or not the contained KeySlot(s) contain
             * a slot that includes only a partial extraction of the
             * involved expressions. For example: (A AND B) in the case
             * of A being a PK column and B being a KV column, the
             * KeySlots representing the AND would return true for
             * isPartialExtraction.
             * @return true if a partial expression extraction was
             * done and false otherwise.
             */
            public boolean isPartialExtraction();
        }
        
        /**
         * 
         * Used during query compilation to represent the constant value of a
         * primary key column based on expressions in the WHERE clause. These
         * are combined together during the compilation of ANDs and ORs to
         * to produce the start and stop scan range.
         *
         */
        static final class KeySlot {
            private final int pkPosition; // Position in primary key
            private final int pkSpan; // Will be > 1 for RVC
            private final KeyPart keyPart; // Used to produce the KeyRanges below
            // Multiple ranges means values that have been ORed together
            private final List<KeyRange> keyRanges;
            // If order rows returned from scan will match desired order declared in query
            private final OrderPreserving orderPreserving; 
            
            KeySlot(KeyPart keyPart, int pkPosition, int pkSpan, List<KeyRange> keyRanges, OrderPreserving orderPreserving) {
                this.pkPosition = pkPosition;
                this.pkSpan = pkSpan;
                this.keyPart = keyPart;
                this.keyRanges = keyRanges;
                this.orderPreserving = orderPreserving;
            }

            public KeyPart getKeyPart() {
                return keyPart;
            }

            public int getPKPosition() {
                return pkPosition;
            }

            public int getPKSpan() {
                return pkSpan;
            }

            public List<KeyRange> getKeyRanges() {
                return keyRanges;
            }

            public final KeySlot concatExtractNodes(Set<Expression> extractNodes) {
                return new KeySlot(
                        new BaseKeyPart(this.getKeyPart().getTable(), this.getKeyPart().getColumn(),
                                    SchemaUtil.concat(this.getKeyPart().getExtractNodes(),extractNodes)),
                        this.getPKPosition(),
                        this.getPKSpan(),
                        this.getKeyRanges(),
                        this.getOrderPreserving());
            }
            
            public OrderPreserving getOrderPreserving() {
                return orderPreserving;
            }
        }

        /**
         * 
         * Implementation of KeySlots for AND and OR expressions. The
         * {@code List<KeySlot> } will be in PK order.
         *
         */
        public static class MultiKeySlot implements KeySlots {
            private final List<KeySlot> childSlots;
            private final boolean partialExtraction;

            private MultiKeySlot(List<KeySlot> childSlots, boolean partialExtraction) {
                this.childSlots = childSlots;
                this.partialExtraction = partialExtraction;
            }

            @Override
            public List<KeySlot> getSlots() {
                return childSlots;
            }

            @Override
            public boolean isPartialExtraction() {
                return partialExtraction;
            }
        }

        /**
         * 
         * Implementation of KeySlots for a constant value, 
         *
         */
        public static class SingleKeySlot implements KeySlots {
            private final List<KeySlot> slots;
            
            SingleKeySlot(KeyPart part, int pkPosition, List<KeyRange> ranges) {
                this(part, pkPosition, 1, ranges);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, List<KeyRange> ranges, OrderPreserving orderPreserving) {
                this(part, pkPosition, 1, ranges, orderPreserving);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, int pkSpan, List<KeyRange> ranges) {
                this(part,pkPosition,pkSpan,ranges, null);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, int pkSpan, List<KeyRange> ranges, OrderPreserving orderPreserving) {
                this.slots = Collections.singletonList(new KeySlot(part, pkPosition, pkSpan, ranges, orderPreserving));
            }
            
            @Override
            public List<KeySlot> getSlots() {
                return slots;
            }

            @Override
            public boolean isPartialExtraction() {
                return this.slots.get(0).getKeyPart().getExtractNodes().isEmpty();
            }
            
        }
        
        public static class BaseKeyPart implements KeyPart {
            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                rhs.evaluate(null, ptr);
                // If the column is fixed width, fill is up to it's byte size
                PDataType type = getColumn().getDataType();
                if (type.isFixedWidth()) {
                    Integer length = getColumn().getMaxLength();
                    if (length != null) {
                        // Go through type to pad as the fill character depends on the type.
                        type.pad(ptr, length, SortOrder.ASC);
                    }
                }
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                KeyRange range = ByteUtil.getKeyRange(key, rhs.getSortOrder().transform(op)/*op*/, type);
                // Constants will have been inverted, so we invert them back here so that
                // RVC comparisons work correctly (see PHOENIX-3383).
                if (rhs.getSortOrder() == SortOrder.DESC) {
                    range = range.invert();
                }
                return range;
            }

            private final PTable table;
            private final PColumn column;
            private final Set<Expression> nodes;

            private BaseKeyPart(PTable table, PColumn column, Set<Expression> nodes) {
                this.table = table;
                this.column = column;
                this.nodes = nodes;
            }

            @Override
            public Set<Expression> getExtractNodes() {
                return nodes;
            }

            @Override
            public PColumn getColumn() {
                return column;
            }

            @Override
            public PTable getTable() {
                return table;
            }
        }

        private static class CoerceKeySlot implements KeyPart {

            private final KeyPart childPart;
            private final ImmutableBytesWritable ptr;
            private final CoerceExpression node;
            private final Set<Expression> extractNodes;

            public CoerceKeySlot(KeyPart childPart, ImmutableBytesWritable ptr,
                                 CoerceExpression node, Set<Expression> extractNodes) {
                this.childPart = childPart;
                this.ptr = ptr;
                this.node = node;
                this.extractNodes = extractNodes;
            }

            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                KeyRange range = childPart.getKeyRange(op, rhs);
                byte[] lower = range.getLowerRange();
                if (!range.lowerUnbound()) {
                    ptr.set(lower);
                    /***
                    Do the reverse translation so we can optimize out the coerce expression
                    For the actual type of the coerceBytes call, we use the node type instead of
                     the rhs type, because for IN, the rhs type will be VARBINARY and no coerce
                     will be done in that case (and we need it to be done).
                     */
                    node.getChild().getDataType().coerceBytes(ptr, node.getDataType(),
                            rhs.getSortOrder(), SortOrder.ASC);
                    lower = ByteUtil.copyKeyBytesIfNecessary(ptr);
                }
                byte[] upper = range.getUpperRange();
                if (!range.upperUnbound()) {
                    ptr.set(upper);
                    // Do the reverse translation so we can optimize out the coerce expression
                    node.getChild().getDataType().coerceBytes(ptr, node.getDataType(),
                            rhs.getSortOrder(), SortOrder.ASC);
                    upper = ByteUtil.copyKeyBytesIfNecessary(ptr);
                }
                range = KeyRange.getKeyRange(lower, range.isLowerInclusive(), upper,
                        range.isUpperInclusive());
                return range;
            }

            @Override
            public Set<Expression> getExtractNodes() {
                return extractNodes;
            }

            @Override
            public PColumn getColumn() {
                return childPart.getColumn();
            }

            @Override
            public PTable getTable() {
                return childPart.getTable();
            }
        }

        private class RowValueConstructorKeyPart implements KeyPart {
            private final RowValueConstructorExpression rvc;
            private final PColumn column;
            private final Set<Expression> nodes;
            private final List<KeySlots> childSlots;

            private RowValueConstructorKeyPart(PColumn column, RowValueConstructorExpression rvc, int span, List<KeySlots> childSlots) {
                this.column = column;
                if (span == rvc.getChildren().size()) {
                    this.rvc = rvc;
                    this.nodes = new LinkedHashSet<>(Collections.singletonList(rvc));
                    this.childSlots = childSlots;
                } else {
                    this.rvc = new RowValueConstructorExpression(rvc.getChildren().subList(0, span),rvc.isStateless());
                    this.nodes = new LinkedHashSet<>();
                    this.childSlots = childSlots.subList(0,  span);
                }
            }

            @Override
            public Set<Expression> getExtractNodes() {
                return nodes;
            }

            @Override
            public PColumn getColumn() {
                return column;
            }

            @Override
            public PTable getTable() {
                return table;
            }

            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                // With row value constructors, we need to convert the operator for any transformation we do on individual values
                // to prevent keys from being increased to the next key as would be done for fixed width values. The next key is
                // done to compensate for the start key (lower range) always being inclusive (thus we convert > to >=) and the
                // end key (upper range) always being exclusive (thus we convert <= to <).
                boolean usedAllOfLHS = !nodes.isEmpty();
                final CompareOp rvcElementOp = op == CompareOp.LESS_OR_EQUAL ? CompareOp.LESS : op == CompareOp.GREATER ? CompareOp.GREATER_OR_EQUAL : op;
                if (op != CompareOp.EQUAL) {
                    // We need to transform the comparison operator for a LHS row value constructor
                    // that is shorter than a RHS row value constructor when we're extracting it.
                    // For example: a < (1,2) is true if a = 1, so we need to switch
                    // the compare op to <= like this: a <= 1. Since we strip trailing nulls
                    // in the rvc, we don't need to worry about the a < (1,null) case.
                    if (usedAllOfLHS) {
                        if (rvc.getChildren().size() < rhs.getChildren().size()) {
                            if (op == CompareOp.LESS) {
                                op = CompareOp.LESS_OR_EQUAL;
                            } else if (op == CompareOp.GREATER_OR_EQUAL) {
                                op = CompareOp.GREATER;
                            }
                        }
                    } else {
                        // If we're not using all of the LHS, we need to expand the range on either
                        // side to take into account the rest of the LHS. For example:
                        // WHERE (pk1, pk3) > ('a',1) AND pk1 = 'a'. In this case, we'll end up
                        // only using (pk1) and ('a'), so if we use a > operator the expression
                        // would end up as degenerate since we'd have a non inclusive range for
                        // ('a'). By switching the operator to extend the range, we end up with
                        // an ('a') inclusive range which is correct.
                        if (rvc.getChildren().size() < rhs.getChildren().size()) {
                            if (op == CompareOp.LESS) {
                                op = CompareOp.LESS_OR_EQUAL;
                            } else if (op == CompareOp.GREATER) {
                                op = CompareOp.GREATER_OR_EQUAL;
                            }
                        }
                    }
                }
                if (!usedAllOfLHS || rvc.getChildren().size() != rhs.getChildren().size()) {
                    // We know that rhs was converted to a row value constructor and that it's a constant
                    rhs= new RowValueConstructorExpression(rhs.getChildren().subList(0, Math.min(rvc.getChildren().size(), rhs.getChildren().size())), rhs.isStateless());
                }
                /*
                 * Recursively transform the RHS row value constructor by applying the same logic as
                 * is done elsewhere during WHERE optimization: optimizing out LHS functions by applying
                 * the appropriate transformation to the RHS key.
                 */
                // Child slot iterator parallel with child expressions of the LHS row value constructor 
                final Iterator<KeySlots> keySlotsIterator = childSlots.iterator();
                try {
                    // Call our static row value expression constructor with the current LHS row value constructor and
                    // the current RHS (which has already been coerced to match the LHS expression). We pass through an
                    // implementation of ExpressionComparabilityWrapper that transforms the RHS key to match the row key
                    // structure of the LHS column. This is essentially optimizing out the expressions on the LHS by
                    // applying the appropriate transformations to the RHS (through the KeyPart#getKeyRange method).
                    // For example, with WHERE (invert(a),b) < ('abc',5), the 'abc' would be inverted by going through the
                    // childPart.getKeyRange defined for the invert function.
                    rhs = BaseExpression.coerce(rvc, rhs, new ExpressionComparabilityWrapper() {

                        @Override
                        public Expression wrap(final Expression lhs, final Expression rhs, boolean rowKeyOrderOptimizable) throws SQLException {
                            final KeyPart childPart = keySlotsIterator.next().getSlots().get(0).getKeyPart();
                            // TODO: DelegateExpression
                            return new BaseTerminalExpressionWrap(childPart, rhs, rvcElementOp,
                                    lhs);
                        }
                        
                    }, table.rowKeyOrderOptimizable());
                } catch (SQLException e) {
                    return null; // Shouldn't happen
                }
                ImmutableBytesWritable ptr = context.getTempPtr();
                if (!rhs.evaluate(null, ptr)) { // Don't return if evaluated to null
                    return null; 
                }
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                KeyRange range = ByteUtil.getKeyRange(key, /*rvc.getChildren().get(rhs.getChildren().size()-1).getSortOrder().transform(op)*/op, PVarbinary.INSTANCE);
                return range;
            }

            private class BaseTerminalExpressionWrap extends BaseTerminalExpression {
                private final KeyPart childPart;
                private final Expression rhs;
                private final CompareOp rvcElementOp;
                private final Expression lhs;

                public BaseTerminalExpressionWrap(KeyPart childPart, Expression rhs,
                                                  CompareOp rvcElementOp, Expression lhs) {
                    this.childPart = childPart;
                    this.rhs = rhs;
                    this.rvcElementOp = rvcElementOp;
                    this.lhs = lhs;
                }

                @Override
                public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
                    if (childPart == null) {
                        return rhs.evaluate(tuple, ptr);
                    }
                    if (!rhs.evaluate(tuple, ptr)) {
                        return false;
                    }
                    if (ptr.getLength() == 0) {
                        ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                        return true;
                    }
                    // The op used to compute rvcElementOp did not take into account the sort order,
                    // and thus we need to transform it here before delegating to the child part
                    // which will do the required inversion.
                    KeyRange range = childPart.getKeyRange(
                            rhs.getSortOrder().transform(rvcElementOp), rhs);
                    // Swap the upper and lower range if descending to compensate for the transform
                    // we did above of the rvcElementOp.
                    if (rhs.getSortOrder() == SortOrder.DESC) {
                        range = KeyRange.getKeyRange(range.getUpperRange(),
                                range.isUpperInclusive(), range.getLowerRange(),
                                range.isLowerInclusive());
                    }
                    // This can happen when an EQUAL operator is used and the expression cannot
                    // possibly match.
                    if (range == KeyRange.EMPTY_RANGE) {
                        return false;
                    }
                    /**
                     We have to take the range and condense it down to a single key. We use which
                     ever part of the range is inclusive (which implies being bound as well). This
                     works in all cases, including this substring one, which produces a lower
                     inclusive range and an upper non inclusive range.
                     (a, substr(b,1,1)) IN (('a','b'), ('c','d'))
                     */
                    byte[] key = range.isLowerInclusive() ?
                            range.getLowerRange() : range.getUpperRange();
                    /**
                     FIXME:
                     this is kind of a hack. The above call will fill a fixed width key,but
                    we don't want to fill the key yet because it can throw off our the logic we
                    use to compute the next key when we evaluate the RHS row value constructor
                    below.  We could create a new childPart with a delegate column that returns
                    null for getByteSize().
                     */
                    if (lhs.getDataType().isFixedWidth() &&
                            lhs.getMaxLength() != null && key.length > lhs.getMaxLength()) {
                        // Don't use PDataType.pad(), as this only grows the value,
                        // while this is shrinking it.
                        key = Arrays.copyOf(key, lhs.getMaxLength());
                    }
                    ptr.set(key);
                    return true;
                }

                @Override
                public PDataType getDataType() {
                    return childPart.getColumn().getDataType();
                }

                @Override
                public boolean isNullable() {
                    return childPart.getColumn().isNullable();
                }

                @Override
                public Integer getMaxLength() {
                    return lhs.getMaxLength();
                }

                @Override
                public Integer getScale() {
                    return childPart.getColumn().getScale();
                }

                @Override
                public SortOrder getSortOrder() {
                    //See PHOENIX-4969: Clean up and unify code paths for RVCs with
                    //  respect to Optimizations for SortOrder
                    //Handle the different paths for InList vs Normal Comparison
                    //The code paths in InList assume the sortOrder is ASC for
                    // their optimizations
                    //The code paths for Comparisons on RVC rewrite equality,
                    // for the non-equality cases return actual sort order
                    //This work around should work
                    // but a more general approach can be taken.
                    //This optimization causes PHOENIX-6662 (when desc pk used with in clause)
//                    if(rvcElementOp == CompareOp.EQUAL ||
//                            rvcElementOp == CompareOp.NOT_EQUAL){
//                        return SortOrder.ASC;
//                    }
                    return childPart.getColumn().getSortOrder();
                }

                @Override
                public <T> T accept(ExpressionVisitor<T> visitor) {
                    return null;
                }
            }
        }
    }
}
