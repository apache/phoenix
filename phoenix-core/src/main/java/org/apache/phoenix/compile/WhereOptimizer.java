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

import static java.util.Collections.singletonList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


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
     * @param statement the statement being compiled
     * @param whereClause the where clause expression
     * @return the new where clause with the key expressions removed
     */
    public static Expression pushKeyExpressionsToScan(StatementContext context, FilterableStatement statement, Expression whereClause) {
        return pushKeyExpressionsToScan(context, statement, whereClause, null);
    }

    // For testing so that the extractedNodes can be verified
    public static Expression pushKeyExpressionsToScan(StatementContext context, FilterableStatement statement,
            Expression whereClause, Set<Expression> extractNodes) {
        PName tenantId = context.getConnection().getTenantId();
        PTable table = context.getCurrentTable().getTable();
    	Integer nBuckets = table.getBucketNum();
    	boolean isSalted = nBuckets != null;
    	RowKeySchema schema = table.getRowKeySchema();
    	boolean isMultiTenant = tenantId != null && table.isMultiTenant();
    	if (isMultiTenant) {
    		tenantId = ScanUtil.padTenantIdIfNecessary(schema, isSalted, tenantId);
    	}

        if (whereClause == null && (tenantId == null || !table.isMultiTenant()) && table.getViewIndexId() == null) {
            context.setScanRanges(ScanRanges.EVERYTHING);
            return whereClause;
        }
        if (LiteralExpression.isFalse(whereClause)) {
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
    
            if (keySlots == null && (tenantId == null || !table.isMultiTenant()) && table.getViewIndexId() == null) {
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
        int[] slotSpan = new int[nPKColumns];
        List<Expression> removeFromExtractNodes = null;
        List<List<KeyRange>> cnf = Lists.newArrayListWithExpectedSize(schema.getMaxFields());
        KeyRange minMaxRange = keySlots.getMinMaxRange();
        if (minMaxRange == null) {
            minMaxRange = KeyRange.EVERYTHING_RANGE;
        }
        boolean hasMinMaxRange = (minMaxRange != KeyRange.EVERYTHING_RANGE);
        int minMaxRangeOffset = 0;
        byte[] minMaxRangePrefix = null;
        boolean hasViewIndex = table.getViewIndexId() != null;
        if (hasMinMaxRange) {
            int minMaxRangeSize = (isSalted ? SaltingUtil.NUM_SALTING_BYTES : 0)
                    + (isMultiTenant ? tenantId.getBytes().length + 1 : 0) 
                    + (hasViewIndex ? MetaDataUtil.getViewIndexIdDataType().getByteSize() : 0);
            minMaxRangePrefix = new byte[minMaxRangeSize];
        }
        
        Iterator<KeyExpressionVisitor.KeySlot> iterator = keySlots.iterator();
        // Add placeholder for salt byte ranges
        if (isSalted) {
            cnf.add(SALT_PLACEHOLDER);
            if (hasMinMaxRange) {
	            System.arraycopy(SALT_PLACEHOLDER.get(0).getLowerRange(), 0, minMaxRangePrefix, minMaxRangeOffset, SaltingUtil.NUM_SALTING_BYTES);
	            minMaxRangeOffset += SaltingUtil.NUM_SALTING_BYTES;
            }
            // Increment the pkPos, as the salt column is in the row schema
            // Do not increment the iterator, though, as there will never be
            // an expression in the keySlots for the salt column
            pkPos++;
        }
        
        // Add tenant data isolation for tenant-specific tables
        if (isMultiTenant) {
            byte[] tenantIdBytes = tenantId.getBytes();
            KeyRange tenantIdKeyRange = KeyRange.getKeyRange(tenantIdBytes);
            cnf.add(singletonList(tenantIdKeyRange));
            if (hasMinMaxRange) {
                System.arraycopy(tenantIdBytes, 0, minMaxRangePrefix, minMaxRangeOffset, tenantIdBytes.length);
                minMaxRangeOffset += tenantIdBytes.length;
                if (!schema.getField(pkPos).getDataType().isFixedWidth()) {
                    minMaxRangePrefix[minMaxRangeOffset] = QueryConstants.SEPARATOR_BYTE;
                    minMaxRangeOffset++;
                }
            }
            pkPos++;
        }
        // Add unique index ID for shared indexes on views. This ensures
        // that different indexes don't interleave.
        if (hasViewIndex) {
            byte[] viewIndexBytes = MetaDataUtil.getViewIndexIdDataType().toBytes(table.getViewIndexId());
            KeyRange indexIdKeyRange = KeyRange.getKeyRange(viewIndexBytes);
            cnf.add(singletonList(indexIdKeyRange));
            if (hasMinMaxRange) {
                System.arraycopy(viewIndexBytes, 0, minMaxRangePrefix, minMaxRangeOffset, viewIndexBytes.length);
                minMaxRangeOffset += viewIndexBytes.length;
            }
            pkPos++;
        }
        
        // Prepend minMaxRange with fixed column values so we can properly intersect the
        // range with the other range.
        if (hasMinMaxRange) {
            minMaxRange = minMaxRange.prependRange(minMaxRangePrefix, 0, minMaxRangeOffset);
        }
        boolean forcedSkipScan = statement.getHint().hasHint(Hint.SKIP_SCAN);
        boolean forcedRangeScan = statement.getHint().hasHint(Hint.RANGE_SCAN);
        boolean hasUnboundedRange = false;
        boolean hasMultiRanges = false;
        boolean hasMultiColumnSpan = false;
        boolean hasNonPointKey = false;
        boolean stopExtracting = false;
        // Concat byte arrays of literals to form scan start key
        while (iterator.hasNext()) {
            KeyExpressionVisitor.KeySlot slot = iterator.next();
            // If the position of the pk columns in the query skips any part of the row k
            // then we have to handle in the next phase through a key filter.
            // If the slot is null this means we have no entry for this pk position.
            if (slot == null || slot.getKeyRanges().isEmpty())  {
                if (!forcedSkipScan || hasMultiColumnSpan) break;
                continue;
            }
            if (slot.getPKPosition() != pkPos) {
                if (!forcedSkipScan || hasMultiColumnSpan) break;
                for (int i=pkPos; i < slot.getPKPosition(); i++) {
                    cnf.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));
                }
            }
            KeyPart keyPart = slot.getKeyPart();
            slotSpan[cnf.size()] = slot.getPKSpan() - 1;
            pkPos = slot.getPKPosition() + slot.getPKSpan();
            hasMultiColumnSpan |= slot.getPKSpan() > 1;
            // Skip span-1 slots as we skip one at the top of the loop
            for (int i = 1; i < slot.getPKSpan() && iterator.hasNext(); i++) {
                iterator.next();
            }
            List<KeyRange> keyRanges = slot.getKeyRanges();
            for (int i = 0; (!hasUnboundedRange || !hasNonPointKey) && i < keyRanges.size(); i++) {
                KeyRange range  = keyRanges.get(i);
                if (range.isUnbound()) {
                    hasUnboundedRange = hasNonPointKey = true;
                } else if (!range.isSingleKey()) {
                    hasNonPointKey = true;
                }
            }
            hasMultiRanges |= keyRanges.size() > 1;
            // Force a range scan if we've encountered a multi-span slot (i.e. RVC)
            // and a non point key, as our skip scan only handles fully qualified
            // RVC in our skip scan. This will force us to not extract nodes any
            // longer as well.
            // TODO: consider ending loop here if true.
            forcedRangeScan |= (hasMultiColumnSpan && hasNonPointKey);
            cnf.add(keyRanges);
            
            // We cannot extract if we have multiple ranges and are forcing a range scan.
            stopExtracting |= forcedRangeScan && hasMultiRanges;
            
            // Will be null in cases for which only part of the expression was factored out here
            // to set the start/end key. An example would be <column> LIKE 'foo%bar' where we can
            // set the start key to 'foo' but still need to match the regex at filter time.
            // Don't extract expressions if we're forcing a range scan and we've already come
            // across a multi-range for a prior slot. The reason is that we have an inexact range after
            // that, so must filter on the remaining conditions (see issue #467).
            if (!stopExtracting) {
                List<Expression> nodesToExtract = keyPart.getExtractNodes();
                // Detect case of a RVC used in a range. We do not want to
                // remove these from the extract nodes.
                if (hasMultiColumnSpan && !hasUnboundedRange) {
                    if (removeFromExtractNodes == null) {
                        removeFromExtractNodes = Lists.newArrayListWithExpectedSize(nodesToExtract.size() + table.getPKColumns().size() - pkPos);
                    }
                    removeFromExtractNodes.addAll(nodesToExtract);
                }
                extractNodes.addAll(nodesToExtract);
            }
            /*
             *  Stop building start/stop key once we encounter An unbound range unless we're
             *  forcing a skip scan and havn't encountered a multi-column span. Even if we're
             *  trying to force a skip scan, we can't execute it over a multi-column span.
             */
            if (hasUnboundedRange && (!forcedSkipScan || hasMultiColumnSpan)) {
                // TODO: when stats are available, we may want to continue this loop if the
                // cardinality of this slot is low. We could potentially even continue this
                // loop in the absence of a range for a key slot.
                break;
            }
            // Set stopExtracting if we're forcing a range scan and have anything other than
            // an equality constraint. We can extract the first one, but no future ones.
            stopExtracting |= forcedRangeScan && hasNonPointKey;
        }
        // If we have fully qualified point keys with multi-column spans (i.e. RVC),
        // we can still use our skip scan. The ScanRanges.create() call will explode
        // out the keys.
        slotSpan = Arrays.copyOf(slotSpan, cnf.size());
        ScanRanges scanRanges = ScanRanges.create(schema, cnf, slotSpan, minMaxRange, forcedRangeScan, nBuckets);
        context.setScanRanges(scanRanges);
        if (whereClause == null) {
            return null;
        } else {
            return whereClause.accept(new RemoveExtractedNodesVisitor(extractNodes));
        }
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
        for (int i = 0; i < expressions.size(); i++) {
            KeyExpressionVisitor visitor = new KeyExpressionVisitor(context, context.getCurrentTable().getTable());
            KeyExpressionVisitor.KeySlots keySlots = expressions.get(i).accept(visitor);
            int minPkPos = Integer.MAX_VALUE; 
            if (keySlots != null) {
                Iterator<KeyExpressionVisitor.KeySlot> iterator = keySlots.iterator();
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
        int maxPkSpan = 0;
        Expression remaining = null;
        while (count < candidates.size()) {
            Expression lhs = count == 0 ? candidates.get(0) : new RowValueConstructorExpression(candidates.subList(0, count + 1), false);
            Expression firstRhs = count == 0 ? sampleValues.get(0).get(0) : new RowValueConstructorExpression(sampleValues.get(0).subList(0, count + 1), true);
            Expression secondRhs = count == 0 ? sampleValues.get(1).get(0) : new RowValueConstructorExpression(sampleValues.get(1).subList(0, count + 1), true);
            Expression testExpression = InListExpression.create(Lists.newArrayList(lhs, firstRhs, secondRhs), false, context.getTempPtr());
            remaining = pushKeyExpressionsToScan(context, statement, testExpression);
            if (context.getScanRanges().isPointLookup()) {
                count++;
                break; // found the best match
            }
            int pkSpan = context.getScanRanges().getPkColumnSpan();
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
        public Expression visitLeave(AndExpression node, List<Expression> l) {
            if (!l.equals(node.getChildren())) {
                if (l.isEmpty()) {
                    // Don't return null here, because then our defaultReturn will kick in
                    return LiteralExpression.newConstant(true, Determinism.ALWAYS);
                }
                if (l.size() == 1) {
                    return l.get(0);
                }
                return new AndExpression(l);
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
            public Iterator<KeySlot> iterator() {
                return Iterators.emptyIterator();
            }

            @Override
            public KeyRange getMinMaxRange() {
                return null;
            }
        };

        private static boolean isDegenerate(List<KeyRange> keyRanges) {
            return keyRanges == null || keyRanges.isEmpty() || (keyRanges.size() == 1 && keyRanges.get(0) == KeyRange.EMPTY_RANGE);
        }
        
        private KeySlots newKeyParts(KeySlot slot, Expression extractNode, KeyRange keyRange) {
            if (keyRange == null) {
                return EMPTY_KEY_SLOTS;
            }
            
            List<KeyRange> keyRanges = slot.getPKSpan() == 1 ? Collections.<KeyRange>singletonList(keyRange) : EVERYTHING_RANGES;
            KeyRange minMaxRange = null;
            if (slot.getPKSpan() > 1) {
                int initPosition = (table.getBucketNum() ==null ? 0 : 1) + (this.context.getConnection().getTenantId() != null && table.isMultiTenant() ? 1 : 0) + (table.getViewIndexId() == null ? 0 : 1);
                if (initPosition == slot.getPKPosition()) {
                    minMaxRange = keyRange;
                }
            }
            return newKeyParts(slot, extractNode, keyRanges, minMaxRange);
        }

        private KeySlots newKeyParts(KeySlot slot, Expression extractNode, List<KeyRange> keyRanges, KeyRange minMaxRange) {
            if (isDegenerate(keyRanges)) {
                return EMPTY_KEY_SLOTS;
            }
            
            List<Expression> extractNodes = extractNode == null || slot.getKeyPart().getExtractNodes().isEmpty()
                  ? Collections.<Expression>emptyList()
                  : Collections.<Expression>singletonList(extractNode);
            return new SingleKeySlot(new BaseKeyPart(slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), slot.getPKSpan(), keyRanges, minMaxRange, slot.getOrderPreserving());
        }

        private KeySlots newKeyParts(KeySlot slot, List<Expression> extractNodes, List<KeyRange> keyRanges, KeyRange minMaxRange) {
            if (isDegenerate(keyRanges)) {
                return EMPTY_KEY_SLOTS;
            }
            
            return new SingleKeySlot(new BaseKeyPart(slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), slot.getPKSpan(), keyRanges, minMaxRange, slot.getOrderPreserving());
        }

        private KeySlots newRowValueConstructorKeyParts(RowValueConstructorExpression rvc, List<KeySlots> childSlots) {
            if (childSlots.isEmpty() || rvc.isStateless()) {
                return null;
            }
            
            int position = -1;
            int initialPosition = -1;
            for (int i = 0; i < childSlots.size(); i++) {
                KeySlots slots = childSlots.get(i);
                KeySlot keySlot = slots.iterator().next();
                List<Expression> childExtractNodes = keySlot.getKeyPart().getExtractNodes();
                // Stop if there was a gap in extraction of RVC elements. This is required if the leading
                // RVC has not row key columns, as we'll still get childSlots if the RVC has trailing row
                // key columns. We can't rule the RVC out completely when the childSlots is less the the
                // RVC length, as a partial, *leading* match is optimizable.
                if (childExtractNodes.size() != 1 || !childExtractNodes.get(0).equals(rvc.getChildren().get(i))) {
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
            // If we don't clear the child list, we end up passing some of
            // the child expressions of previous matches up the tree, causing
            // those expressions to form the scan start/stop key. PHOENIX-1753
            childSlots.clear();
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
            final List<Expression> extractNodes = Collections.<Expression>singletonList(node);
            final KeyPart childPart = slot.getKeyPart();
            final ImmutableBytesWritable ptr = context.getTempPtr();
            return new SingleKeySlot(new KeyPart() {

                @Override
                public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                    KeyRange range = childPart.getKeyRange(op, rhs);
                    byte[] lower = range.getLowerRange();
                    if (!range.lowerUnbound()) {
                        ptr.set(lower);
                        // Do the reverse translation so we can optimize out the coerce expression
                        // For the actual type of the coerceBytes call, we use the node type instead of the rhs type, because
                        // for IN, the rhs type will be VARBINARY and no coerce will be done in that case (and we need it to
                        // be done).
                        node.getChild().getDataType().coerceBytes(ptr, node.getDataType(), rhs.getSortOrder(), node.getChild().getSortOrder());
                        lower = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    }
                    byte[] upper = range.getUpperRange();
                    if (!range.upperUnbound()) {
                        ptr.set(upper);
                        // Do the reverse translation so we can optimize out the coerce expression
                        node.getChild().getDataType().coerceBytes(ptr, node.getDataType(), rhs.getSortOrder(), node.getChild().getSortOrder());
                        upper = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    }
                    return KeyRange.getKeyRange(lower, range.isLowerInclusive(), upper, range.isUpperInclusive());
                }

                @Override
                public List<Expression> getExtractNodes() {
                    return extractNodes;
                }

                @Override
                public PColumn getColumn() {
                    return childPart.getColumn();
                }
            }, slot.getPKPosition(), slot.getKeyRanges());
        }

        private static boolean intersectSlots(KeySlot[] slotArray, KeySlot childSlot) {
            int childPosition = childSlot.getPKPosition();
            int childSpan = childSlot.getPKSpan();
            boolean filled = false;
            for (KeySlot slot : slotArray) {
                if (slot != null) {
                    int position = slot.getPKPosition();
                    int span = slot.getPKSpan();
                    if (childPosition + childSpan > position && childPosition < position + span) {
                        if (childPosition < position || (childPosition == position && childSpan > span)) {
                            slotArray[childPosition] = childSlot = childSlot.intersect(slot);
                            if (childSlot == null) {
                                return false;
                            }
                            filled = true;
                        } else {
                            slotArray[position] = slot = slot.intersect(childSlot);
                            if (slot == null) {
                                return false;
                            }
                            filled = true;
                        }
                    }
                }
            }
            if (!filled) {
                slotArray[childPosition] = childSlot;
            }
            return true;
        }
        
        private KeySlots andKeySlots(AndExpression andExpression, List<KeySlots> childSlots) {
            int nColumns = table.getPKColumns().size();
            KeySlot[] keySlot = new KeySlot[nColumns];
            KeyRange minMaxRange = KeyRange.EVERYTHING_RANGE;
            List<Expression> minMaxExtractNodes = Lists.<Expression>newArrayList();
            int initPosition = (table.getBucketNum() ==null ? 0 : 1) + (this.context.getConnection().getTenantId() != null && table.isMultiTenant() ? 1 : 0) + (table.getViewIndexId() == null ? 0 : 1);
            for (KeySlots childSlot : childSlots) {
                if (childSlot == EMPTY_KEY_SLOTS) {
                    return EMPTY_KEY_SLOTS;
                }
                // FIXME: get rid of this min/max range BS now that a key range can span multiple columns
                if (childSlot.getMinMaxRange() != null) { // Only set if in initial pk position
                    // TODO: potentially use KeySlot.intersect here. However, we can't intersect the key ranges in the slot
                    // with our minMaxRange, since it spans columns and this would mess up our skip scan.
                    minMaxRange = minMaxRange.intersect(childSlot.getMinMaxRange());
                    for (KeySlot slot : childSlot) {
                        if (slot != null) {
                    	    minMaxExtractNodes.addAll(slot.getKeyPart().getExtractNodes());
                        }
                    }
                } else {
                    for (KeySlot slot : childSlot) {
                        // We have a nested AND with nothing for this slot, so continue
                        if (slot == null) {
                            continue;
                        }
                        if (!intersectSlots(keySlot, slot)) {
                            return EMPTY_KEY_SLOTS;
                        }
                    }
                }
            }

            if (!minMaxExtractNodes.isEmpty()) {
                if (keySlot[initPosition] == null) {
                    keySlot[initPosition] = new KeySlot(new BaseKeyPart(table.getPKColumns().get(initPosition), minMaxExtractNodes), initPosition, 1, EVERYTHING_RANGES, null);
                } else {
                    keySlot[initPosition] = keySlot[initPosition].concatExtractNodes(minMaxExtractNodes);
                }
            }
            List<KeySlot> keySlots = Arrays.asList(keySlot);
            // If we have a salt column, skip that slot because
            // they'll never be an expression contained by it.
            keySlots = keySlots.subList(initPosition, keySlots.size());
            return new MultiKeySlot(keySlots, minMaxRange == KeyRange.EVERYTHING_RANGE ? null : minMaxRange);
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
            List<Expression> slotExtractNodes = Lists.<Expression>newArrayList();
            int thePosition = -1;
            boolean extractAll = true;
            // TODO: Have separate list for single span versus multi span
            // For multi-span, we only need to keep a single range.
            List<KeyRange> slotRanges = Lists.newArrayList();
            KeyRange minMaxRange = KeyRange.EMPTY_RANGE;
            for (KeySlots childSlot : childSlots) {
                if (childSlot == EMPTY_KEY_SLOTS) {
                    // TODO: can this ever happen and can we safely filter the expression tree?
                    continue;
                }
                if (childSlot.getMinMaxRange() != null) {
                    if (!slotRanges.isEmpty() && thePosition != initialPos) { // ORing together rvc in initial slot with other slots
                        return null;
                    }
                    minMaxRange = minMaxRange.union(childSlot.getMinMaxRange());
                    thePosition = initialPos;
                    for (KeySlot slot : childSlot) {
                    	if (slot != null) {
                    		List<Expression> extractNodes = slot.getKeyPart().getExtractNodes();
                    		extractAll &= !extractNodes.isEmpty();
                    		slotExtractNodes.addAll(extractNodes);
                    	}
                    }
                } else {
                    // TODO: Do the same optimization that we do for IN if the childSlots specify a fully qualified row key
                    for (KeySlot slot : childSlot) {
                        // We have a nested OR with nothing for this slot, so continue
                        if (slot == null) {
                            return null; //If one childSlot does not have the PK columns, let Phoenix scan all the key ranges of the table. 
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
                        List<Expression> extractNodes = slot.getKeyPart().getExtractNodes();
                        extractAll &= !extractNodes.isEmpty();
                        slotExtractNodes.addAll(extractNodes);
                        slotRanges.addAll(slot.getKeyRanges());
                    }
                }
            }

            if (thePosition == -1) {
                return null;
            }
            // With a mix of both, we can't use skip scan, so empty out the union
            // and only extract the min/max nodes.
            if (!slotRanges.isEmpty() && minMaxRange != KeyRange.EMPTY_RANGE) {
                boolean clearExtracts = false;
                // Union the minMaxRanges together with the slotRanges.
                for (KeyRange range : slotRanges) {
                    if (!clearExtracts) {
                        /*
                         * Detect when to clear the extract nodes by determining if there
                         * are gaps left by combining the ranges. If there are gaps, we
                         * cannot extract the nodes, but must them as filters instead.
                         */
                        KeyRange intersection = minMaxRange.intersect(range);
                        if (intersection == KeyRange.EMPTY_RANGE 
                                || !range.equals(intersection.union(range)) 
                                || !minMaxRange.equals(intersection.union(minMaxRange))) {
                            clearExtracts = true;
                        }
                    }
                    minMaxRange = minMaxRange.union(range);
                }
                if (clearExtracts) {
                    extractAll = false;
                    slotExtractNodes = Collections.emptyList();
                }
                slotRanges = Collections.emptyList();
            }
            if (theSlot == null) {
                theSlot = new KeySlot(new BaseKeyPart(table.getPKColumns().get(initialPos), slotExtractNodes), initialPos, 1, EVERYTHING_RANGES, null);
            } else if (minMaxRange != KeyRange.EMPTY_RANGE && !slotExtractNodes.isEmpty()) {
                theSlot = theSlot.concatExtractNodes(slotExtractNodes);
            }
            return newKeyParts(
                    theSlot, 
                    extractAll ? Collections.<Expression>singletonList(orExpression) : slotExtractNodes, 
                    slotRanges.isEmpty() ? EVERYTHING_RANGES : KeyRange.coalesce(slotRanges), 
                    minMaxRange == KeyRange.EMPTY_RANGE ? null : minMaxRange);
        }

        private final PTable table;
        private final StatementContext context;

        public KeyExpressionVisitor(StatementContext context, PTable table) {
            this.context = context;
            this.table = table;
        }

//        private boolean isFullyQualified(int pkSpan) {
//            int nPKColumns = table.getPKColumns().size();
//            return table.getBucketNum() == null ? pkSpan == nPKColumns : pkSpan == nPKColumns-1;
//        }
        @Override
        public KeySlots defaultReturn(Expression node, List<KeySlots> l) {
            // Passes the CompositeKeyExpression up the tree
            return l.size() == 1 ? l.get(0) : null;
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
            return newCoerceKeyPart(childParts.get(0).iterator().next(), node);
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
                // If we don't clear the child list, we end up passing some of
                // the child expressions of the OR up the tree, causing only
                // those expressions to form the scan start/stop key.
                l.clear();
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
            return new SingleKeySlot(new BaseKeyPart(column, Collections.<Expression>singletonList(node)), node.getPosition(), 1, EVERYTHING_RANGES);
        }

        @Override
        public Iterator<Expression> visitEnter(ComparisonExpression node) {
            Expression rhs = node.getChildren().get(1);
            if (!rhs.isStateless() || node.getFilterOp() == CompareOp.NOT_EQUAL) {
                return Iterators.emptyIterator();
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
            KeySlot childSlot = childSlots.iterator().next();
            KeyPart childPart = childSlot.getKeyPart();
            SortOrder sortOrder = childPart.getColumn().getSortOrder();
            CompareOp op = sortOrder.transform(node.getFilterOp());
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
                return Iterators.emptyIterator();
            }
            return Iterators.singletonIterator(node.getChildren().get(index));
        }

        @Override
        public KeySlots visitLeave(ScalarFunction node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            return newScalarFunctionKeyPart(childParts.get(0).iterator().next(), node);
        }

        @Override
        public Iterator<Expression> visitEnter(LikeExpression node) {
            // TODO: can we optimize something that starts with '_' like this: foo LIKE '_a%' ?
            if (node.getLikeType() == LikeType.CASE_INSENSITIVE || // TODO: remove this when we optimize ILIKE
                ! (node.getChildren().get(1) instanceof LiteralExpression) || node.startsWithWildcard()) {
                return Iterators.emptyIterator();
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
            KeySlot childSlot = childSlots.iterator().next();
            final String startsWith = node.getLiteralPrefix();
            byte[] key = PChar.INSTANCE.toBytes(startsWith, node.getChildren().get(0).getSortOrder());
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
            if (type.isFixedWidth() && columnFixedLength != null) {
                lowerRange = StringUtil.padChar(lowerRange, columnFixedLength);
                upperRange = StringUtil.padChar(upperRange, columnFixedLength);
            }
            KeyRange keyRange = type.getKeyRange(lowerRange, true, upperRange, false);
            // Only extract LIKE expression if pattern ends with a wildcard and everything else was extracted
            return newKeyParts(childSlot, node.endsWithOnlyWildcard() ? node : null, keyRange);
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
            KeySlot childSlot = childParts.get(0).iterator().next();
            KeyPart childPart = childSlot.getKeyPart();
            // Handles cases like WHERE substr(foo,1,3) IN ('aaa','bbb')
            for (Expression key : keyExpressions) {
                KeyRange range = childPart.getKeyRange(CompareOp.EQUAL, key);
                if (range != KeyRange.EMPTY_RANGE) { // null means it can't possibly be in range
                    ranges.add(range);
                }
            }
            return newKeyParts(childSlot, node, new ArrayList<KeyRange>(ranges), null);
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
            KeySlot childSlot = childSlots.iterator().next();
            PColumn column = childSlot.getKeyPart().getColumn();
            PDataType type = column.getDataType();
            boolean isFixedWidth = type.isFixedWidth();
            if (isFixedWidth) { // if column can't be null
                return node.isNegate() ? null : 
                    newKeyParts(childSlot, node, type.getKeyRange(new byte[SchemaUtil.getFixedByteSize(column)], true,
                                                                  KeyRange.UNBOUND, true));
            } else {
                KeyRange keyRange = node.isNegate() ? KeyRange.IS_NOT_NULL_RANGE : KeyRange.IS_NULL_RANGE;
                return newKeyParts(childSlot, node, keyRange);
            }
        }

        private static interface KeySlots extends Iterable<KeySlot> {
            @Override public Iterator<KeySlot> iterator();
            public KeyRange getMinMaxRange();
        }

        private final class KeySlot {
            private final int pkPosition;
            private final int pkSpan;
            private final KeyPart keyPart;
            private final List<KeyRange> keyRanges;
            private final OrderPreserving orderPreserving;

            private KeySlot(KeyPart keyPart, int pkPosition, int pkSpan, List<KeyRange> keyRanges) {
                this (keyPart, pkPosition, pkSpan, keyRanges, OrderPreserving.YES);
            }
            
            private KeySlot(KeyPart keyPart, int pkPosition, int pkSpan, List<KeyRange> keyRanges, OrderPreserving orderPreserving) {
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

            public final KeySlot concatExtractNodes(List<Expression> extractNodes) {
                return new KeySlot(
                        new BaseKeyPart(this.getKeyPart().getColumn(),
                                    SchemaUtil.concat(this.getKeyPart().getExtractNodes(),extractNodes)),
                        this.getPKPosition(),
                        this.getPKSpan(),
                        this.getKeyRanges(),
                        this.getOrderPreserving());
            }
            
            public final KeySlot intersect(KeySlot that) {
                if (this.getPKSpan() == 1 && that.getPKSpan() == 1) {
                    if (this.getPKPosition() != that.getPKPosition()) {
                        throw new IllegalArgumentException("Position must be equal for intersect");
                    }
                    List<KeyRange> keyRanges = KeyRange.intersect(this.getKeyRanges(), that.getKeyRanges());
                    if (isDegenerate(keyRanges)) {
                        return null;
                    }
                    return new KeySlot(
                            new BaseKeyPart(this.getKeyPart().getColumn(),
                                        SchemaUtil.concat(this.getKeyPart().getExtractNodes(),
                                                          that.getKeyPart().getExtractNodes())),
                            this.getPKPosition(),
                            this.getPKSpan(),
                            keyRanges,
                            this.getOrderPreserving());
                } else {
                    // Assumes that only single keys occur in RVCs (i.e. when a PK spans columns)
                    assert(this.getPKSpan() > 1);
                    assert(this.getPKPosition() <= that.getPKPosition());
                    ImmutableBytesWritable ptr = context.getTempPtr();
                    RowKeySchema schema = table.getRowKeySchema();
                    if (this.getPKSpan() > 1 && that.getPKSpan() > 1) {
                        // TODO: Trickiest case: both key slots are multi-span RVCs.
                        // Punt for now: we could intersect these, but it'd be a fair amount of code.
                        // Instead, just keep the original slot and don't extract
                        // the other expressions (so they'll be evaluated row by row).
                        return this;
                    } else {
                        assert(this.getPKSpan() > 1);
                        assert(this.getPKPosition() <= that.getPKPosition());
                        List<KeyRange> newKeyRanges = Lists.newArrayListWithExpectedSize(this.getKeyRanges().size());
                        // We know we have a set of RVCs (i.e. multi-span key ranges)
                        // Get the PK slot value in the RVC for the position of the other KeySlot
                        // If they don't intersect, we cannot have a match for the RVC, so filter it out.
                        // Otherwise, we keep it.
                        for (KeyRange keyRange : this.getKeyRanges()) {
                            assert(keyRange.isSingleKey());
                            byte[] key = keyRange.getLowerRange();
                            int position = this.getPKPosition();
                            int thatPosition = that.getPKPosition();
                            ptr.set(key);
                            if (schema.position(ptr, position, thatPosition)) {
                                // Create a range just for the overlapping column
                                List<KeyRange> slotKeyRanges = Collections.singletonList(KeyRange.getKeyRange(ByteUtil.copyKeyBytesIfNecessary(ptr)));
                                // Intersect with other ranges and add to list if it overlaps
                                if (!isDegenerate(KeyRange.intersect(slotKeyRanges, that.getKeyRanges()))) {
                                    newKeyRanges.add(keyRange);
                                }
                            }
                        }
                        if (isDegenerate(newKeyRanges)) {
                            return null;
                        }
                        return new KeySlot(
                                new BaseKeyPart(this.getKeyPart().getColumn(),
                                            SchemaUtil.concat(this.getKeyPart().getExtractNodes(),
                                                              that.getKeyPart().getExtractNodes())),
                                this.getPKPosition(),
                                this.getPKSpan(),
                                newKeyRanges,
                                this.getOrderPreserving());
                    }
                }
            }

            public OrderPreserving getOrderPreserving() {
                return orderPreserving;
            }
        }

        private static class MultiKeySlot implements KeySlots {
            private final List<KeySlot> childSlots;
            private final KeyRange minMaxRange;

            private MultiKeySlot(List<KeySlot> childSlots, KeyRange minMaxRange) {
                this.childSlots = childSlots;
                this.minMaxRange = minMaxRange;
            }

            @Override
            public Iterator<KeySlot> iterator() {
                return childSlots.iterator();
            }

            @Override
            public KeyRange getMinMaxRange() {
                return minMaxRange;
            }
        }

        private class SingleKeySlot implements KeySlots {
            private final KeySlot slot;
            private final KeyRange minMaxRange;
            
            private SingleKeySlot(KeyPart part, int pkPosition, List<KeyRange> ranges) {
                this(part, pkPosition, 1, ranges);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, List<KeyRange> ranges, OrderPreserving orderPreserving) {
                this(part, pkPosition, 1, ranges, orderPreserving);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, int pkSpan, List<KeyRange> ranges) {
                this(part,pkPosition,pkSpan,ranges, null, null);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, int pkSpan, List<KeyRange> ranges, OrderPreserving orderPreserving) {
                this(part,pkPosition,pkSpan,ranges, null, orderPreserving);
            }
            
            private SingleKeySlot(KeyPart part, int pkPosition, int pkSpan, List<KeyRange> ranges, KeyRange minMaxRange, OrderPreserving orderPreserving) {
                this.slot = new KeySlot(part, pkPosition, pkSpan, ranges, orderPreserving);
                this.minMaxRange = minMaxRange;
            }
            
            @Override
            public Iterator<KeySlot> iterator() {
                return Iterators.<KeySlot>singletonIterator(slot);
            }

            @Override
            public KeyRange getMinMaxRange() {
                return minMaxRange;
            }
            
        }
        
        private static class BaseKeyPart implements KeyPart {
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
                        type.pad(ptr, length);
                    }
                }
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                return ByteUtil.getKeyRange(key, op, type);
            }

            private final PColumn column;
            private final List<Expression> nodes;

            private BaseKeyPart(PColumn column, List<Expression> nodes) {
                this.column = column;
                this.nodes = nodes;
            }

            @Override
            public List<Expression> getExtractNodes() {
                return nodes;
            }

            @Override
            public PColumn getColumn() {
                return column;
            }
        }
        
        private  class RowValueConstructorKeyPart implements KeyPart {
            private final RowValueConstructorExpression rvc;
            private final PColumn column;
            private final List<Expression> nodes;
            private final List<KeySlots> childSlots;

            private RowValueConstructorKeyPart(PColumn column, RowValueConstructorExpression rvc, int span, List<KeySlots> childSlots) {
                this.column = column;
                if (span == rvc.getChildren().size()) {
                    this.rvc = rvc;
                    this.nodes = Collections.<Expression>singletonList(rvc);
                    this.childSlots = childSlots;
                } else {
                    this.rvc = new RowValueConstructorExpression(rvc.getChildren().subList(0, span),rvc.isStateless());
                    this.nodes = Collections.<Expression>emptyList();
                    this.childSlots = childSlots.subList(0,  span);
                }
            }

            @Override
            public List<Expression> getExtractNodes() {
                return nodes;
            }

            @Override
            public PColumn getColumn() {
                return column;
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
                    if (usedAllOfLHS && rvc.getChildren().size() < rhs.getChildren().size()) {
                        if (op == CompareOp.LESS) {
                            op = CompareOp.LESS_OR_EQUAL;
                        } else if (op == CompareOp.GREATER_OR_EQUAL) {
                            op = CompareOp.GREATER;
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
                        public Expression wrap(final Expression lhs, final Expression rhs) throws SQLException {
                            final KeyPart childPart = keySlotsIterator.next().iterator().next().getKeyPart();
                            // TODO: DelegateExpression
                            return new BaseTerminalExpression() {
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
                                    KeyRange range = childPart.getKeyRange(rvcElementOp, rhs);
                                    // This can happen when an EQUAL operator is used and the expression cannot possibly match.
                                    if (range == KeyRange.EMPTY_RANGE) {
                                        return false;
                                    }
                                    // We have to take the range and condense it down to a single key. We use which ever
                                    // part of the range is inclusive (which implies being bound as well). This works in all
                                    // cases, including this substring one, which produces a lower inclusive range and an
                                    // upper non inclusive range.
                                    // (a, substr(b,1,1)) IN (('a','b'), ('c','d'))
                                    byte[] key = range.isLowerInclusive() ? range.getLowerRange() : range.getUpperRange();
                                    // FIXME: this is kind of a hack. The above call will fill a fixed width key, but
                                    // we don't want to fill the key yet because it can throw off our the logic we
                                    // use to compute the next key when we evaluate the RHS row value constructor
                                    // below.  We could create a new childPart with a delegate column that returns
                                    // null for getByteSize().
                                    if (lhs.getDataType().isFixedWidth() && lhs.getMaxLength() != null && key.length > lhs.getMaxLength()) {
                                        // Don't use PDataType.pad(), as this only grows the value, while this is shrinking it.
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
                                    return childPart.getColumn().getSortOrder();
                                }

                                @Override
                                public <T> T accept(ExpressionVisitor<T> visitor) {
                                    return null;
                                }
                            };
                        }
                        
                    });
                } catch (SQLException e) {
                    return null; // Shouldn't happen
                }
                ImmutableBytesWritable ptr = context.getTempPtr();
                if (!rhs.evaluate(null, ptr) || ptr.getLength()==0) {
                    return null; 
                }
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                return ByteUtil.getKeyRange(key, op, PVarbinary.INSTANCE);
            }

        }
    }
}
