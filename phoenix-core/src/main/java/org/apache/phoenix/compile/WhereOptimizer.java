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
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.phoenix.expression.visitor.TraverseNoExpressionVisitor;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;


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
    private static final List<KeyRange> SALT_PLACEHOLDER = Collections.singletonList(PDataType.CHAR.getKeyRange(QueryConstants.SEPARATOR_BYTE_ARRAY));
    
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
        PTable table = context.getResolver().getTables().get(0).getTable();
        if (whereClause == null && (tenantId == null || !table.isMultiTenant())) {
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
    
            if (keySlots == null && (tenantId == null || !table.isMultiTenant())) {
                context.setScanRanges(ScanRanges.EVERYTHING);
                return whereClause;
            }
            // If a parameter is bound to null (as will be the case for calculating ResultSetMetaData and
            // ParameterMetaData), this will be the case. It can also happen for an equality comparison
            // for unequal lengths.
            if (keySlots == KeyExpressionVisitor.DEGENERATE_KEY_PARTS) {
                context.setScanRanges(ScanRanges.NOTHING);
                return null;
            }
        }
        if (keySlots == null) {
            keySlots = KeyExpressionVisitor.DEGENERATE_KEY_PARTS;
        }
        
        if (extractNodes == null) {
            extractNodes = new HashSet<Expression>(table.getPKColumns().size());
        }

        int pkPos = -1;
        Integer nBuckets = table.getBucketNum();
        // We're fully qualified if all columns except the salt column are specified
        int fullyQualifiedColumnCount = table.getPKColumns().size() - (nBuckets == null ? 0 : 1);
        RowKeySchema schema = table.getRowKeySchema();
        List<List<KeyRange>> cnf = Lists.newArrayListWithExpectedSize(schema.getMaxFields());
        boolean forcedSkipScan = statement.getHint().hasHint(Hint.SKIP_SCAN);
        boolean forcedRangeScan = statement.getHint().hasHint(Hint.RANGE_SCAN);
        boolean hasUnboundedRange = false;
        boolean hasAnyRange = false;
        
        Iterator<KeyExpressionVisitor.KeySlot> iterator = keySlots.iterator();
        // Add placeholder for salt byte ranges
        if (nBuckets != null) {
            cnf.add(SALT_PLACEHOLDER);
            // Increment the pkPos, as the salt column is in the row schema
            // Do not increment the iterator, though, as there will never be
            // an expression in the keySlots for the salt column
            pkPos++;
        }
        
        // Add tenant data isolation for tenant-specific tables
        if (tenantId != null && table.isMultiTenant()) {
            KeyRange tenantIdKeyRange = KeyRange.getKeyRange(tenantId.getBytes());
            cnf.add(singletonList(tenantIdKeyRange));
            //if (iterator.hasNext()) iterator.next();
            pkPos++;
        }
        // Add unique index ID for shared indexes on views. This ensures
        // that different indexes don't interleave.
        if (table.getViewIndexId() != null) {
            KeyRange indexIdKeyRange = KeyRange.getKeyRange(MetaDataUtil.getViewIndexIdDataType().toBytes(table.getViewIndexId()));
            cnf.add(singletonList(indexIdKeyRange));
            //if (iterator.hasNext()) iterator.next();
            pkPos++;
        }
        // Concat byte arrays of literals to form scan start key
        while (iterator.hasNext()) {
            KeyExpressionVisitor.KeySlot slot = iterator.next();
            // If the position of the pk columns in the query skips any part of the row k
            // then we have to handle in the next phase through a key filter.
            // If the slot is null this means we have no entry for this pk position.
            if (slot == null || slot.getKeyRanges().isEmpty())  {
                if (!forcedSkipScan) break;
                continue;
            }
            if (slot.getPKPosition() != pkPos + 1) {
                if (!forcedSkipScan) break;
                for (int i=pkPos + 1; i < slot.getPKPosition(); i++) {
                    cnf.add(Collections.singletonList(KeyRange.EVERYTHING_RANGE));
                }
            }
            // We support (a,b) IN ((1,2),(3,4)), so in this case we switch to a flattened schema
            if (fullyQualifiedColumnCount > 1 && slot.getPKSpan() == fullyQualifiedColumnCount && !EVERYTHING_RANGES.equals(slot.getKeyRanges())) {
                schema = nBuckets == null ? SchemaUtil.VAR_BINARY_SCHEMA : SaltingUtil.VAR_BINARY_SALTED_SCHEMA;
            }
            KeyPart keyPart = slot.getKeyPart();
            pkPos = slot.getPKPosition();
            List<KeyRange> keyRanges = slot.getKeyRanges();
            cnf.add(keyRanges);
            for (KeyRange range : keyRanges) {
                if (range.isUnbound()) {
                    hasUnboundedRange = true;
                    break;
                }
            }
            
            // Will be null in cases for which only part of the expression was factored out here
            // to set the start/end key. An example would be <column> LIKE 'foo%bar' where we can
            // set the start key to 'foo' but still need to match the regex at filter time.
            // Don't extract expressions if we're forcing a range scan and we've already come
            // across a range for a prior slot. The reason is that we have an inexact range after
            // that, so must filter on the remaining conditions (see issue #467).
            if (!forcedRangeScan || !hasAnyRange) {
                List<Expression> nodesToExtract = keyPart.getExtractNodes();
                extractNodes.addAll(nodesToExtract);
            }
            // Stop building start/stop key once we encounter a non single key range.
            if (hasUnboundedRange && !forcedSkipScan) {
                // TODO: when stats are available, we may want to continue this loop if the
                // cardinality of this slot is low. We could potentially even continue this
                // loop in the absence of a range for a key slot.
                break;
            }
            hasAnyRange |= keyRanges.size() > 1 || (keyRanges.size() == 1 && !keyRanges.get(0).isSingleKey());
        }
        context.setScanRanges(
                ScanRanges.create(cnf, schema, statement.getHint().hasHint(Hint.RANGE_SCAN), nBuckets),
                keySlots.getMinMaxRange());
        if (whereClause == null) {
            return null;
        } else {
            return whereClause.accept(new RemoveExtractedNodesVisitor(extractNodes));
        }
    }

    private static class RemoveExtractedNodesVisitor extends TraverseNoExpressionVisitor<Expression> {
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
            if (l.size() != node.getChildren().size()) {
                if (l.isEmpty()) {
                    // Don't return null here, because then our defaultReturn will kick in
                    return LiteralExpression.newConstant(true, true);
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
    public static class KeyExpressionVisitor extends TraverseNoExpressionVisitor<KeyExpressionVisitor.KeySlots> {
        private static final KeySlots DEGENERATE_KEY_PARTS = new KeySlots() {
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
        
        private static KeySlots newKeyParts(KeySlot slot, Expression extractNode, KeyRange keyRange) {
            if (keyRange == null) {
                return DEGENERATE_KEY_PARTS;
            }
            
            List<KeyRange> keyRanges = slot.getPKSpan() == 1 ? Collections.<KeyRange>singletonList(keyRange) : EVERYTHING_RANGES;
            KeyRange minMaxRange = slot.getPKSpan() == 1 ? null : keyRange;
            return newKeyParts(slot, extractNode, keyRanges, minMaxRange);
        }

        private static KeySlots newKeyParts(KeySlot slot, Expression extractNode, List<KeyRange> keyRanges, KeyRange minMaxRange) {
            if (isDegenerate(keyRanges)) {
                return DEGENERATE_KEY_PARTS;
            }
            
            List<Expression> extractNodes = extractNode == null || slot.getKeyPart().getExtractNodes().isEmpty()
                  ? Collections.<Expression>emptyList()
                  : Collections.<Expression>singletonList(extractNode);
            return new SingleKeySlot(new BaseKeyPart(slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), slot.getPKSpan(), keyRanges, minMaxRange, slot.getOrderPreserving());
        }

        private static KeySlots newKeyParts(KeySlot slot, List<Expression> extractNodes, List<KeyRange> keyRanges, KeyRange minMaxRange) {
            if (isDegenerate(keyRanges)) {
                return DEGENERATE_KEY_PARTS;
            }
            
            return new SingleKeySlot(new BaseKeyPart(slot.getKeyPart().getColumn(), extractNodes), slot.getPKPosition(), slot.getPKSpan(), keyRanges, minMaxRange, slot.getOrderPreserving());
        }

        private KeySlots newRowValueConstructorKeyParts(RowValueConstructorExpression rvc, List<KeySlots> childSlots) {
            if (childSlots.isEmpty() || rvc.isStateless()) {
                return null;
            }
            
            int positionOffset = (table.getBucketNum() ==null ? 0 : 1) + (this.context.getConnection().getTenantId() != null && table.isMultiTenant() ? 1 : 0) + (table.getViewIndexId() == null ? 0 : 1);
            int position = 0;
            for (KeySlots slots : childSlots) {
                KeySlot keySlot = slots.iterator().next();
                List<Expression> childExtractNodes = keySlot.getKeyPart().getExtractNodes();
                // If columns are not in PK order, then stop iteration
                if (childExtractNodes.size() != 1 
                        || childExtractNodes.get(0) != rvc.getChildren().get(position) 
                        || keySlot.getPKPosition() != position + positionOffset) {
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
                int span = position;
                return new SingleKeySlot(new RowValueConstructorKeyPart(table.getPKColumns().get(positionOffset), rvc, span, childSlots), positionOffset, span, EVERYTHING_RANGES);
            }
            return null;
        }

        private static KeySlots newScalarFunctionKeyPart(KeySlot slot, ScalarFunction node) {
            if (isDegenerate(slot.getKeyRanges())) {
                return DEGENERATE_KEY_PARTS;
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
                return DEGENERATE_KEY_PARTS;
            }
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
                    return childPart.getExtractNodes();
                }

                @Override
                public PColumn getColumn() {
                    return childPart.getColumn();
                }
            }, slot.getPKPosition(), slot.getKeyRanges());
        }

        private KeySlots andKeySlots(AndExpression andExpression, List<KeySlots> childSlots) {
            int nColumns = table.getPKColumns().size();
            KeySlot[] keySlot = new KeySlot[nColumns];
            KeyRange minMaxRange = KeyRange.EVERYTHING_RANGE;
            List<Expression> minMaxExtractNodes = Lists.<Expression>newArrayList();
            int initPosition = (table.getBucketNum() ==null ? 0 : 1) + (this.context.getConnection().getTenantId() != null && table.isMultiTenant() ? 1 : 0) + (table.getViewIndexId() == null ? 0 : 1);
            for (KeySlots childSlot : childSlots) {
                if (childSlot == DEGENERATE_KEY_PARTS) {
                    return DEGENERATE_KEY_PARTS;
                }
                if (childSlot.getMinMaxRange() != null) {
                    // TODO: potentially use KeySlot.intersect here. However, we can't intersect the key ranges in the slot
                    // with our minMaxRange, since it spans columns and this would mess up our skip scan.
                    minMaxRange = minMaxRange.intersect(childSlot.getMinMaxRange());
                    for (KeySlot slot : childSlot) {
                        minMaxExtractNodes.addAll(slot.getKeyPart().getExtractNodes());
                    }
                } else {
                    for (KeySlot slot : childSlot) {
                        // We have a nested AND with nothing for this slot, so continue
                        if (slot == null) {
                            continue;
                        }
                        int position = slot.getPKPosition();
                        KeySlot existing = keySlot[position];
                        if (existing == null) {
                            keySlot[position] = slot;
                        } else {
                            keySlot[position] = existing.intersect(slot);
                            if (keySlot[position] == null) {
                                return DEGENERATE_KEY_PARTS;
                            }
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
                if (childSlot == DEGENERATE_KEY_PARTS) {
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
                        List<Expression> extractNodes = slot.getKeyPart().getExtractNodes();
                        extractAll &= !extractNodes.isEmpty();
                        slotExtractNodes.addAll(extractNodes);
                    }
                } else {
                    // TODO: Do the same optimization that we do for IN if the childSlots specify a fully qualified row key
                    for (KeySlot slot : childSlot) {
                        // We have a nested OR with nothing for this slot, so continue
                        if (slot == null) {
                            continue; // FIXME: I don't think this is ever necessary
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

        private boolean isFullyQualified(int pkSpan) {
            int nPKColumns = table.getPKColumns().size();
            return table.getBucketNum() == null ? pkSpan == nPKColumns : pkSpan == nPKColumns-1;
        }
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
            if (! (node.getChildren().get(1) instanceof LiteralExpression) || node.startsWithWildcard()) {
                return Iterators.emptyIterator();
            }

            return Iterators.singletonIterator(node.getChildren().get(0));
        }

        @Override
        public KeySlots visitLeave(LikeExpression node, List<KeySlots> childParts) {
            if (childParts.isEmpty()) {
                return null;
            }
            // for SUBSTR(<column>,1,3) LIKE 'foo%'
            KeySlots childSlots = childParts.get(0);
            KeySlot childSlot = childSlots.iterator().next();
            final String startsWith = node.getLiteralPrefix();
            byte[] key = PDataType.CHAR.toBytes(startsWith, node.getChildren().get(0).getSortOrder());
            // If the expression is an equality expression against a fixed length column
            // and the key length doesn't match the column length, the expression can
            // never be true.
            // An zero length byte literal is null which can never be compared against as true
            Integer childNodeFixedLength = node.getChildren().get(0).getMaxLength();
            if (childNodeFixedLength != null && key.length > childNodeFixedLength) {
                return DEGENERATE_KEY_PARTS;
            }
            // TODO: is there a case where we'd need to go through the childPart to calculate the key range?
            PColumn column = childSlot.getKeyPart().getColumn();
            PDataType type = column.getDataType();
            KeyRange keyRange = type.getKeyRange(key, true, ByteUtil.nextKey(key), false);
            Integer columnFixedLength = column.getMaxLength();
            if (columnFixedLength != null) {
                keyRange = keyRange.fill(columnFixedLength);
            }
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
            List<KeyRange> ranges = Lists.newArrayListWithExpectedSize(keyExpressions.size());
            KeySlot childSlot = childParts.get(0).iterator().next();
            KeyPart childPart = childSlot.getKeyPart();
            // We can only optimize a row value constructor that is fully qualified
            if (childSlot.getPKSpan() > 1 && !isFullyQualified(childSlot.getPKSpan())) {
                // Just return a key part that has the min/max of the IN list, but doesn't
                // extract the IN list expression.
                return newKeyParts(childSlot, (Expression)null, Collections.singletonList(
                        KeyRange.getKeyRange(
                                ByteUtil.copyKeyBytesIfNecessary(node.getMinKey()), true,
                                ByteUtil.copyKeyBytesIfNecessary(node.getMaxKey()), true)), null);
            }
            // Handles cases like WHERE substr(foo,1,3) IN ('aaa','bbb')
            for (Expression key : keyExpressions) {
                KeyRange range = childPart.getKeyRange(CompareOp.EQUAL, key);
                if (range != KeyRange.EMPTY_RANGE) { // null means it can't possibly be in range
                    ranges.add(range);
                }
            }
            return newKeyParts(childSlot, node, ranges, null);
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

        private static final class KeySlot {
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
                if (this.getPKPosition() != that.getPKPosition()) {
                    throw new IllegalArgumentException("Position must be equal for intersect");
                }
                Preconditions.checkArgument(!this.keyRanges.isEmpty());
                Preconditions.checkArgument(!that.keyRanges.isEmpty());

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

        private static class SingleKeySlot implements KeySlots {
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
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                // If the column is fixed width, fill is up to it's byte size
                PDataType type = getColumn().getDataType();
                if (type.isFixedWidth()) {
                    Integer length = getColumn().getMaxLength();
                    if (length != null) {
                        key = ByteUtil.fillKey(key, length);
                    }
                }
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
               final CompareOp rvcElementOp = op == CompareOp.LESS_OR_EQUAL ? CompareOp.LESS : op == CompareOp.GREATER ? CompareOp.GREATER_OR_EQUAL : op;
                if (op != CompareOp.EQUAL) {
                    boolean usedAllOfLHS = !nodes.isEmpty();
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
                    if (!usedAllOfLHS || rvc.getChildren().size() != rhs.getChildren().size()) {
                        // We know that rhs was converted to a row value constructor and that it's a constant
                        rhs= new RowValueConstructorExpression(rhs.getChildren().subList(0, Math.min(rvc.getChildren().size(), rhs.getChildren().size())), rhs.isStateless());
                    }
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
                                    if (lhs.getDataType().isFixedWidth() && lhs.getMaxLength() != null && key.length != lhs.getMaxLength()) {
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
                return ByteUtil.getKeyRange(key, op, PDataType.VARBINARY);
            }

        }
    }
}
