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
package org.apache.phoenix.schema;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_TTL;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompiledConditionalTTLExpression implements CompiledTTLExpression {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompiledConditionalTTLExpression.class);

    // expression as passed in the DDL statement and stored in syscat
    private final String ttlExpr;
    // compiled expression according to the table schema. For indexes the expression is
    // first re-written to use index column references and then compiled.
    private final Expression compiledExpr;
    // columns referenced in the ttl expression to be added to scan
    private final Set<ColumnReference> conditionExprColumns;

    // Since DeleteColumn updates are not propagated to indexes we mask older columns
    // within GlobalIndexChecker. We need to do the same masking in TTLRegionScanner
    // before we evaluate the ttl expression on the row. We apply this masking on server
    // maintained indexes.
    private final boolean maskOlderCells;

    public CompiledConditionalTTLExpression(String ttlExpr,
                                            Expression compiledExpression,
                                            Set<ColumnReference> conditionExprColumns,
                                            boolean maskOlderCells) {
        Preconditions.checkNotNull(compiledExpression);
        Preconditions.checkNotNull(conditionExprColumns);
        this.ttlExpr = ttlExpr;
        this.compiledExpr = compiledExpression;
        this.conditionExprColumns = conditionExprColumns;
        this.maskOlderCells = maskOlderCells;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompiledConditionalTTLExpression that = (CompiledConditionalTTLExpression) o;
        return ttlExpr.equals(that.ttlExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ttlExpr);
    }

    @Override
    public String getTTLExpression() {
        return ttlExpr;
    }

    @Override
    public String toString() {
        return getTTLExpression();
    }

    /**
     * The cells of the row (i.e., result) read from HBase store are lexicographically ordered
     * for tables using the key part of the cells which includes row, family, qualifier,
     * timestamp and type. The cells belong of a column are ordered from the latest to
     * the oldest. The method leverages this ordering and groups the cells into their columns
     * based on the pair of family name and column qualifier.
     */
    private List<Cell> getLatestRowVersion(List<Cell> result) {
        List<Cell> latestRowVersion = new ArrayList<>();
        Cell currentColumnCell = null;
        long maxDeleteFamilyTS = 0;
        for (Cell cell : result) {
            if (currentColumnCell == null ||
                    !CellUtil.matchingColumn(cell, currentColumnCell)) {
                // found a new column cell which has the latest timestamp
                currentColumnCell = cell;
                if (currentColumnCell.getType() == Cell.Type.DeleteFamily ||
                        currentColumnCell.getType() == Cell.Type.DeleteFamilyVersion) {
                    // DeleteFamily will be first in the lexicographically ordering because
                    // it has no qualifier
                    maxDeleteFamilyTS = currentColumnCell.getTimestamp();
                    // no need to add the DeleteFamily cell since it can't be part of
                    // an expression
                    continue;
                }
                if (currentColumnCell.getTimestamp() > maxDeleteFamilyTS) {
                    // only add the cell if it is not masked by the DeleteFamily
                    latestRowVersion.add(currentColumnCell);
                }
            }
        }
        return latestRowVersion;
    }

    /**
     * This logic has been borrowed from GlobalIndexChecker#removeOlderCells
     * This is needed to correctly handle updates to the index row when some columns
     * are set to null and thus are not updated but an older version of the same column
     * might have a valid value. Since TTLRegionScanner evaluates the row before GlobalIndexChecker
     * processes the row we need to mask such columns in TTLRegionScanner also for correctness.
     * @return
     */
    private List<Cell> maskOlderCells(List<Cell> cellList) {
        Iterator<Cell> cellIterator = cellList.iterator();
        if (!cellIterator.hasNext()) {
            return cellList;
        }
        Cell cell = cellIterator.next();
        long maxTs = cell.getTimestamp();
        long ts;
        boolean allTheSame = true;
        while (cellIterator.hasNext()) {
            cell = cellIterator.next();
            ts = cell.getTimestamp();
            if (ts != maxTs) {
                if (ts > maxTs) {
                    maxTs = ts;
                }
                allTheSame = false;
            }
        }
        if (allTheSame) {
            return cellList;
        }
        List<Cell> result = Lists.newArrayListWithExpectedSize(cellList.size());
        cellIterator = cellList.iterator();
        while (cellIterator.hasNext()) {
            cell = cellIterator.next();
            if (cell.getTimestamp() == maxTs) {
                result.add(cell);
            }
        }
        return result;
    }

    @Override
    /**
     * @param result row to be evaluated against the conditional ttl expression. The cells
     * of the row are assumed to be sorted in lexicographic order.
     * @param isRaw true when the row is from a raw scan like index verification
     * @return DEFAULT_TTL (FOREVER) if the expression evaluates to False else 0
     * if the expression evaluates to true i.e. row is expired
     */
    public long getRowTTLForMasking(List<Cell> result, boolean isRaw) {
        return isExpired(result, isRaw) ? 0 : DEFAULT_TTL;
    }

    @Override
    /**
     * During compaction, we first use the DEFAULT_TTL (FOREVER) value for doing all the checks
     * related to ttl and maxlookback. The actual evaluation of the Conditional TTL expression
     * happens later.
     */
    public long getRowTTLForCompaction(List<Cell> result) {
        return DEFAULT_TTL;
    }

    /**
     * Determines if the row is expired or not based on the result of the evaluation of the
     * Conditional TTL expression.
     * @param result List of cells sorted in lexicographic order
     * @param isRaw true when the row is from a raw scan like index verification or compaction
     * @return True if the expression evaluates to true else false
     */
    public boolean isExpired(List<Cell> result, boolean isRaw) {
        if (result.isEmpty()) {
            return false;
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        List<Cell> latestRowVersion = !isRaw ? result : getLatestRowVersion(result);
        if (maskOlderCells) {
            // check and remove older cells for index scans
            latestRowVersion = maskOlderCells(latestRowVersion);
        }
        if (latestRowVersion.isEmpty()) {
            return false;
        }
        MultiKeyValueTuple row = new MultiKeyValueTuple(latestRowVersion);
        if (!compiledExpr.evaluate(row, ptr)) {
            LOGGER.info("Expression evaluation failed for expr {}", ttlExpr);
            return false;
        }
        Object value = PBoolean.INSTANCE.toObject(ptr);
        return value.equals(Boolean.TRUE);
    }

    /**
     * Returns the columns referenced in the ttl expression to be added to scan
     */
    public Set<ColumnReference> getColumnsReferenced() {
        return conditionExprColumns;
    }

    @VisibleForTesting
    public boolean isMaskingOlderCells() {
        return maskOlderCells;
    }

    private static byte[] serializeExpression(Expression condTTLExpr) throws IOException {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            DataOutput output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, ExpressionType.valueOf(condTTLExpr).ordinal());
            condTTLExpr.write(output);
            return stream.toByteArray();
        }
    }

    private static Expression deSerializeExpression(byte[] serializedExpr) throws IOException {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(serializedExpr)) {
            DataInput input = new DataInputStream(stream);
            int expressionOrdinal = WritableUtils.readVInt(input);
            Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
            expression.readFields(input);
            return expression;
        }
    }

    public static CompiledConditionalTTLExpression createFromProto(PTableProtos.ConditionTTL condition)
            throws IOException {
        String ttlExpr = condition.getTtlExpression();
        Expression compiledExpression = deSerializeExpression(
                condition.getCompiledExpression().toByteArray());
        List<ServerCachingProtos.ColumnReference> exprColumnsList =
                condition.getTtlExpressionColumnsList();
        Set<ColumnReference> conditionExprColumns = new HashSet<>(exprColumnsList.size());
        for (ServerCachingProtos.ColumnReference colRefFromProto : exprColumnsList) {
            conditionExprColumns.add(new ColumnReference(
                    colRefFromProto.getFamily().toByteArray(),
                    colRefFromProto.getQualifier().toByteArray()));
        }
        boolean removeOlderCells = condition.getRemoveOlderCells();
        return new CompiledConditionalTTLExpression(
                ttlExpr,
                compiledExpression,
                conditionExprColumns,
                removeOlderCells);
    }

    @Override
    public PTableProtos.TTLExpression toProto() throws SQLException, IOException {
        PTableProtos.TTLExpression.Builder ttl = PTableProtos.TTLExpression.newBuilder();
        PTableProtos.ConditionTTL.Builder condition = PTableProtos.ConditionTTL.newBuilder();
        condition.setTtlExpression(ttlExpr);
        condition.setCompiledExpression(ByteStringer.wrap(serializeExpression(compiledExpr)));
        for (ColumnReference colRef : conditionExprColumns) {
            ServerCachingProtos.ColumnReference.Builder cRefBuilder =
                    ServerCachingProtos.ColumnReference.newBuilder();
            cRefBuilder.setFamily(ByteStringer.wrap(colRef.getFamily()));
            cRefBuilder.setQualifier(ByteStringer.wrap(colRef.getQualifier()));
            condition.addTtlExpressionColumns(cRefBuilder.build());
        }
        condition.setRemoveOlderCells(maskOlderCells);
        ttl.setCondition(condition.build());
        return ttl.build();
    }
}
