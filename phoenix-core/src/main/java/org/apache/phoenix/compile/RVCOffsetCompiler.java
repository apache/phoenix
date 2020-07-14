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

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.parse.CastParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.OffsetNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowValueConstructorOffsetInternalErrorException;
import org.apache.phoenix.schema.RowValueConstructorOffsetNotAllowedInQueryException;
import org.apache.phoenix.schema.RowValueConstructorOffsetNotCoercibleException;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RVCOffsetCompiler {

    private final static Logger LOGGER = LoggerFactory.getLogger(RVCOffsetCompiler.class);

    private final static RVCOffsetCompiler INSTANCE = new RVCOffsetCompiler();

    private RVCOffsetCompiler() {
    }

    public static RVCOffsetCompiler getInstance() {
        return INSTANCE;
    }

    public CompiledOffset getRVCOffset(StatementContext context, FilterableStatement statement,
            boolean inJoin, boolean inUnion, OffsetNode offsetNode) throws SQLException {
        // We have a RVC offset. See PHOENIX-4845

        // This is a EqualParseNode with LHS and RHS RowValueConstructorParseNodes
        // This is enforced as part of the grammar
        EqualParseNode equalParseNode = (EqualParseNode) offsetNode.getOffsetParseNode();

        RowValueConstructorParseNode
                rvcColumnsParseNode =
                (RowValueConstructorParseNode) equalParseNode.getLHS();
        RowValueConstructorParseNode
                rvcConstantParseNode =
                (RowValueConstructorParseNode) equalParseNode.getRHS();

        // disallow use with aggregations
        if (statement.isAggregate()) {
            throw new RowValueConstructorOffsetNotAllowedInQueryException("RVC Offset not allowed in Aggregates");
        }

        // Get the Select Type should not be join/union
        // Note cannot use the SelectStatement as for Left/Right joins we won't get passed in the join context
        if (inJoin || inUnion) {
            throw new RowValueConstructorOffsetNotAllowedInQueryException("RVC Offset not allowed in Joins or Unions");
        }

        // Get the tables primary keys
        if (context.getResolver().getTables().size() != 1) {
            throw new RowValueConstructorOffsetNotAllowedInQueryException("RVC Offset not allowed with zero or multiple tables");
        }

        PTable pTable = context.getCurrentTable().getTable();

        List<PColumn> columns = pTable.getPKColumns();

        int numUserColumns = columns.size(); // columns specified by the user
        int userColumnIndex = 0; // index into the ordered list, columns, of where user specified start

        // if we are salted we need to take a subset of the pk
        Integer buckets = pTable.getBucketNum();
        if (buckets != null && buckets > 0) { // We are salted
            numUserColumns--;
            userColumnIndex++;
        }

        if (pTable.isMultiTenant() && context.getConnection().getTenantId() != null) {
            // the tenantId is one of the pks and will be handled automatically
            numUserColumns--;
            userColumnIndex++;
        }

        boolean isIndex = false;
        if (PTableType.INDEX.equals(pTable.getType())) {
            isIndex = true;
            // If we are a view index we have to handle the idxId column
            // Note that viewIndexId comes before tenantId (what about salt byte?)
            if (pTable.getViewIndexId() != null) {
                numUserColumns--;
                userColumnIndex++;
            }
        }

        // Sanity check that they are providing all the user defined keys to this table
        if (numUserColumns != rvcConstantParseNode.getChildren().size()) {
            throw new RowValueConstructorOffsetNotCoercibleException(
                    "RVC Offset must exactly cover the tables PK.");
        }

        // Make sure the order is the same and all the user defined columns are mentioned in the column RVC
        if (numUserColumns != rvcColumnsParseNode.getChildren().size()) {
            throw new RowValueConstructorOffsetNotCoercibleException(
                    "RVC Offset must specify the tables PKs.");
        }

        List<ColumnParseNode>
                rvcColumnParseNodeList = buildListOfColumnParseNodes(rvcColumnsParseNode, isIndex);

        // Make sure we have all column parse nodes for the left hand
        if (rvcColumnParseNodeList.size() != numUserColumns) {
            throw new RowValueConstructorOffsetNotCoercibleException(
                    "RVC Offset must specify the tables PKs.");
        }

        // We resolve the mini-where now so we can compare to tables pks PColumns and to produce a row offset
        // Construct a mini where clause
        ParseNode miniWhere = equalParseNode;

        Set<HintNode.Hint> originalHints = statement.getHint().getHints();
        WhereCompiler.WhereExpressionCompiler whereCompiler = new WhereCompiler.WhereExpressionCompiler(context);

        Expression whereExpression;
        try {
            whereExpression = miniWhere.accept(whereCompiler);
        }catch(TypeMismatchException e) {
            throw new RowValueConstructorOffsetNotCoercibleException(
                    "RVC Offset could not be coerced to the tables PKs. " + e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Unexpected error while compiling RVC Offset, got null expression.",e);
            throw new RowValueConstructorOffsetInternalErrorException(
                    "RVC Offset unexpected failure.");
        }

        if (whereExpression == null) {
            LOGGER.error("Unexpected error while compiling RVC Offset, got null expression.");
            throw new RowValueConstructorOffsetInternalErrorException(
                    "RVC Offset unexpected failure.");
        }

        Expression expression;
        try {
            expression =
                    WhereOptimizer
                            .pushKeyExpressionsToScan(context, originalHints, whereExpression, null,
                                    Optional.<byte[]>absent());
        } catch (Exception e) {
            LOGGER.error("Unexpected error while compiling RVC Offset, got null expression.");
            throw new RowValueConstructorOffsetInternalErrorException(
                    "RVC Offset unexpected failure.");
        }

        //If the whereExpression is a single term comparison/isNull it will be entirely removed
        if (expression == null && whereExpression instanceof AndExpression) {
            LOGGER.error("Unexpected error while compiling RVC Offset, got null expression.");
            throw new RowValueConstructorOffsetInternalErrorException(
                    "RVC Offset unexpected failure.");
        }

        // Now that columns etc have been resolved lets check to make sure they match the pk order
        RowKeyColumnExpressionOutput rowKeyColumnExpressionOutput =
                buildListOfRowKeyColumnExpressions(whereExpression, isIndex);

        List<RowKeyColumnExpression>
                rowKeyColumnExpressionList = rowKeyColumnExpressionOutput.getRowKeyColumnExpressions();

        if (rowKeyColumnExpressionList.size() != numUserColumns) {
            LOGGER.warn("Unexpected error while compiling RVC Offset, expected " + numUserColumns
                    + " found " + rowKeyColumnExpressionList.size());
            throw new RowValueConstructorOffsetInternalErrorException(
                    "RVC Offset must specify the table's PKs.");
        }

        for (int i = 0; i < numUserColumns; i++) {
            PColumn column = columns.get(i + userColumnIndex);

            ColumnParseNode columnParseNode = rvcColumnParseNodeList.get(i);

            String columnParseNodeString = columnParseNode.getFullName();
            if (isIndex) {
                columnParseNodeString = IndexUtil.getDataColumnName(columnParseNodeString);
            }

            RowKeyColumnExpression rowKeyColumnExpression = rowKeyColumnExpressionList.get(i);
            String expressionName = rowKeyColumnExpression.getName();

            // Not sure why it is getting quoted
            expressionName = expressionName.replace("\"", "");

            if (isIndex) {
                expressionName = IndexUtil.getDataColumnName(expressionName);
            }

            if (!StringUtils.equals(expressionName, columnParseNodeString)) {
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must specify the table's PKs.");
            }

            String columnString = column.getName().getString();
            if (isIndex) {
                columnString = IndexUtil.getDataColumnName(columnString);
            }
            if (!StringUtils.equals(expressionName, columnString)) {
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must specify the table's PKs.");
            }
        }

        byte[] key;

        // check to see if this was a single key expression
        ScanRanges scanRanges = context.getScanRanges();

        //We do not generate a point lookup today in phoenix if the rowkey has a trailing null, we generate a range scan.
        if (!scanRanges.isPointLookup()) {
            //Since we use a range scan to guarantee we get only the null value and the upper bound is unset this suffices
            //sanity check
            if (!rowKeyColumnExpressionOutput.isTrailingNull()) {
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must be a point lookup.");
            }
            key = scanRanges.getScanRange().getUpperRange();
        } else {
            RowKeySchema.RowKeySchemaBuilder builder = new RowKeySchema.RowKeySchemaBuilder(columns.size());

            for (PColumn column : columns) {
                builder.addField(column, column.isNullable(), column.getSortOrder());
            }

            RowKeySchema rowKeySchema = builder.build();

            //we make a ScanRange with 1 keyslots that cover the entire PK to reuse the code
            KeyRange pointKeyRange = scanRanges.getScanRange();
            KeyRange keyRange = KeyRange.getKeyRange(pointKeyRange.getLowerRange(), false, KeyRange.UNBOUND, true);
            List<KeyRange> myRangeList = Lists.newArrayList(keyRange);
            List<List<KeyRange>> slots = new ArrayList<>();
            slots.add(myRangeList);
            int[] slotSpan = new int[1];

            //subtract 1 see ScanUtil.SINGLE_COLUMN_SLOT_SPAN is 0
            slotSpan[0] = columns.size() - 1;
            key = ScanUtil.getMinKey(rowKeySchema, slots, slotSpan);
        }

        // Note the use of ByteUtil.nextKey() to generate exclusive offset
        CompiledOffset
                compiledOffset =
                new CompiledOffset(Optional.<Integer>absent(),
                        Optional.of(key));

        return compiledOffset;
    }

    @VisibleForTesting
    static class RowKeyColumnExpressionOutput {
        public RowKeyColumnExpressionOutput(List<RowKeyColumnExpression> rowKeyColumnExpressions, boolean trailingNull) {
            this.rowKeyColumnExpressions = rowKeyColumnExpressions;
            this.trailingNull = trailingNull;
        }

        private final List<RowKeyColumnExpression> rowKeyColumnExpressions;
        private final boolean trailingNull;

        public List<RowKeyColumnExpression> getRowKeyColumnExpressions() {
            return rowKeyColumnExpressions;
        }

        public boolean isTrailingNull() {
            return trailingNull;
        }
    }

    @VisibleForTesting
    RowKeyColumnExpressionOutput buildListOfRowKeyColumnExpressions (
            Expression whereExpression, boolean isIndex)
            throws RowValueConstructorOffsetNotCoercibleException, RowValueConstructorOffsetInternalErrorException {

        boolean trailingNull = false;
        List<Expression> expressions;
        if((whereExpression instanceof AndExpression)) {
            expressions = whereExpression.getChildren();
        } else if (whereExpression instanceof ComparisonExpression || whereExpression instanceof IsNullExpression) {
            expressions = Lists.newArrayList(whereExpression);
        } else {
            LOGGER.warn("Unexpected error while compiling RVC Offset, expected either a Comparison/IsNull Expression of a AndExpression got "
                    + whereExpression.getClass().getName());
            throw new RowValueConstructorOffsetInternalErrorException(
                    "RVC Offset must specify the tables PKs.");
        }

        List<RowKeyColumnExpression>
                rowKeyColumnExpressionList =
                new ArrayList<RowKeyColumnExpression>();
        for (int i = 0; i < expressions.size(); i++) {
            Expression child = expressions.get(i);
            if (!(child instanceof ComparisonExpression || child instanceof IsNullExpression)) {
                LOGGER.warn("Unexpected error while compiling RVC Offset");
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must specify the tables PKs.");
            }

            //if this is the last position
            if(i == expressions.size() - 1) {
                if(child instanceof IsNullExpression) {
                    trailingNull = true;
                }
            }

            //For either case comparison/isNull the first child should be the rowkey
            Expression possibleRowKeyColumnExpression = child.getChildren().get(0);

            // Note that since we store indexes in variable length form there may be casts from fixed types to
            // variable length
            if (isIndex) {
                if (possibleRowKeyColumnExpression instanceof CoerceExpression) {
                    // Cast today has 1 child
                    possibleRowKeyColumnExpression =
                            ((CoerceExpression) possibleRowKeyColumnExpression).getChild();
                }
            }

            if (!(possibleRowKeyColumnExpression instanceof RowKeyColumnExpression)) {
                LOGGER.warn("Unexpected error while compiling RVC Offset");
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must specify the tables PKs.");
            }
            rowKeyColumnExpressionList.add((RowKeyColumnExpression) possibleRowKeyColumnExpression);
        }
        return new RowKeyColumnExpressionOutput(rowKeyColumnExpressionList,trailingNull);
    }

    @VisibleForTesting List<ColumnParseNode> buildListOfColumnParseNodes(
            RowValueConstructorParseNode rvcColumnsParseNode, boolean isIndex)
            throws RowValueConstructorOffsetNotCoercibleException {
        List<ColumnParseNode> nodes = new ArrayList<ColumnParseNode>();
        for (ParseNode node : rvcColumnsParseNode.getChildren()) {
            // Note that since we store indexes in variable length form there may be casts from fixed types to
            // variable length
            if (isIndex) {
                if (node instanceof CastParseNode) {
                    // Cast today has 1 child
                    node = node.getChildren().get(0);
                }
            }

            if (!(node instanceof ColumnParseNode)) {
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must specify the tables PKs.");
            } else {
                nodes.add((ColumnParseNode) node);
            }
        }
        return nodes;
    }
}