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

import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.execute.UnionPlan;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

public class UnionCompiler {
    private static final PName UNION_FAMILY_NAME = PNameFactory.newName("unionFamilyName");
    private static final PName UNION_SCHEMA_NAME = PNameFactory.newName("unionSchemaName");
    private static final PName UNION_TABLE_NAME = PNameFactory.newName("unionTableName");

    private static List<TargetDataExpression> checkProjectionNumAndExpressions(
        List<QueryPlan> selectPlans) throws SQLException {
        int columnCount = selectPlans.get(0).getProjector().getColumnCount();
        List<TargetDataExpression> targetTypes = new ArrayList<TargetDataExpression>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            for (QueryPlan plan : selectPlans) {
                if (columnCount !=plan.getProjector().getColumnCount()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode
                        .SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS).setMessage("1st query has " + columnCount + " columns whereas 2nd " +
                            "query has " + plan.getProjector().getColumnCount())
                        .build().buildException();
                }
                ColumnProjector colproj = plan.getProjector().getColumnProjector(i);
                if(targetTypes.size() < i+1 ) {
                    targetTypes.add(new TargetDataExpression(colproj.getExpression()));
                } else {
                    compareExperssions(i, colproj.getExpression(), targetTypes);
                }
            }
        }
        return targetTypes;
    }

    public static TableRef contructSchemaTable(PhoenixStatement statement, List<QueryPlan> plans,
            List<AliasedNode> selectNodes) throws SQLException {
        List<TargetDataExpression> targetTypes = checkProjectionNumAndExpressions(plans);
        QueryPlan plan = plans.get(0);
        List<PColumn> projectedColumns = new ArrayList<PColumn>();
        for (int i = 0; i < plan.getProjector().getColumnCount(); i++) {
            ColumnProjector colProj = plan.getProjector().getColumnProjector(i);
            String name = selectNodes == null ? colProj.getLabel() : selectNodes.get(i).getAlias();
            PName colName = PNameFactory.newName(name);
            PColumnImpl projectedColumn = new PColumnImpl(PNameFactory.newName(name),
                UNION_FAMILY_NAME, targetTypes.get(i).getType(), targetTypes.get(i).getMaxLength(),
                targetTypes.get(i).getScale(), colProj.getExpression().isNullable(), i,
                targetTypes.get(i).getSortOrder(), 500, null, false,
                colProj.getExpression().toString(), false, false, colName.getBytes(), HConstants.LATEST_TIMESTAMP);
            projectedColumns.add(projectedColumn);
        }
        Long scn = statement.getConnection().getSCN();
        PTable tempTable = new PTableImpl.Builder()
                .setType(PTableType.SUBQUERY)
                .setTimeStamp(HConstants.LATEST_TIMESTAMP)
                .setIndexDisableTimestamp(0L)
                .setSequenceNumber(scn == null ? HConstants.LATEST_TIMESTAMP : scn)
                .setImmutableRows(true)
                .setDisableWAL(true)
                .setMultiTenant(true)
                .setStoreNulls(true)
                .setUpdateCacheFrequency(0)
                .setNamespaceMapped(SchemaUtil.isNamespaceMappingEnabled(PTableType.SUBQUERY,
                        statement.getConnection().getQueryServices().getProps()))
                .setAppendOnlySchema(false)
                .setImmutableStorageScheme(ImmutableStorageScheme.ONE_CELL_PER_COLUMN)
                .setQualifierEncodingScheme(QualifierEncodingScheme.NON_ENCODED_QUALIFIERS)
                .setBaseColumnCount(BASE_TABLE_BASE_COLUMN_COUNT)
                .setEncodedCQCounter(PTable.EncodedCQCounter.NULL_COUNTER)
                .setUseStatsForParallelization(true)
                .setExcludedColumns(ImmutableList.of())
                .setTenantId(statement.getConnection().getTenantId())
                .setSchemaName(UNION_SCHEMA_NAME)
                .setTableName(UNION_TABLE_NAME)
                .setRowKeyOrderOptimizable(false)
                .setIndexes(Collections.emptyList())
                .setPhysicalNames(ImmutableList.of())
                .setColumns(projectedColumns)
                .build();
        return new TableRef(null, tempTable, 0, false);
    }

    private static void compareExperssions(int i, Expression expression,
            List<TargetDataExpression> targetTypes) throws SQLException {
        PDataType type = expression.getDataType();
        if (type != null && type.isCoercibleTo(targetTypes.get(i).getType())) {
            ;
        }
        else if (targetTypes.get(i).getType() == null || targetTypes.get(i).getType().isCoercibleTo(type)) {
            targetTypes.get(i).setType(type);
        } else {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode
                .SELECT_COLUMN_TYPE_IN_UNIONALL_DIFFS).setMessage("Column # " + i + " is "
                    + targetTypes.get(i).getType().getSqlTypeName() + " in 1st query where as it is "
                    + type.getSqlTypeName() + " in 2nd query")
                .build().buildException();
        }
        Integer len = expression.getMaxLength();
        if (len != null && (targetTypes.get(i).getMaxLength() == null ||
                len > targetTypes.get(i).getMaxLength())) {
            targetTypes.get(i).setMaxLength(len);
        }
        Integer scale = expression.getScale();
        if (scale != null && (targetTypes.get(i).getScale() == null ||
                scale > targetTypes.get(i).getScale())){
            targetTypes.get(i).setScale(scale);
        }
       SortOrder sortOrder = expression.getSortOrder();
        if (sortOrder != null && (!sortOrder.equals(targetTypes.get(i).getSortOrder())))
            targetTypes.get(i).setSortOrder(SortOrder.getDefault()); 
    }

    private static TupleProjector getTupleProjector(RowProjector rowProj,
            List<PColumn> columns) throws SQLException {
        Expression[] exprs = new Expression[columns.size()];
        int i = 0;
        for (ColumnProjector colProj : rowProj.getColumnProjectors()) {
            exprs[i] = CoerceExpression.create(colProj.getExpression(),
                    columns.get(i).getDataType(), columns.get(i).getSortOrder(),
                    columns.get(i).getMaxLength());
            i++;
        }
        return new TupleProjector(exprs);
    }

    private static class TargetDataExpression {
        private PDataType type;
        private Integer maxLength;
        private Integer scale;
        private SortOrder sortOrder;

        public TargetDataExpression(Expression expr) {
            this.type = expr.getDataType();
            this.maxLength = expr.getMaxLength();
            this.scale = expr.getScale();
            this.sortOrder = expr.getSortOrder();
        }

        public PDataType getType() {
            return type;
        }

        public void setType(PDataType type) {
            this.type = type;
        }

        public Integer getMaxLength() {
            return maxLength;
        }

        public void setMaxLength(Integer maxLength) {
            this.maxLength = maxLength;
        }

        public Integer getScale() {
            return scale;
        }

        public void setScale(Integer scale) {
            this.scale = scale;
        }

        public SortOrder getSortOrder() {
            return sortOrder;
        }

        public void setSortOrder(SortOrder sortOrder) {
            this.sortOrder = sortOrder;
        }
    }

    static List<QueryPlan> convertToTupleProjectionPlan(
            List<QueryPlan> plans,
            TableRef tableRef,
            StatementContext statementContext) throws SQLException {
        List<PColumn> columns =  tableRef.getTable().getColumns();
        for (int i = 0; i < plans.size(); i++) {
            QueryPlan subPlan = plans.get(i);
            TupleProjector projector = getTupleProjector(subPlan.getProjector(), columns);
            subPlan = new TupleProjectionPlan(subPlan, projector, statementContext, null);
            plans.set(i, subPlan);
        }
        return plans;
    }

    /**
     * If every subquery in {@link UnionPlan} is ordered, and {@link QueryPlan#getOutputOrderBys}
     * of each subquery are equal(absolute equality or the same column name is unnecessary, just
     * column types are compatible and columns count is same), and at the same time the outer
     * query of {@link UnionPlan} has group by or order by, we would further examine whether
     * maintaining this order for the entire {@link UnionPlan} can compile out the outer query's
     * group by or order by. If it is sure, then {@link UnionPlan} just to perform a simple
     * merge on the output of each subquery to ensure the overall order of the union all;
     * otherwise, {@link UnionPlan} would not perform any special processing on the output
     * of the subqueries.
     */
    static void optimizeUnionOrderByIfPossible(
            UnionPlan innerUnionPlan,
            SelectStatement outerSelectStatement,
            Supplier<StatementContext> statementContextCreator) throws SQLException {
        innerUnionPlan.enableCheckSupportOrderByOptimize();
        if (!innerUnionPlan.isSupportOrderByOptimize()) {
            return;
        }

        if (!isOptimizeUnionOrderByDeserved(
                innerUnionPlan, outerSelectStatement, statementContextCreator)) {
            // If maintain the order for the entire UnionPlan(by merge on the output of each
            // subquery) could not compile out the outer query's group by or order by, we would
            // not perform any special processing on the output of the subqueries.
            innerUnionPlan.disableSupportOrderByOptimize();
        }
    }

    /**
     * If group by or order by in outerSelectStatement could be compiled out,
     * this optimization is deserved.
     */
    private static boolean isOptimizeUnionOrderByDeserved(
            UnionPlan innerUnionPlan,
            SelectStatement outerSelectStatement,
            Supplier<StatementContext> statementContextCreator) throws SQLException {
        if (!outerSelectStatement.haveGroupBy() && !outerSelectStatement.haveOrderBy()) {
            return false;
        }

        // Just to avoid additional ProjectionCompiler.compile, make the compilation of order by
        // as simple as possible.
        if (!outerSelectStatement.haveGroupBy()
                && outerSelectStatement.getOrderBy().stream().anyMatch(OrderByNode::isIntegerLiteral)) {
            return false;
        }
        StatementContext statementContext = statementContextCreator.get();
        ColumnResolver columResover = innerUnionPlan.getContext().getResolver();
        TableRef tableRef = innerUnionPlan.getTableRef();
        statementContext.setResolver(columResover);
        statementContext.setCurrentTable(tableRef);

        if (outerSelectStatement.haveGroupBy()) {
            // For outer query has group by, we check whether groupBy.isOrderPreserving is true.
            GroupBy groupBy = GroupByCompiler.compile(statementContext, outerSelectStatement);
            outerSelectStatement =
                    HavingCompiler.rewrite(statementContext, outerSelectStatement, groupBy);
            Expression where = WhereCompiler.compile(
                    statementContext,
                    outerSelectStatement,
                    null,
                    null,
                    CompiledOffset.EMPTY_COMPILED_OFFSET.getByteOffset());
            groupBy = groupBy.compile(statementContext, innerUnionPlan, where);
            return groupBy.isOrderPreserving();
        }

        assert outerSelectStatement.haveOrderBy();
        Expression where = WhereCompiler.compile(
                statementContext,
                outerSelectStatement,
                null,
                null,
                CompiledOffset.EMPTY_COMPILED_OFFSET.getByteOffset());
        // For outer query has order by, we check whether orderBy is OrderBy.FWD_ROW_KEY_ORDER_BY.
        OrderBy orderBy = OrderByCompiler.compile(
                statementContext,
                outerSelectStatement,
                GroupBy.EMPTY_GROUP_BY,
                null,
                CompiledOffset.EMPTY_COMPILED_OFFSET,
                null,
                innerUnionPlan,
                where);
        return orderBy == OrderBy.FWD_ROW_KEY_ORDER_BY;
    }
}
