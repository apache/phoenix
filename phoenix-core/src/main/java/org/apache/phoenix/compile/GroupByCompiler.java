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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.http.annotation.Immutable;
import org.apache.phoenix.compile.OrderPreservingTracker.Ordering;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PVarchar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * 
 * Validates GROUP BY clause and builds a {@link GroupBy} instance to encapsulate the
 * group by expressions.
 *
 * 
 * @since 0.1
 */
public class GroupByCompiler {
    @Immutable
    public static class GroupBy {
        private final List<Expression> expressions;
        private final List<Expression> keyExpressions;
        private final String scanAttribName;
        public static final GroupByCompiler.GroupBy EMPTY_GROUP_BY = new GroupBy(new GroupByBuilder());
        
        private GroupBy(GroupByBuilder builder) {
            this.expressions = ImmutableList.copyOf(builder.expressions);
            this.keyExpressions = ImmutableList.copyOf(builder.keyExpressions);
            this.scanAttribName = builder.scanAttribName;
            assert(expressions.size() == keyExpressions.size());
        }
        
        public List<Expression> getExpressions() {
            return expressions;
        }
        
        public List<Expression> getKeyExpressions() {
            return keyExpressions;
        }
        
        public String getScanAttribName() {
            return scanAttribName;
        }
        
        public boolean isEmpty() {
            return expressions.isEmpty();
        }
        
        public static class GroupByBuilder {
            private String scanAttribName;
            private List<Expression> expressions = Collections.emptyList();
            private List<Expression> keyExpressions = Collections.emptyList();

            public GroupByBuilder() {
            }
            
            public GroupByBuilder setScanAttribName(String scanAttribName) {
                this.scanAttribName = scanAttribName;
                return this;
            }
            
            public GroupByBuilder setExpressions(List<Expression> expressions) {
                this.expressions = expressions;
                return this;
            }
            
            public GroupByBuilder setKeyExpressions(List<Expression> keyExpressions) {
                this.keyExpressions = keyExpressions;
                return this;
            }
            
            public GroupBy build() {
                return new GroupBy(this);
            }
        }

        public boolean isOrderPreserving() {
            return !BaseScannerRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS.equals(scanAttribName);
        }
        
        public void explain(List<String> planSteps, Integer limit) {
            if (scanAttribName != null) {
                if (BaseScannerRegionObserver.UNGROUPED_AGG.equals(scanAttribName)) {
                    planSteps.add("    SERVER AGGREGATE INTO SINGLE ROW");
                } else if (BaseScannerRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS.equals(scanAttribName)) {
                    planSteps.add("    SERVER AGGREGATE INTO DISTINCT ROWS BY " + getExpressions() + (limit == null ? "" : " LIMIT " + limit + " GROUP" + (limit.intValue() == 1 ? "" : "S")));                    
                } else {
                    planSteps.add("    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY " + getExpressions() + (limit == null ? "" : " LIMIT " + limit + " GROUP" + (limit.intValue() == 1 ? "" : "S")));                    
                }
            }
        }
    }

    /**
     * Get list of columns in the GROUP BY clause.
     * @param context query context kept between compilation of different query clauses
     * @param statement SQL statement being compiled
     * @return the {@link GroupBy} instance encapsulating the group by clause
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */
    public static GroupBy compile(StatementContext context, SelectStatement statement, TupleProjector tupleProjector, boolean isInRowKeyOrder) throws SQLException {
        List<ParseNode> groupByNodes = statement.getGroupBy();
        /**
         * Distinct can use an aggregate plan if there's no group by.
         * Otherwise, we need to insert a step after the Merge that dedups.
         * Order by only allowed on columns in the select distinct
         */
        if (groupByNodes.isEmpty()) {
            if (statement.isAggregate()) {
                return new GroupBy.GroupByBuilder().setScanAttribName(BaseScannerRegionObserver.UNGROUPED_AGG).build();
            }
            if (!statement.isDistinct()) {
                return GroupBy.EMPTY_GROUP_BY;
            }
            
            groupByNodes = Lists.newArrayListWithExpectedSize(statement.getSelect().size());
            for (AliasedNode aliasedNode : statement.getSelect()) {
                groupByNodes.add(aliasedNode.getNode());
            }
        }

       // Accumulate expressions in GROUP BY
        ExpressionCompiler compiler =
                new ExpressionCompiler(context, GroupBy.EMPTY_GROUP_BY);
        List<Pair<Integer,Expression>> groupBys = Lists.newArrayListWithExpectedSize(groupByNodes.size());
        OrderPreservingTracker tracker = new OrderPreservingTracker(context, GroupBy.EMPTY_GROUP_BY, Ordering.UNORDERED, groupByNodes.size(), tupleProjector);
        for (int i = 0; i < groupByNodes.size(); i++) {
            ParseNode node = groupByNodes.get(i);
            Expression expression = node.accept(compiler);
            if (!expression.isStateless()) {
                if (compiler.isAggregate()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATE_IN_GROUP_BY)
                        .setMessage(expression.toString()).build().buildException();
                }
                tracker.track(expression);
                groupBys.add(new Pair<Integer,Expression>(i,expression));
            }
            compiler.reset();
        }
        
        if (groupBys.isEmpty()) {
            return GroupBy.EMPTY_GROUP_BY;
        }
        
        boolean isRowKeyOrderedGrouping = isInRowKeyOrder && tracker.isOrderPreserving();
        List<Expression> expressions = Lists.newArrayListWithExpectedSize(groupBys.size());
        List<Expression> keyExpressions = expressions;
        String groupExprAttribName;
        // This is true if the GROUP BY is composed of only PK columns. We further check here that
        // there are no "gaps" in the PK columns positions used (i.e. we start with the first PK
        // column and use each subsequent one in PK order).
        if (isRowKeyOrderedGrouping) {
            groupExprAttribName = BaseScannerRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS;
            for (Pair<Integer,Expression> groupBy : groupBys) {
                expressions.add(groupBy.getSecond());
            }
        } else {
            /*
             * Otherwise, our coprocessor needs to collect all distinct groups within a region, sort them, and
             * hold on to them until the scan completes.
             */
            groupExprAttribName = BaseScannerRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS;
            /*
             * Put fixed length nullables at the end, so that we can represent null by the absence of the trailing
             * value in the group by key. If there is more than one, we'll need to convert the ones not at the end
             * into a Decimal so that we can use an empty byte array as our representation for null (which correctly
             * maintains the sort order). We convert the Decimal back to the appropriate type (Integer or Long) when
             * it's retrieved from the result set.
             * 
             * More specifically, order into the following buckets:
             *   1) non nullable fixed width
             *   2) variable width
             *   3) nullable fixed width
             * Within each bucket, order based on the column position in the schema. Putting the fixed width values
             * in the beginning optimizes access to subsequent values.
             */
            Collections.sort(groupBys, new Comparator<Pair<Integer,Expression>>() {
                @Override
                public int compare(Pair<Integer,Expression> gb1, Pair<Integer,Expression> gb2) {
                    Expression e1 = gb1.getSecond();
                    Expression e2 = gb2.getSecond();
                    boolean isFixed1 = e1.getDataType().isFixedWidth();
                    boolean isFixed2 = e2.getDataType().isFixedWidth();
                    boolean isFixedNullable1 = e1.isNullable() &&isFixed1;
                    boolean isFixedNullable2 = e2.isNullable() && isFixed2;
                    if (isFixedNullable1 == isFixedNullable2) {
                        if (isFixed1 == isFixed2) {
                            // Not strictly necessary, but forces the order to match the schema
                            // column order (with PK columns before value columns).
                            //return o1.getColumnPosition() - o2.getColumnPosition();
                            return gb1.getFirst() - gb2.getFirst();
                        } else if (isFixed1) {
                            return -1;
                        } else {
                            return 1;
                        }
                    } else if (isFixedNullable1) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            for (Pair<Integer,Expression> groupBy : groupBys) {
                expressions.add(groupBy.getSecond());
            }
            for (int i = expressions.size()-2; i >= 0; i--) {
                Expression expression = expressions.get(i);
                PDataType keyType = getKeyType(expression);
                if (keyType == expression.getDataType()) {
                    continue;
                }
                // Copy expressions only when keyExpressions will be different than expressions
                if (keyExpressions == expressions) {
                    keyExpressions = new ArrayList<Expression>(expressions);
                }
                // Wrap expression in an expression that coerces the expression to the required type..
                // This is done so that we have a way of expressing null as an empty key when more
                // than one fixed and nullable types are used in a group by clause
                keyExpressions.set(i, CoerceExpression.create(expression, keyType));
            }
        }

        GroupBy groupBy = new GroupBy.GroupByBuilder().setScanAttribName(groupExprAttribName).setExpressions(expressions).setKeyExpressions(keyExpressions).build();
        return groupBy;
    }
    
    private static PDataType getKeyType(Expression expression) {
        PDataType type = expression.getDataType();
        if (!expression.isNullable() || !type.isFixedWidth()) {
            return type;
        }
        if (type.isCastableTo(PDecimal.INSTANCE)) {
            return PDecimal.INSTANCE;
        }
        if (type.isCastableTo(PVarchar.INSTANCE)) {
            return PVarchar.INSTANCE;
        }
        // This might happen if someone tries to group by an array
        throw new IllegalStateException("Multiple occurrences of type " + type + " may not occur in a GROUP BY clause");
    }
    
    private GroupByCompiler() {
    }
}
