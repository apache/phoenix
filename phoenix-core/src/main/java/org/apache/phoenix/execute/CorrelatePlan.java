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
package org.apache.phoenix.execute;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.TupleProjector.ProjectedValueTuple;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

public class CorrelatePlan extends DelegateQueryPlan {    
    private final QueryPlan rhs;
    private final String variableId;
    private final JoinType joinType;
    private final boolean isSingleValueOnly;
    private final RuntimeContext runtimeContext;
    private final KeyValueSchema joinedSchema;
    private final KeyValueSchema lhsSchema;
    private final KeyValueSchema rhsSchema;
    private final int rhsFieldPosition;

    public CorrelatePlan(QueryPlan lhs, QueryPlan rhs, String variableId, 
            JoinType joinType, boolean isSingleValueOnly, 
            RuntimeContext runtimeContext, PTable joinedTable, 
            PTable lhsTable, PTable rhsTable, int rhsFieldPosition) {
        super(lhs);
        if (joinType != JoinType.Inner && joinType != JoinType.Left && joinType != JoinType.Semi && joinType != JoinType.Anti)
            throw new IllegalArgumentException("Unsupported join type '" + joinType + "' by CorrelatePlan");
        
        this.rhs = rhs;
        this.variableId = variableId;
        this.joinType = joinType;
        this.isSingleValueOnly = isSingleValueOnly;
        this.runtimeContext = runtimeContext;
        this.joinedSchema = buildSchema(joinedTable);
        this.lhsSchema = buildSchema(lhsTable);
        this.rhsSchema = buildSchema(rhsTable);
        this.rhsFieldPosition = rhsFieldPosition;
    }

    private static KeyValueSchema buildSchema(PTable table) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        if (table != null) {
            for (PColumn column : table.getColumns()) {
                if (!SchemaUtil.isPKColumn(column)) {
                    builder.addField(column);
                }
            }
        }
        return builder.build();
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> steps = Lists.newArrayList();
        steps.add("NESTED-LOOP-JOIN (" + joinType.toString().toUpperCase() + ") TABLES");
        for (String step : delegate.getExplainPlan().getPlanSteps()) {
            steps.add("    " + step);            
        }
        steps.add("AND" + (rhsSchema.getFieldCount() == 0 ? " (SKIP MERGE)" : ""));
        for (String step : rhs.getExplainPlan().getPlanSteps()) {
            steps.add("    " + step);            
        }
        return new ExplainPlan(steps);
    }

    @Override
    public ResultIterator iterator(final ParallelScanGrouper scanGrouper, final Scan scan)
            throws SQLException {
        return new ResultIterator() {
            private final ValueBitSet destBitSet = ValueBitSet.newInstance(joinedSchema);
            private final ValueBitSet lhsBitSet = ValueBitSet.newInstance(lhsSchema);
            private final ValueBitSet rhsBitSet = 
                    (joinType == JoinType.Semi || joinType == JoinType.Anti) ?
                            ValueBitSet.EMPTY_VALUE_BITSET 
                          : ValueBitSet.newInstance(rhsSchema);
            private final ResultIterator iter = delegate.iterator(scanGrouper, scan);
            private ResultIterator rhsIter = null;
            private Tuple current = null;
            private boolean closed = false;

            @Override
            public void close() throws SQLException {
                if (!closed) {
                    closed = true;
                    iter.close();
                    if (rhsIter != null) {
                        rhsIter.close();
                    }
                }
            }

            @Override
            public Tuple next() throws SQLException {
                if (closed)
                    return null;
                
                Tuple rhsCurrent = null;
                if (rhsIter != null) {
                    rhsCurrent = rhsIter.next();
                    if (rhsCurrent == null) {
                        rhsIter.close();
                        rhsIter = null;
                    } else if (isSingleValueOnly) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS).build().buildException();
                    }
                }
                while (rhsIter == null) {
                    current = iter.next();
                    if (current == null) {
                        close();
                        return null;
                    }
                    runtimeContext.setCorrelateVariableValue(variableId, current);
                    rhsIter = rhs.iterator();
                    rhsCurrent = rhsIter.next();
                    if ((rhsCurrent == null && (joinType == JoinType.Inner || joinType == JoinType.Semi))
                            || (rhsCurrent != null && joinType == JoinType.Anti)) {
                        rhsIter.close();
                        rhsIter = null;
                    }
                }
                
                Tuple joined;
                try {
                    joined = rhsBitSet == ValueBitSet.EMPTY_VALUE_BITSET ?
                            current : TupleProjector.mergeProjectedValue(
                                    convertLhs(current), joinedSchema, destBitSet,
                                    rhsCurrent, rhsSchema, rhsBitSet, rhsFieldPosition, true);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
                                
                if ((joinType == JoinType.Semi || rhsCurrent == null) && rhsIter != null) {
                    rhsIter.close();
                    rhsIter = null;
                }
                
                return joined;
            }

            @Override
            public void explain(List<String> planSteps) {
            }
            
            private ProjectedValueTuple convertLhs(Tuple lhs) throws IOException {
                ProjectedValueTuple t;
                if (lhs instanceof ProjectedValueTuple) {
                    t = (ProjectedValueTuple) lhs;
                } else {
                    ImmutableBytesWritable ptr = getContext().getTempPtr();
                    TupleProjector.decodeProjectedValue(lhs, ptr);
                    lhsBitSet.clear();
                    lhsBitSet.or(ptr);
                    int bitSetLen = lhsBitSet.getEstimatedLength();
                    t = new ProjectedValueTuple(lhs, lhs.getValue(0).getTimestamp(), 
                            ptr.get(), ptr.getOffset(), ptr.getLength(), bitSetLen);

                }
                return t;
            }
        };
    }

    @Override
    public Integer getLimit() {
        return null;
    }

}
