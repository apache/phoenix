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
package org.apache.phoenix.execute.visitor;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.phoenix.compile.GroupByCompiler;
import org.apache.phoenix.compile.ListJarsQueryPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.TraceQueryPlan;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.ClientAggregatePlan;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.execute.CursorFetchPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.LiteralResultIterationPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.SortMergeJoinPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.UnionPlan;
import org.apache.phoenix.execute.UnnestArrayPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.filter.BooleanExpressionFilter;
import org.apache.phoenix.parse.JoinTableNode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of QueryPlanVisitor used to get the number of output rows for a QueryPlan.
 */
public class RowCountVisitor implements QueryPlanVisitor<Double> {

    // An estimate of the ratio of result data from group-by against the input data.
    private final static double GROUPING_FACTOR = 0.1;

    private final static double OUTER_JOIN_FACTOR = 1.15;
    private final static double INNER_JOIN_FACTOR = 0.85;
    private final static double SEMI_OR_ANTI_JOIN_FACTOR = 0.5;

    private final static double UNION_DISTINCT_FACTOR = 0.8;

    @Override
    public Double defaultReturn(QueryPlan plan) {
        return null;
    }

    @Override
    public Double visit(AggregatePlan plan) {
        try {
            Long b = plan.getEstimatedRowsToScan();
            if (b != null) {
                return limit(
                        filter(
                                aggregate(
                                        filter(
                                                b.doubleValue(),
                                                stripSkipScanFilter(
                                                        plan.getContext().getScan().getFilter())),
                                        plan.getGroupBy()),
                                plan.getHaving()),
                        plan.getLimit());
            }
        } catch (SQLException e) {
        }

        return null;
    }

    @Override
    public Double visit(ScanPlan plan) {
        try {
            Long b = plan.getEstimatedRowsToScan();
            if (b != null) {
                return limit(
                        filter(
                                b.doubleValue(),
                                stripSkipScanFilter(plan.getContext().getScan().getFilter())),
                        plan.getLimit());
            }
        } catch (SQLException e) {
        }

        return null;
    }

    @Override
    public Double visit(ClientAggregatePlan plan) {
        Double b = plan.getDelegate().accept(this);
        if (b != null) {
            return limit(
                    filter(
                            aggregate(
                                    filter(b.doubleValue(), plan.getWhere()),
                                    plan.getGroupBy()),
                            plan.getHaving()),
                    plan.getLimit());
        }

        return null;
    }

    @Override
    public Double visit(ClientScanPlan plan) {
        if (plan.getLimit() != null) {
            return (double) plan.getLimit();
        }
        Double b = plan.getDelegate().accept(this);
        if (b != null) {
            return limit(
                    filter(b.doubleValue(), plan.getWhere()),
                    plan.getLimit());
        }

        return null;
    }

    @Override
    public Double visit(LiteralResultIterationPlan plan) {
        return 1.0;
    }

    @Override
    public Double visit(TupleProjectionPlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(HashJoinPlan plan) {
        try {
            QueryPlan lhsPlan = plan.getDelegate();
            Long b = lhsPlan.getEstimatedRowsToScan();
            if (b == null) {
                return null;
            }

            Double rows = filter(b.doubleValue(),
                    stripSkipScanFilter(lhsPlan.getContext().getScan().getFilter()));
            JoinTableNode.JoinType[] joinTypes = plan.getJoinInfo().getJoinTypes();
            HashJoinPlan.SubPlan[] subPlans = plan.getSubPlans();
            for (int i = 0; i < joinTypes.length; i++) {
                Double rhsRows = subPlans[i].getInnerPlan().accept(this);
                if (rhsRows == null) {
                    return null;
                }
                rows = join(rows, rhsRows.doubleValue(), joinTypes[i]);
            }
            if (lhsPlan instanceof AggregatePlan) {
                AggregatePlan aggPlan = (AggregatePlan) lhsPlan;
                rows = filter(aggregate(rows, aggPlan.getGroupBy()), aggPlan.getHaving());
            }
            return limit(rows, lhsPlan.getLimit());
        } catch (SQLException e) {
        }

        return null;
    }

    @Override
    public Double visit(SortMergeJoinPlan plan) {
        Double lhsRows = plan.getLhsPlan().accept(this);
        Double rhsRows = plan.getRhsPlan().accept(this);
        if (lhsRows != null && rhsRows != null) {
            return join(lhsRows, rhsRows, plan.getJoinType());
        }

        return null;
    }

    @Override
    public Double visit(UnionPlan plan) {
        int count = plan.getSubPlans().size();
        double[] inputRows = new double[count];
        for (int i = 0; i < count; i++) {
            Double b = plan.getSubPlans().get(i).accept(this);
            if (b != null) {
                inputRows[i] = b.doubleValue();
            } else {
                return null;
            }
        }

        return limit(union(true, inputRows),plan.getLimit());
    }

    @Override
    public Double visit(UnnestArrayPlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(CursorFetchPlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(ListJarsQueryPlan plan) {
        return 0.0;
    }

    @Override
    public Double visit(TraceQueryPlan plan) {
        return 0.0;
    }

    public static Filter stripSkipScanFilter(Filter filter) {
        if (filter == null) {
            return null;
        }
        if (!(filter instanceof FilterList)) {
            return filter instanceof BooleanExpressionFilter ? filter : null;
        }
        FilterList filterList = (FilterList) filter;
        if (filterList.getOperator() != FilterList.Operator.MUST_PASS_ALL) {
            return filter;
        }
        List<Filter> list = new ArrayList<>();
        for (Filter f : filterList.getFilters()) {
            Filter stripped = stripSkipScanFilter(f);
            if (stripped != null) {
                list.add(stripped);
            }
        }
        return list.isEmpty() ? null : (list.size() == 1 ? list.get(0) : new FilterList(FilterList.Operator.MUST_PASS_ALL, list));
    }


    /*
     * The below methods provide estimation of row count based on the input row count as well as
     * the operator. They should be replaced by more accurate calculation based on histogram and
     * a logical operator layer is expect to facilitate this.
     */

    public static double filter(double inputRows, Filter filter) {
        if (filter == null) {
            return inputRows;
        }
        return 0.5 * inputRows;
    }

    public static double filter(double inputRows, Expression filter) {
        if (filter == null) {
            return inputRows;
        }
        return 0.5 * inputRows;
    }

    public static double aggregate(double inputRows, GroupByCompiler.GroupBy groupBy) {
        if (groupBy.isUngroupedAggregate()) {
            return 1.0;
        }
        return GROUPING_FACTOR * inputRows;
    }

    public static double limit(double inputRows, Integer limit) {
        if (limit == null) {
            return inputRows;
        }
        return limit;
    }

    public static double join(double lhsRows, double[] rhsRows, JoinTableNode.JoinType[] types) {
        assert rhsRows.length == types.length;
        double rows = lhsRows;
        for (int i = 0; i < rhsRows.length; i++) {
            rows = join(rows, rhsRows[i], types[i]);
        }
        return rows;
    }

    public static double join(double lhsRows, double rhsRows, JoinTableNode.JoinType type) {
        double rows;
        switch (type) {
            case Inner: {
                rows = Math.min(lhsRows, rhsRows);
                rows = rows * INNER_JOIN_FACTOR;
                break;
            }
            case Left:
            case Right:
            case Full: {
                rows = Math.max(lhsRows, rhsRows);
                rows = rows * OUTER_JOIN_FACTOR;
                break;
            }
            case Semi:
            case Anti: {
                rows = lhsRows * SEMI_OR_ANTI_JOIN_FACTOR;
                break;
            }
            default: {
                throw new IllegalArgumentException("Invalid join type: " + type);
            }
        }
        return rows;
    }

    public static double union(boolean all, double... inputRows) {
        double rows = 0.0;
        for (double d : inputRows) {
            rows += d;
        }
        if (!all) {
            rows *= UNION_DISTINCT_FACTOR;
        }
        return rows;
    }
}
