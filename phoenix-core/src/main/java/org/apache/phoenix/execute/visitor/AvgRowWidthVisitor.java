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
import org.apache.phoenix.parse.JoinTableNode;

import java.sql.SQLException;

/**
 * Implementation of QueryPlanVisitor used to get the average number of bytes each
 * row for a QueryPlan.
 */
public class AvgRowWidthVisitor implements QueryPlanVisitor<Double> {

    @Override
    public Double defaultReturn(QueryPlan plan) {
        return null;
    }

    @Override
    public Double visit(AggregatePlan plan) {
        try {
            Long byteCount = plan.getEstimatedBytesToScan();
            Long rowCount = plan.getEstimatedRowsToScan();
            if (byteCount != null && rowCount != null) {
                if (byteCount == 0) {
                    return 0.0;
                }
                if (rowCount != 0) {
                    return ((double) byteCount) / rowCount;
                }
            }
        } catch (SQLException e) {
        }

        return null;
    }

    @Override
    public Double visit(ScanPlan plan) {
        try {
            Long byteCount = plan.getEstimatedBytesToScan();
            Long rowCount = plan.getEstimatedRowsToScan();
            if (byteCount != null && rowCount != null) {
                if (byteCount == 0) {
                    return 0.0;
                }
                if (rowCount != 0) {
                    return ((double) byteCount) / rowCount;
                }
            }
        } catch (SQLException e) {
        }

        return null;
    }

    @Override
    public Double visit(ClientAggregatePlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(ClientScanPlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(LiteralResultIterationPlan plan) {
        return (double) plan.getEstimatedSize();
    }

    @Override
    public Double visit(TupleProjectionPlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(HashJoinPlan plan) {
        Double lhsWidth = plan.getDelegate().accept(this);
        if (lhsWidth == null) {
            return null;
        }
        JoinTableNode.JoinType[] joinTypes = plan.getJoinInfo().getJoinTypes();
        HashJoinPlan.SubPlan[] subPlans = plan.getSubPlans();
        Double width = lhsWidth;
        for (int i = 0; i < joinTypes.length; i++) {
            Double rhsWidth = subPlans[i].getInnerPlan().accept(this);
            if (rhsWidth == null) {
                return null;
            }
            width = join(width, rhsWidth, joinTypes[i]);
        }

        return width;
    }

    @Override
    public Double visit(SortMergeJoinPlan plan) {
        Double lhsWidth = plan.getLhsPlan().accept(this);
        Double rhsWidth = plan.getRhsPlan().accept(this);
        if (lhsWidth == null || rhsWidth == null) {
            return null;
        }

        return join(lhsWidth, rhsWidth, plan.getJoinType());
    }

    @Override
    public Double visit(UnionPlan plan) {
        Double sum = 0.0;
        for (QueryPlan subPlan : plan.getSubPlans()) {
            Double avgWidth = subPlan.accept(this);
            if (avgWidth == null) {
                return null;
            }
            sum += avgWidth;
        }

        return sum / plan.getSubPlans().size();
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
        return (double) plan.getEstimatedSize();
    }

    @Override
    public Double visit(TraceQueryPlan plan) {
        return (double) plan.getEstimatedSize();
    }


    /*
     * The below methods provide estimation of row width based on the input row width as well as
     * the operator.
     */

    public static double join(double lhsWidth, double rhsWidth, JoinTableNode.JoinType type) {
        double width;
        switch (type) {
            case Inner:
            case Left:
            case Right:
            case Full: {
                width = lhsWidth + rhsWidth;
                break;
            }
            case Semi:
            case Anti: {
                width = lhsWidth;
                break;
            }
            default: {
                throw new IllegalArgumentException("Invalid join type: " + type);
            }
        }
        return width;
    }
}
