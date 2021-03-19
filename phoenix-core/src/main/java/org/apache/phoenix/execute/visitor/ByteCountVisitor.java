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

/**
 * Implementation of QueryPlanVisitor used to get the number of output bytes for a QueryPlan.
 */
public class ByteCountVisitor implements QueryPlanVisitor<Double> {

    @Override
    public Double defaultReturn(QueryPlan plan) {
        return null;
    }

    @Override
    public Double visit(AggregatePlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(ScanPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(ClientAggregatePlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(ClientScanPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(LiteralResultIterationPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(TupleProjectionPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(HashJoinPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(SortMergeJoinPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(UnionPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(UnnestArrayPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(CursorFetchPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(ListJarsQueryPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    @Override
    public Double visit(TraceQueryPlan plan) {
        return getByteCountFromRowCountAndRowWidth(plan);
    }

    protected Double getByteCountFromRowCountAndRowWidth(QueryPlan plan) {
        Double rowCount = plan.accept(new RowCountVisitor());
        Double rowWidth = plan.accept(new AvgRowWidthVisitor());
        if (rowCount == null || rowWidth == null) {
            return null;
        }

        return rowCount * rowWidth;
    }
}
