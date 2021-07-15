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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.tuple.Tuple;

import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

/**
 * Result scanner that dedups the incoming tuples to make them distinct.
 * <p>
 * Note that the results are held in memory
 *  
 * 
 * @since 1.2
 */
public class DistinctAggregatingResultIterator implements AggregatingResultIterator {
    /**
     * Original AggregatingResultIterator
     */
    private final AggregatingResultIterator targetAggregatingResultIterator;
    private final RowProjector rowProjector;
    /**
     * Cached tuples already seen.
     */
    private final Set<ResultEntry> resultEntries =
            Sets.<ResultEntry>newHashSet();

    private class ResultEntry {
        /**
         * cached hashCode.
         */
        private final int hashCode;
        private final Tuple result;
        /**
         * cached column values.
         */
        private final ImmutableBytesPtr[] columnValues;

        ResultEntry(Tuple result) {
            this.result = result;
            this.columnValues =
                    new ImmutableBytesPtr[rowProjector.getColumnCount()];
            int columnIndex = 0;
            for (ColumnProjector columnProjector : rowProjector.getColumnProjectors()) {
                Expression expression = columnProjector.getExpression();
                ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                if (!expression.evaluate(this.result, ptr)) {
                    columnValues[columnIndex] = null;
                } else {
                    columnValues[columnIndex] = ptr;
                }
                columnIndex++;
            }
            this.hashCode = Arrays.hashCode(columnValues);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (o.getClass() != this.getClass()) {
                return false;
            }
            ResultEntry that = (ResultEntry) o;
            return Arrays.equals(this.columnValues, that.columnValues);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    public DistinctAggregatingResultIterator(AggregatingResultIterator delegate,
            RowProjector rowProjector) {
        this.targetAggregatingResultIterator = delegate;
        this.rowProjector = rowProjector;
    }

    @Override
    public Tuple next() throws SQLException {
        while (true) {
            Tuple nextTuple = this.targetAggregatingResultIterator.next();
            if (nextTuple == null) {
                return null;
            }
            ResultEntry resultEntry = new ResultEntry(nextTuple);
            if (!this.resultEntries.contains(resultEntry)) {
                this.resultEntries.add(resultEntry);
                return nextTuple;
            }
        }
    }

    @Override
    public void close() throws SQLException {
        this.targetAggregatingResultIterator.close();
     }

    @Override
    public void explain(List<String> planSteps) {
        targetAggregatingResultIterator.explain(planSteps);
        planSteps.add("CLIENT DISTINCT ON " + rowProjector.toString());
    }

    @Override
    public void explain(List<String> planSteps,
            ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
        targetAggregatingResultIterator.explain(
                planSteps,
                explainPlanAttributesBuilder);
        explainPlanAttributesBuilder.setClientDistinctFilter(
            rowProjector.toString());
        planSteps.add("CLIENT DISTINCT ON " + rowProjector.toString());
    }

    @Override
    public Aggregator[] aggregate(Tuple result) {
        return targetAggregatingResultIterator.aggregate(result);
    }

    @Override
    public String toString() {
        return "DistinctAggregatingResultIterator [targetAggregatingResultIterator=" + targetAggregatingResultIterator
                + ", rowProjector=" + rowProjector;
    }
}
