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
package org.apache.phoenix.expression.aggregator;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.*;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Client side Aggregator for PERCENT_RANK aggregations
 * 
 * 
 * @since 1.2.1
 */
public class PercentRankClientAggregator extends DistinctValueWithCountClientAggregator {

    private final List<Expression> exps;

    public PercentRankClientAggregator(List<Expression> exps, SortOrder sortOrder) {
        super(sortOrder);
        this.exps = exps;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (cachedResult == null) {
            ColumnExpression columnExp = (ColumnExpression)exps.get(0);
            // Second exp will be a LiteralExpression of Boolean type indicating whether the ordering to
            // be ASC/DESC
            LiteralExpression isAscendingExpression = (LiteralExpression)exps.get(1);
            boolean isAscending = (Boolean)isAscendingExpression.getValue();

            // Third expression will be LiteralExpression
            LiteralExpression valueExp = (LiteralExpression)exps.get(2);
            Map<Object, Integer> sorted = getSortedValueVsCount(isAscending, columnExp.getDataType());
            long distinctCountsSum = 0;
            Object value = valueExp.getValue();
            for (Entry<Object, Integer> entry : sorted.entrySet()) {
                Object colValue = entry.getKey();
                int compareResult = columnExp.getDataType().compareTo(colValue, value, valueExp.getDataType());
                boolean done = isAscending ? compareResult > 0 : compareResult <= 0;
                if (done) break;
                distinctCountsSum += entry.getValue();
            }

            float result = (float)distinctCountsSum / totalCount;
            this.cachedResult = new BigDecimal(result);
        }
        if (buffer == null) {
            initBuffer();
        }
        buffer = PDecimal.INSTANCE.toBytes(this.cachedResult);
        ptr.set(buffer);
        return true;
    }

    @Override
    protected PDataType getResultDataType() {
        return PDecimal.INSTANCE;
    }
}
