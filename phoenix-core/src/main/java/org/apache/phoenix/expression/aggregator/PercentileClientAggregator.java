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
 * Client side Aggregator for PERCENTILE_CONT aggregations
 * 
 * 
 * @since 1.2.1
 */
public class PercentileClientAggregator extends DistinctValueWithCountClientAggregator {

    private final List<Expression> exps;

    public PercentileClientAggregator(List<Expression> exps, SortOrder sortOrder) {
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
            LiteralExpression percentileExp = (LiteralExpression)exps.get(2);
            float p = ((Number)percentileExp.getValue()).floatValue();
            Map<Object, Integer> sorted = getSortedValueVsCount(isAscending, columnExp.getDataType());
            float i = (p * this.totalCount) + 0.5F;
            long k = (long)i;
            float f = i - k;
            Object o1 = null;
            Object o2 = null;
            long distinctCountsSum = 0;
            for (Entry<Object, Integer> entry : sorted.entrySet()) {
                if (o1 != null) {
                    o2 = entry.getKey();
                    break;
                }
                distinctCountsSum += entry.getValue();
                if (distinctCountsSum == k) {
                    o1 = entry.getKey();
                } else if (distinctCountsSum > k) {
                    o1 = o2 = entry.getKey();
                    break;
                }
            }

            double result = 0.0;
            Number n1 = (Number)o1;
            if (o2 == null || o1 == o2) {
                result = n1.doubleValue();
            } else {
                Number n2 = (Number)o2;
                result = (n1.doubleValue() * (1.0F - f)) + (n2.doubleValue() * f);
            }
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
