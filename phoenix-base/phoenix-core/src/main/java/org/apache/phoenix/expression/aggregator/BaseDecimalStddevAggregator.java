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

import java.math.*;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.BigDecimalUtil;
import org.apache.phoenix.util.BigDecimalUtil.Operation;

/**
 * 
 * 
 * @since 1.2.1
 */
public abstract class BaseDecimalStddevAggregator extends DistinctValueWithCountClientAggregator {

    private int colPrecision;
    private int colScale;

    public BaseDecimalStddevAggregator(List<Expression> exps, SortOrder sortOrder) {
        super(sortOrder);
        ColumnExpression stdDevColExp = (ColumnExpression)exps.get(0);
        this.colPrecision = stdDevColExp.getMaxLength();
        this.colScale = stdDevColExp.getScale();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (cachedResult == null) {
            BigDecimal ssd = sumSquaredDeviation();
            ssd = ssd.divide(new BigDecimal(getDataPointsCount()), PDataType.DEFAULT_MATH_CONTEXT);
            // Calculate the precision for the stddev result.
            // There are totalCount #Decimal values for which we are calculating the stddev
            // The resultant precision depends on precision and scale of all these values. (See
            // BigDecimalUtil.getResultPrecisionScale)
            // As of now we are not using the actual precision and scale of individual values but just using the table
            // column's max length(precision) and scale for each of the values.
            int resultPrecision = colPrecision;
            for (int i = 1; i < this.totalCount; i++) {
                // Max precision that we can support is 38 See PDataType.MAX_PRECISION
                if (resultPrecision >= PDataType.MAX_PRECISION) break;
                Pair<Integer, Integer> precisionScale = BigDecimalUtil.getResultPrecisionScale(this.colPrecision,
                        this.colScale, this.colPrecision, this.colScale, Operation.OTHERS);
                resultPrecision = precisionScale.getFirst();
            }
            BigDecimal result = new BigDecimal(Math.sqrt(ssd.doubleValue()), new MathContext(resultPrecision,
                    RoundingMode.HALF_UP));
            result.setScale(this.colScale, RoundingMode.HALF_UP);
            cachedResult = result;
        }
        if (buffer == null) {
            initBuffer();
        }
        buffer = PDecimal.INSTANCE.toBytes(cachedResult);
        ptr.set(buffer);
        return true;
    }

    protected abstract long getDataPointsCount();

    private BigDecimal sumSquaredDeviation() {
        BigDecimal m = mean();
        BigDecimal result = BigDecimal.ZERO;
        for (Entry<ImmutableBytesPtr, Integer> entry : valueVsCount.entrySet()) {
            BigDecimal colValue = (BigDecimal) PDecimal.INSTANCE.toObject(entry.getKey());
            BigDecimal delta = colValue.subtract(m);
            result = result.add(delta.multiply(delta).multiply(new BigDecimal(entry.getValue())));
        }
        return result;
    }

    private BigDecimal mean() {
        BigDecimal sum = BigDecimal.ZERO;
        for (Entry<ImmutableBytesPtr, Integer> entry : valueVsCount.entrySet()) {
            BigDecimal colValue = (BigDecimal) PDecimal.INSTANCE.toObject(entry.getKey());
            sum = sum.add(colValue.multiply(new BigDecimal(entry.getValue())));
        }
        return sum.divide(new BigDecimal(totalCount), PDataType.DEFAULT_MATH_CONTEXT);
    }

    @Override
    protected PDataType getResultDataType() {
        return PDecimal.INSTANCE;
    }
}
