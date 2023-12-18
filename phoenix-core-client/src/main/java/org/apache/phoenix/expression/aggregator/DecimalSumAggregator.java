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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SizedUtil;


/**
 * 
 * Aggregator that sums BigDecimal values
 *
 * 
 * @since 0.1
 */
public class DecimalSumAggregator extends BaseAggregator {
    private BigDecimal sum = BigDecimal.ZERO;
    private byte[] sumBuffer;
    
    public DecimalSumAggregator(SortOrder sortOrder, ImmutableBytesWritable ptr) {
        super(sortOrder);
        if (ptr != null) {
            initBuffer();
            sum = (BigDecimal) PDecimal.INSTANCE.toObject(ptr);
        }
    }
    
    private PDataType getInputDataType() {
        return PDecimal.INSTANCE;
    }
    
    private int getBufferLength() {
        return getDataType().getByteSize();
    }

    private void initBuffer() {
        sumBuffer = new byte[getBufferLength()];
    }
    
    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        BigDecimal value = (BigDecimal)getDataType().toObject(ptr, getInputDataType(), sortOrder);
        sum = sum.add(value);
        if (sumBuffer == null) {
            sumBuffer = new byte[getDataType().getByteSize()];
        }
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (sumBuffer == null) {
            return false;
        }
        int len = getDataType().toBytes(sum, sumBuffer, 0);
        ptr.set(sumBuffer, 0, len);
        return true;
    }
    
    @Override
    public final PDataType getDataType() {
        return PDecimal.INSTANCE;
    }
    
    @Override
    public void reset() {
        sum = BigDecimal.ZERO;
        sumBuffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "DECIMAL SUM [sum=" + sum + "]";
    }

    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.BIG_DECIMAL_SIZE + SizedUtil.ARRAY_SIZE + getDataType().getByteSize();
    }
}
