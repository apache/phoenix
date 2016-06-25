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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.SizedUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 * Aggregator that sums integral number values distinct
 * 
 *
 */
abstract public class NumberSumDistinctAggregator extends BaseAggregator {
    private long sum = 0;
    private byte[] buffer;
    protected Set<Long> set = new HashSet<>();

    public NumberSumDistinctAggregator(SortOrder sortOrder) {
        super(sortOrder);
    }

    public NumberSumDistinctAggregator(SortOrder sortOrder, ImmutableBytesWritable ptr) {
        this(sortOrder);
        if (ptr != null) {
            initBuffer();
            sum = PLong.INSTANCE.getCodec().decodeLong(ptr, sortOrder);
            set.add(sum);
        }
    }

    public long getSum() {
        return sum;
    }

    abstract protected PDataType getInputDataType();

    private int getBufferLength() {
        return getDataType().getByteSize();
    }

    private void initBuffer() {
        buffer = new byte[getBufferLength()];
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        // Get either IntNative or LongNative depending on input type
        Long value = getInputDataType().getCodec().decodeLong(ptr, sortOrder);
        if (!set.contains(value)) {
            sum += value;
            if (buffer == null) {
                initBuffer();
            }
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (buffer == null) {
            if (isNullable()) {
                return false;
            }
            initBuffer();
        }
        getDataType().getCodec().encodeLong(sum, buffer, 0);
        ptr.set(buffer);
        return true;
    }

    @Override
    public final PDataType getDataType() {
        return PLong.INSTANCE;
    }

    @Override
    public void reset() {
        sum = 0;
        buffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "DISTINCT SUM [sum=" + sum + "]";
    }

    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.LONG_SIZE + SizedUtil.ARRAY_SIZE
                + getBufferLength();
    }

}
