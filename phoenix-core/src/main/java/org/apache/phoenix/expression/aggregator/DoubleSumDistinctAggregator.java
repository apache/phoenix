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
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.util.SizedUtil;

import java.util.HashSet;
import java.util.Set;

public class DoubleSumDistinctAggregator extends BaseAggregator {

    private double sum = 0;
    private byte[] buffer;
    protected Set<Double> set = new HashSet<>();

    public DoubleSumDistinctAggregator(SortOrder sortOrder, ImmutableBytesWritable ptr) {
        super(sortOrder);
        if (ptr != null) {
            initBuffer();
            sum = PDouble.INSTANCE.getCodec().decodeDouble(ptr, sortOrder);
            set.add(sum);
        }
    }
    
    protected PDataType getInputDataType() {
        return PDouble.INSTANCE;
    }
    
    private void initBuffer() {
        buffer = new byte[getDataType().getByteSize()];
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        Double value = getInputDataType().getCodec().decodeDouble(ptr, sortOrder);
        if (!set.contains(value)) {
            sum += value;
            set.add(value);
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
        getDataType().getCodec().encodeDouble(sum, buffer, 0);
        ptr.set(buffer);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDouble.INSTANCE;
    }
    
    @Override
    public String toString() {
        return "DISTINCT SUM [sum=" + sum + "]";
    }
    
    @Override
    public void reset() {
        sum = 0;
        buffer = null;
        super.reset();
    }
    
    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.LONG_SIZE + SizedUtil.ARRAY_SIZE + getDataType().getByteSize();
    }

}
