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
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SizedUtil;


/**
 * Aggregator that finds the min of values. Inverse of {@link MaxAggregator}.
 *
 * 
 * @since 0.1
 */
abstract public class MinAggregator extends BaseAggregator {
    /** Used to store the accumulate the results of the MIN function */
    protected final ImmutableBytesWritable value = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
    
    public MinAggregator(SortOrder sortOrder) {
        super(sortOrder);
    }

    @Override
    public void reset() {
        value.set(ByteUtil.EMPTY_BYTE_ARRAY);
        super.reset();
    }

    @Override
    public int getSize() {
        return super.getSize() + /*value*/ SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE;
    }

    /**
     * Compares two bytes writables, and returns true if the first one should be
     * kept, and false otherwise. For the MIN function, this method will return
     * true if the first bytes writable is less than the second.
     * 
     * @param ibw1 the first bytes writable
     * @param ibw2 the second bytes writable
     * @return true if the first bytes writable should be kept
     */
    protected boolean keepFirst(ImmutableBytesWritable ibw1, ImmutableBytesWritable ibw2) {
        return 0 >= getDataType().compareTo(ibw1, sortOrder, ibw2, sortOrder, getDataType());
    }

    private boolean isNull() {
        return value.get() == ByteUtil.EMPTY_BYTE_ARRAY;
    }
    
    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (isNull()) {
            value.set(ptr.get(), ptr.getOffset(), ptr.getLength());
        } else {
            if (!keepFirst(value, ptr)) {
                // replace the value with the new value
                value.set(ptr.get(), ptr.getOffset(), ptr.getLength());
            }
        }
    }
    
    @Override
    public String toString() {
        return "MIN [value=" + Bytes.toStringBinary(value.get(),value.getOffset(),value.getLength()) + "]";
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (isNull()) {
            return false;
        }
        ptr.set(value.get(), value.getOffset(), value.getLength());
        return true;
    }
}
