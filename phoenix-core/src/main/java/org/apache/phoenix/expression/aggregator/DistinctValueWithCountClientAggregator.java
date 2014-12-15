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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.iq80.snappy.Snappy;

/**
 * Client side Aggregator which will aggregate data and find distinct values with number of occurrences for each.
 * 
 * 
 * @since 1.2.1
 */
public abstract class DistinctValueWithCountClientAggregator extends BaseAggregator {
    protected Map<ImmutableBytesPtr, Integer> valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();
    protected byte[] buffer;
    protected long totalCount = 0L;
    protected Object cachedResult;

    public DistinctValueWithCountClientAggregator(SortOrder sortOrder) {
        super(sortOrder);
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (tuple instanceof SingleKeyValueTuple) {
            // Case when scanners do look ahead and re-aggregate result row.The result is already available in the ptr
            PDataType resultDataType = getResultDataType();
            cachedResult = resultDataType.toObject(ptr, resultDataType, sortOrder);
        } else {
            InputStream is;
            try {
                if (Bytes.equals(ptr.get(), ptr.getOffset(), 1, DistinctValueWithCountServerAggregator.COMPRESS_MARKER,
                        0, 1)) {
                    // This reads the uncompressed length from the front of the compressed input
                    int uncompressedLength = Snappy.getUncompressedLength(ptr.get(), ptr.getOffset() + 1);
                    byte[] uncompressed = new byte[uncompressedLength];
                    // This will throw CorruptionException, a RuntimeException if the snappy data is invalid.
                    // We're making a RuntimeException out of a checked IOException below so assume it's ok
                    // to let any CorruptionException escape.
                    Snappy.uncompress(ptr.get(), ptr.getOffset() + 1, ptr.getLength() - 1, uncompressed, 0);
                    is = new ByteArrayInputStream(uncompressed, 0, uncompressedLength);
                } else {
                    is = new ByteArrayInputStream(ptr.get(), ptr.getOffset() + 1, ptr.getLength() - 1);
                }
                DataInputStream in = new DataInputStream(is);
                int mapSize = WritableUtils.readVInt(in);
                for (int i = 0; i < mapSize; i++) {
                    int keyLen = WritableUtils.readVInt(in);
                    byte[] keyBytes = new byte[keyLen];
                    in.read(keyBytes, 0, keyLen);
                    ImmutableBytesPtr key = new ImmutableBytesPtr(keyBytes);
                    int value = WritableUtils.readVInt(in);
                    Integer curCount = valueVsCount.get(key);
                    if (curCount == null) {
                        valueVsCount.put(key, value);
                    } else {
                        valueVsCount.put(key, curCount + value);
                    }
                    totalCount += value;
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe); // Impossible as we're using a ByteArrayInputStream
            }
        }
        if (buffer == null) {
            initBuffer();
        }
    }

    protected void initBuffer() {
        buffer = new byte[getBufferLength()];
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public void reset() {
        valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();
        buffer = null;
        totalCount = 0L;
        cachedResult = null;
        super.reset();
    }
    
    protected Map<Object, Integer> getSortedValueVsCount(final boolean ascending, final PDataType type) {
        // To sort the valueVsCount
        Comparator<Object> comparator = new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                if (ascending) { 
                    return type.compareTo(o1, o2); 
                }
                return type.compareTo(o2, o1);
            }
        };
        Map<Object, Integer> sorted = new TreeMap<Object, Integer>(comparator);
        for (Entry<ImmutableBytesPtr, Integer> entry : valueVsCount.entrySet()) {
            sorted.put(type.toObject(entry.getKey(), sortOrder), entry.getValue());
        }
        return sorted;
    }

    protected int getBufferLength() {
        return getResultDataType().getByteSize();
    }

    protected abstract PDataType getResultDataType();
}
