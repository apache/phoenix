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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SizedUtil;
import org.iq80.snappy.Snappy;

/**
 * Server side Aggregator which will aggregate data and find distinct values with number of occurrences for each.
 * 
 * 
 * @since 1.2.1
 */
public class DistinctValueWithCountServerAggregator extends BaseAggregator {
    public static final int DEFAULT_ESTIMATED_DISTINCT_VALUES = 10000;
    public static final byte[] COMPRESS_MARKER = new byte[] { (byte)1 };

    private int compressThreshold;
    private byte[] buffer = null;
    protected Map<ImmutableBytesPtr, Integer> valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();

    public DistinctValueWithCountServerAggregator(Configuration conf) {
        super(SortOrder.getDefault());
        compressThreshold = conf.getInt(QueryServices.DISTINCT_VALUE_COMPRESS_THRESHOLD_ATTRIB,
                QueryServicesOptions.DEFAULT_DISTINCT_VALUE_COMPRESS_THRESHOLD);
    }

    public DistinctValueWithCountServerAggregator(Configuration conf, DistinctValueWithCountClientAggregator clientAgg) {
        this(conf);
        valueVsCount = clientAgg.valueVsCount;
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        ImmutableBytesPtr key = new ImmutableBytesPtr(ptr.get(), ptr.getOffset(), ptr.getLength());
        Integer count = this.valueVsCount.get(key);
        if (count == null) {
            this.valueVsCount.put(key, 1);
        } else {
            this.valueVsCount.put(key, ++count);
        }
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // This serializes the Map. The format is as follows
        // Map size(VInt ie. 1 to 5 bytes) +
        // ( key length [VInt ie. 1 to 5 bytes] + key bytes + value [VInt ie. 1 to 5 bytes] )*
        int serializationSize = countMapSerializationSize();
        buffer = new byte[serializationSize];
        int offset = 1;
        offset += ByteUtil.vintToBytes(buffer, offset, this.valueVsCount.size());
        for (Entry<ImmutableBytesPtr, Integer> entry : this.valueVsCount.entrySet()) {
            ImmutableBytesPtr key = entry.getKey();
            offset += ByteUtil.vintToBytes(buffer, offset, key.getLength());
            System.arraycopy(key.get(), key.getOffset(), buffer, offset, key.getLength());
            offset += key.getLength();
            offset += ByteUtil.vintToBytes(buffer, offset, entry.getValue().intValue());
        }
        if (serializationSize > compressThreshold) {
            // The size for the map serialization is above the threshold. We will do the Snappy compression here.
            byte[] compressed = new byte[COMPRESS_MARKER.length + Snappy.maxCompressedLength(buffer.length)];
            System.arraycopy(COMPRESS_MARKER, 0, compressed, 0, COMPRESS_MARKER.length);
            int compressedLen = Snappy.compress(buffer, 1, buffer.length - 1, compressed, COMPRESS_MARKER.length);
            ptr.set(compressed, 0, compressedLen + 1);
            return true;
        }
        ptr.set(buffer, 0, offset);
        return true;
    }

    // The #bytes required to serialize the count map.
    // Here let us assume to use 4 bytes for each of the int items. Normally it will consume lesser
    // bytes as we will use vints.
    // TODO Do we need to consider 5 as the number of bytes for each of the int field? Else there is
    // a chance of ArrayIndexOutOfBoundsException when all the int fields are having very large
    // values. Will that ever occur?
    private int countMapSerializationSize() {
        int size = Bytes.SIZEOF_INT;// Write the number of entries in the Map
        for (ImmutableBytesPtr key : this.valueVsCount.keySet()) {
            // Add up the key and key's lengths (Int) and the value
            size += key.getLength() + Bytes.SIZEOF_INT + Bytes.SIZEOF_INT;
        }
        return size;
    }

    // The heap size which will be taken by the count map.
    private int countMapHeapSize() {
        int size = 0;
        if (this.valueVsCount.size() > 0) {
            for (ImmutableBytesPtr key : this.valueVsCount.keySet()) {
                size += SizedUtil.MAP_ENTRY_SIZE + // entry
                        Bytes.SIZEOF_INT + // key size
                        key.getLength() + SizedUtil.ARRAY_SIZE; // value size
            }
        } else {
            // Initially when the getSize() is called, we dont have any entries in the map so as to
            // tell the exact heap need. Let us approximate the #entries
            SizedUtil.sizeOfMap(DEFAULT_ESTIMATED_DISTINCT_VALUES,
                    SizedUtil.IMMUTABLE_BYTES_PTR_SIZE, Bytes.SIZEOF_INT);
        }
        return size;
    }

    @Override
    public final PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public void reset() {
        valueVsCount = new HashMap<ImmutableBytesPtr, Integer>();
        buffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "DISTINCT VALUE vs COUNT";
    }

    @Override
    public int getSize() {
        // TODO make this size correct.??
        // This size is being called initially at the begin of the scanner open. At that time we any
        // way can not tell the exact size of the Map. The Aggregators get size from all Aggregator
        // and stores in a variable for future use. This size of the Aggregators is being used in
        // Grouped unordered scan. Do we need some changes there in that calculation?
        return super.getSize() + SizedUtil.ARRAY_SIZE + countMapHeapSize();
    }
}
