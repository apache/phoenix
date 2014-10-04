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
package org.apache.phoenix.cache.aggcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.TupleUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class servers as an adapter between the in-memory LRU cache and the Spill data structures. It
 * takes care of serializing / deserializing the key/value groupby tuples, and spilling them to the
 * correct spill partition
 */
public class SpillManager implements Closeable {

    // Wrapper class for DESERIALIZED groupby key/value tuples
    public static class CacheEntry<T extends ImmutableBytesWritable> implements
            Map.Entry<T, Aggregator[]> {

        protected T key;
        protected Aggregator[] aggs;

        public CacheEntry(T key, Aggregator[] aggs) {
            this.key = key;
            this.aggs = aggs;
        }

        public Aggregator[] getValue(Configuration conf) {
            return aggs;
        }

        public int getKeyLength() {
            return key.getLength();
        }

        @Override
        public Aggregator[] getValue() {
            return aggs;
        }

        @Override
        public Aggregator[] setValue(Aggregator[] arg0) {
            this.aggs = arg0;
            return aggs;
        }

        @Override
        public T getKey() {
            return key;
        }

    }

    private final ArrayList<SpillMap> spillMaps;
    private final int numSpillFiles;

    private final ServerAggregators aggregators;
    private final Configuration conf;

    /**
     * SpillManager takes care of spilling and loading tuples from spilled data structs
     * @param numSpillFiles
     * @param serverAggregators
     */
    public SpillManager(int numSpillFiles, ServerAggregators serverAggregators,
            Configuration conf, SpillableGroupByCache.QueryCache cache) {
        try {
            int estValueSize = serverAggregators.getEstimatedByteSize();
            spillMaps = Lists.newArrayList();
            this.numSpillFiles = numSpillFiles;
            this.aggregators = serverAggregators;
            this.conf = conf;
            File spillFilesDir = conf.get(QueryServices.SPOOL_DIRECTORY) != null ?
                new File(conf.get(QueryServices.SPOOL_DIRECTORY)) : null;
            
            // Ensure that a single element fits onto a page!!!
            Preconditions.checkArgument(SpillFile.DEFAULT_PAGE_SIZE > estValueSize);

            // Create a list of spillFiles
            // Each Spillfile only handles up to 2GB data
            for (int i = 0; i < numSpillFiles; i++) {
                SpillFile file = SpillFile.createSpillFile(spillFilesDir);
                spillMaps.add(new SpillMap(file, SpillFile.DEFAULT_PAGE_SIZE, estValueSize, cache));
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not init the SpillManager");
        }
    }

    // serialize a key/value tuple into a byte array
    // WARNING: expensive
    private byte[] serialize(ImmutableBytesPtr key, Aggregator[] aggs,
            ServerAggregators serverAggs) throws IOException {

        DataOutputStream output = null;
        ByteArrayOutputStream bai = null;
        try {
            bai = new ByteArrayOutputStream();
            output = new DataOutputStream(bai);
            // key length
            WritableUtils.writeVInt(output, key.getLength());
            // key
            output.write(key.get(), key.getOffset(), key.getLength());
            byte[] aggsByte = serverAggs.toBytes(aggs);
            // aggs length
            WritableUtils.writeVInt(output, aggsByte.length);
            // aggs
            output.write(aggsByte);
            return bai.toByteArray();
        } finally {

            if (bai != null) {
                bai.close();
                bai = null;
            }
            if (output != null) {
                output.close();
                output = null;
            }
        }
    }

    /**
     * Helper method to deserialize the key part from a serialized byte array
     * @param data
     * @return
     * @throws IOException
     */
    static ImmutableBytesPtr getKey(byte[] data) throws IOException {
        DataInputStream input = null;
        try {
            input = new DataInputStream(new ByteArrayInputStream(data));
            // key length
            int keyLength = WritableUtils.readVInt(input);
            int offset = WritableUtils.getVIntSize(keyLength);
            // key
            return new ImmutableBytesPtr(data, offset, keyLength);
        } finally {
            if (input != null) {
                input.close();
                input = null;
            }
        }
    }

    
    // Instantiate Aggregators from a serialized byte array
    private Aggregator[] getAggregators(byte[] data) throws IOException {
        DataInputStream input = null;
        try {
            input = new DataInputStream(new ByteArrayInputStream(data));
            // key length
            int keyLength = WritableUtils.readVInt(input);
            int vIntKeyLength = WritableUtils.getVIntSize(keyLength);
            ImmutableBytesPtr ptr = new ImmutableBytesPtr(data, vIntKeyLength, keyLength);

            // value length
            input.skip(keyLength);
            int valueLength = WritableUtils.readVInt(input);
            int vIntValLength = WritableUtils.getVIntSize(keyLength);
            KeyValue keyValue =
                    KeyValueUtil.newKeyValue(ptr.get(), ptr.getOffset(), ptr.getLength(),
                        QueryConstants.SINGLE_COLUMN_FAMILY, QueryConstants.SINGLE_COLUMN,
                        QueryConstants.AGG_TIMESTAMP, data, vIntKeyLength + keyLength + vIntValLength, valueLength);
            Tuple result = new SingleKeyValueTuple(keyValue);
            TupleUtil.getAggregateValue(result, ptr);
            KeyValueSchema schema = aggregators.getValueSchema();
            ValueBitSet tempValueSet = ValueBitSet.newInstance(schema);
            tempValueSet.clear();
            tempValueSet.or(ptr);

            int i = 0, maxOffset = ptr.getOffset() + ptr.getLength();
            SingleAggregateFunction[] funcArray = aggregators.getFunctions();
            Aggregator[] sAggs = new Aggregator[funcArray.length];
            Boolean hasValue;
            schema.iterator(ptr);
            while ((hasValue = schema.next(ptr, i, maxOffset, tempValueSet)) != null) {
                SingleAggregateFunction func = funcArray[i];
                sAggs[i++] =
                        hasValue ? func.newServerAggregator(conf, ptr) : func
                                .newServerAggregator(conf);
            }
            return sAggs;

        } finally {
            Closeables.closeQuietly(input);
        }
    }

    /**
     * Helper function to deserialize a byte array into a CacheEntry
     * @param <K>
     * @param bytes
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public <K extends ImmutableBytesWritable> CacheEntry<K> toCacheEntry(byte[] bytes)
            throws IOException {
        ImmutableBytesPtr key = SpillManager.getKey(bytes);
        Aggregator[] aggs = getAggregators(bytes);

        return new CacheEntry<K>((K) key, aggs);
    }

    // Determines the partition, i.e. spillFile the tuple should get spilled to.
    private int getPartition(ImmutableBytesWritable key) {
        // Simple implementation hash mod numFiles
        return Math.abs(key.hashCode()) % numSpillFiles;
    }

    /**
     * Function that spills a key/value groupby tuple into a partition Spilling always triggers a
     * serialize call
     * @param key
     * @param value
     * @throws IOException
     */
    public void spill(ImmutableBytesWritable key, Aggregator[] value) throws IOException {
        SpillMap spillMap = spillMaps.get(getPartition(key));
        ImmutableBytesPtr keyPtr = new ImmutableBytesPtr(key);
        byte[] data = serialize(keyPtr, value, aggregators);
        spillMap.put(keyPtr, data);
    }

    /**
     * Function that loads a spilled key/value groupby tuple from one of the spill partitions into
     * the LRU cache. Loading always involves deserialization
     * @throws IOException
     */
    public Aggregator[] loadEntry(ImmutableBytesWritable key) throws IOException {
        SpillMap spillMap = spillMaps.get(getPartition(key));
        byte[] data = spillMap.get(key);
        if (data != null) {
            return getAggregators(data);
        }
        return null;
    }

    /**
     * Close the attached spillMap
     */
    @Override
    public void close() {
        for (int i = 0; i < spillMaps.size(); i++) {
            Closeables.closeQuietly(spillMaps.get(i).getSpillFile());
        }
    }

    /**
     * Function returns an iterator over all spilled Tuples
     */
    public SpillMapIterator newDataIterator() {
        return new SpillMapIterator();
    }

    private final class SpillMapIterator implements Iterator<byte[]> {

        int index = 0;
        Iterator<byte[]> spillIter = spillMaps.get(index).iterator();

        @Override
        public boolean hasNext() {
            if (!spillIter.hasNext()) {
                if (index < (numSpillFiles - 1)) {
                    // Current spillFile exhausted get iterator over new one
                    spillIter = spillMaps.get(++index).iterator();
                }
            }
            return spillIter.hasNext();
        }

        @Override
        public byte[] next() {
            return spillIter.next();
        }

        @Override
        public void remove() {
            throw new IllegalAccessError("Remove is not supported for this type of iterator");
        }
    }
}
