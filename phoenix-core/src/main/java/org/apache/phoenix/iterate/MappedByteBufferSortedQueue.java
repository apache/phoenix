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
package org.apache.phoenix.iterate;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.iterate.OrderedResultIterator.ResultEntry;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ResultUtil;

import com.google.common.collect.MinMaxPriorityQueue;

public class MappedByteBufferSortedQueue extends MappedByteBufferQueue<ResultEntry> {
    private Comparator<ResultEntry> comparator;
    private final int limit;

    public MappedByteBufferSortedQueue(Comparator<ResultEntry> comparator,
            Integer limit, int thresholdBytes) throws IOException {
        super(thresholdBytes);
        this.comparator = comparator;
        this.limit = limit == null ? -1 : limit;
    }

    @Override
    protected org.apache.phoenix.iterate.MappedByteBufferQueue.MappedByteBufferSegmentQueue<ResultEntry> createSegmentQueue(
            int index, int thresholdBytes) {
        return new MappedByteBufferResultEntryPriorityQueue(index, thresholdBytes, limit, comparator);
    }

    @Override
    protected Comparator<org.apache.phoenix.iterate.MappedByteBufferQueue.MappedByteBufferSegmentQueue<ResultEntry>> getSegmentQueueComparator() {
        return new Comparator<MappedByteBufferSegmentQueue<ResultEntry>>() {
            @Override
            public int compare(MappedByteBufferSegmentQueue<ResultEntry> q1,
                    MappedByteBufferSegmentQueue<ResultEntry> q2) {
                return comparator.compare(q1.peek(), q2.peek());
            }};
    }

    private static class MappedByteBufferResultEntryPriorityQueue extends MappedByteBufferSegmentQueue<ResultEntry> {    	
        private MinMaxPriorityQueue<ResultEntry> results = null;
        
    	public MappedByteBufferResultEntryPriorityQueue(int index,
                int thresholdBytes, int limit, Comparator<ResultEntry> comparator) {
            super(index, thresholdBytes, limit >= 0);
            this.results = limit < 0 ? 
                    MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator).create()
                  : MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator).maximumSize(limit).create();
        }

        @Override
        protected Queue<ResultEntry> getInMemoryQueue() {
            return results;
        }

        @Override
        protected int sizeOf(ResultEntry e) {
            return sizeof(e.sortKeys) + sizeof(toKeyValues(e));
        }

        @SuppressWarnings("deprecation")
        @Override
        protected void writeToBuffer(MappedByteBuffer buffer, ResultEntry e) {
            int totalLen = 0;
            List<KeyValue> keyValues = toKeyValues(e);
            for (KeyValue kv : keyValues) {
                totalLen += (kv.getLength() + Bytes.SIZEOF_INT);
            }
            buffer.putInt(totalLen);
            for (KeyValue kv : keyValues) {
                buffer.putInt(kv.getLength());
                buffer.put(kv.getBuffer(), kv.getOffset(), kv
                        .getLength());
            }
            ImmutableBytesWritable[] sortKeys = e.sortKeys;
            buffer.putInt(sortKeys.length);
            for (ImmutableBytesWritable sortKey : sortKeys) {
                if (sortKey != null) {
                    buffer.putInt(sortKey.getLength());
                    buffer.put(sortKey.get(), sortKey.getOffset(),
                            sortKey.getLength());
                } else {
                    buffer.putInt(0);
                }
            }
        }

        @Override
        protected ResultEntry readFromBuffer(MappedByteBuffer buffer) {            
            int length = buffer.getInt();
            if (length < 0)
                return null;
            
            byte[] rb = new byte[length];
            buffer.get(rb);
            Result result = ResultUtil.toResult(new ImmutableBytesWritable(rb));
            ResultTuple rt = new ResultTuple(result);
            int sortKeySize = buffer.getInt();
            ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[sortKeySize];
            for (int i = 0; i < sortKeySize; i++) {
                int contentLength = buffer.getInt();
                if (contentLength > 0) {
                    byte[] sortKeyContent = new byte[contentLength];
                    buffer.get(sortKeyContent);
                    sortKeys[i] = new ImmutableBytesWritable(sortKeyContent);
                } else {
                    sortKeys[i] = null;
                }
            }
            
            return new ResultEntry(sortKeys, rt);
        }

        private List<KeyValue> toKeyValues(ResultEntry entry) {
            Tuple result = entry.getResult();
            int size = result.size();
            List<KeyValue> kvs = new ArrayList<KeyValue>(size);
            for (int i = 0; i < size; i++) {
                kvs.add(org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(result.getValue(i)));
            }
            return kvs;
        }

        private int sizeof(List<KeyValue> kvs) {
            int size = Bytes.SIZEOF_INT; // totalLen

            for (KeyValue kv : kvs) {
                size += kv.getLength();
                size += Bytes.SIZEOF_INT; // kv.getLength
            }

            return size;
        }

        private int sizeof(ImmutableBytesWritable[] sortKeys) {
            int size = Bytes.SIZEOF_INT;
            if (sortKeys != null) {
                for (ImmutableBytesWritable sortKey : sortKeys) {
                    if (sortKey != null) {
                        size += sortKey.getLength();
                    }
                    size += Bytes.SIZEOF_INT;
                }
            }
            return size;
        }
    }
}
