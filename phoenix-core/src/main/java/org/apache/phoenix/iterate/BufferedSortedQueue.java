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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ResultUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.MinMaxPriorityQueue;

public class BufferedSortedQueue extends BufferedQueue<ResultEntry> {
    private Comparator<ResultEntry> comparator;
    private final int limit;

    public BufferedSortedQueue(Comparator<ResultEntry> comparator,
            Integer limit, long thresholdBytes) throws IOException {
        super(thresholdBytes);
        this.comparator = comparator;
        this.limit = limit == null ? -1 : limit;
    }

    @Override
    protected BufferedSegmentQueue<ResultEntry> createSegmentQueue(
            int index, long thresholdBytes) {
        return new BufferedResultEntryPriorityQueue(index, thresholdBytes, limit, comparator);
    }

    @Override
    protected Comparator<BufferedSegmentQueue<ResultEntry>> getSegmentQueueComparator() {
        return new Comparator<BufferedSegmentQueue<ResultEntry>>() {
            @Override
            public int compare(BufferedSegmentQueue<ResultEntry> q1,
                    BufferedSegmentQueue<ResultEntry> q2) {
                return comparator.compare(q1.peek(), q2.peek());
            }};
    }

    private static class BufferedResultEntryPriorityQueue extends BufferedSegmentQueue<ResultEntry> {
        private MinMaxPriorityQueue<ResultEntry> results = null;
        
        public BufferedResultEntryPriorityQueue(int index,
                long thresholdBytes, int limit, Comparator<ResultEntry> comparator) {
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
        protected long sizeOf(ResultEntry e) {
            return ResultEntry.sizeOf(e);
        }

        @Override
        protected void writeToStream(DataOutputStream os, ResultEntry e) throws IOException {
            int totalLen = 0;
            List<KeyValue> keyValues = toKeyValues(e);
            for (KeyValue kv : keyValues) {
                totalLen += (kv.getLength() + Bytes.SIZEOF_INT);
            }
            os.writeInt(totalLen);
            for (KeyValue kv : keyValues) {
                os.writeInt(kv.getLength());
                os.write(kv.getBuffer(), kv.getOffset(), kv
                        .getLength());
            }
            ImmutableBytesWritable[] sortKeys = e.sortKeys;
            os.writeInt(sortKeys.length);
            for (ImmutableBytesWritable sortKey : sortKeys) {
                if (sortKey != null) {
                    os.writeInt(sortKey.getLength());
                    os.write(sortKey.get(), sortKey.getOffset(),
                            sortKey.getLength());
                } else {
                    os.writeInt(0);
                }
            }
        }

        @Override
        protected ResultEntry readFromStream(DataInputStream is) throws IOException {
            int length = is.readInt();
            if (length < 0)
                return null;

            byte[] rb = new byte[length];
            is.readFully(rb);
            Result result = ResultUtil.toResult(new ImmutableBytesWritable(rb));
            ResultTuple rt = new ResultTuple(result);
            int sortKeySize = is.readInt();
            ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[sortKeySize];
            for (int i = 0; i < sortKeySize; i++) {
                int contentLength = is.readInt();
                if (contentLength > 0) {
                    byte[] sortKeyContent = new byte[contentLength];
                    is.readFully(sortKeyContent);
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
                kvs.add(PhoenixKeyValueUtil.maybeCopyCell(result.getValue(i)));
            }
            return kvs;
        }

    }
}
