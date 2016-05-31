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

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.iterate.OrderedResultIterator.ResultEntry;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.util.ResultUtil;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;

public class DeferredByteBufferSortedQueue extends ByteBufferQueue<ResultEntry> {

    private Comparator<ResultEntry> comparator;
    private MemoryManager memoryManager;
    private final int limit;

    public DeferredByteBufferSortedQueue(Comparator<ResultEntry> comparator, MemoryManager memoryManager,
                                         Integer limit, int thresholdBytes) throws IOException {
        super(thresholdBytes);
        this.memoryManager = memoryManager;
        this.comparator = comparator;
        this.limit = limit == null ? -1 : limit;
    }

    @Override
    protected DeferredByteBufferSegmentQueue<ResultEntry> createSegmentQueue(
            int index, int thresholdBytes) {
        return new DeferredByteBufferResultEntryPriorityQueue(index, thresholdBytes,
                limit, comparator, memoryManager);
    }

    @Override
    protected Comparator<BufferSegmentQueue<ResultEntry>> getSegmentQueueComparator() {
        return new Comparator<BufferSegmentQueue<ResultEntry>>() {
            @Override
            public int compare(BufferSegmentQueue<ResultEntry> q1,
                               BufferSegmentQueue<ResultEntry> q2) {
                return comparator.compare(q1.peek(), q2.peek());
            }};
    }

    private static class DeferredByteBufferResultEntryPriorityQueue extends DeferredByteBufferSegmentQueue<ResultEntry> {
        private MinMaxPriorityQueue<ResultEntry> results = null;

        
    	public DeferredByteBufferResultEntryPriorityQueue(int index, int thresholdBytes, int limit,
                                                          Comparator<ResultEntry> comparator, MemoryManager memoryManager) {
            super(index, thresholdBytes, limit >= 0, memoryManager);
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

        @Override
        protected void writeToBuffer(OutputStream outputStream, ResultEntry e) {
            try {
                int totalLen = 0;
                List<KeyValue> keyValues = toKeyValues(e);
                for (KeyValue kv : keyValues) {
                    totalLen += (kv.getLength() + Bytes.SIZEOF_INT);
                }
                for (KeyValue kv : keyValues) {
                    outputStream.write(kv.getLength());
                    outputStream.write(kv.getBuffer(), kv.getOffset(), kv
                            .getLength());
                }
                ImmutableBytesWritable[] sortKeys = e.sortKeys;
                outputStream.write(sortKeys.length);
                for (ImmutableBytesWritable sortKey : sortKeys) {
                    if (sortKey != null) {
                        outputStream.write(sortKey.getLength());
                        outputStream.write(sortKey.get(), sortKey.getOffset(),
                                sortKey.getLength());
                    } else {
                        outputStream.write(0);
                    }
                }
                outputStream.write(totalLen);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

        }

        @Override
        protected ResultEntry readFromBuffer(DataInput dataInput) {
            try {
                int length = WritableUtils.readVInt(dataInput);

                if (length < 0)
                    return null;

                byte[] rb = new byte[length];
                dataInput.readFully(rb);
                Result result = ResultUtil.toResult(new ImmutableBytesWritable(rb));
                ResultTuple rt = new ResultTuple(result);
                int sortKeySize = WritableUtils.readVInt(dataInput);
                ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[sortKeySize];
                for (int i = 0; i < sortKeySize; i++) {
                    int contentLength = WritableUtils.readVInt(dataInput);
                    if (contentLength > 0) {
                        byte[] sortKeyContent = new byte[contentLength];
                        dataInput.readFully(sortKeyContent);
                        sortKeys[i] = new ImmutableBytesWritable(sortKeyContent);
                    } else {
                        sortKeys[i] = null;
                    }
                }

                return new ResultEntry(sortKeys, rt);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

    }


}
