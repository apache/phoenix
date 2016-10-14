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
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ResultUtil;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SpoolingByteBufferSortedQueue extends SpoolingByteBufferQueue<ResultEntry> {
    private Comparator<ResultEntry> comparator;
    private final int limit;

    private MemoryManager mm;
    private String spoolDirectory;


    public SpoolingByteBufferSortedQueue(MemoryManager mm, final int thresholdBytes, String spoolDirectory,
                                         Comparator<ResultEntry> comparator, Integer limit) throws IOException {
        super(thresholdBytes);
        this.comparator = comparator;

        this.mm = mm;
        this.spoolDirectory = spoolDirectory;
        this.limit = limit == null ? -1 : limit;
    }

    @Override
    protected SpoolingByteBufferSegmentQueue<ResultEntry> createSegmentQueue(int index) {
        return new SpoolingByteBufferResultEntryPriorityQueue(index, mm,
                thresholdBytes, spoolDirectory, comparator, limit);
    }

    @Override
    protected Comparator<SpoolingByteBufferSegmentQueue<ResultEntry>> getSegmentQueueComparator() {
        return new Comparator<SpoolingByteBufferSegmentQueue<ResultEntry>>() {
            @Override
            public int compare(SpoolingByteBufferSegmentQueue<ResultEntry> q1,
                               SpoolingByteBufferSegmentQueue<ResultEntry> q2) {
                return comparator.compare(q1.peek(), q2.peek());
            }};
    }

    private static class SpoolingByteBufferResultEntryPriorityQueue extends SpoolingByteBufferSegmentQueue<ResultEntry> {
        private MinMaxPriorityQueue<ResultEntry> results = null;
        
    	public SpoolingByteBufferResultEntryPriorityQueue(int index,  MemoryManager mm, final int thresholdBytes,
                                                          String spoolDirectory, Comparator<ResultEntry> comparator, int limit) {
            super(index, mm, thresholdBytes, spoolDirectory);
            this.results = limit < 0 ? 
                    MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator).create()
                  : MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator).maximumSize(limit).create();
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





        @Override
        protected InMemoryResultQueue<ResultEntry> createInMemoryResultQueue(byte[] bytes,
                                                                             MemoryManager.MemoryChunk memoryChunk) {
            return new InMemoryResultQueue<ResultEntry>(bytes, memoryChunk) {
                @Override
                protected ResultEntry advance(AtomicInteger offset) {


                    if (offset.get() < bytes.length) {

                        int length = ByteUtil.vintFromBytes(bytes, offset.get());
                        offset.addAndGet(WritableUtils.getVIntSize(length));

                        byte[] rb = new byte[length];
                        System.arraycopy(bytes, offset.get(), rb, 0, length);
                        offset.addAndGet(length);

                        Result result = ResultUtil.toResult(new ImmutableBytesWritable(rb));
                        ResultTuple rt = new ResultTuple(result);

                        int sortKeySize = ByteUtil.vintFromBytes(bytes, offset.get());
                        offset.addAndGet(WritableUtils.getVIntSize(sortKeySize));
                        ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[sortKeySize];



                        for (int i = 0; i < sortKeySize; i++) {
                            int contentLength = ByteUtil.vintFromBytes(bytes, offset.get());
                            offset.addAndGet(WritableUtils.getVIntSize(contentLength));
                            if (contentLength > 0) {
                                byte[] sortKeyContent = new byte[contentLength];
                                System.arraycopy(bytes, offset.get(), sortKeyContent, 0, contentLength);
                                offset.addAndGet(contentLength);
                                sortKeys[i] = new ImmutableBytesWritable(sortKeyContent);
                            } else {
                                sortKeys[i] = null;
                            }
                        }

                        return new ResultEntry(sortKeys, rt);
                    }

                    return null;
                }
            };
        }

        @Override
        protected OnDiskResultQueue<ResultEntry> createOnDiskResultQueue(File file) {

            return new OnDiskResultQueue<ResultEntry>(file){
                @Override
                protected ResultEntry advance(DataInputStream dataInput) {
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
            };
        }

        @Override
        protected boolean writeRecord(ResultEntry e, OutputStream outputStream) {
            boolean result;
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
                result = true;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return result;
        }

    }
}
