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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.iterate.OrderedResultIterator.ResultEntry;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ResultUtil;

import com.google.common.collect.MinMaxPriorityQueue;

public class MappedByteBufferSortedQueue extends AbstractQueue<ResultEntry> {
    private Comparator<ResultEntry> comparator;
    private final int limit;
    private final int thresholdBytes;
    private List<MappedByteBufferPriorityQueue> queues = new ArrayList<MappedByteBufferPriorityQueue>();
    private MappedByteBufferPriorityQueue currentQueue = null;
    private int currentIndex = 0;
    MinMaxPriorityQueue<IndexedResultEntry> mergedQueue = null;

    public MappedByteBufferSortedQueue(Comparator<ResultEntry> comparator,
            Integer limit, int thresholdBytes) throws IOException {
        this.comparator = comparator;
        this.limit = limit == null ? -1 : limit;
        this.thresholdBytes = thresholdBytes;
        this.currentQueue = new MappedByteBufferPriorityQueue(0,
                this.limit, thresholdBytes, comparator);
        this.queues.add(currentQueue);
    }

    @Override
    public boolean offer(ResultEntry e) {
        try {
            boolean isFlush = this.currentQueue.writeResult(e);
            if (isFlush) {
                currentIndex++;
                currentQueue = new MappedByteBufferPriorityQueue(currentIndex,
                        limit, thresholdBytes, comparator);
                queues.add(currentQueue);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        return true;
    }

    @Override
    public ResultEntry poll() {
        if (mergedQueue == null) {
            mergedQueue = MinMaxPriorityQueue.<ResultEntry> orderedBy(
                    comparator).maximumSize(queues.size()).create();
            for (MappedByteBufferPriorityQueue queue : queues) {
                try {
                    IndexedResultEntry next = queue.getNextResult();
                    if (next != null) {
                        mergedQueue.add(next);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (!mergedQueue.isEmpty()) {
            IndexedResultEntry re = mergedQueue.pollFirst();
            if (re != null) {
                IndexedResultEntry next = null;
                try {
                    next = queues.get(re.getIndex()).getNextResult();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (next != null) {
                    mergedQueue.add(next);
                }
                return re;
            }
        }
        return null;
    }

    @Override
    public ResultEntry peek() {
        if (mergedQueue == null) {
            mergedQueue = MinMaxPriorityQueue.<ResultEntry> orderedBy(
                    comparator).maximumSize(queues.size()).create();
            for (MappedByteBufferPriorityQueue queue : queues) {
                try {
                    IndexedResultEntry next = queue.getNextResult();
                    if (next != null) {
                        mergedQueue.add(next);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (!mergedQueue.isEmpty()) {
            IndexedResultEntry re = mergedQueue.peekFirst();
            if (re != null) {
                return re;
            }
        }
        return null;
    }

    @Override
    public Iterator<ResultEntry> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        int size = 0;
        for (MappedByteBufferPriorityQueue queue : queues) {
            size += queue.size();
        }
        return size;
    }
    
    public long getByteSize() {
        return currentQueue.getInMemByteSize();
    }

    public void close() {
        if (queues != null) {
            for (MappedByteBufferPriorityQueue queue : queues) {
                queue.close();
            }
        }
    }

    private static class IndexedResultEntry extends ResultEntry {
        private int index;

        public IndexedResultEntry(int index, ResultEntry resultEntry) {
            super(resultEntry.sortKeys, resultEntry.result);
            this.index = index;
        }

        public int getIndex() {
            return this.index;
        }
    }

    private static class MappedByteBufferPriorityQueue {
        private static final long DEFAULT_MAPPING_SIZE = 1024;
        
        private final int limit;
        private final int thresholdBytes;
        private long totalResultSize = 0;
        private int maxResultSize = 0;
        private long mappingSize = 0;
        private long writeIndex = 0;
        private long readIndex = 0;
        private MappedByteBuffer writeBuffer;
        private MappedByteBuffer readBuffer;
        private FileChannel fc;
        private RandomAccessFile af;
        private File file;
        private boolean isClosed = false;
        MinMaxPriorityQueue<ResultEntry> results = null;
        private boolean flushBuffer = false;
        private int index;
        private int flushedCount;

        public MappedByteBufferPriorityQueue(int index, int limit, int thresholdBytes,
                Comparator<ResultEntry> comparator) throws IOException {
            this.index = index;
            this.limit = limit;
            this.thresholdBytes = thresholdBytes;
            results = limit < 0 ? 
                    MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator).create()
                  : MinMaxPriorityQueue.<ResultEntry> orderedBy(comparator).maximumSize(limit).create();
        }
        
        public int size() {
            if (flushBuffer)
                return flushedCount;
            return results.size();
        }
        
        public long getInMemByteSize() {
            if (flushBuffer)
                return 0;
            return totalResultSize;
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

        @SuppressWarnings("deprecation")
        public boolean writeResult(ResultEntry entry) throws IOException {
            if (flushBuffer)
                throw new IOException("Results already flushed");
            
            int sortKeySize = sizeof(entry.sortKeys);
            int resultSize = sizeof(toKeyValues(entry)) + sortKeySize;
            boolean added = results.add(entry);
            if (added) {
                maxResultSize = Math.max(maxResultSize, resultSize);
                totalResultSize = limit < 0 ? (totalResultSize + resultSize) : maxResultSize * results.size();
                if (totalResultSize >= thresholdBytes) {
                    this.file = File.createTempFile(UUID.randomUUID().toString(), null);
                    this.af = new RandomAccessFile(file, "rw");
                    this.fc = af.getChannel();
                    mappingSize = Math.min(Math.max(maxResultSize, DEFAULT_MAPPING_SIZE), totalResultSize);
                    writeBuffer = fc.map(MapMode.READ_WRITE, writeIndex, mappingSize);
                
                    int resSize = results.size();
                    for (int i = 0; i < resSize; i++) {                
                        int totalLen = 0;
                        ResultEntry re = results.pollFirst();
                        List<KeyValue> keyValues = toKeyValues(re);
                        for (KeyValue kv : keyValues) {
                            totalLen += (kv.getLength() + Bytes.SIZEOF_INT);
                        }
                        writeBuffer.putInt(totalLen);
                        for (KeyValue kv : keyValues) {
                            writeBuffer.putInt(kv.getLength());
                            writeBuffer.put(kv.getBuffer(), kv.getOffset(), kv
                                    .getLength());
                        }
                        ImmutableBytesWritable[] sortKeys = re.sortKeys;
                        writeBuffer.putInt(sortKeys.length);
                        for (ImmutableBytesWritable sortKey : sortKeys) {
                            if (sortKey != null) {
                                writeBuffer.putInt(sortKey.getLength());
                                writeBuffer.put(sortKey.get(), sortKey.getOffset(),
                                        sortKey.getLength());
                            } else {
                                writeBuffer.putInt(0);
                            }
                        }
                        // buffer close to exhausted, re-map.
                        if (mappingSize - writeBuffer.position() < maxResultSize) {
                            writeIndex += writeBuffer.position();
                            writeBuffer = fc.map(MapMode.READ_WRITE, writeIndex, mappingSize);
                        }
                    }
                    writeBuffer.putInt(-1); // end
                    flushedCount = results.size();
                    results.clear();
                    flushBuffer = true;
                }
            }
            return flushBuffer;
        }

        public IndexedResultEntry getNextResult() throws IOException {
            if (isClosed)
                return null;
            
            if (!flushBuffer) {
                ResultEntry re = results.poll();
                if (re == null) {
                    reachedEnd();
                    return null;
                }
                return new IndexedResultEntry(index, re);
            }
            
            if (readBuffer == null) {
                readBuffer = this.fc.map(MapMode.READ_ONLY, readIndex, mappingSize);
            }
            
            int length = readBuffer.getInt();
            if (length < 0) {
                reachedEnd();
                return null;
            }
            
            byte[] rb = new byte[length];
            readBuffer.get(rb);
            Result result = ResultUtil.toResult(new ImmutableBytesWritable(rb));
            ResultTuple rt = new ResultTuple(result);
            int sortKeySize = readBuffer.getInt();
            ImmutableBytesWritable[] sortKeys = new ImmutableBytesWritable[sortKeySize];
            for (int i = 0; i < sortKeySize; i++) {
                int contentLength = readBuffer.getInt();
                if (contentLength > 0) {
                    byte[] sortKeyContent = new byte[contentLength];
                    readBuffer.get(sortKeyContent);
                    sortKeys[i] = new ImmutableBytesWritable(sortKeyContent);
                } else {
                    sortKeys[i] = null;
                }
            }
            // buffer close to exhausted, re-map.
            if (mappingSize - readBuffer.position() < maxResultSize) {
                readIndex += readBuffer.position();
                readBuffer = fc.map(MapMode.READ_ONLY, readIndex, mappingSize);
            }
            
            return new IndexedResultEntry(index, new ResultEntry(sortKeys, rt));
        }

        private void reachedEnd() {
            this.isClosed = true;
            if (this.fc != null) {
                try {
                    this.fc.close();
                } catch (IOException ignored) {
                }
                this.fc = null;
            }
            if (this.af != null) {
                try {
                    this.af.close();
                } catch (IOException ignored) {
                }
                this.af = null;
            }
            if (this.file != null) {
                file.delete();
                file = null;
            }
        }

        public void close() {
            if (!isClosed) {
                this.reachedEnd();
            }
        }
    }
}
