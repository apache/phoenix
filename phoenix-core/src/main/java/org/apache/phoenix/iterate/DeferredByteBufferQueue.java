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

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.phoenix.memory.MemoryManager;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;


public abstract class DeferredByteBufferQueue<T> extends AbstractQueue<T> {
    protected MemoryManager mm;
    protected String spoolDirectory;
    protected final int thresholdBytes;
    private List<DeferredByteBufferSegmentQueue<T>> queues;
    private int currentIndex;
    private DeferredByteBufferSegmentQueue<T> currentQueue;
    private MinMaxPriorityQueue<DeferredByteBufferSegmentQueue<T>> mergedQueue;

    public DeferredByteBufferQueue(MemoryManager mm, int thresholdBytes,String spoolDirectory) {
        this.thresholdBytes = thresholdBytes;
        this.mm = mm;
        this.spoolDirectory = spoolDirectory;
        this.queues = Lists.<DeferredByteBufferSegmentQueue<T>> newArrayList();
        this.currentIndex = -1;
        this.currentQueue = null;
        this.mergedQueue = null;
    }
    
    abstract protected DeferredByteBufferSegmentQueue<T> createSegmentQueue(int index);
    
    abstract protected Comparator<DeferredByteBufferSegmentQueue<T>> getSegmentQueueComparator();
    
    protected final List<DeferredByteBufferSegmentQueue<T>> getSegmentQueues() {
        return queues.subList(0, currentIndex + 1);
    }

    @Override
    public boolean offer(T e) {
        boolean startNewQueue = this.currentQueue == null || this.currentQueue.isFlushed();
        if (startNewQueue) {
            currentIndex++;
            if (currentIndex < queues.size()) {
                currentQueue = queues.get(currentIndex);
            } else {
                currentQueue = createSegmentQueue(currentIndex);
                queues.add(currentQueue);
            }
        }

        return this.currentQueue.offer(e);
    }

    @Override
    public T poll() {
        initMergedQueue();
        if (mergedQueue != null && !mergedQueue.isEmpty()) {
            DeferredByteBufferSegmentQueue<T> queue = mergedQueue.poll();
            T re = queue.poll();
            if (queue.peek() != null) {
                mergedQueue.add(queue);
            }
            return re;
        }
        return null;
    }

    @Override
    public T peek() {
        initMergedQueue();
        if (mergedQueue != null && !mergedQueue.isEmpty()) {
            return mergedQueue.peek().peek();
        }
        return null;
    }
    
    @Override
    public void clear() {
        for (DeferredByteBufferSegmentQueue<T> queue : getSegmentQueues()) {
            queue.clear();
        }
        currentIndex = -1;
        currentQueue = null;
        mergedQueue = null;
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        int size = 0;
        for (DeferredByteBufferSegmentQueue<T> queue : getSegmentQueues()) {
            size += queue.size();
        }
        return size;
    }
    
    public long getByteSize() {
        return currentQueue == null ? 0 : currentQueue.getInMemByteSize();
    }

    public void close() {
        for (DeferredByteBufferSegmentQueue<T> queue : queues) {
            queue.close();
        }
        queues.clear();
    }
    
    private void initMergedQueue() {
        if (mergedQueue == null && currentIndex >= 0) {
            mergedQueue = MinMaxPriorityQueue.<DeferredByteBufferSegmentQueue<T>> orderedBy(
                    getSegmentQueueComparator()).maximumSize(currentIndex + 1).create();
            for (DeferredByteBufferSegmentQueue<T> queue : getSegmentQueues()) {
                T re = queue.peek();
                if (re != null) {
                    mergedQueue.add(queue);
                }
            }
        }        
    }

    public abstract static class DeferredByteBufferSegmentQueue<T> extends AbstractQueue<T> {
        protected MemoryManager mm;
        protected String spoolDirectory;

        private final int index;
        private final int thresholdBytes;
        private final boolean hasMaxQueueSize;
        private long totalResultSize = 0;
        private int maxResultSize = 0;
        private DeferredResultIterator<T> deferredResultIterator;
        private boolean isClosed = false;
        private boolean flushBuffer = false;
        private int flushedCount = 0;
        private T current = null;

        public DeferredByteBufferSegmentQueue(int index, int thresholdBytes, boolean hasMaxQueueSize) {
            this.index = index;
            this.thresholdBytes = thresholdBytes;
            this.hasMaxQueueSize = hasMaxQueueSize;
        }
        
        abstract protected Queue<T> getInMemoryQueue();
        abstract protected int sizeOf(T e);

        abstract protected  DeferredResultIterator<T> createDeferredResultIterator(Queue<T> inMemoryQueue, MemoryManager mm,
                                                                                   final int thresholdBytes, String spoolDirectory);
        public int index() {
            return this.index;
        }
        
        public int size() {
            if (flushBuffer)
                return flushedCount;
            return getInMemoryQueue().size();
        }
        
        public long getInMemByteSize() {
            if (flushBuffer)
                return 0;
            return totalResultSize;
        }
        
        public boolean isFlushed() {
            return flushBuffer;
        }

        @Override
        public boolean offer(T e) {
            if (isClosed || flushBuffer)
                return false;
            
            boolean added = getInMemoryQueue().add(e);
            if (added) {
                try {
                    flush(e);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            
            return added;
        }
        
        @Override
        public T peek() {
            if (current == null && !isClosed) {
                current = next();
            }
            
            return current;
        }
        
        @Override
        public T poll() {
            T ret = peek();
            if (!isClosed) {
                current = next();
            } else {
                current = null;
            }
            
            return ret;
        }

        @Override
        public Iterator<T> iterator() {
            if (isClosed)
                return null;
            
            if (!flushBuffer) {
                return getInMemoryQueue().iterator();
            } else {
                return deferredResultIterator.iterator();
            }
        }

        @Override
        public void clear() {
            getInMemoryQueue().clear();
            this.totalResultSize = 0;
            this.maxResultSize = 0;
            this.flushBuffer = false;
            this.flushedCount = 0;
            this.current = null;


        }
        
        public void close() {
            if (!isClosed) {
                clear();
                this.isClosed = true;
            }
        }
        
        private T next() {
            T ret;
            if (!flushBuffer) {
                ret = getInMemoryQueue().poll();
            } else {
                ret =  deferredResultIterator.poll();
            }
            if (ret == null) {
                close();
            }
            
            return ret;
        }

        private void flush(T entry) throws IOException {
            Queue<T> inMemQueue = getInMemoryQueue();
            int resultSize = sizeOf(entry);
            maxResultSize = Math.max(maxResultSize, resultSize);
            totalResultSize = hasMaxQueueSize ? maxResultSize * inMemQueue.size() : (totalResultSize + resultSize);
            if (totalResultSize >= thresholdBytes) {

                deferredResultIterator = createDeferredResultIterator(inMemQueue, mm, thresholdBytes, spoolDirectory);


                flushedCount = inMemQueue.size();
                inMemQueue.clear();
                flushBuffer = true;
            }
        }

    }
}

