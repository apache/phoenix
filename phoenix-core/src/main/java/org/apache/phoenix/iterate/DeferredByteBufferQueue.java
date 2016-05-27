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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public abstract class DeferredByteBufferQueue<T> extends AbstractQueue<T> {
    private final int thresholdBytes;
    private List<DeferredByteBufferSegmentQueue<T>> queues;
    private int currentIndex;
    private DeferredByteBufferSegmentQueue<T> currentQueue;
    private MinMaxPriorityQueue<DeferredByteBufferSegmentQueue<T>> mergedQueue;

    private static final Logger logger = LoggerFactory.getLogger(DeferredByteBufferQueue.class);

    public DeferredByteBufferQueue(int thresholdBytes) {
        this.thresholdBytes = thresholdBytes;
        this.queues = Lists.<DeferredByteBufferSegmentQueue<T>> newArrayList();
        this.currentIndex = -1;
        this.currentQueue = null;
        this.mergedQueue = null;
    }
    
    abstract protected DeferredByteBufferSegmentQueue<T> createSegmentQueue(int index, int thresholdBytes);
    
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
                currentQueue = createSegmentQueue(currentIndex, thresholdBytes);
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


}

