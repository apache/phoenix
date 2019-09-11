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
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;

public abstract class SizeBoundQueue<T> extends AbstractQueue<T> implements SizeAwareQueue<T> {

    private long maxSizeBytes;
    private Queue<T> delegate;
    private long currentSize;

    public SizeBoundQueue(long maxSizeBytes, Queue<T> delegate) {
        assert maxSizeBytes > 0;
        this.maxSizeBytes = maxSizeBytes;
        this.delegate = delegate;
    }

    abstract public long sizeOf(T e);

    @Override
    public boolean offer(T e) {
        boolean success = false;
        long elementSize = sizeOf(e);
        if ((currentSize + elementSize) < maxSizeBytes) {
            success = delegate.offer(e);
            if (success) {
                currentSize += elementSize;
            }
        }
        return success;
    }

    @Override
    public boolean add(T e) {
        try {
            return super.add(e);
        } catch (IllegalStateException ex) {
            throw new IllegalStateException(
                    "Queue full. Consider increasing memory threshold or spooling to disk. Max size: " + maxSizeBytes + ", Current size: " + currentSize + ", Number of elements:" + size(), ex);
        }
    }

    @Override
    public T poll() {
        T e = delegate.poll();
        if (e != null) {
            currentSize -= sizeOf(e);
        }
        return e;
    }

    @Override
    public T peek() {
        return delegate.peek();
    }

    @Override
    public void close() throws IOException {
        delegate.clear();
    }

    @Override
    public long getByteSize() {
        return currentSize;
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Override
    public int size() {
        return delegate.size();
    }

}
