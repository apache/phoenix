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

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MEMORY_CHUNK_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MEMORY_WAIT_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SPOOL_FILE_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SPOOL_FILE_SIZE;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.DeferredFileOutputStream;

import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;


import java.io.*;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;


/**
 *
 * Result iterator that spools the results of a scan to disk once an in-memory threshold has been reached.
 * If the in-memory threshold is not reached, the results are held in memory with no disk writing perfomed.
 * 
 * <p>
 * Spooling is deprecated and shouldn't be used while implementing new features. As of HBase 0.98.17, 
 * we rely on pacing the server side scanners instead of pulling rows from the server and  potentially 
 * spooling to a temporary file created on clients.
 * </p>
 *  
 * @since 0.1
 */
public abstract class DeferredResultIterator<T> {

    private ResultQueue<T> spoolFrom;

    DeferredResultIterator(Queue<T> inMemoryQueue, MemoryManager mm,
                           final int thresholdBytes, String spoolDirectory)  {

        long startTime = System.currentTimeMillis();
        final MemoryChunk chunk  = mm.allocate(0, thresholdBytes);
        long waitTime = System.currentTimeMillis() - startTime;
        GLOBAL_MEMORY_WAIT_TIME.update(waitTime);

        int size = (int)chunk.getSize();
        DeferredFileOutputStream spoolTo = new DeferredFileOutputStream(size, "ResultSpooler",".bin", new File(spoolDirectory)) {
            @Override
            protected void thresholdReached() throws IOException {
                try {
                    super.thresholdReached();
                } finally {
                    chunk.close();
                }
            }
        };

        DataOutputStream out = new DataOutputStream(spoolTo);

        for(T t : inMemoryQueue){
            writeRecord(t, out);
        }

        if (spoolTo.isInMemory()) {
            byte[] data = spoolTo.getData();
            chunk.resize(data.length);
            spoolFrom =  createInMemoryResultQueue(data, chunk);
            GLOBAL_MEMORY_CHUNK_BYTES.update(data.length);
        } else {
            long sizeOfSpoolFile = spoolTo.getFile().length();
            GLOBAL_SPOOL_FILE_SIZE.update(sizeOfSpoolFile);
            GLOBAL_SPOOL_FILE_COUNTER.increment();
            spoolFrom = createOnDiskResultQueue(spoolTo.getFile());
            if (spoolTo.getFile() != null) {
                spoolTo.getFile().deleteOnExit();
            }
        }
    }


    Iterator<T> iterator(){
        return spoolFrom.iterator();
    }

    public T peek()  {
        return spoolFrom.peek();
    }


    public boolean hasNext(){
        return spoolFrom.size() != 0;
    }

    public T poll()  {
        return spoolFrom.poll();
    }

    public void close() throws IOException {
        spoolFrom.close();
    }

    private static abstract class ResultQueue<T> extends AbstractQueue<T> implements Closeable {}

    protected abstract InMemoryResultQueue<T> createInMemoryResultQueue(byte[] bytes, MemoryChunk memoryChunk);

    protected abstract OnDiskResultQueue<T> createOnDiskResultQueue(File file);

    protected abstract boolean writeRecord(T t, OutputStream outputStream);
    /**
     *
     * Backing result iterator if it was not necessary to spool results to disk.
     *
     *
     * @since 0.1
     */
    protected static abstract class InMemoryResultQueue<T> extends ResultQueue<T> {
        private final MemoryChunk memoryChunk;
        protected final byte[] bytes;
        protected T next;
        private AtomicInteger offset = new AtomicInteger(0);

        protected InMemoryResultQueue(byte[] bytes, MemoryChunk memoryChunk)  {
            this.bytes = bytes;
            this.memoryChunk = memoryChunk;
            advance(offset);
        }

        protected abstract T advance(AtomicInteger offset);

        @Override
        public T peek() {
            return next;
        }

        @Override
        public boolean offer(T t) {
            return false;
        }

        @Override
        public T poll() {
            T current = next;
            advance(offset);
            return current;
        }
        @Override
        public void close() {
            memoryChunk.close();
        }


        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>(){
                AtomicInteger iteratorOffset = new AtomicInteger(offset.get());
                private T next = advance(iteratorOffset);

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public T next() {
                    T current = next;
                    next = advance(iteratorOffset);
                    return current;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Unsupported remove() in InMemoryResultQueue Iterator");
                }
            };
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException("Unsupported size() in InMemoryResultQueue");
        }
    }

    /**
     *
     * Backing result iterator if results were spooled to disk
     *
     *
     * @since 0.1
     */
    protected static abstract class OnDiskResultQueue<T> extends ResultQueue<T>{
        private final File file;
        private DataInputStream spoolFrom;
        private DataInputStream iteratorSpoolFrom;
        private CountingInputStream countingInputStream;
        private T next;
        private boolean isClosed;

        protected OnDiskResultQueue (File file) {
            this.file = file;
        }

        private synchronized void init() throws IOException {
            if (spoolFrom == null) {
                countingInputStream = new CountingInputStream(new FileInputStream(file));
                spoolFrom = new DataInputStream(new BufferedInputStream(countingInputStream));
                iteratorSpoolFrom = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                next = advance(spoolFrom);
            }
        }

        private synchronized void reachedEnd() throws IOException {
            next = null;
            isClosed = true;
            try {
                if (spoolFrom != null) {
                    spoolFrom.close();
                    iteratorSpoolFrom.close();
                }
            } finally {
                file.delete();
            }
        }

        protected abstract T advance(DataInputStream spoolFrom);

        @Override
        public synchronized T peek()  {
            try {
                init();
                return next;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean offer(T t) {
            return false;
        }

        @Override
        public synchronized T poll() {
            try {
                init();
                T current = next;
                advance(spoolFrom);
                return current;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public synchronized void close() {
            try {
                if (!isClosed) {
                    reachedEnd();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public Iterator<T> iterator() {
            try {
                iteratorSpoolFrom.reset();
                iteratorSpoolFrom.skip(countingInputStream.getByteCount());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return new Iterator<T>() {

                private T next = advance(iteratorSpoolFrom);

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public T next() {
                    T current = next;
                    next = advance(iteratorSpoolFrom);
                    return current;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Unsupported remove() in OnDiskResultQueue Iterator");
                }
            };
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException("Unsupported size() in OnDiskResultQueue");
        }

    }

}
