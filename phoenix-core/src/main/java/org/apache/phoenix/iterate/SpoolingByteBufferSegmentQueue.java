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

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.DeferredFileOutputStream;

import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;


import java.io.*;
import java.util.AbstractQueue;
import java.util.Iterator;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.*;


public abstract class SpoolingByteBufferSegmentQueue<T>  extends AbstractQueue<T> {

    private ResultQueue<T> spoolFrom;

    private boolean closed ;
    private boolean flushed;
    private DeferredFileOutputStream spoolTo;
    private MemoryChunk chunk;
    private int size = 0;
    private long inMemByteSize = 0L;
    private int index;



    SpoolingByteBufferSegmentQueue(int index, MemoryManager mm, final int thresholdBytes, String spoolDirectory)  {

        long startTime = System.currentTimeMillis();
        chunk  = mm.allocate(0, thresholdBytes);
        long waitTime = System.currentTimeMillis() - startTime;
        GLOBAL_MEMORY_WAIT_TIME.update(waitTime);

        int size = (int)chunk.getSize();
        spoolTo = new DeferredFileOutputStream(size, "ResultSpooler",".bin", new File(spoolDirectory)) {
            @Override
            protected void thresholdReached() throws IOException {
                try {
                    super.thresholdReached();
                } finally {
                    chunk.close();
                }
            }
        };


    }

    public int index() {
        return this.index;
    }



    protected abstract InMemoryResultQueue<T> createInMemoryResultQueue(byte[] bytes, MemoryChunk memoryChunk);

    protected abstract OnDiskResultQueue<T> createOnDiskResultQueue(File file);

    @Override
    public boolean offer(T t) {
        if (closed || flushed){
            return false;
        }
        boolean result = writeRecord(t, spoolTo);
        if(result){
            if(!spoolTo.isInMemory()){
                flushToDisk();
            }
            size++;
        }


        return result;
    }

    protected abstract boolean writeRecord(T t, OutputStream outputStream);

    private void flushToMemory(){
        byte[] data = spoolTo.getData();
        chunk.resize(data.length);
        spoolFrom = createInMemoryResultQueue(data, chunk);
        GLOBAL_MEMORY_CHUNK_BYTES.update(data.length);
        flushed = true;
    }


    private void flushToDisk(){
        long sizeOfSpoolFile = spoolTo.getFile().length();
        GLOBAL_SPOOL_FILE_SIZE.update(sizeOfSpoolFile);
        GLOBAL_SPOOL_FILE_COUNTER.increment();
        spoolFrom = createOnDiskResultQueue(spoolTo.getFile());
        if (spoolTo.getFile() != null) {
            spoolTo.getFile().deleteOnExit();
        }
        inMemByteSize = 0;
        flushed = true;
    }


    public boolean isFlushed(){
        return flushed;
    }

    public T peek() {
        if(!flushed){
            flushToMemory();
        }
        return spoolFrom.peek();
    }

    @Override
    public T poll() {
        if(!flushed){
            flushToMemory();
        }
        return spoolFrom.poll();
    }

    public void close() throws IOException {
        if(spoolFrom != null){
            spoolFrom.close();
        }
    }

    @Override
    public Iterator<T> iterator() {
        if(!flushed){
            flushToMemory();
        }
        return spoolFrom.iterator();
    }

    @Override
    public int size() {
        return size ;
    }

    public long getInMemByteSize(){
        return inMemByteSize;
    };

    private static abstract class ResultQueue<T> extends  AbstractQueue<T> implements  Closeable{}

    protected static abstract class InMemoryResultQueue<T> extends ResultQueue<T> {
        private final MemoryChunk memoryChunk;
        protected final byte[] bytes;
        private T next;
        private AtomicInteger offset = new AtomicInteger(0);

        protected InMemoryResultQueue(byte[] bytes, MemoryChunk memoryChunk) {
            this.bytes = bytes;
            this.memoryChunk = memoryChunk;
            advance(offset);
        }

        protected abstract T advance(AtomicInteger offset);

        @Override
        public boolean offer(T t) {
            return false;
        }

        @Override
        public T peek(){
            return next;
        }

        @Override
        public T poll() {
            T current = next;
            next = advance(offset);
            return current;
        }


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
    protected static abstract class OnDiskResultQueue<T> extends ResultQueue<T> {
        private final File file;
        private DataInputStream spoolFrom;
        private DataInputStream iteratorSpoolFrom;
        private CountingInputStream countingInputStream;
        private T next;
        private boolean isClosed;


        protected OnDiskResultQueue(File file) {
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
                }
            } finally {
                file.delete();
            }
        }

        protected abstract T advance(DataInputStream spoolFrom);


        @Override
        public boolean offer(T t) {
            return false;
        }


        @Override
        public synchronized T peek() {
            try {
                init();
                return next;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
        public synchronized void close()  throws IOException{
            if (!isClosed) {
                reachedEnd();
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
