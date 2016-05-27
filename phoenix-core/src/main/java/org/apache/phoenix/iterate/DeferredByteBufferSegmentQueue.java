package org.apache.phoenix.iterate;

import com.google.common.collect.Lists;
import org.apache.commons.io.output.DeferredFileOutputStream;

import java.io.*;
import java.util.*;

public abstract  class DeferredByteBufferSegmentQueue<T> extends AbstractQueue<T> {
    protected static final int EOF = -1;
    // at least create 128 KB MappedByteBuffers

    private final int index;
    private final int thresholdBytes;
    private final boolean hasMaxQueueSize;
    private long totalResultSize = 0;
    private int maxResultSize = 0;
    private File file;
    private boolean isClosed = false;
    private boolean flushBuffer = false;
    private int flushedCount = 0;
    private T current = null;
    private SegmentQueueFileIterator thisIterator;
    // iterators to close on close()
    private List<SegmentQueueFileIterator> iterators;

    public DeferredByteBufferSegmentQueue(int index, int thresholdBytes, boolean hasMaxQueueSize) {
        this.index = index;
        this.thresholdBytes = thresholdBytes;
        this.hasMaxQueueSize = hasMaxQueueSize;
        this.iterators = Lists.<SegmentQueueFileIterator> newArrayList();
    }

    abstract protected Queue<T> getInMemoryQueue();
    abstract protected int sizeOf(T e);
    abstract protected void writeToBuffer(OutputStream outputStream, T e);
    abstract protected T readFromBuffer(DataInput dataInput);

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

        if (!flushBuffer)
            return getInMemoryQueue().iterator();

        SegmentQueueFileIterator iterator = new SegmentQueueFileIterator(thisIterator);
        iterators.add(iterator);
        return iterator;
    }

    @Override
    public void clear() {
        getInMemoryQueue().clear();
        this.totalResultSize = 0;
        this.maxResultSize = 0;
        this.flushBuffer = false;
        this.flushedCount = 0;
        this.current = null;
        if (thisIterator != null) {
            thisIterator.close();
            thisIterator = null;
        }
        for (SegmentQueueFileIterator iter : iterators) {
            iter.close();
        }
        iterators.clear();
        if (this.file != null) {
            file.delete();
            file = null;
        }
    }

    public void close() {
        if (!isClosed) {
            clear();
            this.isClosed = true;
        }
    }

    private T next() {
        T ret = null;
        if (!flushBuffer) {
            ret = getInMemoryQueue().poll();
        } else {
            if (thisIterator == null) {
                thisIterator = new SegmentQueueFileIterator();
            }
            ret = thisIterator.next();
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
            this.file = File.createTempFile(UUID.randomUUID().toString(), null);

            DeferredFileOutputStream spoolTo = new DeferredFileOutputStream(thresholdBytes, file) {
                @Override
                protected void thresholdReached() throws IOException {
                    try {
                        super.thresholdReached();
                    } finally {
//                            chunk.close();
                    }
                }
            };

            int resSize = inMemQueue.size();
            for (int i = 0; i < resSize; i++) {
                writeToBuffer(spoolTo, inMemQueue.poll());
            }

            spoolTo.write(EOF); // end
            spoolTo.flush();
            flushedCount = resSize;
            inMemQueue.clear();
            flushBuffer = true;
        }
    }

    private class SegmentQueueFileIterator implements Iterator<T>, Closeable {
        private boolean isEnd;
        private long readIndex;
        private DataInputStream dataInput;
        private T next;

        public SegmentQueueFileIterator() {
            init(0);
        }

        public SegmentQueueFileIterator(SegmentQueueFileIterator iterator) {
            if (iterator != null && iterator.isEnd) {
                this.isEnd = true;
            } else {
                init(iterator == null ? 0 : iterator.readIndex);
            }
        }

        private void init(long readIndex) {
            this.isEnd = false;
            this.readIndex = readIndex;
            this.next = null;

            try {
                BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
                bufferedInputStream.skip(readIndex);
                this.dataInput = new DataInputStream(bufferedInputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            if (!isEnd && next == null) {
                next = readNext();
            }

            return next != null;
        }

        @Override
        public T next() {
            if (!hasNext())
                return null;

            T ret = next;
            next = readNext();
            return ret;
        }

        private T readNext() {
            if (isEnd)
                return null;

            T e = readFromBuffer(dataInput);
            if (e == null) {
                close();
                return null;
            }

            return e;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            this.isEnd = true;
            if (this.dataInput != null) {
                try {
                    this.dataInput.close();
                } catch (IOException ignored) {
                }
            }

        }
    }
}