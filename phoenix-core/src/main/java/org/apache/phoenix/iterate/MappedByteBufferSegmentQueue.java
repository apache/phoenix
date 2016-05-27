package org.apache.phoenix.iterate;

import com.google.common.collect.Lists;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public abstract class MappedByteBufferSegmentQueue<T> extends AbstractQueue<T> {
    protected static final int EOF = -1;
    // at least create 128 KB MappedByteBuffers
    private static final long DEFAULT_MAPPING_SIZE = 128 * 1024;

    private final int index;
    private final int thresholdBytes;
    private final boolean hasMaxQueueSize;
    private long totalResultSize = 0;
    private int maxResultSize = 0;
    private long mappingSize = 0;
    private File file;
    private boolean isClosed = false;
    private boolean flushBuffer = false;
    private int flushedCount = 0;
    private T current = null;
    private SegmentQueueFileIterator thisIterator;
    // iterators to close on close()
    private List<SegmentQueueFileIterator> iterators;

    public MappedByteBufferSegmentQueue(int index, int thresholdBytes, boolean hasMaxQueueSize) {
        this.index = index;
        this.thresholdBytes = thresholdBytes;
        this.hasMaxQueueSize = hasMaxQueueSize;
        this.iterators = Lists.<SegmentQueueFileIterator> newArrayList();
    }

    abstract protected Queue<T> getInMemoryQueue();
    abstract protected int sizeOf(T e);
    abstract protected void writeToBuffer(MappedByteBuffer buffer, T e);
    abstract protected T readFromBuffer(MappedByteBuffer buffer);

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
        this.mappingSize = 0;
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
            RandomAccessFile af = new RandomAccessFile(file, "rw");
            FileChannel fc = af.getChannel();
            int writeIndex = 0;
            mappingSize = Math.min(Math.max(maxResultSize, DEFAULT_MAPPING_SIZE), totalResultSize);
            MappedByteBuffer writeBuffer = fc.map(FileChannel.MapMode.READ_WRITE, writeIndex, mappingSize);

            int resSize = inMemQueue.size();
            for (int i = 0; i < resSize; i++) {
                T e = inMemQueue.poll();
                writeToBuffer(writeBuffer, e);
                // buffer close to exhausted, re-map.
                if (mappingSize - writeBuffer.position() < maxResultSize) {
                    writeIndex += writeBuffer.position();
                    writeBuffer = fc.map(FileChannel.MapMode.READ_WRITE, writeIndex, mappingSize);
                }
            }
            writeBuffer.putInt(EOF); // end
            fc.force(true);
            fc.close();
            af.close();
            flushedCount = resSize;
            inMemQueue.clear();
            flushBuffer = true;
        }
    }

    private class SegmentQueueFileIterator implements Iterator<T>, Closeable {
        private boolean isEnd;
        private long readIndex;
        private RandomAccessFile af;
        private FileChannel fc;
        private MappedByteBuffer readBuffer;
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
                this.af = new RandomAccessFile(file, "r");
                this.fc = af.getChannel();
                this.readBuffer = fc.map(FileChannel.MapMode.READ_ONLY, readIndex, mappingSize);
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

            T e = readFromBuffer(readBuffer);
            if (e == null) {
                close();
                return null;
            }

            // buffer close to exhausted, re-map.
            if (mappingSize - readBuffer.position() < maxResultSize) {
                readIndex += readBuffer.position();
                try {
                    readBuffer = fc.map(FileChannel.MapMode.READ_ONLY, readIndex, mappingSize);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
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
            if (this.fc != null) {
                try {
                    this.fc.close();
                } catch (IOException ignored) {
                }
            }
            if (this.af != null) {
                try {
                    this.af.close();
                } catch (IOException ignored) {
                }
                this.af = null;
            }
        }
    }
}