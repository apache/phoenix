package org.apache.phoenix.iterate;

import com.google.common.collect.Lists;
import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.tuple.Tuple;

import java.io.*;
import java.util.*;

public abstract class BufferSegmentQueue<T> extends AbstractQueue<T> {
    protected static final int EOF = -1;

    protected final int index;
    protected final int thresholdBytes;
    protected final boolean hasMaxQueueSize;
    protected long totalResultSize = 0;
    protected int maxResultSize = 0;

    protected File file;
    private boolean isClosed = false;
    protected boolean flushBuffer = false;
    protected int flushedCount = 0;

    private T current = null;

    protected SegmentQueueFileIterator thisIterator;
    // iterators to close on close()
    protected List<SegmentQueueFileIterator> iterators;

    public BufferSegmentQueue(int index, int thresholdBytes, boolean hasMaxQueueSize) {
        this.index = index;
        this.thresholdBytes = thresholdBytes;
        this.hasMaxQueueSize = hasMaxQueueSize;
        this.iterators = Lists.<SegmentQueueFileIterator> newArrayList();
    }

    abstract protected Queue<T> getInMemoryQueue();
    abstract protected int sizeOf(T e);


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

        SegmentQueueFileIterator iterator = createSegmentQueueFileIterator(thisIterator);
        iterators.add(iterator);
        return iterator;
    }


    protected abstract SegmentQueueFileIterator createSegmentQueueFileIterator(SegmentQueueFileIterator iterator);
    protected abstract SegmentQueueFileIterator createSegmentQueueFileIterator();

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
                thisIterator = createSegmentQueueFileIterator();
            }
            ret = thisIterator.next();
        }

        if (ret == null) {
            close();
        }

        return ret;
    }

    protected abstract  void flush(T entry) throws IOException;

    protected abstract class SegmentQueueFileIterator implements Iterator<T>, Closeable {
        protected boolean isEnd;
        protected long readIndex;
        protected T next;

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

        protected abstract void init(long readIndex);

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

        protected abstract T readNext();

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        abstract public void close();
    }


    protected List<KeyValue> toKeyValues(OrderedResultIterator.ResultEntry entry) {
        Tuple result = entry.getResult();
        int size = result.size();
        List<KeyValue> kvs = new ArrayList<KeyValue>(size);
        for (int i = 0; i < size; i++) {
            kvs.add(org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(result.getValue(i)));
        }
        return kvs;
    }

    protected int sizeof(List<KeyValue> kvs) {
        int size = Bytes.SIZEOF_INT; // totalLen

        for (KeyValue kv : kvs) {
            size += kv.getLength();
            size += Bytes.SIZEOF_INT; // kv.getLength
        }

        return size;
    }

    protected int sizeof(ImmutableBytesWritable[] sortKeys) {
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
}