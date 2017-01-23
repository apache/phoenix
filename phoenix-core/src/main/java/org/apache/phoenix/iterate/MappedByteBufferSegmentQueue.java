package org.apache.phoenix.iterate;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public abstract class MappedByteBufferSegmentQueue<T> extends BufferSegmentQueue<T> {
    private static final long DEFAULT_MAPPING_SIZE = 128 * 1024;

    private long mappingSize = 0;


    public MappedByteBufferSegmentQueue(int index, int thresholdBytes, boolean hasMaxQueueSize) {
        super(index, thresholdBytes, hasMaxQueueSize);
    }

    abstract protected void writeToBuffer(MappedByteBuffer buffer, T e);
    abstract protected T readFromBuffer(MappedByteBuffer buffer);


    @Override
    protected void flush(T entry) throws IOException {
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

    @Override
    protected SegmentQueueFileIterator createSegmentQueueFileIterator(SegmentQueueFileIterator iterator){
        return new MappedSegmentQueueFileIterator(iterator);
    }

    @Override
    protected SegmentQueueFileIterator createSegmentQueueFileIterator(){
        return new MappedSegmentQueueFileIterator();
    }

    private class MappedSegmentQueueFileIterator extends SegmentQueueFileIterator {
        private RandomAccessFile af;
        private FileChannel fc;
        private MappedByteBuffer readBuffer;

        public MappedSegmentQueueFileIterator() {
            super();
        }

        public MappedSegmentQueueFileIterator(SegmentQueueFileIterator iterator) {
            super(iterator);
        }

        @Override
        protected void init(long readIndex) {
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
        protected T readNext() {
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