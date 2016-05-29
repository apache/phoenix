package org.apache.phoenix.iterate;

import org.apache.commons.io.output.DeferredFileOutputStream;

import java.io.*;
import java.util.*;

public abstract  class DeferredByteBufferSegmentQueue<T> extends BufferSegmentQueue<T> {


    public DeferredByteBufferSegmentQueue(int index, int thresholdBytes, boolean hasMaxQueueSize) {
        super(index, thresholdBytes, hasMaxQueueSize);

    }

    abstract protected void writeToBuffer(OutputStream outputStream, T e);
    abstract protected T readFromBuffer(DataInput dataInput);


    @Override
    protected SegmentQueueFileIterator createSegmentQueueFileIterator(SegmentQueueFileIterator iterator){
        return new DeferredSegmentQueueFileIterator(iterator);
    }

    @Override
    protected SegmentQueueFileIterator createSegmentQueueFileIterator(){
        return new DeferredSegmentQueueFileIterator();
    }

    @Override
    protected void flush(T entry) throws IOException {
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

    private class DeferredSegmentQueueFileIterator extends SegmentQueueFileIterator {
        private DataInputStream dataInput;

        public DeferredSegmentQueueFileIterator() {
            super();
        }

        public DeferredSegmentQueueFileIterator(SegmentQueueFileIterator iterator) {
            super(iterator);
        }



        @Override
        protected void init(long readIndex) {
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
        protected T readNext() {
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