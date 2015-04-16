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

import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.NUM_SPOOL_FILE;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.SPOOL_FILE_SIZE;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ResultUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TupleUtil;

/**
 *
 * Result iterator that spools the results of a scan to disk once an in-memory threshold has been reached.
 * If the in-memory threshold is not reached, the results are held in memory with no disk writing perfomed.
 *
 *
 * @since 0.1
 */
public class SpoolingResultIterator implements PeekingResultIterator {
    private final PeekingResultIterator spoolFrom;
    
    public static class SpoolingResultIteratorFactory implements ParallelIteratorFactory {
        private final QueryServices services;

        public SpoolingResultIteratorFactory(QueryServices services) {
            this.services = services;
        }
        @Override
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner, Scan scan) throws SQLException {
            return new SpoolingResultIterator(scanner, services);
        }

    }

    public SpoolingResultIterator(ResultIterator scanner, QueryServices services) throws SQLException {
        this (scanner, services.getMemoryManager(),
                services.getProps().getInt(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES),
                services.getProps().getLong(QueryServices.MAX_SPOOL_TO_DISK_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SPOOL_TO_DISK_BYTES),
                services.getProps().get(QueryServices.SPOOL_DIRECTORY, QueryServicesOptions.DEFAULT_SPOOL_DIRECTORY));
    }

    /**
    * Create a result iterator by iterating through the results of a scan, spooling them to disk once
    * a threshold has been reached. The scanner passed in is closed prior to returning.
    * @param scanner the results of a table scan
    * @param mm memory manager tracking memory usage across threads.
    * @param thresholdBytes the requested threshold.  Will be dialed down if memory usage (as determined by
    *  the memory manager) is exceeded.
    * @throws SQLException
    */
    SpoolingResultIterator(ResultIterator scanner, MemoryManager mm, final int thresholdBytes, final long maxSpoolToDisk, final String spoolDirectory) throws SQLException {
        boolean success = false;
        final MemoryChunk chunk = mm.allocate(0, thresholdBytes);
        DeferredFileOutputStream spoolTo = null;
        try {
            // Can't be bigger than int, since it's the max of the above allocation
            int size = (int)chunk.getSize();
            spoolTo = new DeferredFileOutputStream(size, "ResultSpooler",".bin", new File(spoolDirectory)) {
                @Override
                protected void thresholdReached() throws IOException {
                    super.thresholdReached();
                    chunk.close();
                }
            };
            DataOutputStream out = new DataOutputStream(spoolTo);
            final long maxBytesAllowed = maxSpoolToDisk == -1 ?
            		Long.MAX_VALUE : thresholdBytes + maxSpoolToDisk;
            long bytesWritten = 0L;
            for (Tuple result = scanner.next(); result != null; result = scanner.next()) {
                int length = TupleUtil.write(result, out);
                bytesWritten += length;
                if(bytesWritten > maxBytesAllowed){
                		throw new SpoolTooBigToDiskException("result too big, max allowed(bytes): " + maxBytesAllowed);
                }
            }
            if (spoolTo.isInMemory()) {
                byte[] data = spoolTo.getData();
                chunk.resize(data.length);
                spoolFrom = new InMemoryResultIterator(data, chunk);
            } else {
                NUM_SPOOL_FILE.increment();
                SPOOL_FILE_SIZE.update(spoolTo.getFile().length());
                spoolFrom = new OnDiskResultIterator(spoolTo.getFile());
                if (spoolTo.getFile() != null) {
                    spoolTo.getFile().deleteOnExit();
                }
            }
            success = true;
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            try {
                scanner.close();
            } finally {
                try {
                    if (spoolTo != null) {
                        if(!success && spoolTo.getFile() != null){
                            spoolTo.getFile().delete();
                        }
                        spoolTo.close();
                    }
                } catch (IOException ignored) {
                  // ignore close error
                } finally {
                    if (!success) {
                        chunk.close();
                    }
                }
            }
        }
    }

    @Override
    public Tuple peek() throws SQLException {
        return spoolFrom.peek();
    }

    @Override
    public Tuple next() throws SQLException {
        return spoolFrom.next();
    }

    @Override
    public void close() throws SQLException {
        spoolFrom.close();
    }

    /**
     *
     * Backing result iterator if it was not necessary to spool results to disk.
     *
     *
     * @since 0.1
     */
    private static class InMemoryResultIterator implements PeekingResultIterator {
        private final MemoryChunk memoryChunk;
        private final byte[] bytes;
        private Tuple next;
        private int offset;

        private InMemoryResultIterator(byte[] bytes, MemoryChunk memoryChunk) throws SQLException {
            this.bytes = bytes;
            this.memoryChunk = memoryChunk;
            advance();
        }

        private Tuple advance() throws SQLException {
            if (offset >= bytes.length) {
                return next = null;
            }
            int resultSize = ByteUtil.vintFromBytes(bytes, offset);
            offset += WritableUtils.getVIntSize(resultSize);
            ImmutableBytesWritable value = new ImmutableBytesWritable(bytes,offset,resultSize);
            offset += resultSize;
            Tuple result = new ResultTuple(ResultUtil.toResult(value));
            return next = result;
        }

        @Override
        public Tuple peek() throws SQLException {
            return next;
        }

        @Override
        public Tuple next() throws SQLException {
            Tuple current = next;
            advance();
            return current;
        }

        @Override
        public void close() {
            memoryChunk.close();
        }

        @Override
        public void explain(List<String> planSteps) {
        }
    }

    /**
     *
     * Backing result iterator if results were spooled to disk
     *
     *
     * @since 0.1
     */
    private static class OnDiskResultIterator implements PeekingResultIterator {
        private final File file;
        private DataInputStream spoolFrom;
        private Tuple next;
        private boolean isClosed;

        private OnDiskResultIterator (File file) {
            this.file = file;
        }

        private synchronized void init() throws IOException {
            if (spoolFrom == null) {
                spoolFrom = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                advance();
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

        private synchronized Tuple advance() throws IOException {
            if (isClosed) {
                return next;
            }
            int length;
            try {
                length = WritableUtils.readVInt(spoolFrom);
            } catch (EOFException e) {
                reachedEnd();
                return next;
            }
            int totalBytesRead = 0;
            int offset = 0;
            byte[] buffer = new byte[length];
            while(totalBytesRead < length) {
                int bytesRead = spoolFrom.read(buffer, offset, length);
                if (bytesRead == -1) {
                    reachedEnd();
                    return next;
                }
                offset += bytesRead;
                totalBytesRead += bytesRead;
            }
            next = new ResultTuple(ResultUtil.toResult(new ImmutableBytesWritable(buffer,0,length)));
            return next;
        }

        @Override
        public synchronized Tuple peek() throws SQLException {
            try {
                init();
                return next;
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        }

        @Override
        public synchronized Tuple next() throws SQLException {
            try {
                init();
                Tuple current = next;
                advance();
                return current;
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        }

        @Override
        public synchronized void close() throws SQLException {
            try {
                if (!isClosed) {
                    reachedEnd();
                }
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        }

        @Override
        public void explain(List<String> planSteps) {
        }
    }

    @Override
    public void explain(List<String> planSteps) {
    }
}
