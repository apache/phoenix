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
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * {@code PeekingResultIterator} implementation that loads data in chunks. This is intended for
 * basic scan plans, to avoid loading large quantities of data from HBase in one go.
 */
public class ChunkedResultIterator implements PeekingResultIterator {

    private final ParallelIterators.ParallelIteratorFactory delegateIteratorFactory;
    private SingleChunkResultIterator singleChunkResultIterator;
    private final StatementContext context;
    private final TableRef tableRef;
    private Scan scan;
    private final long chunkSize;
    private PeekingResultIterator resultIterator;

    public static class ChunkedResultIteratorFactory implements ParallelIterators.ParallelIteratorFactory {

        private final ParallelIterators.ParallelIteratorFactory delegateFactory;
        private final TableRef tableRef;

        public ChunkedResultIteratorFactory(ParallelIterators.ParallelIteratorFactory
                delegateFactory, TableRef tableRef) {
            this.delegateFactory = delegateFactory;
            this.tableRef = tableRef;
        }

        @Override
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner) throws SQLException {
            // TODO It doesn't seem right to do this selection here, but it's not currently clear
            // where a better place is to do it
            // For a HashJoin the scan can't be restarted where it left off, so we don't use
            // a ChunkedResultIterator
            if (HashJoinInfo.isHashJoin(context.getScan())) {
                return delegateFactory.newIterator(context, scanner);
            } else {
                return new ChunkedResultIterator(delegateFactory, context, tableRef,
                        context.getConnection().getQueryServices().getProps().getLong(
                                            QueryServices.SCAN_RESULT_CHUNK_SIZE,
                                            QueryServicesOptions.DEFAULT_SCAN_RESULT_CHUNK_SIZE));
            }
        }
    }

    public ChunkedResultIterator(ParallelIterators.ParallelIteratorFactory delegateIteratorFactory,
            StatementContext context, TableRef tableRef, long chunkSize) {
        this.delegateIteratorFactory = delegateIteratorFactory;
        this.context = context;
        this.tableRef = tableRef;
        this.scan = context.getScan();
        this.chunkSize = chunkSize;
    }

    @Override
    public Tuple peek() throws SQLException {
        return getResultIterator().peek();
    }

    @Override
    public Tuple next() throws SQLException {
        return getResultIterator().next();
    }

    @Override
    public void explain(List<String> planSteps) {
        resultIterator.explain(planSteps);
    }

    @Override
    public void close() throws SQLException {
        if (resultIterator != null) {
            resultIterator.close();
        }
        if (singleChunkResultIterator != null) {
            singleChunkResultIterator.close();
        }
    }

    private PeekingResultIterator getResultIterator() throws SQLException {
        if (resultIterator == null) {
            singleChunkResultIterator = new SingleChunkResultIterator(
                    new TableResultIterator(context, tableRef, scan), chunkSize);
            resultIterator = delegateIteratorFactory.newIterator(context, singleChunkResultIterator);
        } else if (resultIterator.peek() == null && !singleChunkResultIterator.isEndOfStreamReached()) {
            singleChunkResultIterator.close();
            try {
                this.scan = new Scan(scan);
            } catch (IOException e) {
                throw new PhoenixIOException(e);
            }
            scan.setStartRow(Bytes.add(singleChunkResultIterator.getLastKey(), new byte[]{0}));
            singleChunkResultIterator = new SingleChunkResultIterator(
                    new TableResultIterator(context, tableRef, scan), chunkSize);
            resultIterator = delegateIteratorFactory.newIterator(context, singleChunkResultIterator);
        }
        return resultIterator;
    }

    /**
     * ResultIterator that runs over a single chunk of results (i.e. a portion of a scan).
     */
    private static class SingleChunkResultIterator implements ResultIterator {

        private int rowCount = 0;
        private boolean endOfStreamReached;
        private Tuple lastTuple;
        private final ResultIterator delegate;
        private final long chunkSize;

        private SingleChunkResultIterator(ResultIterator delegate, long chunkSize) {
            this.delegate = delegate;
            this.chunkSize = chunkSize;
        }

        @Override
        public Tuple next() throws SQLException {
            if (isChunkComplete() || isEndOfStreamReached()) {
                return null;
            }
            Tuple next = delegate.next();
            if (next != null) {
                lastTuple = next;
                rowCount++;
            } else {
                endOfStreamReached = true;
            }
            return next;
        }

        @Override
        public void explain(List<String> planSteps) {
            delegate.explain(planSteps);
        }

        @Override
        public void close() throws SQLException {
            delegate.close();
        }

        /**
         * Returns true if the current chunk has been fully iterated over.
         */
        public boolean isChunkComplete() {
            return rowCount == chunkSize;
        }

        /**
         * Returns true if the end of all chunks has been reached.
         */
        public boolean isEndOfStreamReached() {
            return endOfStreamReached;
        }

        /**
         * Returns the last-encountered key.
         */
        public byte[] getLastKey() {
            ImmutableBytesWritable keyPtr = new ImmutableBytesWritable();
            lastTuple.getKey(keyPtr);
            return keyPtr.get();
        }
    }
}
