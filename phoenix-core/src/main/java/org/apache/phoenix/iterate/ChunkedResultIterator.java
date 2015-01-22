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

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.STARTKEY_OFFSET;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * {@code PeekingResultIterator} implementation that loads data in chunks. This is intended for
 * basic scan plans, to avoid loading large quantities of data from HBase in one go.
 */
public class ChunkedResultIterator implements PeekingResultIterator {
    private static final Logger logger = LoggerFactory.getLogger(ChunkedResultIterator.class);

    private final ParallelIteratorFactory delegateIteratorFactory;
    private ImmutableBytesWritable lastKey = new ImmutableBytesWritable();
    private final StatementContext context;
    private final TableRef tableRef;
    private Scan scan;
    private final long chunkSize;
    private PeekingResultIterator resultIterator;

    public static class ChunkedResultIteratorFactory implements ParallelIteratorFactory {

        private final ParallelIteratorFactory delegateFactory;
        private final TableRef tableRef;

        public ChunkedResultIteratorFactory(ParallelIteratorFactory
                delegateFactory, TableRef tableRef) {
            this.delegateFactory = delegateFactory;
            this.tableRef = tableRef;
        }

        @Override
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner, Scan scan) throws SQLException {
            scanner.close(); //close the iterator since we don't need it anymore.
            if (logger.isDebugEnabled()) logger.debug(LogUtil.addCustomAnnotations("ChunkedResultIteratorFactory.newIterator over " + tableRef.getTable().getName().getString() + " with " + scan, ScanUtil.getCustomAnnotations(scan)));
            return new ChunkedResultIterator(delegateFactory, context, tableRef, scan,
                    context.getConnection().getQueryServices().getProps().getLong(
                                        QueryServices.SCAN_RESULT_CHUNK_SIZE,
                                        QueryServicesOptions.DEFAULT_SCAN_RESULT_CHUNK_SIZE));
        }
    }

    public ChunkedResultIterator(ParallelIteratorFactory delegateIteratorFactory,
            StatementContext context, TableRef tableRef, Scan scan, long chunkSize) throws SQLException {
        this.delegateIteratorFactory = delegateIteratorFactory;
        this.context = context;
        this.tableRef = tableRef;
        this.scan = scan;
        this.chunkSize = chunkSize;
        // Instantiate single chunk iterator and the delegate iterator in constructor
        // to get parallel scans kicked off in separate threads. If we delay this,
        // we'll get serialized behavior (see PHOENIX-
        if (logger.isDebugEnabled()) logger.debug(LogUtil.addCustomAnnotations("Get first chunked result iterator over " + tableRef.getTable().getName().getString() + " with " + scan, ScanUtil.getCustomAnnotations(scan)));
        ResultIterator singleChunkResultIterator = new SingleChunkResultIterator(
                new TableResultIterator(context, tableRef, scan), chunkSize);
        resultIterator = delegateIteratorFactory.newIterator(context, singleChunkResultIterator, scan);
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
        resultIterator.close();
    }

    private PeekingResultIterator getResultIterator() throws SQLException {
        if (resultIterator.peek() == null && lastKey != null) {
            resultIterator.close();
            scan = ScanUtil.newScan(scan);
            scan.setStartRow(ByteUtil.copyKeyBytesIfNecessary(lastKey));
            if (logger.isDebugEnabled()) logger.debug(LogUtil.addCustomAnnotations("Get next chunked result iterator over " + tableRef.getTable().getName().getString() + " with " + scan, ScanUtil.getCustomAnnotations(scan)));
            ResultIterator singleChunkResultIterator = new SingleChunkResultIterator(
                    new TableResultIterator(context, tableRef, scan), chunkSize);
            resultIterator = delegateIteratorFactory.newIterator(context, singleChunkResultIterator, scan);
        }
        return resultIterator;
    }

    /**
     * ResultIterator that runs over a single chunk of results (i.e. a portion of a scan).
     */
    private class SingleChunkResultIterator implements ResultIterator {

        private int rowCount = 0;
        private boolean chunkComplete;
        private final ResultIterator delegate;
        private final long chunkSize;

        private SingleChunkResultIterator(ResultIterator delegate, long chunkSize) {
            Preconditions.checkArgument(chunkSize > 0);
            this.delegate = delegate;
            this.chunkSize = chunkSize;
        }

        @Override
        public Tuple next() throws SQLException {
            if (chunkComplete || lastKey == null) {
                return null;
            }
            Tuple next = delegate.next();
            if (next != null) {
                // We actually keep going past the chunk size until the row key changes. This is
                // necessary for (at least) hash joins, as they can return multiple rows with the
                // same row key. Stopping a chunk at a row key boundary is necessary in order to
                // be able to start the next chunk on the next row key
                if (rowCount == chunkSize) {
                    next.getKey(lastKey);
                    if (scan.getAttribute(STARTKEY_OFFSET) != null) {
                        addRegionStartKeyToLaskKey();
                    }
                } else if (rowCount > chunkSize && rowKeyChanged(next)) {
                    chunkComplete = true;
                    return null;
                }
                rowCount++;
            } else {
                lastKey = null;
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

        private boolean rowKeyChanged(Tuple newTuple) {
            byte[] currentKey = lastKey.get();
            int offset = lastKey.getOffset();
            int length = lastKey.getLength();
            newTuple.getKey(lastKey);
            if (scan.getAttribute(STARTKEY_OFFSET) != null) {
                addRegionStartKeyToLaskKey();
            }

            return Bytes.compareTo(currentKey, offset, length, lastKey.get(), lastKey.getOffset(), lastKey.getLength()) != 0;
        }

        /**
         * Prefix region start key to last key to form actual row key in case of local index scan.
         */
        private void addRegionStartKeyToLaskKey() {
            byte[] offsetBytes = scan.getAttribute(STARTKEY_OFFSET);
            if (offsetBytes != null) {
                int startKeyOffset = Bytes.toInt(offsetBytes);
                byte[] actualLastkey =
                        new byte[startKeyOffset + lastKey.getLength() - lastKey.getOffset()];
                System.arraycopy(scan.getStartRow(), 0, actualLastkey, 0, startKeyOffset);
                System.arraycopy(lastKey.get(), lastKey.getOffset(), actualLastkey,
                    startKeyOffset, lastKey.getLength());
                lastKey.set(actualLastkey);
            }
        }

		@Override
		public String toString() {
			return "SingleChunkResultIterator [rowCount=" + rowCount
					+ ", chunkComplete=" + chunkComplete + ", delegate="
					+ delegate + ", chunkSize=" + chunkSize + "]";
		}
    }
}
