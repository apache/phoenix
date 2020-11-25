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
package org.apache.phoenix.transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.DelegateHTable;
import org.apache.phoenix.execute.PhoenixTxIndexMutationGenerator;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.transaction.TephraTransactionProvider.TephraTransactionClient;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.tephra.Transaction;
import org.apache.tephra.Transaction.VisibilityLevel;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.hbase.TransactionAwareHTable;
import org.apache.tephra.visibility.FenceWait;
import org.apache.tephra.visibility.VisibilityFence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;


public class TephraTransactionContext implements PhoenixTransactionContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(TephraTransactionContext.class);
    private static final TransactionCodec CODEC = new TransactionCodec();

    private final List<TransactionAware> txAwares;
    private final TransactionContext txContext;
    private Transaction tx;
    private TransactionSystemClient txServiceClient;

    public TephraTransactionContext() {
        this.txServiceClient = null;
        this.txAwares = Lists.newArrayList();
        this.txContext = null;
    }

    public TephraTransactionContext(byte[] txnBytes) throws IOException {
        this();
        this.tx = CODEC.decode(txnBytes);
    }

    public TephraTransactionContext(PhoenixConnection connection) throws SQLException {
        PhoenixTransactionClient client = connection.getQueryServices().initTransactionClient(getProvider());  
        assert (client instanceof TephraTransactionClient);
        this.txServiceClient = ((TephraTransactionClient)client).getTransactionClient();
        this.txAwares = Collections.emptyList();
        this.txContext = new TransactionContext(txServiceClient);
    }

    private TephraTransactionContext(PhoenixTransactionContext ctx, boolean subTask) {
        assert (ctx instanceof TephraTransactionContext);
        TephraTransactionContext tephraTransactionContext = (TephraTransactionContext) ctx;
        this.txServiceClient = tephraTransactionContext.txServiceClient;  

        if (subTask) {
            this.tx = tephraTransactionContext.getTransaction();
            this.txAwares = Lists.newArrayList();
            this.txContext = null;
        } else {
            this.txAwares = Collections.emptyList();
            this.txContext = tephraTransactionContext.getContext();
        }
    }

    @Override
    public TransactionFactory.Provider getProvider() {
        return TransactionFactory.Provider.TEPHRA;
    }
    
    @Override
    public void begin() throws SQLException {
        if (txContext == null) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.NULL_TRANSACTION_CONTEXT).build()
                    .buildException();
        }

        try {
            txContext.start();
        } catch (TransactionFailureException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void commit() throws SQLException {

        if (txContext == null || !isTransactionRunning()) {
            return;
        }

        try {
            txContext.finish();
        } catch (TransactionFailureException e) {
            if (e instanceof TransactionConflictException) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION)
                        .setMessage(e.getMessage()).setRootCause(e).build()
                        .buildException();
            }
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void abort() throws SQLException {

        if (txContext == null || !isTransactionRunning()) {
            return;
        }

        try {
            txContext.abort();
        } catch (TransactionFailureException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void checkpoint(boolean hasUncommittedData) throws SQLException {
        if (hasUncommittedData) {
            try {
                if (txContext == null) {
                    tx = txServiceClient.checkpoint(tx);
                } else {
                    assert (txContext != null);
                    txContext.checkpoint();
                    tx = txContext.getCurrentTransaction();
                }
            } catch (TransactionFailureException e) {
                throw new SQLException(e);
            }
        }

        // Since we're querying our own table while mutating it, we must exclude
        // see our current mutations, otherwise we can get erroneous results
        // (for DELETE)
        // or get into an infinite loop (for UPSERT SELECT).
        if (txContext == null) {
            tx.setVisibility(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
        } else {
            assert (txContext != null);
            txContext.getCurrentTransaction().setVisibility(
                    VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
        }
    }

    @Override
    public void commitDDLFence(PTable dataTable)
            throws SQLException {
        byte[] key = dataTable.getName().getBytes();

        try {
            FenceWait fenceWait = VisibilityFence.prepareWait(key,
                    txServiceClient);
            fenceWait.await(10000, TimeUnit.MILLISECONDS);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Added write fence at ~"
                        + getCurrentTransaction().getReadPointer());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e)
                    .build().buildException();
        } catch (TimeoutException | TransactionFailureException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TX_UNABLE_TO_GET_WRITE_FENCE)
                    .setSchemaName(dataTable.getSchemaName().getString())
                    .setTableName(dataTable.getTableName().getString()).build()
                    .buildException();
        } finally {
            // The client expects a transaction to be in progress on the txContext while the
            // VisibilityFence.prepareWait() starts a new tx and finishes/aborts it. After it's
            // finished, we start a new one here.
            // TODO: seems like an autonomous tx capability in Tephra would be useful here.
            this.begin();
        }
    }

    @Override
    public void markDMLFence(PTable table) {
        if (table.getType() == PTableType.INDEX) {
            return;
        }
        
        byte[] logicalKey = table.getName().getBytes();
        TransactionAware logicalTxAware = VisibilityFence.create(logicalKey);

        if (this.txContext == null) {
            this.txAwares.add(logicalTxAware);
        } else {
            this.txContext.addTransactionAware(logicalTxAware);
        }

        byte[] physicalKey = table.getPhysicalName().getBytes();
        if (Bytes.compareTo(physicalKey, logicalKey) != 0) {
            TransactionAware physicalTxAware = VisibilityFence
                    .create(physicalKey);
            if (this.txContext == null) {
                this.txAwares.add(physicalTxAware);
            } else {
                this.txContext.addTransactionAware(physicalTxAware);
            }
        }
    }

    @Override
    public void join(PhoenixTransactionContext ctx) {
        if (ctx == PhoenixTransactionContext.NULL_CONTEXT) {
            return;
        }
        assert (ctx instanceof TephraTransactionContext);
        TephraTransactionContext tephraContext = (TephraTransactionContext) ctx;

        if (txContext != null) {
            for (TransactionAware txAware : tephraContext.getAwares()) {
                txContext.addTransactionAware(txAware);
            }
        } else {
            txAwares.addAll(tephraContext.getAwares());
        }
    }

    private Transaction getCurrentTransaction() {
        return tx != null ? tx : txContext != null ? txContext.getCurrentTransaction() : null;
    }

    @Override
    public boolean isTransactionRunning() {
        return getCurrentTransaction() != null;
    }

    @Override
    public void reset() {
        tx = null;
        txAwares.clear();
    }

    @Override
    public long getTransactionId() {
        Transaction tx = getCurrentTransaction();
        return tx == null ? HConstants.LATEST_TIMESTAMP : tx.getTransactionId(); // First write pointer - won't change with checkpointing
    }

    @Override
    public long getReadPointer() {
        Transaction tx = getCurrentTransaction();

        if (tx == null) {
            return (-1);
        }

        return tx.getReadPointer();
    }

    // For testing
    @Override
    public long getWritePointer() {
        Transaction tx = getCurrentTransaction();
        return tx == null ? HConstants.LATEST_TIMESTAMP : tx.getWritePointer();
    }

    @Override
    public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel) {
        VisibilityLevel tephraVisibilityLevel = null;

        switch (visibilityLevel) {
        case SNAPSHOT:
            tephraVisibilityLevel = VisibilityLevel.SNAPSHOT;
            break;
        case SNAPSHOT_EXCLUDE_CURRENT:
            tephraVisibilityLevel = VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
            break;
        case SNAPSHOT_ALL:
            tephraVisibilityLevel = VisibilityLevel.SNAPSHOT_ALL;
            break;
        default:
            assert (false);
        }

        Transaction tx = getCurrentTransaction();
        assert(tx != null);
        tx.setVisibility(tephraVisibilityLevel);
    }

    @Override
    public PhoenixVisibilityLevel getVisibilityLevel() {
        VisibilityLevel visibilityLevel = null;

        Transaction tx = getCurrentTransaction();
        assert(tx != null);
        visibilityLevel = tx.getVisibilityLevel();

        PhoenixVisibilityLevel phoenixVisibilityLevel;
        switch (visibilityLevel) {
        case SNAPSHOT:
            phoenixVisibilityLevel = PhoenixVisibilityLevel.SNAPSHOT;
            break;
        case SNAPSHOT_EXCLUDE_CURRENT:
            phoenixVisibilityLevel = PhoenixVisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
            break;
        case SNAPSHOT_ALL:
            phoenixVisibilityLevel = PhoenixVisibilityLevel.SNAPSHOT_ALL;
            break;
        default:
            phoenixVisibilityLevel = null;
        }

        return phoenixVisibilityLevel;
    }

    @Override
    public byte[] encodeTransaction() throws SQLException {
        Transaction tx = getCurrentTransaction();
        assert (tx != null);

        try {
            byte[] encodedTxBytes = CODEC.encode(tx);
            encodedTxBytes = Arrays.copyOf(encodedTxBytes, encodedTxBytes.length + 1);
            encodedTxBytes[encodedTxBytes.length - 1] = getProvider().getCode();
            return encodedTxBytes;
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     * TephraTransactionContext specific functions
     */

    Transaction getTransaction() {
        return this.getCurrentTransaction();
    }

    TransactionContext getContext() {
        return this.txContext;
    }

    List<TransactionAware> getAwares() {
        return txAwares;
    }

    void addTransactionAware(TransactionAware txAware) {
        if (this.txContext != null) {
            txContext.addTransactionAware(txAware);
        } else if (this.tx != null) {
            txAwares.add(txAware);
            assert (tx != null);
            txAware.startTx(tx);
        }
    }

    @Override
    public PhoenixTransactionContext newTransactionContext(PhoenixTransactionContext context, boolean subTask) {
        return new TephraTransactionContext(context, subTask);
    }
    
    @Override
    public Table getTransactionalTable(Table htable, boolean isConflictFree) {
        TransactionAwareHTable transactionAwareHTable = new TransactionAwareHTable(htable, isConflictFree ? TxConstants.ConflictDetection.NONE : TxConstants.ConflictDetection.ROW);
        this.addTransactionAware(transactionAwareHTable);
        return transactionAwareHTable;
    }

    @Override
    public Table getTransactionalTableWriter(PhoenixConnection connection, PTable table, Table htable, boolean isIndex) throws SQLException {
        // If we have indexes, wrap the HTable in a delegate HTable that
        // will attach the necessary index meta data in the event of a
        // rollback
        TransactionAwareHTable transactionAwareHTable;
        // Don't add immutable indexes (those are the only ones that would participate
        // during a commit), as we don't need conflict detection for these.
        if (isIndex) {
            transactionAwareHTable = new TransactionAwareHTable(htable, TxConstants.ConflictDetection.NONE);
            transactionAwareHTable.startTx(getTransaction());
        } else {
            htable = new RollbackHookHTableWrapper(htable, table, connection);
            transactionAwareHTable = new TransactionAwareHTable(htable, table.isImmutableRows() ? TxConstants.ConflictDetection.NONE : TxConstants.ConflictDetection.ROW);
            // Even for immutable, we need to do this so that an abort has the state
            // necessary to generate the rows to delete.
            this.addTransactionAware(transactionAwareHTable);
        }
        return transactionAwareHTable;
    }
    
    /**
     * 
     * Wraps Tephra data table HTables to catch when a rollback occurs so
     * that index Delete mutations can be generated and applied (as
     * opposed to storing them in the Tephra change set). This technique
     * allows the Tephra API to be used directly with HBase APIs and
     * Phoenix APIs since we can detect the rollback as a callback
     * when the Tephra rollback is called.
     *
     */
    private static class RollbackHookHTableWrapper extends DelegateHTable {
        private final PTable table;
        private final PhoenixConnection connection;

        private RollbackHookHTableWrapper(Table delegate, PTable table, PhoenixConnection connection) {
            super(delegate);
            this.table = table;
            this.connection = connection;
        }

        /**
         * Called by Tephra when a transaction is aborted. We have this wrapper so that we get an opportunity to attach
         * our index meta data to the mutations such that we can also undo the index mutations.
         */
        @Override
        public void delete(List<Delete> deletes) throws IOException {
            ServerCache cache = null;
            try {
                if (deletes.isEmpty()) { return; }
                // Attach meta data for server maintained indexes
                ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable();
                if (table.getIndexMaintainers(indexMetaDataPtr, connection)) {
                    cache = IndexMetaDataCacheClient.setMetaDataOnMutations(connection, table, deletes, indexMetaDataPtr);
                }

                // Send deletes for client maintained indexes
                List<PTable> indexes = IndexUtil.getClientMaintainedIndexes(table);
                if (!indexes.isEmpty()) {
                    PhoenixTxIndexMutationGenerator generator = PhoenixTxIndexMutationGenerator.newGenerator(connection, table, indexes, deletes.get(0)
                            .getAttributesMap());
                    Collection<Pair<Mutation, byte[]>> indexUpdates = generator.getIndexUpdates(delegate,
                            deletes.iterator());
                    for (PTable index : indexes) {
                        byte[] physicalName = index.getPhysicalName().getBytes();
                        try (Table hindex = connection.getQueryServices().getTable(physicalName)) {
                            List<Mutation> indexDeletes = Lists.newArrayListWithExpectedSize(deletes.size());
                            for (Pair<Mutation, byte[]> mutationPair : indexUpdates) {
                                if (Bytes.equals(mutationPair.getSecond(), physicalName)) {
                                    indexDeletes.add(mutationPair.getFirst());
                                }
                                Object[] results = new Object[indexDeletes.size()];
                                hindex.batch(indexDeletes, results);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    }
                }

                delegate.delete(deletes);
            } catch (SQLException e) {
                throw new IOException(e);
            } finally {
                if (cache != null) {
                    SQLCloseables.closeAllQuietly(Collections.singletonList(cache));
                }
            }
        }
    }    
}
