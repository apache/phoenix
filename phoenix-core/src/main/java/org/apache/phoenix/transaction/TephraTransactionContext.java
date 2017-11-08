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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.transaction.PhoenixTransactionContext.PhoenixVisibilityLevel;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.tephra.Transaction;
import org.apache.tephra.Transaction.VisibilityLevel;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.PooledClientProvider;
import org.apache.tephra.distributed.TransactionService;
import org.apache.tephra.distributed.TransactionServiceClient;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.HDFSTransactionStateStorage;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.util.TxUtils;
import org.apache.tephra.visibility.FenceWait;
import org.apache.tephra.visibility.VisibilityFence;
import org.apache.tephra.zookeeper.TephraZKClientService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.inject.util.Providers;

public class TephraTransactionContext implements PhoenixTransactionContext {

    private static final TransactionCodec CODEC = new TransactionCodec();

    private static TransactionSystemClient txClient = null;
    private static ZKClientService zkClient = null;
    private static TransactionService txService = null;
    private static TransactionManager txManager = null;

    private final List<TransactionAware> txAwares;
    private final TransactionContext txContext;
    private Transaction tx;
    private TransactionSystemClient txServiceClient;
    private TransactionFailureException e;

    public TephraTransactionContext() {
        this.txServiceClient = null;
        this.txAwares = Lists.newArrayList();
        this.txContext = null;
    }

    public TephraTransactionContext(byte[] txnBytes) throws IOException {
        this();
        this.tx = (txnBytes != null && txnBytes.length > 0) ? CODEC
                .decode(txnBytes) : null;
    }

    public TephraTransactionContext(PhoenixConnection connection) {
        this.txServiceClient = txClient;  
        this.txAwares = Collections.emptyList();
        this.txContext = new TransactionContext(txServiceClient);
    }

    public TephraTransactionContext(PhoenixTransactionContext ctx,
            PhoenixConnection connection, boolean subTask) {
        this.txServiceClient = txClient;  
        assert (ctx instanceof TephraTransactionContext);
        TephraTransactionContext tephraTransactionContext = (TephraTransactionContext) ctx;

        if (subTask) {
            this.tx = tephraTransactionContext.getTransaction();
            this.txAwares = Lists.newArrayList();
            this.txContext = null;
        } else {
            this.txAwares = Collections.emptyList();
            this.txContext = tephraTransactionContext.getContext();
        }

        this.e = null;
    }

    @Override
    public void setInMemoryTransactionClient(Configuration config) {
        TransactionManager txnManager = new TransactionManager(config);
        txClient = this.txServiceClient = new InMemoryTxSystemClient(txnManager);
    }

    @Override
    public ZKClientService setTransactionClient(Configuration config, ReadOnlyProps props, ConnectionInfo connectionInfo) {
        String zkQuorumServersString = props.get(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM);
        if (zkQuorumServersString==null) {
            zkQuorumServersString = connectionInfo.getZookeeperQuorum()+":"+connectionInfo.getPort();
        }

        int timeOut = props.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
        // Create instance of the tephra zookeeper client
        ZKClientService txZKClientService  = ZKClientServices.delegate(
            ZKClients.reWatchOnExpire(
                ZKClients.retryOnFailure(
                     new TephraZKClientService(zkQuorumServersString, timeOut, null,
                             ArrayListMultimap.<String, byte[]>create()), 
                         RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS))
                     )
                );
        txZKClientService.startAndWait();
        ZKDiscoveryService zkDiscoveryService = new ZKDiscoveryService(txZKClientService);
        PooledClientProvider pooledClientProvider = new PooledClientProvider(
                config, zkDiscoveryService);
        txClient = this.txServiceClient = new TransactionServiceClient(config,pooledClientProvider);
        
        return txZKClientService;
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
            this.e = e;

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
            if (e != null) {
                txContext.abort(e);
                e = null;
            } else {
                txContext.abort();
            }
        } catch (TransactionFailureException e) {
            this.e = null;
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
    public void commitDDLFence(PTable dataTable, Logger logger)
            throws SQLException {
        byte[] key = dataTable.getName().getBytes();

        try {
            FenceWait fenceWait = VisibilityFence.prepareWait(key,
                    txServiceClient);
            fenceWait.await(10000, TimeUnit.MILLISECONDS);

            if (logger.isInfoEnabled()) {
                logger.info("Added write fence at ~"
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
        }
    }

    public void markDMLFence(PTable table) {
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
        this.e = null;
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
            return CODEC.encode(tx);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long getMaxTransactionsPerSecond() {
        return TxConstants.MAX_TX_PER_MS;
    }

    @Override
    public boolean isPreExistingVersion(long version) {
        return TxUtils.isPreExistingVersion(version);
    }

    @Override
    public RegionObserver getCoProcessor() {
        return new TransactionProcessor();
    }

    @Override
    public byte[] getFamilyDeleteMarker() {
        return TxConstants.FAMILY_DELETE_QUALIFIER;
    }

    @Override
    public void setTxnConfigs(Configuration config, String tmpFolder, int defaultTxnTimeoutSeconds) throws IOException {
        config.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
        config.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
        config.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 1);
        config.setInt(TxConstants.Service.CFG_DATA_TX_BIND_PORT, Networks.getRandomPort());
        config.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, tmpFolder);
        config.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, defaultTxnTimeoutSeconds);
        config.unset(TxConstants.Manager.CFG_TX_HDFS_USER);
        config.setLong(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, 5L);
    }

    @Override
    public void setupTxManager(Configuration config, String url) throws SQLException {

        if (txService != null) {
            return;
        }

        ConnectionInfo connInfo = ConnectionInfo.create(url);
        zkClient = ZKClientServices.delegate(
          ZKClients.reWatchOnExpire(
            ZKClients.retryOnFailure(
              ZKClientService.Builder.of(connInfo.getZookeeperConnectionString())
                .setSessionTimeout(config.getInt(HConstants.ZK_SESSION_TIMEOUT,
                        HConstants.DEFAULT_ZK_SESSION_TIMEOUT))
                .build(),
              RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
            )
          )
        );
        zkClient.startAndWait();

        DiscoveryService discovery = new ZKDiscoveryService(zkClient);
        txManager = new TransactionManager(config, new HDFSTransactionStateStorage(config, new SnapshotCodecProvider(config), new TxMetricsCollector()), new TxMetricsCollector());
        txService = new TransactionService(config, zkClient, discovery, Providers.of(txManager));
        txService.startAndWait();
    }

    @Override
    public void tearDownTxManager() {
        try {
            if (txService != null) txService.stopAndWait();
        } finally {
            try {
                if (zkClient != null) zkClient.stopAndWait();
            } finally {
                txService = null;
                zkClient = null;
                txManager = null;
            }
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
}
