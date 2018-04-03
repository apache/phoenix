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
import java.util.HashSet;
import java.util.Set;
import java.util.Random;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.InMemoryCommitTable;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.transaction.CellUtils;
import org.apache.omid.transaction.HBaseCellId;
import org.apache.omid.transaction.HBaseOmidClientConfiguration;
import org.apache.omid.transaction.HBaseTransaction;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.OmidSnapshotFilter;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionException;
import org.apache.omid.transaction.TransactionManager;
//import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.transaction.Transaction.Status;
import org.apache.omid.transaction.OmidCompactor;
import org.apache.omid.tso.TSOMockModule;
//import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;
import org.apache.omid.tso.client.TSOClient;
import org.apache.omid.TestUtils;


import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.InvalidProtocolBufferException;

public class OmidTransactionContext implements PhoenixTransactionContext {

    private static HBaseTransactionManager transactionManager = null;

    private TransactionManager tm;
    private HBaseTransaction tx;

    private static CommitTable.Client commitTableClient = null;
    private static final long MAX_NON_TX_TIMESTAMP = (long) (System.currentTimeMillis() * 1.1);

    // For testing
    private TSOServer tso;

    private Random rand = new Random();

    public OmidTransactionContext() {
        this.tx = null;
        this.tm = null;
        this.tso = null;
    }

    public OmidTransactionContext(PhoenixConnection connection) {
        this.tm = transactionManager;
        this.tx = null;
        this.tso = null;
    }

    public OmidTransactionContext(byte[] txnBytes) throws InvalidProtocolBufferException {
        this();

        if (txnBytes != null && txnBytes.length > 0) {
            TSOProto.Transaction transaction = TSOProto.Transaction.parseFrom(txnBytes);
            tx = new HBaseTransaction(transaction.getTimestamp(), transaction.getEpoch(), new HashSet<HBaseCellId>(), new HashSet<HBaseCellId>(), transactionManager);
//            tx = new HBaseTransaction(transaction.getTransactionId(), transaction.getEpoch(), new HashSet<HBaseCellId>(), null);
        } else {
            tx = null;
        }
    }

    public OmidTransactionContext(PhoenixTransactionContext ctx,
            PhoenixConnection connection, boolean subTask) {

        this.tm = transactionManager;
        
        assert (ctx instanceof OmidTransactionContext);
        OmidTransactionContext omidTransactionContext = (OmidTransactionContext) ctx;
        
        
        if (subTask) {
            if (omidTransactionContext.isTransactionRunning()) {
                Transaction transaction = omidTransactionContext.getTransaction();
                this.tx = new HBaseTransaction(transaction.getTransactionId(), transaction.getEpoch(), new HashSet<HBaseCellId>(), new HashSet<HBaseCellId>(), transactionManager, transaction.getReadTimestamp(), transaction.getWriteTimestamp());
            } else {
                this.tx = null;
            }

            this.tm = null;
        } else {
            this.tx = omidTransactionContext.getTransaction();
        }

        this.tso = null;
    }

    @Override
    public void begin() throws SQLException {
        if (tm == null) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.NULL_TRANSACTION_CONTEXT).build()
                    .buildException();
        }


        try {
            tx = (HBaseTransaction) tm.begin();
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void commit() throws SQLException {
        if (tx == null || tm == null)
            return;

        try {
            tm.commit(tx);
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        } catch (RollbackException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void abort() throws SQLException {
        if (tx == null || tm == null || tx.getStatus() != Status.RUNNING) {
            return;
        }

        try {
            tm.rollback(tx);
        } catch (TransactionException e) {
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
                tx.checkpoint();
            } catch (TransactionException e) {
                throw new SQLException(e);
            }
        }
        tx.setVisibilityLevel(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
    }

    @Override
    public void commitDDLFence(PTable dataTable, Logger logger) throws SQLException {

        try {
            tx = (HBaseTransaction) transactionManager.fence(dataTable.getName().getBytes());
            if (logger.isInfoEnabled()) {
                logger.info("Added write fence at ~"
                        + tx.getReadTimestamp());
            }
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TX_UNABLE_TO_GET_WRITE_FENCE)
            .setSchemaName(dataTable.getSchemaName().getString())
            .setTableName(dataTable.getTableName().getString()).build()
            .buildException();
        }
    }

    public void markDMLFence(PTable table) {

    }

    @Override
    public void join(PhoenixTransactionContext ctx) {
        assert (ctx instanceof OmidTransactionContext);
        OmidTransactionContext omidContext = (OmidTransactionContext) ctx;
        
        HBaseTransaction transaction = omidContext.getTransaction();
        if (transaction == null || tx == null) return;

        Set<HBaseCellId> writeSet = transaction.getWriteSet();

        for (HBaseCellId cell : writeSet) {
            tx.addWriteSetElement(cell);
        }
    }

    @Override
    public boolean isTransactionRunning() {
        return (tx != null);
    }

    @Override
    public void reset() {
        tx = null;
    }

    @Override
    public long getTransactionId() {
        return tx.getTransactionId();
    }

    @Override
    public long getReadPointer() {
        return tx.getReadTimestamp();
    }

    @Override
    public long getWritePointer() {
        return tx.getWriteTimestamp();
    }

    @Override
    public PhoenixVisibilityLevel getVisibilityLevel() {
        VisibilityLevel visibilityLevel = null;

        assert(tx != null && tx instanceof HBaseTransaction);
        visibilityLevel = ((HBaseTransaction) tx).getVisibilityLevel();

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
    public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel) {

        VisibilityLevel omidVisibilityLevel = null;

        switch (visibilityLevel) {
        case SNAPSHOT:
            omidVisibilityLevel = VisibilityLevel.SNAPSHOT;
            break;
        case SNAPSHOT_EXCLUDE_CURRENT:
            omidVisibilityLevel = VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
            break;
        case SNAPSHOT_ALL:
            omidVisibilityLevel = VisibilityLevel.SNAPSHOT_ALL;
            break;
        default:
            assert (false);
        }

        assert(tx != null && tx instanceof HBaseTransaction);
        ((HBaseTransaction) tx).setVisibilityLevel(omidVisibilityLevel);

    }

    @Override
    public byte[] encodeTransaction() throws SQLException {
        assert(tx != null);

        TSOProto.Transaction.Builder transactionBuilder = TSOProto.Transaction.newBuilder();

        transactionBuilder.setTimestamp(tx.getTransactionId());
        transactionBuilder.setEpoch(tx.getEpoch());

        return transactionBuilder.build().toByteArray();
    }

    @Override
    public long getMaxTransactionsPerSecond() {
        // TODO get the number from the TSO config
        return 1_000_000;
    }

    @Override
    public boolean isPreExistingVersion(long version) {
        // TODO Ohad to complete according the timestamp setting in Omid
        return version < MAX_NON_TX_TIMESTAMP;
    }

    @Override
    public BaseRegionObserver getCoprocessor() {
        return new OmidSnapshotFilter(commitTableClient);
    }

    @Override
    public BaseRegionObserver getGarbageCollector() {
        return new OmidCompactor(true);
    }

    @Override
    public void setInMemoryTransactionClient(Configuration config) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public ZKClientService setTransactionClient(Configuration config, ReadOnlyProps props,
            ConnectionInfo connectionInfo) throws SQLException {
        if (transactionManager == null) {
            try {
                HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
                clientConf.setConflictAnalysisLevel(ConflictDetectionLevel.ROW);
                transactionManager = (HBaseTransactionManager) HBaseTransactionManager.newInstance(clientConf);
            } catch (IOException | InterruptedException e) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.TRANSACTION_FAILED)
                .setMessage(e.getMessage()).setRootCause(e).build()
                .buildException();
            }
        }

        return null;
    }

    @Override
    public byte[] getFamilyDeleteMarker() {
        return CellUtils.FAMILY_DELETE_QUALIFIER;
    }

    @Override
    public void setTxnConfigs(Configuration config, String tmpFolder, int defaultTxnTimeoutSeconds) throws IOException {
        // TODO Auto-generated method stub

    }

    // For testing
    @Override
    public void setupTxManager(Configuration config, String url) throws SQLException {
        // TSO Setup
        TSOServerConfig tsoConfig = new TSOServerConfig();

        int  port = rand.nextInt(65534) + 1;

        tsoConfig.setPort(port);
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setTimestampType("WORLD_TIME");
        //tsoConfig.setTimestampType("INCREMENTAL");
        Injector injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();

//        TSOServerConfig tsoConfig = new TSOServerConfig();
//        tsoConfig.setPort(54758);
//        tsoConfig.setConflictMapSize(1000);
//        try {
//            tso = TSOServer.getInitializedTsoServer(tsoConfig);
//            tso.startAndWait();
//        } catch (Exception e) {
//            throw new SQLExceptionInfo.Builder(
//                    SQLExceptionCode.TRANSACTION_FAILED)
//                    .setMessage(e.getMessage()).setRootCause(e).build()
//                    .buildException();
//        }

        OmidClientConfiguration clientConfig = new OmidClientConfiguration();
        clientConfig.setConnectionString("localhost:" + port);
        clientConfig.setConflictAnalysisLevel(ConflictDetectionLevel.ROW);



        InMemoryCommitTable commitTable = (InMemoryCommitTable) injector.getInstance(CommitTable.class);

        try {
            // Create the associated Handler
            TSOClient client = TSOClient.newInstance(clientConfig);

            HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
            clientConf.setConnectionString("localhost:" + port);
            clientConf.setConflictAnalysisLevel(ConflictDetectionLevel.ROW);
            //        clientConf.setHBaseConfiguration(hbaseConf);
            commitTableClient = commitTable.getClient();

//            omidSnapshotFilter.setCommitTableClient(commitTableClient);

            transactionManager = HBaseTransactionManager.builder(clientConf).commitTableClient(commitTableClient)
                    .tsoClient(client).build();
        } catch (IOException | InterruptedException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
            .setMessage(e.getMessage()).setRootCause(e).build()
            .buildException();
        }
        
//        HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
//        clientConf.setConnectionString("localhost:1234");

//        try {
//            transactionManager = HBaseTransactionManager.newInstance(clientConf);
//        } catch (IOException | InterruptedException e) {
//            throw new SQLExceptionInfo.Builder(
//                    SQLExceptionCode.TRANSACTION_FAILED)
//                    .setMessage(e.getMessage()).setRootCause(e).build()
//                    .buildException();
//        }
    }

    // For testing
    @Override
    public void tearDownTxManager() throws SQLException {
        try {
            if (transactionManager != null) {
                transactionManager.close();
            }
            if (tso != null) {
                tso.stopAndWait();
            }
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    /**
     *  OmidTransactionContext specific functions 
     */

    public HBaseTransaction getTransaction() {
        return tx;
    }
}
