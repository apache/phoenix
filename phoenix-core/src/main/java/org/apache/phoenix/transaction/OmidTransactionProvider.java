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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.InMemoryCommitTable;
import org.apache.omid.transaction.HBaseOmidClientConfiguration;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.TTable;
import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.omid.tso.client.TSOClient;
import org.apache.phoenix.coprocessor.OmidGCProcessor;
import org.apache.phoenix.coprocessor.OmidTransactionalProcessor;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.transaction.TransactionFactory.Provider;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class OmidTransactionProvider implements PhoenixTransactionProvider {
    private static final OmidTransactionProvider INSTANCE = new OmidTransactionProvider();
    public static final String OMID_TSO_PORT = "phoenix.omid.tso.port";
    public static final String OMID_TSO_CONFLICT_MAP_SIZE = "phoenix.omid.tso.conflict.map.size";
    public static final String OMID_TSO_TIMESTAMP_TYPE = "phoenix.omid.tso.timestamp.type";
    public static final int DEFAULT_OMID_TSO_CONFLICT_MAP_SIZE = 1000;
    public static final String DEFAULT_OMID_TSO_TIMESTAMP_TYPE = "WORLD_TIME";

    private HBaseTransactionManager transactionManager = null;
    private volatile CommitTable.Client commitTableClient = null;

    public static final OmidTransactionProvider getInstance() {
        return INSTANCE;
    }

    private OmidTransactionProvider() {
    }

    @Override
    public String toString() {
        return getProvider().toString();
    }
    
    @Override
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException {
        // Remove last byte (which is used to identify transaction provider)
        return new OmidTransactionContext(Arrays.copyOf(txnBytes,txnBytes.length-1));
    }

    @Override
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection) throws SQLException {
        return new OmidTransactionContext(connection);
    }

    @Override
    public PhoenixTransactionClient getTransactionClient(Configuration config, ConnectionInfo connectionInfo) throws SQLException{
        if (transactionManager == null) {
            try {
                HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
                clientConf.setConflictAnalysisLevel(OmidClientConfiguration.ConflictDetectionLevel.ROW);
                transactionManager = (HBaseTransactionManager) HBaseTransactionManager.newInstance(clientConf);
            } catch (IOException | InterruptedException e) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.TRANSACTION_FAILED)
                        .setMessage(e.getMessage()).setRootCause(e).build()
                        .buildException();
            }
        }

        return new OmidTransactionClient(transactionManager);
    }

    static class OmidTransactionClient implements PhoenixTransactionClient {
        private final HBaseTransactionManager transactionManager;

        public OmidTransactionClient(HBaseTransactionManager transactionManager) {
            this.transactionManager = transactionManager;
        }

        public HBaseTransactionManager getTransactionClient() {
            return transactionManager;
        }

        @Override
        public void close() throws IOException {}
    }

    // For testing only
    public CommitTable.Client getCommitTableClient() {
        return commitTableClient;
    }
    
    @Override
    public PhoenixTransactionService getTransactionService(Configuration config, ConnectionInfo connectionInfo) throws  SQLException{
        TSOServerConfig tsoConfig = new TSOServerConfig();
        TSOServer tso;

        String portStr = config.get(OMID_TSO_PORT);
        if (portStr == null) {
            throw new IllegalArgumentException(OMID_TSO_PORT + " config parameter must be bound");
        }
        int  port = Integer.parseInt(portStr);

        tsoConfig.setPort(port);
        tsoConfig.setConflictMapSize(config.getInt(OMID_TSO_CONFLICT_MAP_SIZE, DEFAULT_OMID_TSO_CONFLICT_MAP_SIZE));
        tsoConfig.setTimestampType(config.get(OMID_TSO_TIMESTAMP_TYPE, DEFAULT_OMID_TSO_TIMESTAMP_TYPE));

        Injector injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();

        OmidClientConfiguration clientConfig = new OmidClientConfiguration();
        clientConfig.setConnectionString("localhost:" + port);
        clientConfig.setConflictAnalysisLevel(OmidClientConfiguration.ConflictDetectionLevel.ROW);

        InMemoryCommitTable commitTable = (InMemoryCommitTable) injector.getInstance(CommitTable.class);

        try {
            // Create the associated Handler
            TSOClient client = TSOClient.newInstance(clientConfig);

            HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
            clientConf.setConnectionString("localhost:" + port);
            clientConf.setConflictAnalysisLevel(OmidClientConfiguration.ConflictDetectionLevel.ROW);
            clientConf.setHBaseConfiguration(config);
            commitTableClient = commitTable.getClient();

            transactionManager = HBaseTransactionManager.builder(clientConf).commitTableClient(commitTableClient)
                    .tsoClient(client).build();
        } catch (IOException | InterruptedException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }

        return new OmidTransactionService(tso, transactionManager);
    }

    static class OmidTransactionService implements PhoenixTransactionService {
        private final HBaseTransactionManager transactionManager;
        private TSOServer tso;

        public OmidTransactionService(TSOServer tso, HBaseTransactionManager transactionManager) {
            this.tso = tso;
            this.transactionManager = transactionManager;
        }

        public void start() {

        }

        @Override
        public void close() throws IOException {
            if (transactionManager != null) {
                transactionManager.close();
            }
            if (tso != null) {
                tso.stopAndWait();
            }
        }
    }

    @Override
    public Class<? extends RegionObserver> getCoprocessor() {
        return OmidTransactionalProcessor.class;
    }

    @Override
    public Class<? extends RegionObserver> getGCCoprocessor() {
        return OmidGCProcessor.class;
    }

    @Override
    public Provider getProvider() {
        return TransactionFactory.Provider.OMID;
    }

    @Override
    public boolean isUnsupported(Feature feature) {
        // FIXME: if we initialize a Set with the unsupported features
        // and check for containment, we run into a test failure
        // in SetPropertyOnEncodedTableIT.testSpecifyingColumnFamilyForTTLFails()
        // due to TableProperty.colFamSpecifiedException being null
        // (though it's set in the constructor). I suspect some
        // mysterious class loader issue. The below works fine
        // as a workaround.
        return true;
        // return (feature == Feature.ALTER_NONTX_TO_TX || feature == Feature.COLUMN_ENCODING || feature == Feature.MAINTAIN_LOCAL_INDEX_ON_SERVER);
    }

    @Override
    public Put markPutAsCommitted(Put put, long timestamp, long commitTimestamp) throws IOException {
        return TTable.markPutAsCommitted(put, timestamp, timestamp);
    }
}
