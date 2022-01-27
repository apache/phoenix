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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.Client;
import org.apache.omid.committable.CommitTable.Writer;
import org.apache.omid.committable.InMemoryCommitTable;
import org.apache.omid.transaction.HBaseOmidClientConfiguration;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.TSOServerConfig.WAIT_STRATEGY;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.omid.tso.client.TSOClient;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.transaction.TransactionFactory.Provider;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.TransactionService;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.HDFSTransactionStateStorage;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.shaded.org.apache.twill.discovery.DiscoveryService;
import org.apache.tephra.shaded.org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.tephra.shaded.org.apache.twill.zookeeper.RetryStrategies;
import org.apache.tephra.shaded.org.apache.twill.zookeeper.ZKClientService;
import org.apache.tephra.shaded.org.apache.twill.zookeeper.ZKClientServices;
import org.apache.tephra.shaded.org.apache.twill.zookeeper.ZKClients;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Providers;

public class TestTransactionServiceManager {

    public static final String OMID_TSO_PORT = "phoenix.omid.tso.port";
    public static final String OMID_TSO_CONFLICT_MAP_SIZE = "phoenix.omid.tso.conflict.map.size";
    public static final String OMID_TSO_TIMESTAMP_TYPE = "phoenix.omid.tso.timestamp.type";
    public static final int DEFAULT_OMID_TSO_CONFLICT_MAP_SIZE = 1000;
    public static final String DEFAULT_OMID_TSO_TIMESTAMP_TYPE = "WORLD_TIME";

    public static OmidTransactionService startAndInjectOmidTransactionService(
            OmidTransactionProvider transactionProvider, Configuration config,
            ConnectionInfo connectionInfo, int port) throws SQLException {
        TSOServerConfig tsoConfig = new TSOServerConfig();
        TSOServer tso;

        tsoConfig.setPort(port);
        tsoConfig.setConflictMapSize(
            config.getInt(OMID_TSO_CONFLICT_MAP_SIZE, DEFAULT_OMID_TSO_CONFLICT_MAP_SIZE));
        tsoConfig.setTimestampType(
            config.get(OMID_TSO_TIMESTAMP_TYPE, DEFAULT_OMID_TSO_TIMESTAMP_TYPE));
        tsoConfig.setWaitStrategy(WAIT_STRATEGY.LOW_CPU.toString());

        Injector injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        tso = injector.getInstance(TSOServer.class);
        tso.startAsync();
        tso.awaitRunning();

        OmidClientConfiguration clientConfig = new OmidClientConfiguration();
        clientConfig.setConnectionString("localhost:" + port);
        clientConfig.setConflictAnalysisLevel(OmidClientConfiguration.ConflictDetectionLevel.ROW);

        InMemoryCommitTable commitTable =
                (InMemoryCommitTable) injector.getInstance(CommitTable.class);

        HBaseTransactionManager transactionManager;
        Client commitTableClient;
        Writer commitTableWriter;
        try {
            // Create the associated Handler
            TSOClient client = TSOClient.newInstance(clientConfig);

            HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
            clientConf.setConnectionString("localhost:" + port);
            clientConf.setConflictAnalysisLevel(OmidClientConfiguration.ConflictDetectionLevel.ROW);
            clientConf.setHBaseConfiguration(config);
            commitTableClient = commitTable.getClient();
            commitTableWriter = commitTable.getWriter();
            transactionManager =
                    HBaseTransactionManager.builder(clientConf).commitTableClient(commitTableClient)
                            .commitTableWriter(commitTableWriter).tsoClient(client).build();
        } catch (IOException | InterruptedException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build().buildException();
        }

        transactionProvider.injectTestService(transactionManager, commitTableClient);

        return new OmidTransactionService(tso, transactionManager);
    }

    public static class OmidTransactionService implements PhoenixTransactionService {
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
                tso.stopAsync();
                tso.awaitTerminated();
            }
        }
    }

    public static TephraTransactionService startAndInjectTephraTransactionService(
            TephraTransactionProvider transactionProvider,
            Configuration config, ConnectionInfo connInfo, int port) {
        config.setInt(TxConstants.Service.CFG_DATA_TX_BIND_PORT, port);
        int retryTimeOut =
                config.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_DISCOVERY_TIMEOUT_SEC,
                    TxConstants.Service.DEFAULT_DATA_TX_CLIENT_DISCOVERY_TIMEOUT_SEC);
        ZKClientService zkClient =
                ZKClientServices.delegate(ZKClients.reWatchOnExpire(ZKClients.retryOnFailure(
                    ZKClientService.Builder.of(connInfo.getZookeeperConnectionString())
                            .setSessionTimeout(config.getInt(HConstants.ZK_SESSION_TIMEOUT,
                                HConstants.DEFAULT_ZK_SESSION_TIMEOUT))
                            .build(),
                    RetryStrategies.exponentialDelay(500, retryTimeOut, TimeUnit.MILLISECONDS))));

        DiscoveryService discovery = new ZKDiscoveryService(zkClient);
        TransactionManager txManager =
                new TransactionManager(
                        config, new HDFSTransactionStateStorage(config,
                                new SnapshotCodecProvider(config), new TxMetricsCollector()),
                        new TxMetricsCollector());
        TransactionService txService =
                new TransactionService(config, zkClient, discovery, Providers.of(txManager));
        TephraTransactionService service = new TephraTransactionService(zkClient, txService);
        service.start();

        return service;
    }

    public static class TephraTransactionService implements PhoenixTransactionService {
        private final ZKClientService zkClient;
        private final TransactionService txService;

        public TephraTransactionService(ZKClientService zkClient, TransactionService txService) {
            this.zkClient = zkClient;
            this.txService = txService;
        }

        public void start() {
            zkClient.startAndWait();
            txService.startAndWait();
        }

        @Override
        public void close() throws IOException {
            try {
                if (txService != null) txService.stopAndWait();
            } finally {
                if (zkClient != null) zkClient.stopAndWait();
            }
        }

    }

    public static PhoenixTransactionService startTransactionService(TransactionFactory.Provider provider, Configuration config, ConnectionInfo connInfo, int port) throws SQLException {
        PhoenixTransactionProvider transactionProvider = provider.getTransactionProvider();
        if(provider == Provider.TEPHRA) {
            return startAndInjectTephraTransactionService((TephraTransactionProvider)transactionProvider, config, connInfo, port);
        } else if (provider == Provider.OMID) {
            return startAndInjectOmidTransactionService((OmidTransactionProvider)transactionProvider, config, connInfo, port);
        }
        throw new UnsupportedOperationException("Unknown transaction provider");
    }

}