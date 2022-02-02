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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.TransactionService;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.HDFSTransactionStateStorage;
import org.apache.tephra.shaded.org.apache.twill.discovery.DiscoveryService;
import org.apache.tephra.shaded.org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.tephra.shaded.org.apache.twill.zookeeper.RetryStrategies;
import org.apache.tephra.shaded.org.apache.twill.zookeeper.ZKClientService;
import org.apache.tephra.shaded.org.apache.twill.zookeeper.ZKClientServices;
import org.apache.tephra.shaded.org.apache.twill.zookeeper.ZKClients;
import org.apache.tephra.snapshot.SnapshotCodecProvider;

import com.google.inject.util.Providers;

public class TephraTransactionService implements PhoenixTransactionService {
    private final ZKClientService zkClient;
    private final TransactionService txService;

    public TephraTransactionService(ZKClientService zkClient, TransactionService txService) {
        this.zkClient = zkClient;
        this.txService = txService;
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