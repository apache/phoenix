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
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.phoenix.coprocessor.TephraTransactionalProcessor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.transaction.TransactionFactory.Provider;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.PooledClientProvider;
import org.apache.tephra.distributed.TransactionService;
import org.apache.tephra.distributed.TransactionServiceClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.HDFSTransactionStateStorage;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.zookeeper.TephraZKClientService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import com.google.common.collect.ArrayListMultimap;
import com.google.inject.util.Providers;

public class TephraTransactionProvider implements PhoenixTransactionProvider {
    private static final TephraTransactionProvider INSTANCE = new TephraTransactionProvider();
    
    public static final TephraTransactionProvider getInstance() {
        return INSTANCE;
    }
    
    private TephraTransactionProvider() {
    }
    
    @Override
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException {
       return new TephraTransactionContext(txnBytes);
    }
    
    @Override
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection) {
        return new TephraTransactionContext(connection);
    }

    @Override
    public PhoenixTransactionClient getTransactionClient(Configuration config, ConnectionInfo connectionInfo) {
        if (connectionInfo.isConnectionless()) {
            TransactionManager txnManager = new TransactionManager(config);
            TransactionSystemClient txClient = new InMemoryTxSystemClient(txnManager);
            return new TephraTransactionClient(txClient);
            
        }
        String zkQuorumServersString = config.get(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM);
        if (zkQuorumServersString==null) {
            zkQuorumServersString = connectionInfo.getZookeeperConnectionString();
        }

        int timeOut = config.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
        // Create instance of the tephra zookeeper client
        ZKClientService zkClientService  = ZKClientServices.delegate(
            ZKClients.reWatchOnExpire(
                ZKClients.retryOnFailure(
                     new TephraZKClientService(zkQuorumServersString, timeOut, null,
                             ArrayListMultimap.<String, byte[]>create()), 
                         RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS))
                     )
                );
        //txZKClientService.startAndWait();
        ZKDiscoveryService zkDiscoveryService = new ZKDiscoveryService(zkClientService);
        PooledClientProvider pooledClientProvider = new PooledClientProvider(
                config, zkDiscoveryService);
        TransactionServiceClient txClient = new TransactionServiceClient(config,pooledClientProvider);
        TephraTransactionClient client = new TephraTransactionClient(zkClientService, txClient);
        client.start();
        
        return client;
    }

    @Override
    public PhoenixTransactionService getTransactionService(Configuration config, ConnectionInfo connInfo) {
        ZKClientService zkClient = ZKClientServices.delegate(
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

        //zkClient.startAndWait();
        DiscoveryService discovery = new ZKDiscoveryService(zkClient);
        TransactionManager txManager = new TransactionManager(config, new HDFSTransactionStateStorage(config, new SnapshotCodecProvider(config), new TxMetricsCollector()), new TxMetricsCollector());
        TransactionService txService = new TransactionService(config, zkClient, discovery, Providers.of(txManager));
        TephraTransactionService service = new TephraTransactionService(zkClient, txService);
        //txService.startAndWait();            
        service.start();
        return service;
    }

    static class TephraTransactionService implements PhoenixTransactionService {
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
    
    static class TephraTransactionClient implements PhoenixTransactionClient {
        private final ZKClientService zkClient;
        private final TransactionSystemClient txClient;

        public TephraTransactionClient(TransactionSystemClient txClient) {
            this(null, txClient);
        }
        
        public TephraTransactionClient(ZKClientService zkClient, TransactionSystemClient txClient) {
            this.zkClient = zkClient;
            this.txClient = txClient;
        }
        
        public void start() {
            zkClient.startAndWait();
        }
        
        public TransactionSystemClient getTransactionClient() {
            return txClient;
        }
        
        @Override
        public void close() throws IOException {
            zkClient.stopAndWait();
        }
        
    }
    
    @Override
    public Class<? extends RegionObserver> getCoprocessor() {
        return TephraTransactionalProcessor.class;
    }

    @Override
    public Provider getProvider() {
        return TransactionFactory.Provider.TEPHRA;
    }

    @Override
    public boolean isUnsupported(Feature feature) {
        return false;
    }

}
