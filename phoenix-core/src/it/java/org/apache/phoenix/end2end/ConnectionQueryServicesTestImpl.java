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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.transaction.PhoenixTransactionClient;
import org.apache.phoenix.transaction.PhoenixTransactionService;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.transaction.TransactionFactory.Provider;
import org.apache.phoenix.util.SQLCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * 
 * Implementation of ConnectionQueryServices for tests running against
 * the mini cluster
 *
 * 
 * @since 0.1
 */
public class ConnectionQueryServicesTestImpl extends ConnectionQueryServicesImpl {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionQueryServicesTestImpl.class);
    protected int NUM_SLAVES_BASE = 1; // number of slaves for the cluster
    // Track open connections to free them on close as unit tests don't always do this.
    private Set<PhoenixConnection> connections = Sets.newHashSet();
    private final PhoenixTransactionService[] txServices = new PhoenixTransactionService[TransactionFactory.Provider.values().length];
    
    public ConnectionQueryServicesTestImpl(QueryServices services, ConnectionInfo info, Properties props) throws SQLException {
        super(services, info, props);
    }
    
    @Override
    public synchronized void addConnection(PhoenixConnection connection) throws SQLException {
        connections.add(connection);
    }
    
    @Override
    public synchronized void removeConnection(PhoenixConnection connection) throws SQLException {
        connections.remove(connection);
    }

    @Override
    public void close() throws SQLException {
        try {
            Collection<PhoenixConnection> connections;
            synchronized(this) {
                // Make copy to prevent ConcurrentModificationException (TODO: figure out why this is necessary)
                connections = new ArrayList<>(this.connections);
                this.connections = Sets.newHashSet();
                
                // shut down the tx client service if we created one to support transactions
                for (PhoenixTransactionService service : txServices) {
                    if (service != null) {
                        try {
                            service.close();
                        } catch (IOException e) {
                            logger.warn(e.getMessage(), e);
                        }
                    }
                }

            }
            SQLCloseables.closeAll(connections);
            long unfreedBytes = clearCache();
            assertEquals("Found unfreed bytes in server-side cache", 0, unfreedBytes);
        } finally {
            super.close();
        }
    }
    
    @Override
    public synchronized PhoenixTransactionClient initTransactionClient(Provider provider) {
        PhoenixTransactionService txService = txServices[provider.ordinal()];
        if (txService == null) {
            txService = txServices[provider.ordinal()] = provider.getTransactionProvider().getTransactionService(config, connectionInfo);
        }
        return super.initTransactionClient(provider);
    }
}
