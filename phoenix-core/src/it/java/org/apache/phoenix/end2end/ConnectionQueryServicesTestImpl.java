/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.RpcController;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.transaction.PhoenixTransactionClient;
import org.apache.phoenix.transaction.PhoenixTransactionService;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.transaction.TransactionFactory.Provider;
import org.apache.phoenix.transaction.TransactionServiceManager;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.TestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

/**
 * Implementation of ConnectionQueryServices for tests running against the mini cluster
 * @since 0.1
 */
public class ConnectionQueryServicesTestImpl extends ConnectionQueryServicesImpl {
  private static final Logger LOGGER =
    LoggerFactory.getLogger(ConnectionQueryServicesTestImpl.class);
  // Track open connections to free them on close as unit tests don't always do this.
  private Set<PhoenixConnection> connections =
    Collections.newSetFromMap(new ConcurrentHashMap<PhoenixConnection, Boolean>());
  // Use Provider.values() instead of Provider.available() here because array accesses will be
  // indexed by ordinal.
  private final PhoenixTransactionService[] txServices =
    new PhoenixTransactionService[TransactionFactory.Provider.values().length];
  // All connection services in the JVM share one test transaction service for each provider.
  // The provider is a singleton, and each region keeps the commit table client that is current
  // when the region opens. A second service replaces that client, and readers can then miss
  // commits. The connection service that starts a shared service owns it and closes it.
  private static final PhoenixTransactionService[] SHARED_TX_SERVICES =
    new PhoenixTransactionService[TransactionFactory.Provider.values().length];

  public ConnectionQueryServicesTestImpl(QueryServices services, ConnectionInfo info,
    Properties props) throws SQLException {
    super(services, info, props);
  }

  @Override
  public void addConnection(PhoenixConnection connection) throws SQLException {
    connections.add(connection);
    super.addConnection(connection);
  }

  @Override
  public void removeConnection(PhoenixConnection connection) throws SQLException {
    connections.remove(connection);
    super.removeConnection(connection);
  }

  public RpcController getController() {
    return super.getController();
  }

  @Override
  public void close() throws SQLException {
    try {
      Collection<PhoenixConnection> connections;
      synchronized (this) {
        // Make copy to prevent ConcurrentModificationException (TODO: figure out why this is
        // necessary)
        connections = new ArrayList<>(this.connections);
        this.connections = Sets.newHashSet();

        // Shut down the transaction services that this instance owns. Release the shared slot
        // in the same step, so that the next connection service starts a new service.
        for (int i = 0; i < txServices.length; i++) {
          PhoenixTransactionService service = txServices[i];
          if (service != null) {
            synchronized (SHARED_TX_SERVICES) {
              if (SHARED_TX_SERVICES[i] == service) {
                SHARED_TX_SERVICES[i] = null;
              }
              try {
                service.close();
              } catch (IOException e) {
                LOGGER.warn(e.getMessage(), e);
              }
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
  public synchronized PhoenixTransactionClient initTransactionClient(Provider provider)
    throws SQLException {
    int i = provider.ordinal();
    if (txServices[i] == null) {
      // Start a service only when no other connection service owns a live one.
      synchronized (SHARED_TX_SERVICES) {
        if (SHARED_TX_SERVICES[i] == null) {
          int port = TestUtil.getRandomPort();
          txServices[i] = SHARED_TX_SERVICES[i] = TransactionServiceManager
            .startTransactionService(provider, config, connectionInfo, port);
        }
      }
    }
    return super.initTransactionClient(provider);
  }
}
