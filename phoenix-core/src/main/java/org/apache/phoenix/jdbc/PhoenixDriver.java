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
package org.apache.phoenix.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.ConnectionlessQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesImpl;
import org.apache.phoenix.util.SQLCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * JDBC Driver implementation of Phoenix for production.
 * To use this driver, specify the following URL:
 *     jdbc:phoenix:<zookeeper quorum server name>;
 * Only an embedded driver is currently supported (Phoenix client
 * runs in the same JVM as the driver). Connections are lightweight
 * and are not pooled. The last part of the URL, the hbase zookeeper
 * quorum server name, determines the hbase cluster to which queries
 * will be routed.
 * 
 * 
 * @since 0.1
 */
public final class PhoenixDriver extends PhoenixEmbeddedDriver {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixDriver.class);
    public static final PhoenixDriver INSTANCE;
    private static volatile String driverShutdownMsg;
    static {
        try {
            DriverManager.registerDriver( INSTANCE = new PhoenixDriver() );
            // Add shutdown hook to release any resources that were never closed
            // In theory not necessary, but it won't hurt anything
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        INSTANCE.close();
                    } catch (SQLException e) {
                        logger.warn("Unable to close PhoenixDriver on shutdown", e);
                    } finally {
                        driverShutdownMsg = "Phoenix driver closed because server is shutting down";
                    }
                }
            });
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to register " + PhoenixDriver.class.getName() + ": "+ e.getMessage());
        }
    }
    // One entry per cluster here
    private final ConcurrentMap<ConnectionInfo,ConnectionQueryServices> connectionQueryServicesMap = new ConcurrentHashMap<ConnectionInfo,ConnectionQueryServices>(3);

    public PhoenixDriver() { // for Squirrel
        // Use production services implementation
        super();
    }
    
    // writes guarded by "this"
    private volatile QueryServices services;
    
    @GuardedBy("closeLock")
    private volatile boolean closed = false;
    private final ReadWriteLock closeLock = new ReentrantReadWriteLock();
    

    @Override
    public QueryServices getQueryServices() {
        try {
            closeLock.readLock().lock();
            checkClosed();

            // Lazy initialize QueryServices so that we only attempt to create an HBase Configuration
            // object upon the first attempt to connect to any cluster. Otherwise, an attempt will be
            // made at driver initialization time which is too early for some systems.
            QueryServices result = services;
            if (result == null) {
                synchronized(this) {
                    result = services;
                    if(result == null) {
                        services = result = new QueryServicesImpl(getDefaultProps());
                    }
                }
            }
            return result;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // Accept the url only if test=true attribute not set
        return super.acceptsURL(url) && !isTestUrl(url);
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            closeLock.readLock().lock();
            checkClosed();
            return super.connect(url, info);
        } finally {
            closeLock.readLock().unlock();
        }
    }
    
    @Override
    protected ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException {
        try {
            closeLock.readLock().lock();
            checkClosed();
            ConnectionInfo connInfo = ConnectionInfo.create(url);
            QueryServices services = getQueryServices();
            ConnectionInfo normalizedConnInfo = connInfo.normalize(services.getProps());
            ConnectionQueryServices connectionQueryServices = connectionQueryServicesMap.get(normalizedConnInfo);
            if (connectionQueryServices == null) {
                if (normalizedConnInfo.isConnectionless()) {
                    connectionQueryServices = new ConnectionlessQueryServicesImpl(services, normalizedConnInfo);
                } else {
                    connectionQueryServices = new ConnectionQueryServicesImpl(services, normalizedConnInfo, info);
                }
                ConnectionQueryServices prevValue = connectionQueryServicesMap.putIfAbsent(normalizedConnInfo, connectionQueryServices);
                if (prevValue != null) {
                    connectionQueryServices = prevValue;
                }
            }
            boolean success = false;
            SQLException sqlE = null;
            try {
                connectionQueryServices.init(url, info);
                success = true;
            } catch (SQLException e) {
                sqlE = e;
            }
            finally {
                if (!success) {
                    try {
                        connectionQueryServices.close();
                    } catch (SQLException e) {
                        if (sqlE == null) {
                            sqlE = e;
                        } else {
                            sqlE.setNextException(e);
                        }
                    } finally {
                        // Remove from map, as initialization failed
                        connectionQueryServicesMap.remove(normalizedConnInfo);
                        if (sqlE != null) {
                            throw sqlE;
                        }
                    }
                }
            }
            return connectionQueryServices;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private void checkClosed() {
        if (closed) {
            throwDriverClosedException();
        }
    }
    
    private void throwDriverClosedException() {
        throw new IllegalStateException(driverShutdownMsg != null ? driverShutdownMsg : "The Phoenix jdbc driver has been closed.");
    }

    @Override
    public synchronized void close() throws SQLException {
        try {
            closeLock.writeLock().lock();
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }

        try {
            Collection<ConnectionQueryServices> connectionQueryServices = connectionQueryServicesMap.values();
            try {
                SQLCloseables.closeAll(connectionQueryServices);
            } finally {
                connectionQueryServices.clear();
            }
        } finally {
            if (services != null) {
                try {
                    services.close();
                } finally {
                    ExecutorService executor = services.getExecutor();
                    // Even if something wrong happened while closing services above, we still
                    // want to set it to null. Otherwise, we will end up having a possibly non-working
                    // services instance. 
                    services = null;
                    executor.shutdown();
                }
            }
        }
    }
}
