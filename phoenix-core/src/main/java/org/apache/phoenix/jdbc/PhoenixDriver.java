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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.ConnectionlessQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesImpl;
import org.apache.phoenix.util.SQLCloseables;


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
    public static final PhoenixDriver INSTANCE;
    static {
        try {
            DriverManager.registerDriver( INSTANCE = new PhoenixDriver() );
        } catch (SQLException e) {
            throw new IllegalStateException("Untable to register " + PhoenixDriver.class.getName() + ": "+ e.getMessage());
        }
    }
    private final ConcurrentMap<ConnectionInfo,ConnectionQueryServices> connectionQueryServicesMap = new ConcurrentHashMap<ConnectionInfo,ConnectionQueryServices>(3);

    public PhoenixDriver() { // for Squirrel
        // Use production services implementation
        super();
    }

    private volatile QueryServices services;

    @Override
    public QueryServices getQueryServices() {
    	// Lazy initialize QueryServices so that we only attempt to create an HBase Configuration
    	// object upon the first attempt to connect to any cluster. Otherwise, an attempt will be
    	// made at driver initialization time which is too early for some systems.
    	QueryServices result = services;
    	if (result == null) {
    		synchronized(this) {
    			result = services;
    			if(result == null) {
    				services = result = new QueryServicesImpl();
    			}
    		}
    	}
    	return result;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // Accept the url only if test=true attribute not set
        return super.acceptsURL(url) && !isTestUrl(url);
    }

    @Override
    protected ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException {
        ConnectionInfo connInfo = ConnectionInfo.create(url);
        ConnectionInfo normalizedConnInfo = connInfo.normalize(getQueryServices().getProps());
        ConnectionQueryServices connectionQueryServices = connectionQueryServicesMap.get(normalizedConnInfo);
        if (connectionQueryServices == null) {
            if (normalizedConnInfo.isConnectionless()) {
                connectionQueryServices = new ConnectionlessQueryServicesImpl(getQueryServices());
            } else {
                connectionQueryServices = new ConnectionQueryServicesImpl(getQueryServices(), normalizedConnInfo);
            }
            connectionQueryServices.init(url, info);
            ConnectionQueryServices prevValue = connectionQueryServicesMap.putIfAbsent(normalizedConnInfo, connectionQueryServices);
            if (prevValue != null) {
                connectionQueryServices = prevValue;
            }
        }
        return connectionQueryServices;
    }

    @Override
    public void close() throws SQLException {
        try {
            SQLCloseables.closeAll(connectionQueryServicesMap.values());
        } finally {
            connectionQueryServicesMap.clear();            
        }
    }
}
