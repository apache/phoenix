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

import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ConnectionQueryServicesTestImpl;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionlessQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesTestImpl;
import org.apache.phoenix.util.ReadOnlyProps;



/**
 * 
 * JDBC Driver implementation of Phoenix for testing.
 * To use this driver, specify test=true in url.
 * 
 * 
 * @since 0.1
 */
public class PhoenixTestDriver extends PhoenixEmbeddedDriver {
    private ConnectionQueryServices queryServices;
    private final ReadOnlyProps overrideProps;
    private QueryServices services;

    public PhoenixTestDriver() {
        this.overrideProps = ReadOnlyProps.EMPTY_PROPS;
    }

    // For tests to override the default configuration
    public PhoenixTestDriver(ReadOnlyProps overrideProps) {
        this.overrideProps = overrideProps;
    }

    @Override
    public synchronized QueryServices getQueryServices() {
        if (services == null) {
            services = new QueryServicesTestImpl(overrideProps);
        }
        return services;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // Accept the url only if test=true attribute set
        return super.acceptsURL(url) && isTestUrl(url);
    }

    @Override // public for testing
    public ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException {
        if (queryServices != null) {
            return queryServices;
        }
        QueryServices services = getQueryServices();
        ConnectionInfo connInfo = ConnectionInfo.create(url);
        if (connInfo.isConnectionless()) {
            queryServices =  new ConnectionlessQueryServicesImpl(services);
        } else {
            queryServices =  new ConnectionQueryServicesTestImpl(services, connInfo);
        }
        queryServices.init(url, info);
        return queryServices;
    }
    
    @Override
    public void close() throws SQLException {
        try {
            queryServices.close();
        } finally {
            queryServices = null;
        }
    }
}
