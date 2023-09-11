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

import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;

import javax.annotation.concurrent.Immutable;

import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseable;



/**
 * 
 * Abstract base class for JDBC Driver implementation of Phoenix
 * 
 * 
 * @since 0.1
 */
@Immutable
public abstract class PhoenixEmbeddedDriver implements Driver, SQLCloseable {
    /**
     * The protocol for Phoenix Network Client 
     */ 
    private final static String DNC_JDBC_PROTOCOL_SUFFIX = "//";
    private final static String DRIVER_NAME = "PhoenixEmbeddedDriver";
    private static final String TEST_URL_AT_END =  "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    private static final String TEST_URL_IN_MIDDLE = TEST_URL_AT_END + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;

    private static final String[] SUPPORTED_PROTOCOLS =
            new String[] { PhoenixRuntime.JDBC_PROTOCOL, PhoenixRuntime.JDBC_PROTOCOL_ZK,
                PhoenixRuntime.JDBC_PROTOCOL_MASTER, PhoenixRuntime.JDBC_PROTOCOL_RPC };

    private final static DriverPropertyInfo[] EMPTY_INFO = new DriverPropertyInfo[0];
    public final static String MAJOR_VERSION_PROP = "DriverMajorVersion";
    public final static String MINOR_VERSION_PROP = "DriverMinorVersion";
    public final static String DRIVER_NAME_PROP = "DriverName";
    
    public static final ReadOnlyProps DEFAULT_PROPS = new ReadOnlyProps(
            ImmutableMap.of(
                    MAJOR_VERSION_PROP, Integer.toString(MetaDataProtocol.PHOENIX_MAJOR_VERSION),
                    MINOR_VERSION_PROP, Integer.toString(MetaDataProtocol.PHOENIX_MINOR_VERSION),
                    DRIVER_NAME_PROP, DRIVER_NAME));

    PhoenixEmbeddedDriver() {
    }
    
    protected ReadOnlyProps getDefaultProps() {
        return DEFAULT_PROPS;
    }
    
    abstract public QueryServices getQueryServices() throws SQLException;
     
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            for (String protocol : SUPPORTED_PROTOCOLS) {
                // A connection string of "jdbc:phoenix" is supported, since
                // all the connection information can potentially be gotten
                // out of the HBase config file
                if (!url.startsWith(protocol)) {
                    continue;
                }
                if (url.length() == protocol.length()) {
                    return true;
                }
                // Same as above, except for "jdbc:phoenix;prop=<value>..."
                if (PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR == url.charAt(protocol.length())) {
                    return true;
                }
                if (PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR == url.charAt(protocol.length())) {
                    int protoLength = protocol.length() + 1;
                    // A connection string of "jdbc:phoenix:" matches this driver,
                    // but will end up as a MALFORMED_CONNECTION_URL exception later.
                    if (url.length() == protoLength) {
                        return true;
                    }
                    // Explicitly ignore connections of "jdbc:phoenix:thin"; leave them for
                    // the thin client
                    if (url.startsWith(PhoenixRuntime.JDBC_THIN_PROTOCOL)) {
                        return false;
                    }
                    // A connection string of the form "jdbc:phoenix://" means that
                    // the driver is remote which isn't supported, so return false.
                    if (!url.startsWith(DNC_JDBC_PROTOCOL_SUFFIX, protoLength)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        return createConnection(url, info);
    }

    protected final Connection createConnection(String url, Properties info) throws SQLException {
        Properties augmentedInfo = PropertiesUtil.deepCopy(info);
        augmentedInfo.putAll(getDefaultProps().asMap());
        if (url.contains("|")) {
            // High availability connection using two clusters
            Optional<HighAvailabilityGroup> haGroup = HighAvailabilityGroup.get(url, augmentedInfo);
            if (haGroup.isPresent()) {
                return haGroup.get().connect(augmentedInfo);
            } else {
                // If empty HA group is returned, fall back to single cluster.
                url =
                        HighAvailabilityGroup.getFallbackCluster(url, info).orElseThrow(
                            () -> new SQLException(
                                    "HA group can not be initialized, fallback to single cluster"));
            }
        }
        ConnectionQueryServices cqs = getConnectionQueryServices(url, augmentedInfo);
        return cqs.connect(url, augmentedInfo);
    }

    /**
     * Get or create if necessary a QueryServices that is associated with the HBase zookeeper quorum
     * name (part of the connection URL). This will cause the underlying Configuration held by the
     * QueryServices to be shared for all connections to the same HBase cluster.
     * @param url connection URL
     * @param info connection properties
     * @return new or cached QuerySerices used to establish a new Connection.
     * @throws SQLException
     */
    protected abstract ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException;
    
    @Override
    public int getMajorVersion() {
        return MetaDataProtocol.PHOENIX_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MetaDataProtocol.PHOENIX_MINOR_VERSION;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return EMPTY_INFO;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public void close() throws SQLException {
    }
    

    public static boolean isTestUrl(String url) {
        return url.endsWith(TEST_URL_AT_END) || url.contains(TEST_URL_IN_MIDDLE);
    }
}
