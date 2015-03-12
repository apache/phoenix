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
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import javax.annotation.concurrent.Immutable;

import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;



/**
 * 
 * Abstract base class for JDBC Driver implementation of Phoenix
 * 
 * 
 * @since 0.1
 */
@Immutable
public abstract class PhoenixEmbeddedDriver implements Driver, org.apache.phoenix.jdbc.Jdbc7Shim.Driver, SQLCloseable {
    /**
     * The protocol for Phoenix Network Client 
     */ 
    private final static String DNC_JDBC_PROTOCOL_SUFFIX = "//";
    private final static String DRIVER_NAME = "PhoenixEmbeddedDriver";
    private static final String TERMINATOR = "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
    private static final String DELIMITERS = TERMINATOR + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    private static final String TEST_URL_AT_END =  "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    private static final String TEST_URL_IN_MIDDLE = TEST_URL_AT_END + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;

    private final static DriverPropertyInfo[] EMPTY_INFO = new DriverPropertyInfo[0];
    public final static String MAJOR_VERSION_PROP = "DriverMajorVersion";
    public final static String MINOR_VERSION_PROP = "DriverMinorVersion";
    public final static String DRIVER_NAME_PROP = "DriverName";
    
    public static final ReadOnlyProps DEFFAULT_PROPS = new ReadOnlyProps(
            ImmutableMap.of(
                    MAJOR_VERSION_PROP, Integer.toString(MetaDataProtocol.PHOENIX_MAJOR_VERSION),
                    MINOR_VERSION_PROP, Integer.toString(MetaDataProtocol.PHOENIX_MINOR_VERSION),
                    DRIVER_NAME_PROP, DRIVER_NAME));
    
    PhoenixEmbeddedDriver() {
    }
    
    protected ReadOnlyProps getDefaultProps() {
        return DEFFAULT_PROPS;
    }
    
    abstract public QueryServices getQueryServices();
     
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            // A connection string of "jdbc:phoenix" is supported, since
            // all the connection information can potentially be gotten
            // out of the HBase config file
            if (url.length() == PhoenixRuntime.JDBC_PROTOCOL.length()) {
                return true;
            }
            // Same as above, except for "jdbc:phoenix;prop=<value>..."
            if (PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR == url.charAt(PhoenixRuntime.JDBC_PROTOCOL.length())) {
                return true;
            }
            if (PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR == url.charAt(PhoenixRuntime.JDBC_PROTOCOL.length())) {
                int protoLength = PhoenixRuntime.JDBC_PROTOCOL.length() + 1;
                // A connection string of "jdbc:phoenix:" matches this driver,
                // but will end up as a MALFORMED_CONNECTION_URL exception later.
                if (url.length() == protoLength) {
                    return true;
                }
                // A connection string of the form "jdbc:phoenix://" means that
                // the driver is remote which isn't supported, so return false.
                if (!url.startsWith(DNC_JDBC_PROTOCOL_SUFFIX, protoLength)) {
                    return true;
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

        Properties augmentedInfo = PropertiesUtil.deepCopy(info);
        augmentedInfo.putAll(getDefaultProps().asMap());
        ConnectionQueryServices connectionServices = getConnectionQueryServices(url, augmentedInfo);
        PhoenixConnection connection = connectionServices.connect(url, augmentedInfo);
        return connection;
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
    
    /**
     * 
     * Class to encapsulate connection info for HBase
     *
     * 
     * @since 0.1.1
     */
    public static class ConnectionInfo {
        private static SQLException getMalFormedUrlException(String url) {
            return new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
            .setMessage(url).build().buildException();
        }
        
        /**
         * Detect url with quorum:1,quorum:2 as HBase does not handle different port numbers
         * for different quorum hostnames.
         * @param portStr
         * @return
         */
        private static boolean isMultiPortUrl(String portStr) {
            int commaIndex = portStr.indexOf(',');
            if (commaIndex > 0) {
                try {
                    Integer.parseInt(portStr.substring(0, commaIndex));
                    return true;
                } catch (NumberFormatException otherE) {
                }
            }
            return false;
        }
        
        protected static ConnectionInfo create(String url) throws SQLException {
            StringTokenizer tokenizer = new StringTokenizer(url == null ? "" : url.substring(PhoenixRuntime.JDBC_PROTOCOL.length()),DELIMITERS, true);
            int nTokens = 0;
            String[] tokens = new String[5];
            String token = null;
            while (tokenizer.hasMoreTokens() && !(token=tokenizer.nextToken()).equals(TERMINATOR) && tokenizer.hasMoreTokens() && nTokens < tokens.length) {
                token = tokenizer.nextToken();
                // This would mean we have an empty string for a token which is illegal
                if (DELIMITERS.contains(token)) {
                    throw getMalFormedUrlException(url);
                }
                tokens[nTokens++] = token;
            }
            if (tokenizer.hasMoreTokens() && !TERMINATOR.equals(token)) {
                throw getMalFormedUrlException(url);
            }
            String quorum = null;
            Integer port = null;
            String rootNode = null;
            String principal = null;
            String keytabFile = null;
            int tokenIndex = 0;
            if (nTokens > tokenIndex) {
                quorum = tokens[tokenIndex++]; // Found quorum
                if (nTokens > tokenIndex) {
                    try {
                        port = Integer.parseInt(tokens[tokenIndex]);
                        if (port < 0) {
                            throw getMalFormedUrlException(url);
                        }
                        tokenIndex++; // Found port
                    } catch (NumberFormatException e) { // No port information
                        if (isMultiPortUrl(tokens[tokenIndex])) {
                            throw getMalFormedUrlException(url);
                        }
                    }
                    if (nTokens > tokenIndex) {
                        if (tokens[tokenIndex].startsWith("/")) {
                            rootNode = tokens[tokenIndex++]; // Found rootNode
                        }
                        if (nTokens > tokenIndex) {
                            principal = tokens[tokenIndex++]; // Found principal
                            if (nTokens > tokenIndex) {
                                keytabFile = tokens[tokenIndex++]; // Found keytabFile
                            }
                        }
                    }
                }
            }
            return new ConnectionInfo(quorum,port,rootNode, principal, keytabFile);
        }
        
        public ConnectionInfo normalize(ReadOnlyProps props) throws SQLException {
            String zookeeperQuorum = this.getZookeeperQuorum();
            Integer port = this.getPort();
            String rootNode = this.getRootNode();
            String keytab = this.getKeytab();
            String principal = this.getPrincipal();
            // Normalize connInfo so that a url explicitly specifying versus implicitly inheriting
            // the default values will both share the same ConnectionQueryServices.
            if (zookeeperQuorum == null) {
                zookeeperQuorum = props.get(QueryServices.ZOOKEEPER_QUARUM_ATTRIB);
                if (zookeeperQuorum == null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                    .setMessage(this.toString()).build().buildException();
                }
            }

            if (port == null) {
                if (!isConnectionless) {
                    String portStr = props.get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
                    if (portStr != null) {
                        try {
                            port = Integer.parseInt(portStr);
                        } catch (NumberFormatException e) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                            .setMessage(this.toString()).build().buildException();
                        }
                    }
                }
            } else if (isConnectionless) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage("Port may not be specified when using the connectionless url \"" + this.toString() + "\"").build().buildException();
            }
            if (rootNode == null) {
                if (!isConnectionless) {
                    rootNode = props.get(QueryServices.ZOOKEEPER_ROOT_NODE_ATTRIB);
                }
            } else if (isConnectionless) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage("Root node may not be specified when using the connectionless url \"" + this.toString() + "\"").build().buildException();
            }
            if(principal == null){
                if (!isConnectionless) {
                   principal = props.get(QueryServices.HBASE_CLIENT_PRINCIPAL);
                }
            }
            if(keytab == null){
            	 if (!isConnectionless) {
            		 keytab = props.get(QueryServices.HBASE_CLIENT_KEYTAB);
            	 }
            }
            return new ConnectionInfo(zookeeperQuorum, port, rootNode, principal, keytab);
        }
        
        private final Integer port;
        private final String rootNode;
        private final String zookeeperQuorum;
        private final boolean isConnectionless;
        private final String principal;
        private final String keytab;
        
        // used for testing
        ConnectionInfo(String zookeeperQuorum, Integer port, String rootNode, String principal, String keytab) {
            this.zookeeperQuorum = zookeeperQuorum;
            this.port = port;
            this.rootNode = rootNode;
            this.isConnectionless = PhoenixRuntime.CONNECTIONLESS.equals(zookeeperQuorum);
            this.principal = principal;
            this.keytab = keytab;
        }
        
        // used for testing
        ConnectionInfo(String zookeeperQuorum, Integer port, String rootNode) {
        	this(zookeeperQuorum, port, rootNode, null, null);
        }

        public ReadOnlyProps asProps() {
            Map<String, String> connectionProps = Maps.newHashMapWithExpectedSize(3);
            if (getZookeeperQuorum() != null) {
                connectionProps.put(QueryServices.ZOOKEEPER_QUARUM_ATTRIB, getZookeeperQuorum());
            }
            if (getPort() != null) {
                connectionProps.put(QueryServices.ZOOKEEPER_PORT_ATTRIB, getPort().toString());
            }
            if (getRootNode() != null) {
                connectionProps.put(QueryServices.ZOOKEEPER_ROOT_NODE_ATTRIB, getRootNode());
            }
            if (getPrincipal() != null && getKeytab() != null) {
                connectionProps.put(QueryServices.HBASE_CLIENT_PRINCIPAL, getPrincipal());
                connectionProps.put(QueryServices.HBASE_CLIENT_KEYTAB, getKeytab());
            }
            return connectionProps.isEmpty() ? ReadOnlyProps.EMPTY_PROPS : new ReadOnlyProps(
                    connectionProps.entrySet().iterator());
        }
        
        public boolean isConnectionless() {
            return isConnectionless;
        }
        
        public String getZookeeperQuorum() {
            return zookeeperQuorum;
        }

        public Integer getPort() {
            return port;
        }

        public String getRootNode() {
            return rootNode;
        }
        
        public String getKeytab() {
            return keytab;
        }

        public String getPrincipal() {
            return principal;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((zookeeperQuorum == null) ? 0 : zookeeperQuorum.hashCode());
            result = prime * result + ((port == null) ? 0 : port.hashCode());
            result = prime * result + ((rootNode == null) ? 0 : rootNode.hashCode());
            result = prime * result + ((principal == null) ? 0 : principal.hashCode());
            result = prime * result + ((keytab == null) ? 0 : keytab.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ConnectionInfo other = (ConnectionInfo) obj;
            if (zookeeperQuorum == null) {
                if (other.zookeeperQuorum != null) return false;
            } else if (!zookeeperQuorum.equals(other.zookeeperQuorum)) return false;
            if (port == null) {
                if (other.port != null) return false;
            } else if (!port.equals(other.port)) return false;
            if (rootNode == null) {
                if (other.rootNode != null) return false;
            } else if (!rootNode.equals(other.rootNode)) return false;
            if (principal == null) {
                if (other.principal != null) return false;
            } else if (!principal.equals(other.principal)) return false;
            if (keytab == null) {
                if (other.keytab != null) return false;
            } else if (!keytab.equals(other.keytab)) return false;
            return true;
        }
        
        @Override
		public String toString() {
			return zookeeperQuorum + (port == null ? "" : ":" + port)
					+ (rootNode == null ? "" : ":" + rootNode)
					+ (principal == null ? "" : ":" + principal)
					+ (keytab == null ? "" : ":" + keytab);
		}
    }

    public static boolean isTestUrl(String url) {
        return url.endsWith(TEST_URL_AT_END) || url.contains(TEST_URL_IN_MIDDLE);
    }
}
