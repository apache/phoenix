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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.logging.Logger;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseable;
import org.slf4j.LoggerFactory;

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
public abstract class PhoenixEmbeddedDriver implements Driver, SQLCloseable {
    /**
     * The protocol for Phoenix Network Client 
     */ 
    private static final Log LOG = LogFactory.getLog(PhoenixEmbeddedDriver.class);
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
    
    abstract public QueryServices getQueryServices() throws SQLException;
     
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
        private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConnectionInfo.class);
        private static final Object KERBEROS_LOGIN_LOCK = new Object();
        private static final char WINDOWS_SEPARATOR_CHAR = '\\';
        private static SQLException getMalFormedUrlException(String url) {
            return new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
            .setMessage(url).build().buildException();
        }
        
		public String getZookeeperConnectionString() {
			return getZookeeperQuorum() + ":" + getPort();
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
        
        public static ConnectionInfo create(String url) throws SQLException {
            url = url == null ? "" : url;
            if (url.isEmpty() || url.equalsIgnoreCase("jdbc:phoenix:")
                    || url.equalsIgnoreCase("jdbc:phoenix")) {
                return defaultConnectionInfo(url);
            }
            url = url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)
                    ? url.substring(PhoenixRuntime.JDBC_PROTOCOL.length())
                    : PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + url;
            StringTokenizer tokenizer = new StringTokenizer(url, DELIMITERS, true);
            int nTokens = 0;
            String[] tokens = new String[5];
            String token = null;
            while (tokenizer.hasMoreTokens() &&
                    !(token=tokenizer.nextToken()).equals(TERMINATOR) &&
                    tokenizer.hasMoreTokens() && nTokens < tokens.length) {
                token = tokenizer.nextToken();
                // This would mean we have an empty string for a token which is illegal
                if (DELIMITERS.contains(token)) {
                    throw getMalFormedUrlException(url);
                }
                tokens[nTokens++] = token;
            }
            // Look-forward to see if the last token is actually the C:\\ path
            if (tokenizer.hasMoreTokens() && !TERMINATOR.equals(token)) {
                String extraToken = tokenizer.nextToken();
                if (WINDOWS_SEPARATOR_CHAR == extraToken.charAt(0)) {
                  String prevToken = tokens[nTokens - 1];
                  tokens[nTokens - 1] = prevToken + ":" + extraToken;
                  if (tokenizer.hasMoreTokens() && !(token=tokenizer.nextToken()).equals(TERMINATOR)) {
                      throw getMalFormedUrlException(url);
                  }
                } else {
                    throw getMalFormedUrlException(url);
                }
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
                                // There's still more after, try to see if it's a windows file path
                                if (tokenIndex < tokens.length) {
                                    String nextToken = tokens[tokenIndex++];
                                    // The next token starts with the directory separator, assume
                                    // it's still the keytab path.
                                    if (null != nextToken && WINDOWS_SEPARATOR_CHAR == nextToken.charAt(0)) {
                                        keytabFile = keytabFile + ":" + nextToken;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return new ConnectionInfo(quorum,port,rootNode, principal, keytabFile);
        }
        
        public ConnectionInfo normalize(ReadOnlyProps props, Properties info) throws SQLException {
            String zookeeperQuorum = this.getZookeeperQuorum();
            Integer port = this.getPort();
            String rootNode = this.getRootNode();
            String keytab = this.getKeytab();
            String principal = this.getPrincipal();
            // Normalize connInfo so that a url explicitly specifying versus implicitly inheriting
            // the default values will both share the same ConnectionQueryServices.
            if (zookeeperQuorum == null) {
                zookeeperQuorum = props.get(QueryServices.ZOOKEEPER_QUORUM_ATTRIB);
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
            if (!isConnectionless()) {
                boolean credsProvidedInUrl = null != principal && null != keytab;
                boolean credsProvidedInProps = info.containsKey(QueryServices.HBASE_CLIENT_PRINCIPAL) && info.containsKey(QueryServices.HBASE_CLIENT_KEYTAB);
                if (credsProvidedInUrl || credsProvidedInProps) {
                    // PHOENIX-3189 Because ConnectionInfo is immutable, we must make sure all parts of it are correct before
                    // construction; this also requires the Kerberos user credentials object (since they are compared by reference
                    // and not by value. If the user provided a principal and keytab via the JDBC url, we must make sure that the
                    // Kerberos login happens *before* we construct the ConnectionInfo object. Otherwise, the use of ConnectionInfo
                    // to determine when ConnectionQueryServices impl's should be reused will be broken.
                    try {
                        // Check if we need to authenticate with kerberos so that we cache the correct ConnectionInfo
                        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                        if (!currentUser.hasKerberosCredentials() || !isSameName(currentUser.getUserName(), principal)) {
                            synchronized (KERBEROS_LOGIN_LOCK) {
                                // Double check the current user, might have changed since we checked last. Don't want
                                // to re-login if it's the same user.
                                currentUser = UserGroupInformation.getCurrentUser();
                                if (!currentUser.hasKerberosCredentials() || !currentUser.getUserName().equals(principal)) {
                                    final Configuration config = getConfiguration(props, info, principal, keytab);
                                    logger.info("Trying to connect to a secure cluster as {} with keytab {}", config.get(QueryServices.HBASE_CLIENT_PRINCIPAL),
                                            config.get(QueryServices.HBASE_CLIENT_KEYTAB));
                                    UserGroupInformation.setConfiguration(config);
                                    User.login(config, QueryServices.HBASE_CLIENT_KEYTAB, QueryServices.HBASE_CLIENT_PRINCIPAL, null);
                                    logger.info("Successful login to secure cluster");
                                }
                            }
                        } else {
                            // The user already has Kerberos creds, so there isn't anything to change in the ConnectionInfo.
                            logger.debug("Already logged in as {}", currentUser);
                        }
                    } catch (IOException e) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                            .setRootCause(e).build().buildException();
                    }
                } else {
                    logger.debug("Principal and keytab not provided, not attempting Kerberos login");
                }
            } // else, no connection, no need to login
            // Will use the current User from UGI
            return new ConnectionInfo(zookeeperQuorum, port, rootNode, principal, keytab);
        }

        // Visible for testing
        static boolean isSameName(String currentName, String newName) throws IOException {
            return isSameName(currentName, newName, null);
        }

        static boolean isSameName(String currentName, String newName, String hostname) throws IOException {
            // Make sure to replace "_HOST" if it exists before comparing the principals.
            if (newName.contains(org.apache.hadoop.security.SecurityUtil.HOSTNAME_PATTERN)) {
                newName = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(newName, hostname);
            }
            return currentName.equals(newName);
        }

        /**
         * Constructs a Configuration object to use when performing a Kerberos login.
         * @param props QueryServices properties
         * @param info User-provided properties
         * @param principal Kerberos user principal
         * @param keytab Path to Kerberos user keytab
         * @return Configuration object suitable for Kerberos login
         */
        private Configuration getConfiguration(ReadOnlyProps props, Properties info, String principal, String keytab) {
            final Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
            // Add QueryServices properties
            for (Entry<String,String> entry : props) {
                config.set(entry.getKey(), entry.getValue());
            }
            // Add any user-provided properties (via DriverManager)
            if (info != null) {
                for (Object key : info.keySet()) {
                    config.set((String) key, info.getProperty((String) key));
                }
            }
            // Set the principal and keytab if provided from the URL (overriding those provided in Properties)
            if (null != principal) {
                config.set(QueryServices.HBASE_CLIENT_PRINCIPAL, principal);
            }
            if (null != keytab) {
                config.set(QueryServices.HBASE_CLIENT_KEYTAB, keytab);
            }
            return config;
        }
        
        private final Integer port;
        private final String rootNode;
        private final String zookeeperQuorum;
        private final boolean isConnectionless;
        private final String principal;
        private final String keytab;
        private final User user;
        
        public ConnectionInfo(String zookeeperQuorum, Integer port, String rootNode, String principal, String keytab) {
            this.zookeeperQuorum = zookeeperQuorum;
            this.port = port;
            this.rootNode = rootNode;
            this.isConnectionless = PhoenixRuntime.CONNECTIONLESS.equals(zookeeperQuorum);
            this.principal = principal;
            this.keytab = keytab;
            try {
                this.user = User.getCurrent();
            } catch (IOException e) {
                throw new RuntimeException("Couldn't get the current user!!");
            }
            if (null == this.user) {
                throw new RuntimeException("Acquired null user which should never happen");
            }
        }
        
        public ConnectionInfo(String zookeeperQuorum, Integer port, String rootNode) {
        	this(zookeeperQuorum, port, rootNode, null, null);
        }

        /**
         * Copy constructor for all members except {@link #user}.
         *
         * @param other The instance to copy
         */
        public ConnectionInfo(ConnectionInfo other) {
            this(other.zookeeperQuorum, other.port, other.rootNode, other.principal, other.keytab);
        }

        public ReadOnlyProps asProps() {
            Map<String, String> connectionProps = Maps.newHashMapWithExpectedSize(3);
            if (getZookeeperQuorum() != null) {
                connectionProps.put(QueryServices.ZOOKEEPER_QUORUM_ATTRIB, getZookeeperQuorum());
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

        public User getUser() {
            return user;
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
            // `user` is guaranteed to be non-null
            result = prime * result + user.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ConnectionInfo other = (ConnectionInfo) obj;
            // `user` is guaranteed to be non-null
            if (!other.user.equals(user)) return false;
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

        public String toUrl() {
            return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR
                    + toString();
        }

        private static ConnectionInfo defaultConnectionInfo(String url) throws SQLException {
            Configuration config =
                    HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
            String quorum = config.get(HConstants.ZOOKEEPER_QUORUM);
            if (quorum == null || quorum.isEmpty()) {
                throw getMalFormedUrlException(url);
            }
            String clientPort = config.get(HConstants.ZOOKEEPER_CLIENT_PORT);
            Integer port = clientPort==null ? null : Integer.parseInt(clientPort);
            if (port == null || port < 0) {
                throw getMalFormedUrlException(url);
            }
            String znodeParent = config.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            LOG.debug("Getting default jdbc connection url " + quorum + ":" + port + ":" + znodeParent);
            return new ConnectionInfo(quorum, port, znodeParent);
        }
    }

    public static boolean isTestUrl(String url) {
        return url.endsWith(TEST_URL_AT_END) || url.contains(TEST_URL_IN_MIDDLE);
    }
}
