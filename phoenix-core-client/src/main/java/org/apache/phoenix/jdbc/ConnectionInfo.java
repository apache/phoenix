/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.slf4j.LoggerFactory;

/**
 * Class to encapsulate connection info for HBase
 * @since 0.1.1
 */
public abstract class ConnectionInfo {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ConnectionInfo.class);
    protected static final Object KERBEROS_LOGIN_LOCK = new Object();
    protected static final char WINDOWS_SEPARATOR_CHAR = '\\';
    protected static final String REALM_EQUIVALENCY_WARNING_MSG =
            "Provided principal does not contain a realm and the default realm cannot be"
            + " determined. Ignoring realm equivalency check.";
    protected static final String TERMINATOR = "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
    protected static final String DELIMITERS = TERMINATOR + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    protected static final String CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY =
            "hbase.client.registry.impl";

    protected static final boolean HAS_MASTER_REGISTRY;
    protected static final boolean HAS_RPC_REGISTRY;

    static {
        String version = VersionInfo.getVersion();
        if (VersionInfo.getMajorVersion(version) >= 3) {
            HAS_MASTER_REGISTRY = true;
            HAS_RPC_REGISTRY = true;
        } else {
            if (VersionInfo.compareVersion(VersionInfo.getVersion(), "2.3.0") < 0) {
                HAS_MASTER_REGISTRY = false;
                HAS_RPC_REGISTRY = false;
            } else if (VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0") < 0) {
                HAS_MASTER_REGISTRY = true;
                HAS_RPC_REGISTRY = false;
            } else {
                HAS_MASTER_REGISTRY = true;
                HAS_RPC_REGISTRY = true;
            }
        }
    }

    protected static SQLException getMalFormedUrlException(String url) {
        return new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage(url).build().buildException();
    }

    protected final boolean isConnectionless;
    protected final String principal;
    protected final String keytab;
    protected final User user;
    protected final String haGroup;

    protected ConnectionInfo(boolean isConnectionless, String principal, String keytab, User user,
            String haGroup) {
        super();
        this.isConnectionless = isConnectionless;
        this.principal = principal;
        this.keytab = keytab;
        this.user = user;
        this.haGroup = haGroup;
    }

    protected static String unescape(String escaped) {
        return escaped.replaceAll("\\\\:", "=");
    }

    public static ConnectionInfo createNoLogin(String url, ReadOnlyProps props, Properties info)
            throws SQLException {
        Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        return create(url, conf, props, info, true);
    }

    public static ConnectionInfo create(String url, ReadOnlyProps props, Properties info)
            throws SQLException {
        Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        return create(url, conf, props, info);
    }

    public static ConnectionInfo createNoLogin(String url, Configuration configuration,
            ReadOnlyProps props, Properties info) throws SQLException {
        return create(url, configuration, props, info, true);
    }

    public static ConnectionInfo create(String url, Configuration configuration,
            ReadOnlyProps props, Properties info) throws SQLException {
        return create(url, configuration, props, info, false);
    }

    public static ConnectionInfo create(String url, Configuration configuration,
            ReadOnlyProps props, Properties info, boolean doNotLogin) throws SQLException {
        // registry-independent URL preprocessing
        url = url == null ? "" : url;
        url = unescape(url);

        // Assume missing prefix
        if (url.isEmpty()) {
            url = PhoenixRuntime.JDBC_PROTOCOL;
        }
        if (!url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + url;
        }

        if (configuration == null) {
            configuration = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        }

        Builder builder;

        if (url.toLowerCase().startsWith(PhoenixRuntime.JDBC_PROTOCOL_ZK)) {
            builder = new ZKConnectionInfo.Builder(url, configuration, props, info);
        } else if (url.toLowerCase().startsWith(PhoenixRuntime.JDBC_PROTOCOL_MASTER)) {
            builder = new MasterConnectionInfo.Builder(url, configuration, props, info);
        } else if (url.toLowerCase().startsWith(PhoenixRuntime.JDBC_PROTOCOL_RPC)) {
            builder = new RPCConnectionInfo.Builder(url, configuration, props, info);
        } else if (url.toLowerCase().startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            // The generic protocol was specified. Try to Determine the protocol from the config
            if (MasterConnectionInfo.Builder.isMaster(configuration, props, info)) {
                builder = new MasterConnectionInfo.Builder(url, configuration, props, info);
            } else if (RPCConnectionInfo.Builder.isRPC(configuration, props, info)) {
                builder = new RPCConnectionInfo.Builder(url, configuration, props, info);
            } else if (ZKConnectionInfo.Builder.isZK(configuration, props, info)) {
                builder = new ZKConnectionInfo.Builder(url, configuration, props, info);
            } else {
                // No registry class set in config. Use version-dependent default
                if (VersionInfo.getMajorVersion(VersionInfo.getVersion()) >= 3) {
                    builder = new RPCConnectionInfo.Builder(url, configuration, props, info);
                } else {
                    builder = new ZKConnectionInfo.Builder(url, configuration, props, info);
                }
            }
        } else {
            throw getMalFormedUrlException(url);
        }

        builder.setDoNotLogin(doNotLogin);
        return builder.create();
    }

    protected static List<String> handleWindowsKeytab(String url, List<String> parts)
            throws SQLException {

        if (parts.size() == 7) {
            // We could check for FileSystems.getDefault().getSeparator()), but then
            // we wouldn't be able to test on Unix.
            if (parts.get(6) != null && parts.get(6).startsWith("\\")) {
                // Reconstruct windows path
                parts.set(5, parts.get(5) + ":" + parts.get(6));
                parts.remove(6);
            } else {
                throw getMalFormedUrlException(url);
            }
        }

        return parts;
    }

    // Visible for testing
    static boolean isSameName(String currentName, String newName) throws IOException {
        return isSameName(currentName, newName, null, getDefaultKerberosRealm());
    }

    /**
     * Computes the default kerberos realm if one is available. If one cannot be computed, null is
     * returned.
     * @return The default kerberos realm, or null.
     */
    static String getDefaultKerberosRealm() {
        try {
            return KerberosUtil.getDefaultRealm();
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                // Include the stacktrace at DEBUG
                LOGGER.debug(REALM_EQUIVALENCY_WARNING_MSG, e);
            } else {
                // Limit the content at WARN
                LOGGER.warn(REALM_EQUIVALENCY_WARNING_MSG);
            }
        }
        return null;
    }

    static boolean isSameName(String currentName, String newName, String hostname)
            throws IOException {
        return isSameName(currentName, newName, hostname, getDefaultKerberosRealm());
    }

    static boolean isSameName(String currentName, String newName, String hostname,
            String defaultRealm) throws IOException {
        final boolean newNameContainsRealm = newName.indexOf('@') != -1;
        // Make sure to replace "_HOST" if it exists before comparing the principals.
        if (newName.contains(org.apache.hadoop.security.SecurityUtil.HOSTNAME_PATTERN)) {
            if (newNameContainsRealm) {
                newName =
                        org.apache.hadoop.security.SecurityUtil.getServerPrincipal(newName,
                            hostname);
            } else {
                // If the principal ends with "/_HOST", replace "_HOST" with the hostname.
                if (newName.endsWith("/_HOST")) {
                    newName = newName.substring(0, newName.length() - 5) + hostname;
                }
            }
        }
        // The new name doesn't contain a realm and we could compute a default realm
        if (!newNameContainsRealm && defaultRealm != null) {
            return currentName.equals(newName + "@" + defaultRealm);
        }
        // We expect both names to contain a realm, so we can do a simple equality check
        return currentName.equals(newName);
    }

    /**
     * Create a new Configuration object that merges the CQS properties and the Connection
     * properties into the HBase configuration object
     * @param props CQS properties
     * @param info JDBC connection properties
     * @return merged configuration
     */
    protected static Configuration mergeConfiguration(Configuration configIn, ReadOnlyProps props,
            Properties info) {
        // TODO is cloning the configuration a performance problem ?
        Configuration config;
        if (configIn != null) {
            config = new Configuration(configIn);
        } else {
            // props/info contains everything
            config = new Configuration(false);
        }
        // Add QueryServices properties
        if (props != null) {
            for (Entry<String, String> entry : props) {
                config.set(entry.getKey(), entry.getValue());
            }
        }
        // Add any user-provided properties (via DriverManager)
        if (info != null) {
            for (Object key : info.keySet()) {
                config.set((String) key, info.getProperty((String) key));
            }
        }
        return config;
    }

    protected Map<String, String> getCommonProps() {
        Map<String, String> connectionProps = new HashMap<>();
        if (getPrincipal() != null && getKeytab() != null) {
            connectionProps.put(QueryServices.HBASE_CLIENT_PRINCIPAL, getPrincipal());
            connectionProps.put(QueryServices.HBASE_CLIENT_KEYTAB, getKeytab());
        }
        return connectionProps;
    }

    public abstract ReadOnlyProps asProps();

    public boolean isConnectionless() {
        return isConnectionless;
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

    public String getHaGroup() {
        return haGroup;
    }

    public abstract String toUrl();

    public abstract String getZookeeperConnectionString();

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ConnectionInfo other = (ConnectionInfo) obj;
        // `user` is guaranteed to be non-null
        if (!other.user.equals(user)) return false;
        if (principal == null) {
            if (other.principal != null) return false;
        } else if (!principal.equals(other.principal)) return false;
        if (keytab == null) {
            if (other.keytab != null) return false;
        } else if (!keytab.equals(other.keytab)) return false;
        if (haGroup == null) {
            if (other.haGroup != null) return false;
        } else if (!haGroup.equals(other.haGroup)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((principal == null) ? 0 : principal.hashCode());
        result = prime * result + ((keytab == null) ? 0 : keytab.hashCode());
        result = prime * result + ((haGroup == null) ? 0 : haGroup.hashCode());
        // `user` is guaranteed to be non-null
        result = prime * result + user.hashCode();
        return result;
    }

    protected boolean anyNotNull(Object... params) {
        for (Object param : params) {
            if (param != null) {
                return true;
            }
        }
        return false;
    }

    public abstract ConnectionInfo withPrincipal(String principal);

    /**
     * Parent of the Builder classes for the immutable ConnectionInfo classes
     *
     * @since
     */
    protected abstract static class Builder {

        protected boolean isConnectionless;
        protected String principal;
        protected String keytab;
        protected User user;
        protected String haGroup;
        protected boolean doNotLogin = false;

        // Only used for building, not part of ConnectionInfo
        protected final String url;
        protected final Configuration config;
        protected final ReadOnlyProps props;
        protected final Properties info;

        public Builder(String url, Configuration config, ReadOnlyProps props, Properties info) {
            this.config = config;
            this.url = url;
            this.props = props;
            this.info = info;
        }

        protected abstract ConnectionInfo create() throws SQLException;

        protected abstract void normalize() throws SQLException;

        protected String get(String key, String defValue) {
            String result = null;
            if (info != null) {
                result = info.getProperty(key);
            }
            if (result == null) {
                if (props != null) {
                    result = props.get(key);
                }
                if (result == null) {
                    result = config.get(key, defValue);
                }
            }
            return result;
        }

        protected String get(String key) {
            return get(key, null);
        }

        protected void setHaGroup() {
            if (info != null) {
                haGroup = info.getProperty(HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR);
            }
        }

        protected void setDoNotLogin(boolean doNotLogin) {
            this.doNotLogin = doNotLogin;
        }

        protected void handleKerberosAndLogin() throws SQLException {
            // Previously we have ignored the kerberos properties defined in hbase-site.xml,
            // but now we use them
            try {
                this.user = User.getCurrent();
            } catch (IOException e) {
                throw new RuntimeException("Couldn't get the current user!!", e);
            }
            if (null == this.user) {
                throw new RuntimeException("Acquired null user which should never happen");
            }

            if (isConnectionless) {
                return;
            }

            if (principal == null) {
                principal = get(QueryServices.HBASE_CLIENT_PRINCIPAL);
            }
            if (keytab == null) {
                keytab = get(QueryServices.HBASE_CLIENT_KEYTAB);
            }
            if ((principal == null) && (keytab != null)) {
                throw getMalFormedUrlException(url);
            }
            // We allow specifying a principal without a keytab, in which case
            // the principal is not used for kerberos, but is set as the connection user
            if (principal != null && keytab != null && !doNotLogin) {
                // PHOENIX-3189 Because ConnectionInfo is immutable, we must make sure all parts of
                // it are correct before
                // construction; this also requires the Kerberos user credentials object (since they
                // are compared by reference
                // and not by value. If the user provided a principal and keytab via the JDBC url,
                // we must make sure that the
                // Kerberos login happens *before* we construct the ConnectionInfo object.
                // Otherwise, the use of ConnectionInfo
                // to determine when ConnectionQueryServices impl's should be reused will be broken.
                try {
                    // Check if we need to authenticate with kerberos so that we cache the correct
                    // ConnectionInfo
                    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                    if (!currentUser.hasKerberosCredentials()
                            || !isSameName(currentUser.getUserName(), principal)) {
                        synchronized (KERBEROS_LOGIN_LOCK) {
                            // Double check the current user, might have changed since we checked
                            // last. Don't want
                            // to re-login if it's the same user.
                            currentUser = UserGroupInformation.getCurrentUser();
                            if (!currentUser.hasKerberosCredentials()
                                    || !isSameName(currentUser.getUserName(), principal)) {
                                LOGGER.info("Trying to connect to a secure cluster as {} "
                                        + "with keytab {}",
                                    principal, keytab);
                                // We are intentionally changing the passed in Configuration object
                                if (null != principal) {
                                    config.set(QueryServices.HBASE_CLIENT_PRINCIPAL, principal);
                                }
                                if (null != keytab) {
                                    config.set(QueryServices.HBASE_CLIENT_KEYTAB, keytab);
                                }
                                UserGroupInformation.setConfiguration(config);
                                User.login(config, QueryServices.HBASE_CLIENT_KEYTAB,
                                    QueryServices.HBASE_CLIENT_PRINCIPAL, null);
                                user = User.getCurrent();
                                LOGGER.info("Successful login to secure cluster");
                            }
                        }
                    } else {
                        // The user already has Kerberos creds, so there isn't anything to change in
                        // the ConnectionInfo.
                        LOGGER.debug("Already logged in as {}", currentUser);
                    }
                } catch (IOException e) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                            .setRootCause(e).build().buildException();
                }
            } else {
                LOGGER.debug("Principal and keytab not provided, not attempting Kerberos login");
            }
        }

        protected String normalizeHostsList(String quorum, Integer defaultPort)
                throws SQLException {
            // The input host:port separator char is "=" (after unescaping)
            String[] quorumParts = quorum.split(",");
            String[] normalizedParts = new String[quorumParts.length];
            for (int i = 0; i < quorumParts.length; i++) {
                String[] hostAndPort = quorumParts[i].trim().split(":");
                if (hostAndPort.length == 1) {
                    normalizedParts[i] = hostAndPort[0].trim().toLowerCase() + ":" + defaultPort;
                } else if (hostAndPort.length == 2) {
                    normalizedParts[i] = quorumParts[i].trim().toLowerCase();
                } else {
                    throw getMalFormedUrlException(url);
                }
            }
            // We are sorting the host:port strings, so the sorting result may be unexpected, but
            // as long as it's consistent, it doesn't matter.
            Arrays.sort(normalizedParts);
            return String.join(",", normalizedParts);
            // TODO
            // HBase will perform a further reverse lookup based normalization on the hosts,
            // but we skip that.
            // In the unlikely worst case, we generate separate CQSI objects instead of sharing them
        }

        protected StringTokenizer getTokenizerWithoutProtocol() throws SQLException {
            StringTokenizer tokenizer = new StringTokenizer(url, DELIMITERS, true);
            try {
                // Walk the first three tokens "jdbc", ":", "phoenix"/"phoenix+master"/"phoenix-zk"
                // This should succeed, as we check for the "jdbc:phoenix" prefix when accepting the
                // URL
                if (!tokenizer.nextToken().toLowerCase().equals("jdbc")) {
                    throw new Exception();
                }
                if (!tokenizer.nextToken().toLowerCase().equals(":")) {
                    throw new Exception();
                }
                if (!tokenizer.nextToken().toLowerCase().startsWith("phoenix")) {
                    throw new Exception();
                }
            } catch (Exception e) {
                throw getMalFormedUrlException(url);
            }
            return tokenizer;
        }

        protected static String get(String key, Configuration config, ReadOnlyProps props,
                Properties info) {
            String result = null;
            if (info != null) {
                result = info.getProperty(key);
            }
            if (result == null) {
                if (props != null) {
                    result = props.get(key);
                }
                if (result == null) {
                    result = config.get(key, null);
                }
            }
            return result;
        }
    }
}
