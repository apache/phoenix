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

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;

/**
 * ConnectionInfo class for org.apache.hadoop.hbase.client.ZKConnectionRegistry
 *
 * This used to be the only supported Registry in Phoenix (and the only one implemented by HBase)
 *
 */
public class ZKConnectionInfo extends ConnectionInfo {

    public static final String ZK_REGISTRY_NAME =
            "org.apache.hadoop.hbase.client.ZKConnectionRegistry";

    private final Integer zkPort;
    private final String zkRootNode;
    private final String zkHosts;

    private ZKConnectionInfo(boolean isConnectionless, String principal, String keytab, User user,
            String haGroup, String zkHosts, Integer zkPort, String zkRootNode) {
        super(isConnectionless, principal, keytab, user, haGroup);
        this.zkPort = zkPort;
        this.zkRootNode = zkRootNode;
        this.zkHosts = zkHosts;
    }

    public String getZkHosts() {
        return zkHosts;
    }

    public Integer getZkPort() {
        return zkPort;
    }

    public String getZkRootNode() {
        return zkRootNode;
    }

    @Override
    public String getZookeeperConnectionString() {
        // Normalized form includes ports
        return getZkHosts();
    }

    @Override
    public ReadOnlyProps asProps() {
        if (isConnectionless) {
            return ReadOnlyProps.EMPTY_PROPS;
        }

        Map<String, String> connectionProps = getCommonProps();
        connectionProps.put(CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
            ZK_REGISTRY_NAME);

        if (getZkHosts() != null) {
            //This has the highest priority
            connectionProps.put(HConstants.CLIENT_ZOOKEEPER_QUORUM, getZkHosts());
        }
        //Port is already normalized into zkHosts
        if (getZkRootNode() != null) {
            connectionProps.put(HConstants.ZOOKEEPER_ZNODE_PARENT, getZkRootNode());
        }
        return connectionProps.isEmpty() ? ReadOnlyProps.EMPTY_PROPS
                : new ReadOnlyProps(connectionProps.entrySet().iterator());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((zkHosts == null) ? 0 : zkHosts.hashCode());
        //Port is already included in zkHosts
        result = prime * result + ((zkRootNode == null) ? 0 : zkRootNode.hashCode());
        result = prime * result + super.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        ZKConnectionInfo other = (ZKConnectionInfo) obj;
        if (zkHosts == null) {
            if (other.zkHosts != null) {
                return false;
            }
        } else if (!zkHosts.equals(other.zkHosts)) {
            return false;
        }
        //Port is already normalized into zkHosts
        if (zkRootNode == null) {
            if (other.zkRootNode != null) {
                return false;
            }
        } else if (!zkRootNode.equals(other.zkRootNode)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(zkHosts.replaceAll(":", "\\\\:"));
        if (anyNotNull(zkPort, zkRootNode, principal, keytab)) {
            sb.append(zkPort == null ? ":" : ":" + zkPort);
        }
        if (anyNotNull(zkRootNode, principal, keytab)) {
            sb.append(zkRootNode == null ? ":" : ":" + zkRootNode);
        }
        if (anyNotNull(principal, keytab)) {
            sb.append(principal == null ? ":" : ":" + principal);
        }
        if (anyNotNull(keytab)) {
            sb.append(keytab == null ? ":" : ":" + keytab);
        }
        return sb.toString();
    }

    @Override
    public String toUrl() {
        return PhoenixRuntime.JDBC_PROTOCOL_ZK + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR
                + toString();
    }

    @Override
    public ConnectionInfo withPrincipal(String principal) {
        return new ZKConnectionInfo(isConnectionless, principal, keytab, user,
            haGroup, zkHosts, zkPort, zkRootNode);
    }

    /**
     * Builder helper class for ZKConnectionInfo
     *
     */
    protected static class Builder extends ConnectionInfo.Builder {
        private Integer zkPort;
        private String zkRootNode;
        private String zkHosts;

        public Builder(String url, Configuration config, ReadOnlyProps props, Properties info) {
            super(url, config, props, info);
        }

        @Override
        protected ConnectionInfo create() throws SQLException {
            parse();
            normalize();
            handleKerberosAndLogin();
            setHaGroup();
            return build();
        }

        /**
         * Detect url with quorum:1,quorum:2 as HBase does not handle different port numbers
         * for different quorum hostnames.
         * @param portStr
         * @return
         */
        private boolean isMultiPortUrl(String portStr) {
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

        private void parse() throws SQLException {
            StringTokenizer tokenizer = getTokenizerWithoutProtocol();
            int nTokens = 0;
            String[] tokens = new String[5];
            String token = null;
            boolean wasDelimiter = false;
            boolean first = true;
            while (tokenizer.hasMoreTokens() && !(token = tokenizer.nextToken()).equals(TERMINATOR)
                    && nTokens < tokens.length) {
                // This would mean we have an empty string for a token which is illegal
                if (DELIMITERS.contains(token)) {
                    if (wasDelimiter && !first) {
                        tokens[nTokens++] = "";
                    }
                    wasDelimiter = true;
                } else {
                    tokens[nTokens++] = token;
                    wasDelimiter = false;
                }
                first = false;
            }
            // Look-forward to see if the last token is actually the C:\\ path
            if (tokenizer.hasMoreTokens() && !TERMINATOR.equals(token)) {
                String extraToken = tokenizer.nextToken();
                if (WINDOWS_SEPARATOR_CHAR == extraToken.charAt(0)) {
                    String prevToken = tokens[nTokens - 1];
                    tokens[nTokens - 1] = prevToken + ":" + extraToken;
                    if (tokenizer.hasMoreTokens()
                            && !(token = tokenizer.nextToken()).equals(TERMINATOR)) {
                        throw getMalFormedUrlException(url);
                    }
                } else {
                    throw getMalFormedUrlException(url);
                }
            }
            int tokenIndex = 0;
            if (nTokens > tokenIndex) {
                zkHosts = tokens[tokenIndex++]; // Found quorum
                if (nTokens > tokenIndex) {
                    try {
                        zkPort = Integer.parseInt(tokens[tokenIndex]);
                        if (zkPort < 0) {
                            throw getMalFormedUrlException(url);
                        }
                        tokenIndex++; // Found port
                    } catch (NumberFormatException e) { // No port information
                        if (tokens[tokenIndex].isEmpty()) {
                            tokenIndex++; // Found empty port
                        }
                        if (isMultiPortUrl(tokens[tokenIndex])) {
                            throw getMalFormedUrlException(url);
                        }
                        // Otherwise assume port is simply omitted
                    }
                    if (nTokens > tokenIndex) {
                        if (tokens[tokenIndex].startsWith("/") || tokens[tokenIndex].isEmpty()) {
                            zkRootNode = tokens[tokenIndex++]; // Found rootNode
                        }
                        if (nTokens > tokenIndex) {
                            principal = tokens[tokenIndex++]; // Found principal
                            if (nTokens > tokenIndex) {
                                keytab = tokens[tokenIndex++]; // Found keytabFile
                                // There's still more after, try to see if it's a windows file path
                                if (tokenIndex < tokens.length) {
                                    String nextToken = tokens[tokenIndex++];
                                    // The next token starts with the directory separator, assume
                                    // it's still the keytab path.
                                    if (null != nextToken
                                            && WINDOWS_SEPARATOR_CHAR == nextToken.charAt(0)) {
                                        keytab = keytab + ":" + nextToken;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        protected ConnectionInfo build() {
            return new ZKConnectionInfo(isConnectionless, principal, keytab, user, haGroup, zkHosts,
                    zkPort, zkRootNode);
        }

        @Override
        protected void normalize() throws SQLException {
            // Treat empty as null
            if (zkHosts != null && zkHosts.isEmpty()) {
                zkHosts = null;
            }
            if (zkRootNode != null && zkRootNode.isEmpty()) {
                zkRootNode = null;
            }
            isConnectionless = PhoenixRuntime.CONNECTIONLESS.equals(zkHosts);

            if (isConnectionless) {
                if (zkPort != null || zkRootNode != null) {
                    throw getMalFormedUrlException(url);
                } else {
                    return;
                }
            }

            // Normalize connInfo so that a url explicitly specifying versus implicitly inheriting
            // the default values will both share the same ConnectionQueryServices.
            if (zkPort == null) {
                String zkPortString = get(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT);
                if (zkPortString == null) {
                    zkPortString = get(HConstants.ZOOKEEPER_CLIENT_PORT);
                }
                if (zkPortString == null) {
                    zkPort = HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT;
                } else {
                    zkPort = Integer.parseInt(zkPortString);
                }
            }

            if (zkHosts == null) {
                zkHosts = get(HConstants.CLIENT_ZOOKEEPER_QUORUM);
                if (zkHosts == null) {
                    zkHosts = get(HConstants.ZOOKEEPER_QUORUM);
                }
                if (zkHosts == null) {
                    throw getMalFormedUrlException(
                        "Quorum not specified and hbase.client.zookeeper.quorum is not set"
                                + " in configuration : " + url);
                }
            } else {
                zkHosts = zkHosts.replaceAll("=", ":");
            }

            zkHosts = normalizeHostsList(zkHosts, zkPort);
            // normalize out zkPort
            zkPort = null;

            if (zkRootNode == null) {
                zkRootNode =
                        get(HConstants.ZOOKEEPER_ZNODE_PARENT,
                            HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
            }
        }

        public static boolean isZK(Configuration config, ReadOnlyProps props, Properties info) {
            // Default is handled by the caller
            return config != null && ZK_REGISTRY_NAME
                    .equals(get(CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, config, props, info));
        }
    }

}
