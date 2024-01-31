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
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;

/**
 * ConnectionInfo class for org.apache.hadoop.hbase.client.RpcConnectionRegistry
 *
 * @since 138
 */
public class RPCConnectionInfo extends AbstractRPCConnectionInfo {

    // We may be on an older HBase version, which does not even have RpcConnectionRegistry
    private static final String BOOTSTRAP_NODES = "hbase.client.bootstrap.servers";
    private static final String RPC_REGISTRY_CLASS_NAME =
            "org.apache.hadoop.hbase.client.RpcConnectionRegistry";

    protected RPCConnectionInfo(boolean isConnectionless, String principal, String keytab,
            User user, String haGroup, String bootstrapServers) {
        super(isConnectionless, principal, keytab, user, haGroup);
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public ReadOnlyProps asProps() {
        if (isConnectionless) {
            return ReadOnlyProps.EMPTY_PROPS;
        }

        Map<String, String> connectionProps = getCommonProps();

        connectionProps.put(CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
            RPC_REGISTRY_CLASS_NAME);

        if (getBoostrapServers() != null) {
            // This is already normalized to include ports
            connectionProps.put(BOOTSTRAP_NODES, bootstrapServers);
        }

        return connectionProps.isEmpty() ? ReadOnlyProps.EMPTY_PROPS
                : new ReadOnlyProps(connectionProps.entrySet().iterator());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bootstrapServers == null) ? 0 : bootstrapServers.hashCode());
        // Port is already provided in or normalized into bootstrapServers
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        RPCConnectionInfo other = (RPCConnectionInfo) obj;
        if (bootstrapServers == null) {
            if (other.bootstrapServers != null) {
                return false;
            }
        } else if (!bootstrapServers.equals(other.bootstrapServers)) {
            return false;
        }
        // Port is already provided in or normalized into bootstrapServers
        return true;
    }

    @Override
    public String toUrl() {
        return PhoenixRuntime.JDBC_PROTOCOL_RPC + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR
                + toString();
    }

    @Override
    public ConnectionInfo withPrincipal(String principal) {
        return new RPCConnectionInfo(isConnectionless, principal, keytab, user,
            haGroup, bootstrapServers);
    }

    /**
     * Builder parent for RPCConnectionInfo.
     */
    protected static class Builder extends AbstractRPCConnectionInfo.Builder {

        public Builder(String url, Configuration config, ReadOnlyProps props, Properties info)
                throws SQLException {
            super(url, config, props, info);
            if (!HAS_RPC_REGISTRY) {
                throw getMalFormedUrlException(
                    "Hbase version does not support Master registry for: " + url);
            }
        }

        @Override
        protected void normalize() throws SQLException {
            if (hostsList != null && hostsList.isEmpty()) {
                hostsList = null;
            }
            if (portString != null && portString.isEmpty()) {
                portString = null;
            }

            // We don't have a default port for RPC Connections
            // Well, we do if we fall back to Master
            boolean noServerListinURL = false;
            if (hostsList == null) {
                hostsList = getBootstrapServerAddr();
                noServerListinURL = true;
                if (hostsList == null) {
                    // Fall back to MasterRegistry behaviour
                    normalizeMaster();
                    return;
                }
            } else {
                hostsList = hostsList.replaceAll("=", ":");
            }

            isConnectionless = PhoenixRuntime.CONNECTIONLESS.equals(hostsList);

            if (portString != null) {
                try {
                    port = Integer.parseInt(portString);
                    if (port < 0) {
                        throw new Exception();
                    }
                } catch (Exception e) {
                    throw getMalFormedUrlException(url);
                }
            }

            if (isConnectionless) {
                if (port != null) {
                    throw getMalFormedUrlException(url);
                } else {
                    return;
                }
            }

            // RpcConnectionRegistry doesn't have a default port property, be we accept the legacy
            // format
            // from the URL if both host list and port is provided
            if (port != null && !noServerListinURL) {
                hostsList = normalizeHostsList(hostsList, port);
            }
        }

        public String getBootstrapServerAddr() {
            String configuredBootstrapNodes = get(BOOTSTRAP_NODES);
            if (!Strings.isNullOrEmpty(configuredBootstrapNodes)) {
                return configuredBootstrapNodes;
            } else {
                return null;
            }
        }

        @Override
        protected ConnectionInfo build() {
            return new RPCConnectionInfo(isConnectionless, principal, keytab, user, haGroup,
                    hostsList);
        }

        public static boolean isRPC(Configuration config, ReadOnlyProps props, Properties info) {
            // Default is handled by the caller
            return config != null && RPC_REGISTRY_CLASS_NAME
                    .equals(get(CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, config, props, info));
        }
    }
}
