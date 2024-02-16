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
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;

/**
 * ConnectionInfo used for org.apache.hadoop.hbase.client.MasterRegistry
 *
 */
public class MasterConnectionInfo extends AbstractRPCConnectionInfo {

    private static final String MASTER_REGISTRY_CLASS_NAME =
            "org.apache.hadoop.hbase.client.MasterRegistry";

    protected MasterConnectionInfo(boolean isConnectionless, String principal, String keytab,
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
            MASTER_REGISTRY_CLASS_NAME);

        if (bootstrapServers != null) {
            // This is already normalized to include ports
            connectionProps.put(HConstants.MASTER_ADDRS_KEY, bootstrapServers);
        }

        return connectionProps.isEmpty() ? ReadOnlyProps.EMPTY_PROPS
                : new ReadOnlyProps(connectionProps.entrySet().iterator());
    }

    @Override
    public String toUrl() {
        return PhoenixRuntime.JDBC_PROTOCOL_MASTER + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR
                + toString();
    }


    @Override
    public ConnectionInfo withPrincipal(String principal) {
        return new MasterConnectionInfo(isConnectionless, principal, keytab, user,
            haGroup, bootstrapServers);
    }

    /**
     * Builder class for MasterConnectionInfo
     *
     * @since 138
     */
    protected static class Builder extends AbstractRPCConnectionInfo.Builder {

        public Builder(String url, Configuration config, ReadOnlyProps props, Properties info)
                throws SQLException {
            super(url, config, props, info);
            if (!HAS_MASTER_REGISTRY) {
                throw getMalFormedUrlException(
                    "HBase version does not support Master registry for: " + url);
            }
        }

        @Override
        protected void normalize() throws SQLException {
            normalizeMaster();
        }

        @Override
        protected ConnectionInfo build() {
            return new MasterConnectionInfo(isConnectionless, principal, keytab, user, haGroup,
                    hostsList);
        }

        public static boolean isMaster(Configuration config, ReadOnlyProps props, Properties info) {
            // Default is handled by the caller
            return config != null && MASTER_REGISTRY_CLASS_NAME
                    .equals(get(CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, config, props, info));
        }
    }
}
