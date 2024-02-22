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
import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;

/**
 * Encapsulates the common logic for HRPC based ConnectionInfo classes.
 *
 */
public abstract class AbstractRPCConnectionInfo extends ConnectionInfo {

    private static final String MASTER_ADDRS_KEY = "hbase.masters";
    private static final String MASTER_HOSTNAME_KEY = "hbase.master.hostname";

    protected String bootstrapServers;

    public String getBoostrapServers() {
        return bootstrapServers;
    }

    protected AbstractRPCConnectionInfo(boolean isConnectionless, String principal, String keytab,
            User user, String haGroup) {
        super(isConnectionless, principal, keytab, user, haGroup);
    }

    @Override
    public String getZookeeperConnectionString() {
        throw new UnsupportedOperationException("MasterRegistry is used");
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
        AbstractRPCConnectionInfo other = (AbstractRPCConnectionInfo) obj;
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
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(bootstrapServers.replaceAll(":", "\\\\:"));
        if (anyNotNull(principal, keytab)) {
            sb.append(principal == null ? ":::" : ":::" + principal);
        }
        if (anyNotNull(keytab)) {
            sb.append(keytab == null ? ":" : ":" + keytab);
        }
        return sb.toString();
    }

    /**
     * Abstract Builder parent for HRPC based ConnectionInfo classes.
     *
     * @since 138
     */
    protected abstract static class Builder extends ConnectionInfo.Builder {
        String hostsList;
        String portString;
        Integer port;

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

        private void parse() throws SQLException {
            StringTokenizer tokenizer = getTokenizerWithoutProtocol();

            // Unlike for the ZK URL, there is no heuristics to figure out missing parts.
            // Unspecified parts inside the URL must be indicated by ::.
            boolean wasSeparator = true;
            boolean first = true;
            ArrayList<String> parts = new ArrayList<>(7);
            String token = null;
            while (tokenizer.hasMoreTokens()
                    && !(token = tokenizer.nextToken()).equals(TERMINATOR)) {
                if (DELIMITERS.contains(token)) {
                    if (wasSeparator && !first) {
                        parts.add(null);
                    }
                    wasSeparator = true;
                } else {
                    parts.add(token);
                    wasSeparator = false;
                }
                first = false;
                if (parts.size() > 6) {
                    throw getMalFormedUrlException(url);
                }
            }

            if (parts.size() == 6) {
                // We could check for FileSystems.getDefault().getSeparator()), but then
                // we wouldn't be able to test on Unix.
                if (parts.get(5).startsWith("\\")) {
                    // Reconstruct windows path
                    parts.set(4, parts.get(4) + ":" + parts.get(5));
                    parts.remove(5);
                } else {
                    throw getMalFormedUrlException(url);
                }
            }

            while (parts.size() < 7) {
                parts.add(null);
            }
            hostsList = parts.get(0);
            portString = parts.get(1);
            if (portString != null) {
                try {
                    port = Integer.parseInt(parts.get(1));
                    if (port < 0) {
                        throw new Exception();
                    }
                } catch (Exception e) {
                    throw getMalFormedUrlException(url);
                }
            }
            if (parts.get(2) != null && !parts.get(2).isEmpty()) {
                // This MUST be empty
                throw getMalFormedUrlException(url);
            }
            principal = parts.get(3);
            keytab = parts.get(4);
        }

        protected void normalizeMaster() throws SQLException {
            if (hostsList != null && hostsList.isEmpty()) {
                hostsList = null;
            }
            if (portString != null && portString.isEmpty()) {
                portString = null;
            }
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

            if (port == null) {
                port = getDefaultMasterPort();
            }
            // At this point, masterPort is guaranteed not to be 0

            isConnectionless = PhoenixRuntime.CONNECTIONLESS.equals(hostsList);

            if (isConnectionless) {
                if (port != null) {
                    throw getMalFormedUrlException(url);
                } else {
                    return;
                }
            }

            if (hostsList == null) {
                hostsList = getMasterAddr(port);
                if (hostsList == null) {
                    throw getMalFormedUrlException(
                        "Hbase masters are not specified and in URL, and are not set in the configuration files: "
                                + url);
                }
            } else {
                hostsList = hostsList.replaceAll("=", ":");
            }

            hostsList = normalizeHostsList(hostsList, port);
        }

        /**
         * Copied from org.apache.hadoop.hbase.client.MasterRegistry (which is private) Supplies the
         * default master port we should use given the provided configuration.
         * @param conf Configuration to parse from.
         */
        private  int getDefaultMasterPort() {
            String portString = get(HConstants.MASTER_PORT);
            if (portString == null) {
                port = HConstants.DEFAULT_MASTER_PORT;
            } else {
                port = Integer.parseInt(portString);
            }
            if (port == 0) {
                // Master port may be set to 0. We should substitute the default port in that case.
                return HConstants.DEFAULT_MASTER_PORT;
            }
            return port;
        }

        /**
         * Adopted from org.apache.hadoop.hbase.client.MasterRegistry Builds the default master
         * address end point if it is not specified in the configuration.
         */
        private String getMasterAddr(int port) {
            String masterAddrFromConf = get(MASTER_ADDRS_KEY);
            if (!Strings.isNullOrEmpty(masterAddrFromConf)) {
                return masterAddrFromConf;
            }
            String hostname = get(MASTER_HOSTNAME_KEY);
            if (hostname != null) {
                return String.format("%s:%d", hostname, port);
            } else {
                return null;
            }
        }

        protected abstract ConnectionInfo build();
    }
}
