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

import java.sql.Connection;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.exception.FailoverSQLException;

import net.jcip.annotations.Immutable;

/**
 * A failover policy defines how failover connection deals with existing connections in case of
 * cluster role transition is detected.
 *
 * When an HBase cluster is not in ACTIVE role any more, all connections against it will get closed.
 * To handle a failover event, the failover connection will use the failover policy for taking
 * further actions. Those supported failover policies are defined here, but in future we can load
 * the policy implemented and configured by user at runtime for each connection. The default policy
 * requires that clients have to deal with the failover exception explicitly.
 */
@Immutable
@FunctionalInterface
public interface FailoverPolicy {
    String PHOENIX_HA_FAILOVER_POLICY_ATTR = "phoenix.ha.failover.policy";
    String PHOENIX_HA_FAILOVER_COUNT_ATTR = "phoenix.ha.failover.count";

    /**
     * Should try to failover by connecting to current ACTIVE HBase cluster (if any).
     *
     * @param exception the exception caught upon which this method is possible called
     * @param failoverCount how many time so far this failover has been attempted
     * @return true if caller should get a new phoenix connection against the ACTIVE HBase cluster
     */
    boolean shouldFailover(Exception exception, int failoverCount);

    /**
     * With this policy, clients have to deal with the failover exception explicitly if any.
     *
     * A {@link FailoverSQLException} exception will be thrown to the client when they try to use
     * the closed connections. Specially, the high availability (HA) framework will not connect to
     * the new ACTIVE cluster automatically for clients, but instead a client should:
     *  - re-connect to this HA group, in which case it will get a new connection wrapping a Phoenix
     *    connection to the newly ACTIVE cluster; OR
     *  - call static method {@link FailoverPhoenixConnection#failover(Connection,long)} explicitly.
     *    After that, it can create new Statement/ResultSet and retry the business logic.
     * If neither cluster is ACTIVE, connect requests to this HA group will keep getting exception.
     */
    class ExplicitFailoverPolicy implements FailoverPolicy {
        public static final String NAME = "explicit";
        private static final ExplicitFailoverPolicy INSTANCE = new ExplicitFailoverPolicy();

        @Override
        public boolean shouldFailover(Exception e, int failoverCount) {
            return false;
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    /**
     * With this, failover connection will wrap a new Phoenix connection to the new ACTIVE cluster.
     *
     * If the current operation (e.g. commit or create Statement) fails, the failover connection
     * will try to wrap a new Phoenix connection according to this policy.  After that, the client
     * will be able to create new Statement/ResultSet created against this failover connection.
     * While the HA group is failing over, the failover connection may not be able to failover by
     * wrapping a new phoenix connection.  In that case, clients trying to use this the failover
     * connection will get {@link FailoverSQLException} exception.
     *
     * The failover to ACTIVE cluster is best-effort; if it succeeds, clients do not notice target
     * cluster changed. Some cases are not yet well supported with this failover policy, for e.g.
     * after failover, the uncommitted mutations are not populated into the new connection.
     *
     * In case of {@link FailoverSQLException} exception, clients can still re-connect to this HA
     * group by creating a new failover connection, OR call static method failover() explicitly.
     */
    class FailoverToActivePolicy implements FailoverPolicy {
        public static final String NAME = "active";
        private static final int MAX_FAILOVER_COUNT_DEFAULT = 3;

        private final int maxFailoverCount;

        private FailoverToActivePolicy() {
            this.maxFailoverCount = MAX_FAILOVER_COUNT_DEFAULT;
        }

        private FailoverToActivePolicy(int maxFailoverCount) {
            this.maxFailoverCount = maxFailoverCount;
        }

        @Override
        public boolean shouldFailover(Exception e, int failoverCount) {
            return failoverCount < maxFailoverCount && e instanceof FailoverSQLException;
        }

        @Override
        public String toString() {
            return NAME + "(maxFailoverCount=" + maxFailoverCount + ")";
        }
    }

    /**
     * Get the failover policy from client properties.
     */
    static FailoverPolicy get(Properties properties) {
        String name = properties.getProperty(PHOENIX_HA_FAILOVER_POLICY_ATTR);
        if (StringUtils.isEmpty(name)) {
            return ExplicitFailoverPolicy.INSTANCE;
        }

        switch (name.toLowerCase()) {
        case ExplicitFailoverPolicy.NAME:
            return ExplicitFailoverPolicy.INSTANCE;
        case FailoverToActivePolicy.NAME:
            String maxFailoverCount = properties.getProperty(PHOENIX_HA_FAILOVER_COUNT_ATTR);
            return StringUtils.isEmpty(maxFailoverCount)
                    ? new FailoverToActivePolicy()
                    : new FailoverToActivePolicy(Integer.parseInt(maxFailoverCount));
        default:
            throw new IllegalArgumentException(
                    String.format("Unsupported %s '%s'", PHOENIX_HA_FAILOVER_POLICY_ATTR, name));
        }
    }
}
