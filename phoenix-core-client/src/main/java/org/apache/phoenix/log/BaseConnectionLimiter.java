/**
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
package org.apache.phoenix.log;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesMetricsManager;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.GuardedBy;
import java.sql.SQLException;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.PHOENIX_CONNECTIONS_THROTTLED_COUNTER;
import static org.apache.phoenix.query.QueryServices.QUERY_SERVICES_NAME;

/**
 * A base class for concrete implementation of ConnectionLimiter.
 * 1. Should be called only when holding the ConnectionQueryServicesImpl.connectionCountLock
 * 2. Does the basic accounting on open and close connection calls.
 * 3. If connection throttling is enabled checks and calls onLimit if the threshold is breached.
 */
public abstract class BaseConnectionLimiter implements ConnectionLimiter {
    protected int connectionCount = 0;
    protected int internalConnectionCount = 0;
    protected int connectionThrottledCounter = 0;
    protected String profileName;
    protected int maxConnectionsAllowed;
    protected int maxInternalConnectionsAllowed;
    protected boolean shouldThrottleNumConnections;

    protected BaseConnectionLimiter(String profileName, boolean shouldThrottleNumConnections, int maxConnectionsAllowed, int maxInternalConnectionsAllowed) {
        this.profileName = profileName;
        this.shouldThrottleNumConnections = shouldThrottleNumConnections;
        this.maxConnectionsAllowed = maxConnectionsAllowed;
        this.maxInternalConnectionsAllowed = maxInternalConnectionsAllowed;
    }
    @Override
    @GuardedBy("ConnectionQueryServicesImpl.connectionCountLock")
    public void acquireConnection(PhoenixConnection connection) throws SQLException {
        Preconditions.checkNotNull(connection.getUniqueID(), "Got null UUID for Phoenix Connection!");

        /*
         * If we are throttling connections internal connections and client created connections
         *   are counted separately against each respective quota.
         */
        if (shouldThrottleNumConnections) {
            int futureConnections = 1 + ( connection.isInternalConnection() ? internalConnectionCount : connectionCount);
            int allowedConnections = connection.isInternalConnection() ? maxInternalConnectionsAllowed : maxConnectionsAllowed;
            // if throttling threshold is reached, try reclaiming garbage collected phoenix connections.
            if ((allowedConnections != 0) && (futureConnections > allowedConnections) && (onSweep(connection.isInternalConnection()) == 0)) {
                GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER.increment();
                connectionThrottledCounter++;
                String connectionQueryServiceName = connection.getQueryServices()
                        .getConfiguration().get(QUERY_SERVICES_NAME);
                // Since this is ever-increasing counter and only gets reset at JVM restart
                // Both global and connection query service level,
                // we won't create histogram for this metric.
                ConnectionQueryServicesMetricsManager.updateMetrics(
                        connectionQueryServiceName,
                        PHOENIX_CONNECTIONS_THROTTLED_COUNTER, connectionThrottledCounter);

                // Let the concrete classes handle the onLimit.
                // They can either throw the exception back or handle it.
                SQLException connectionThrottledException = connection.isInternalConnection() ?
                        new SQLExceptionInfo.Builder(SQLExceptionCode.NEW_INTERNAL_CONNECTION_THROTTLED).
                                build().buildException() :
                        new SQLExceptionInfo.Builder(SQLExceptionCode.NEW_CONNECTION_THROTTLED).
                                build().buildException();
                throw connectionThrottledException;
            }
        }

        if (connection.isInternalConnection()) {
            internalConnectionCount++;
        } else {
            connectionCount++;
        }

    }

    @Override
    @GuardedBy("ConnectionQueryServicesImpl.connectionCountLock")
    public void returnConnection(PhoenixConnection connection) {
        if (connection.isInternalConnection() && internalConnectionCount > 0) {
            --internalConnectionCount;
        } else if (!connection.isInternalConnection() && connectionCount > 0) {
            --connectionCount;
        }
    }

    @Override
    @GuardedBy("ConnectionQueryServicesImpl.connectionCountLock")
    public boolean isLastConnection() {
        return connectionCount + internalConnectionCount - 1 <= 0;
    }
    @Override
    public boolean isShouldThrottleNumConnections() {
        return shouldThrottleNumConnections;
    }

    @VisibleForTesting
    @GuardedBy("ConnectionQueryServicesImpl.connectionCountLock")
    public int getConnectionCount() {
        return connectionCount;
    }

    @Override
    public int onSweep(boolean internal) {
        return 0;
    }

    @VisibleForTesting
    @GuardedBy("ConnectionQueryServicesImpl.connectionCountLock")
    public int getInternalConnectionCount() {
        return internalConnectionCount;
    }

    public int getMaxConnectionsAllowed() {
        return maxConnectionsAllowed;
    }

    public int getMaxInternalConnectionsAllowed() {
        return maxInternalConnectionsAllowed;
    }

}

