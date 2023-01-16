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

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER;

/**
 * An implementation of a ConnectionLimiter which logs activity info at configured intervals
 * for the active connections in the map when throttling threshold is reached.
 */
public class LoggingConnectionLimiter extends BaseConnectionLimiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingConnectionLimiter.class);
    private static long MIN_IN_MILLIS = 60 * 1000;
    protected final boolean enableActivityLogging;
    protected final long loggingIntervalInMillis;
    protected long lastLoggedTimeInMillis;
    protected long lastCollectedTimeInMillis;
    // Map Phoenix connection UUID to its connection activity logger object
    protected final Map<UUID, ConnectionActivityLogger> openConnectionActivityLoggers;

    private LoggingConnectionLimiter(Builder builder) {
        super(builder.profileName, builder.shouldThrottleNumConnections, builder.maxConnectionsAllowed, builder.maxInternalConnectionsAllowed);
        this.enableActivityLogging = builder.enableActivityLogging;
        this.loggingIntervalInMillis = builder.loggingIntervalInMins * MIN_IN_MILLIS;
        this.lastCollectedTimeInMillis = this.lastLoggedTimeInMillis = System.currentTimeMillis();
        this.openConnectionActivityLoggers = Maps.newHashMap();
    }

    @Override
    public void acquireConnection(PhoenixConnection connection) throws SQLException {
        super.acquireConnection(connection);
        if ((this.enableActivityLogging) && (this.openConnectionActivityLoggers.size() < this.maxConnectionsAllowed + this.maxInternalConnectionsAllowed)) {
            ConnectionActivityLogger logger = new ConnectionActivityLogger(connection, LogLevel.INFO);
            this.openConnectionActivityLoggers.put(connection.getUniqueID(), logger);
        }
    }

    @Override
    public void returnConnection(PhoenixConnection connection) {
        super.returnConnection(connection);
        UUID phxConnUniqueID = connection.getUniqueID();
        Preconditions.checkNotNull(phxConnUniqueID, "Got null UUID for Phoenix Connection!");
        if (this.enableActivityLogging) {
            this.openConnectionActivityLoggers.remove(phxConnUniqueID);
        }
    }

    @Override
    public int onSweep(boolean internal)  {
        long currentTimeInMillis = System.currentTimeMillis();
        boolean shouldCollectNow = (currentTimeInMillis - lastCollectedTimeInMillis) >= loggingIntervalInMillis;
        int garbageCollectedConnections = 0;
        if (this.enableActivityLogging && shouldCollectNow)  {
            Iterator<Map.Entry<UUID, ConnectionActivityLogger>> iterator = openConnectionActivityLoggers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<UUID, ConnectionActivityLogger> entry = iterator.next();
                ConnectionActivityLogger logger = entry.getValue();
                // check for reclaim only for connection/logger that match the sweep type. for e.g internal or external client connections.
                boolean checkReclaimable = ((logger.isInternalConnection() && internal) || (!logger.isInternalConnection() && !internal));
                if (checkReclaimable) {
                    PhoenixConnection monitoredConnection = logger.getConnection();
                    LOGGER.info(String.format("connection-sweep-activity-log for %s: %s", logger.getConnectionID(), logger.getActivityLog()));
                    if (monitoredConnection == null) {
                        garbageCollectedConnections += collectConnection(internal);
                        iterator.remove();
                    }
                }
            }
            LOGGER.info(String.format("connection-profile-metrics-log for %s: internal=%s, freed=%d, current=%d, open=%d, throttled=%d",
                    this.profileName,
                    internal,
                    garbageCollectedConnections,
                    internal ? getInternalConnectionCount(): getConnectionCount(),
                    internal ?
                            GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS.getMetric().getValue() :
                            GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getValue(),
                    GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER.getMetric().getValue()));

            // Register the last logged time
            lastCollectedTimeInMillis = currentTimeInMillis;

        }
        return garbageCollectedConnections;
    }

    private int collectConnection(boolean internal) {
        if (internal && internalConnectionCount > 0) {
            --internalConnectionCount;
            GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS.decrement();
            return 1;
        } else if (!internal && connectionCount > 0) {
            --connectionCount;
            GLOBAL_OPEN_PHOENIX_CONNECTIONS.decrement();
            return 1;
        }
        return 0;
    }

    @VisibleForTesting
    public Map<String, String> getActivityLog() throws SQLException {
        Map<String, String> activityLog = Maps.newHashMap();
        if (this.enableActivityLogging) {
            for (ConnectionActivityLogger connectionLogger : openConnectionActivityLoggers.values()) {
                activityLog.put(connectionLogger.getConnectionID(), connectionLogger.getActivityLog());
            }
        }
        return activityLog;
    }

    public static class Builder {

        protected String profileName;
        protected boolean enableActivityLogging;
        protected int loggingIntervalInMins;
        protected int maxConnectionsAllowed;
        protected int maxInternalConnectionsAllowed;
        protected boolean shouldThrottleNumConnections;

        public Builder(boolean shouldThrottleNumConnections) {
            this.shouldThrottleNumConnections = shouldThrottleNumConnections;
        }

        public Builder withConnectionProfile(String profileName) {
            this.profileName = profileName;
            return this;
        }

        public Builder withMaxAllowed(int maxAllowed) {
            this.maxConnectionsAllowed = maxAllowed;
            return this;
        }

        public Builder withMaxInternalAllowed(int maxInternalAllowed) {
            this.maxInternalConnectionsAllowed = maxInternalAllowed;
            return this;
        }

        public Builder withLogging(boolean enabled) {
            this.enableActivityLogging = enabled;
            return this;
        }
        public Builder withLoggingIntervalInMins(int loggingIntervalInMins) {
            this.loggingIntervalInMins = loggingIntervalInMins;
            return this;
        }

        public ConnectionLimiter build() {
            return new LoggingConnectionLimiter(this);
        }

    }
}
