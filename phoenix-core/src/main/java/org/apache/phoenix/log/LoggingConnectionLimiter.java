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

import java.lang.ref.WeakReference;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

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
    // Map Phoenix connection UUID to its connection object
    protected final Map<UUID, WeakReference<PhoenixConnection>> openPhoenixConnectionsMap;

    private LoggingConnectionLimiter(Builder builder) {
        super(builder.shouldThrottleNumConnections, builder.maxConnectionsAllowed, builder.maxInternalConnectionsAllowed);
        this.enableActivityLogging = builder.enableActivityLogging;
        this.loggingIntervalInMillis = builder.loggingIntervalInMins * MIN_IN_MILLIS;
        this.lastLoggedTimeInMillis = System.currentTimeMillis();
        this.openPhoenixConnectionsMap = Maps.newHashMap();
    }

    @Override
    public void acquireConnection(PhoenixConnection connection) throws SQLException {
        super.acquireConnection(connection);
        if ((this.enableActivityLogging) && (this.openPhoenixConnectionsMap.size() < this.maxConnectionsAllowed + this.maxInternalConnectionsAllowed)) {
            connection.setActivityLogger(new ConnectionActivityLogger(connection, LogLevel.INFO));
            this.openPhoenixConnectionsMap.put(connection.getUniqueID(), new WeakReference<PhoenixConnection>(connection));
        }
    }

    @Override
    public void returnConnection(PhoenixConnection connection) {
        super.returnConnection(connection);
        UUID phxConnUniqueID = connection.getUniqueID();
        Preconditions.checkNotNull(phxConnUniqueID, "Got null UUID for Phoenix Connection!");
        this.openPhoenixConnectionsMap.remove(phxConnUniqueID);
    }

    @Override
    public void onLimit()  {
        long currentTimeInMillis = System.currentTimeMillis();
        boolean shouldLogNow = (currentTimeInMillis - lastLoggedTimeInMillis) >= loggingIntervalInMillis;
        if (this.enableActivityLogging && shouldLogNow)  {
            for (WeakReference<PhoenixConnection> connectionReference : openPhoenixConnectionsMap.values()) {
                PhoenixConnection monitoredConnection = connectionReference.get();
                if (monitoredConnection == null) continue;
                LOGGER.info(String.format("activity-log for %s: %s",monitoredConnection.getUniqueID().toString(), monitoredConnection.getActivityLog()));
            }
            // Register the last logged time
            lastLoggedTimeInMillis = currentTimeInMillis;
        }
    }

    @VisibleForTesting
    public Map<String, String> getActivityLog() throws SQLException {
        Map<String, String> activityLog = Maps.newHashMap();
        if (this.enableActivityLogging) {
            for (WeakReference<PhoenixConnection> connectionReference : openPhoenixConnectionsMap.values()) {
                PhoenixConnection monitoredConnection = connectionReference.get();
                if (monitoredConnection == null) continue;
                activityLog.put(monitoredConnection.getUniqueID().toString(), monitoredConnection.getActivityLog());
            }
        }
        return activityLog;
    }

    public static class Builder {

        protected boolean enableActivityLogging;
        protected int loggingIntervalInMins;
        protected int maxConnectionsAllowed;
        protected int maxInternalConnectionsAllowed;
        protected boolean shouldThrottleNumConnections;

        public Builder(boolean shouldThrottleNumConnections) {
            this.shouldThrottleNumConnections = shouldThrottleNumConnections;
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
