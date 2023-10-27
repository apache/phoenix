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

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Logger for connection related activities.
 * See also {@link ActivityLogInfo}
 */
public class ConnectionActivityLogger {
    private LogLevel logLevel;
    private boolean isInternalConnection;
    private UUID connectionID;
    private WeakReference<PhoenixConnection> connectionReference;
    List<String> activityList = Stream.of(ActivityLogInfo.values()).map(f -> "").collect(Collectors.toList());

    public ConnectionActivityLogger(PhoenixConnection connection, LogLevel level) {
        logLevel = level;
        this.isInternalConnection = connection.isInternalConnection();
        this.connectionID = connection.getUniqueID();
        this.connectionReference = new WeakReference<PhoenixConnection>(connection);
        connection.setActivityLogger(this);
        log(ActivityLogInfo.START_TIME, String.valueOf(EnvironmentEdgeManager.currentTimeMillis()));
        PName tenantName = connection.getTenantId();
        if (tenantName != null) {
            log(ActivityLogInfo.TENANT_ID, tenantName.getString());
        }
        // TODO: CQS_NAME (Connection Profile Name)

    }

    public ConnectionActivityLogger() {
        logLevel = LogLevel.OFF;
    }

    public static final ConnectionActivityLogger NO_OP_LOGGER = new ConnectionActivityLogger() {

        @Override
        public void log(ActivityLogInfo ActivityLogInfo, String info) {}

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public boolean isInfoEnabled() {
            return false;
        }

        @Override
        public boolean isLevelEnabled(LogLevel logLevel) {
            return false;
        }

        @Override
        public boolean isInternalConnection() {
            return false;
        }

        @Override
        public PhoenixConnection getConnection() { return null; }

        @Override
        public String getActivityLog() {return "";}

        @Override
        public String getConnectionID() {return "";}

    };

    public String getConnectionID() {
        return connectionID.toString();
    }

    public boolean isInternalConnection() {
        return isInternalConnection;
    }

    public PhoenixConnection getConnection() {
        return connectionReference.get();
    }

    /**
     * Set logging info for a given activity
     */
    public void log(ActivityLogInfo activity, String info) {
        if (logLevel == LogLevel.OFF) return;
        activityList.set(activity.ordinal(), info);
    }

    /**
     * Get the formatted log for external logging.
     */
    public String getActivityLog() {
        return IntStream
                .range(0, ActivityLogInfo.values().length)
                .filter((i) -> {return !Strings.isNullOrEmpty(activityList.get(i)) && isLevelEnabled(ActivityLogInfo.values()[i].getLogLevel());})
                .mapToObj(i -> new StringBuilder().append(ActivityLogInfo.values()[i].shortName).append("=").append(activityList.get(i)).toString())
                .collect(Collectors.joining(", "));
    }
    /**
     * Is Info logging currently enabled?
     * Call this method to prevent having to perform expensive operations (for example, String concatenation) when the log level is more than info.
     * @return
     */
    public boolean isInfoEnabled(){
        return isLevelEnabled(LogLevel.INFO);
    }

    /**
     *  Is debug logging currently enabled?
     *  Call this method to prevent having to perform expensive operations (for example, String concatenation) when the log level is more than debug.
     */
    public boolean isDebugEnabled(){
        return isLevelEnabled(LogLevel.DEBUG);
    }

    public boolean isLevelEnabled(LogLevel logLevel) {
        return this.logLevel != null && logLevel != LogLevel.OFF ? logLevel.ordinal() <= this.logLevel.ordinal()
                : false;
    }
}
