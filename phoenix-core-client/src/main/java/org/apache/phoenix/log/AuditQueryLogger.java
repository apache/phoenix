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
package org.apache.phoenix.log;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.MetricType;

import java.util.Map;


/*
 * Wrapper for query translator
 */
public class AuditQueryLogger extends QueryLogger {
    private LogLevel auditLogLevel;


    private AuditQueryLogger(PhoenixConnection connection) {
        super(connection);
        auditLogLevel = connection.getAuditLogLevel();

    }

    private AuditQueryLogger() {
        super();
        auditLogLevel = LogLevel.OFF;
    }

    public static final AuditQueryLogger NO_OP_INSTANCE = new AuditQueryLogger() {
        @Override
        public void log(QueryLogInfo queryLogInfo, Object info) {

        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public boolean isInfoEnabled() {
            return false;
        }

        @Override
        public void sync(
                Map<String, Map<MetricType, Long>> readMetrics, Map<MetricType, Long> overAllMetrics) {

        }

        @Override
        public void syncAudit(
                Map<String, Map<MetricType, Long>> readMetrics, Map<MetricType, Long> overAllMetrics) {

        }

        @Override
        public boolean isSynced(){
            return true;
        }
    };

    public static AuditQueryLogger getInstance(PhoenixConnection connection, boolean isSystemTable) {
        if (connection.getAuditLogLevel() == LogLevel.OFF || isSystemTable) {
            return NO_OP_INSTANCE;
        }
        return new AuditQueryLogger(connection);
    }


    /**
     *  Is audit logging currently enabled?
     *  Call this method to prevent having to perform expensive operations (for example,
     *  String concatenation) when the audit log level is more than info.
     */
    public boolean isAuditLoggingEnabled(){
        return isAuditLevelEnabled(LogLevel.INFO);
    }

    private boolean isAuditLevelEnabled(LogLevel logLevel){
        return this.auditLogLevel != null && logLevel != LogLevel.OFF ? logLevel.ordinal() <= this.auditLogLevel.ordinal()
                : false;
    }



    public void sync(Map<String, Map<MetricType, Long>> readMetrics, Map<MetricType, Long> overAllMetrics) {
        syncBase(readMetrics, overAllMetrics, auditLogLevel);
    }

    public void syncAudit() {
        syncAudit(null, null);
    }

    /**
     *  We force LogLevel.TRACE here because in QueryLogInfo the minimum LogLevel for
     *  TABLE_NAME_I is Debug and for BIND_PARAMETERS_I is TRACE and we would like to see
     *  these parameters even in INFO level when using DDL and DML operations.
     */
    public void syncAudit(Map<String, Map<MetricType, Long>> readMetrics, Map<MetricType, Long> overAllMetrics) {
        syncBase(readMetrics, overAllMetrics, LogLevel.TRACE);
    }

}
