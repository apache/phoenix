/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.log;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.BIND_PARAMETERS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLIENT_IP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.EXCEPTION_TRACE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.EXPLAIN_PLAN;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GLOBAL_SCAN_DETAILS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NO_OF_RESULTS_ITERATED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY_STATUS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCAN_METRICS_JSON;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.START_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.USER;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;


public enum QueryLogInfo {
    
    CLIENT_IP_I(CLIENT_IP, LogLevel.INFO, PVarchar.INSTANCE),
    QUERY_I(QUERY, LogLevel.INFO,PVarchar.INSTANCE),
    BIND_PARAMETERS_I(BIND_PARAMETERS, LogLevel.TRACE,PVarchar.INSTANCE),
    QUERY_ID_I(QUERY_ID, LogLevel.INFO,PVarchar.INSTANCE),
    TENANT_ID_I(TENANT_ID, LogLevel.INFO,PVarchar.INSTANCE),
    START_TIME_I(START_TIME, LogLevel.INFO,PTimestamp.INSTANCE),
    USER_I(USER, LogLevel.INFO,PVarchar.INSTANCE),
    EXPLAIN_PLAN_I(EXPLAIN_PLAN,LogLevel.DEBUG,PVarchar.INSTANCE),
    GLOBAL_SCAN_DETAILS_I(GLOBAL_SCAN_DETAILS, LogLevel.DEBUG,PVarchar.INSTANCE),
    NO_OF_RESULTS_ITERATED_I(NO_OF_RESULTS_ITERATED, LogLevel.INFO,PLong.INSTANCE),
    EXCEPTION_TRACE_I(EXCEPTION_TRACE, LogLevel.DEBUG,PVarchar.INSTANCE),
    QUERY_STATUS_I(QUERY_STATUS, LogLevel.INFO,PVarchar.INSTANCE),
    SCAN_METRICS_JSON_I(SCAN_METRICS_JSON, LogLevel.TRACE,PVarchar.INSTANCE), 
    TABLE_NAME_I(TABLE_NAME, LogLevel.DEBUG,PVarchar.INSTANCE);
    
    public final String columnName;
    public final LogLevel logLevel;
    public final PDataType dataType;

    private QueryLogInfo(String columnName, LogLevel logLevel, PDataType dataType) {
        this.columnName = columnName;
        this.logLevel=logLevel;
        this.dataType=dataType;
    }

    public String getColumnName() {
        return columnName;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    public PDataType getDataType() {
        return dataType;
    }
    
    
}
