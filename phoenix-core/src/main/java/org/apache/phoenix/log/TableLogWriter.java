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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LOG_TABLE;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Writes RingBuffer log event into table 
 * 
 */
public class TableLogWriter implements LogWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogWriter.class);
    private volatile Connection connection;
    private boolean isClosed;
    private PreparedStatement upsertStatement;
    private Configuration config;
    private Map<MetricType,Integer> metricOrdinals=new HashMap<MetricType,Integer>();

    public TableLogWriter(Configuration configuration) {
        this.config=configuration;
    }
    
    private PreparedStatement buildUpsertStatement(Connection conn) throws SQLException {
        StringBuilder buf = new StringBuilder("UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"(");
        int queryLogEntries=0;
        for (QueryLogInfo info : QueryLogInfo.values()) {
            buf.append(info.columnName);
            buf.append(',');
            queryLogEntries++;
        }
        for (MetricType metric : MetricType.values()) {
            if (metric.logLevel() != LogLevel.OFF) {
                metricOrdinals.put(metric, ++queryLogEntries);
                buf.append(metric.columnName());
                buf.append(',');
            }
        }
        buf.setLength(buf.length()-1);
        buf.append(") VALUES (");
        for (int i = 0; i < QueryLogInfo.values().length; i++) {
            buf.append("?,");
        }
        for (MetricType metric : MetricType.values()) {
            if (metric.logLevel() != LogLevel.OFF) {
                buf.append("?,");
            }
        }
        buf.setLength(buf.length()-1);
        buf.append(")");
        return conn.prepareStatement(buf.toString());
    }

    @Override
    public void write(RingBufferEvent event) throws SQLException, IOException {
        if (isClosed()) {
            LOGGER.warn("Unable to commit query log as Log committer is already closed");
            return;
        }
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    connection = QueryUtil.getConnectionForQueryLog(this.config);
                    this.upsertStatement = buildUpsertStatement(connection);
                }
            }
        }

        if (connection.isReadOnly()) {
            return;
        }

        ImmutableMap<QueryLogInfo, Object> queryInfoMap = event.getQueryInfo();
        for (QueryLogInfo info : QueryLogInfo.values()) {
            if (queryInfoMap.containsKey(info) && info.logLevel.ordinal() <= event.getConnectionLogLevel().ordinal()) {
                upsertStatement.setObject(info.ordinal() + 1, queryInfoMap.get(info));
            } else {
                upsertStatement.setObject(info.ordinal() + 1, null);
            }
        }
        Map<MetricType, Long> overAllMetrics = event.getOverAllMetrics();
        Map<String, Map<MetricType, Long>> readMetrics = event.getReadMetrics();

        for (MetricType metric : MetricType.values()) {
            if (overAllMetrics != null && overAllMetrics.containsKey(metric)
                    && metric.isLoggingEnabled(event.getConnectionLogLevel())) {
                upsertStatement.setObject(metricOrdinals.get(metric), overAllMetrics.get(metric));
            } else {
                if (metric.logLevel() != LogLevel.OFF) {
                    upsertStatement.setObject(metricOrdinals.get(metric), null);
                }
            }
        }

        if (readMetrics != null && !readMetrics.isEmpty()) {
            for (Map.Entry<String, Map<MetricType, Long>> entry : readMetrics.entrySet()) {
                upsertStatement.setObject(QueryLogInfo.TABLE_NAME_I.ordinal() + 1, entry.getKey());
                for (MetricType metric : entry.getValue().keySet()) {
                    if (metric.isLoggingEnabled(event.getConnectionLogLevel())) {
                        upsertStatement.setObject(metricOrdinals.get(metric), entry.getValue().get(metric));
                    }
                }
                upsertStatement.executeUpdate();
            }
        } else {
            upsertStatement.executeUpdate();
        }
        connection.commit();
    }
    
    @Override
    public void close() throws IOException {
        if (isClosed()) { return; }
        isClosed = true;
        try {
            if (connection != null) {
                // It should internally close all the statements
                connection.close();
            }
        } catch (SQLException e) {
            // TODO Ignore?
        }
    }

    public boolean isClosed(){
        return isClosed;
    }

}
