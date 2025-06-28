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

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PDataType;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.Format;
import java.util.List;
import java.util.Map;

/**
 * This interface is for phoenix connections that provide metrics to PhoenixRuntime
 */
public interface PhoenixMonitoredConnection extends Connection {
    /**
     * @return map of Table Name String to a Map of Metric Type to current value for mutations
     */
    Map<String, Map<MetricType, Long>> getMutationMetrics();

    /**
     * @return map of Table Name String to a Map of Metric Type to current value for reads
     */
    Map<String, Map<MetricType, Long>> getReadMetrics();

    /**
     * @return true if request metrics are enabled false otherwise
     */
    boolean isRequestLevelMetricsEnabled();

    /**
     * Clears the local metrics values by setting them back to 0.  Useful for multistatement connections and extracting
     * metrics for individual DML.
     */
    void clearMetrics();
    ConnectionQueryServices getQueryServices() throws SQLException;
    PTable getTable(PTableKey key) throws SQLException;
    PTable getTable(String name) throws SQLException;
    PTable getTableNoCache(String name) throws SQLException;
    Consistency getConsistency()  throws SQLException;
    PName getTenantId() throws SQLException;
    MutationState getMutationState() throws SQLException;
    PMetaData getMetaDataCache() throws SQLException;
    int getMutateBatchSize() throws SQLException;
    int executeStatements(Reader reader, List<Object> binds,
                          PrintStream out) throws IOException, SQLException;
    Format getFormatter(PDataType type) throws SQLException;
    void setRunningUpgrade(boolean isRunningUpgrade) throws SQLException;
    PTable getTable(String tenantId, String fullTableName)
            throws SQLException;
     PTable getTableNoCache(PName tenantId, String name) throws SQLException;
    void setIsClosing(boolean imitateIsClosing) throws SQLException;
    PreparedStatement prepareStatement(String sql) throws SQLException;

}
