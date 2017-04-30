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

package org.apache.phoenix.queryserver.server;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.jdbc.StatementInfo;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.phoenix.queryserver.metrics.PqsMetricsSystem;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;


public class PQSMetricsMeta extends JdbcMeta {

    private static final Logger LOG = LoggerFactory.getLogger(PQSMetricsMeta.class);
    public static final PqsMetricsSystem pqsMetricsSystem = new PqsMetricsSystem();

    public PQSMetricsMeta(String url) throws SQLException {
        super(url);
    }

    public PQSMetricsMeta(String url, String user, String password) throws SQLException {
        super(url, user, password);
    }

    public PQSMetricsMeta(String url, Properties info) throws SQLException {
        super(url, info);
    }

    public PQSMetricsMeta(String url, Properties info, MetricsSystem metrics) throws SQLException {
        super(url, info, metrics);
    }

    @Override
    public void closeConnection(ConnectionHandle ch) {
        Connection conn = null;
        Map<String, Map<String, Long>> mutationWriteMetrics = null;
        Map<String, Map<String, Long>> mutationReadMetrics = null;
        try {
            conn = super.getConnection(ch.id);
        } catch (SQLException e) {
            LOG.error(" error while getting hold of connection object ",e);
        }
        if (conn != null) {
            try {
                //get Phoenix Metrics and create Gauges if it does not exists.
                mutationWriteMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
                mutationReadMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(conn);
                pqsMetricsSystem.pqsSink.writeMapOfMap(mutationReadMetrics, PqsMetricsSystem.connectionMetrics);
                pqsMetricsSystem.pqsSink.writeMapOfMap(mutationWriteMetrics, PqsMetricsSystem.connectionMetrics);
                PhoenixRuntime.resetMetrics(conn);
            } catch (SQLException e) {
                LOG.warn("sql exception when trying to get connection level metrics ");
            }

        }
        super.closeConnection(ch);

    }

    @Override
    public void closeStatement(StatementHandle h) {
        StatementInfo statementInfo = super.getStatementCache().getIfPresent(h.id);
        Map<String, Long> overAllQueryMetrics = null;
        Map<String, Map<String, Long>> requestReadMetrics = null;

        // get phoenix metrics from this.
        if (statementInfo != null ) {
            ResultSet resultSet = statementInfo.getResultSet();
            if (resultSet != null) {
                try {
                    overAllQueryMetrics = PhoenixRuntime.getOverAllReadRequestMetrics(resultSet);
                    requestReadMetrics = PhoenixRuntime.getRequestReadMetrics(resultSet);
                    pqsMetricsSystem.pqsSink.writeMap(overAllQueryMetrics, PqsMetricsSystem.statementLevelMetrics);
                    pqsMetricsSystem.pqsSink.writeMapOfMap(requestReadMetrics, PqsMetricsSystem.statementLevelMetrics);
                    PhoenixRuntime.resetMetrics(resultSet);
                } catch (SQLException e) {
                    LOG.warn("sql exception when trying to get connection level metrics ");
                }
            }
        }
        super.closeStatement(h);
    }
}
