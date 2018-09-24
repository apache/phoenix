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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.phoenix.util.PhoenixRuntime;

public class LoggingPhoenixResultSet extends DelegateResultSet {
    
    private PhoenixMetricsLog phoenixMetricsLog;
    private String sql;
    private boolean areMetricsLogged;

    public LoggingPhoenixResultSet(ResultSet rs, PhoenixMetricsLog phoenixMetricsLog, String sql) {
        super(rs);
        this.phoenixMetricsLog = phoenixMetricsLog;
        this.sql = sql;
        this.areMetricsLogged = false;
    }
    
    @Override
    public void close() throws SQLException {
        if (!rs.isClosed()) {
            super.close();
        }
        if (!this.areMetricsLogged) {
            phoenixMetricsLog.logOverAllReadRequestMetrics(PhoenixRuntime.getOverAllReadRequestMetricInfo(rs), sql);
            phoenixMetricsLog.logRequestReadMetrics(PhoenixRuntime.getRequestReadMetricInfo(rs), sql);
            PhoenixRuntime.resetMetrics(rs);
            this.areMetricsLogged = true;
        }
    }

}
