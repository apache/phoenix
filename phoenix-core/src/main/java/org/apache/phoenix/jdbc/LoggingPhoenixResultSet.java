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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingPhoenixResultSet extends DelegateResultSet {
    
    private static final Logger logger = LoggerFactory.getLogger(LoggingPhoenixResultSet.class);
    private PhoenixMetricsLog phoenixMetricsLog;
    
    public LoggingPhoenixResultSet(ResultSet rs, PhoenixMetricsLog phoenixMetricsLog) {
        super(rs);
        this.phoenixMetricsLog = phoenixMetricsLog;
    }
    
    @Override
    public void close() throws SQLException {
        phoenixMetricsLog.logOverAllReadRequestMetrics(logger, PhoenixRuntime.getOverAllReadRequestMetricInfo(rs));
        phoenixMetricsLog.logRequestReadMetrics(logger, PhoenixRuntime.getRequestReadMetricInfo(rs));
        PhoenixRuntime.resetMetrics(rs);
        super.close();
    }

}
