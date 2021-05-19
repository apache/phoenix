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
package org.apache.phoenix.monitoring;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * TableMetricsManager will be replaced by this case
 * incase of tableMetrics flag is set to  false.
 */


public class NoOpTableMetricsManager extends TableMetricsManager {

    public static final NoOpTableMetricsManager noOpsTableMetricManager = new NoOpTableMetricsManager();

    private NoOpTableMetricsManager() {
        super();
    }

    @Override public void updateMetrics(String tableName, MetricType type, long value) {

    }

    @Override public void pushMetricsFromConnInstance(Map<String, Map<MetricType, Long>> map) {

    }

    @Override public void clearTableLevelMetrics() {

    }

    @Override public Map<String, List<PhoenixTableMetric>> getTableLevelMetrics() {
        return Collections.emptyMap();
    }

    @Override public TableClientMetrics getTableClientMetrics(String tableName) {
        return null;
    }

}
