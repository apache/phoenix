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
package org.apache.phoenix.monitoring.connectionqueryservice;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.monitoring.ConnectionQueryServicesMetric;
import org.apache.phoenix.monitoring.HistogramDistribution;
import org.apache.phoenix.monitoring.MetricType;

/**
 * ConnectionQueryServicesMetricsManager will be replaced by this class when
 * {@link org.apache.phoenix.query.QueryServices#CONNECTION_QUERY_SERVICE_METRICS_ENABLED} flag is
 * set to  false.
 */
public class NoOpConnectionQueryServicesMetricsManager extends ConnectionQueryServicesMetricsManager {

    public static final NoOpConnectionQueryServicesMetricsManager NO_OP_CONN_QUERY_SERVICES_METRICS_MANAGER =
            new NoOpConnectionQueryServicesMetricsManager();

    private NoOpConnectionQueryServicesMetricsManager() {
        super();
    }

    void updateMetricsValue(String connectionQueryServiceName, MetricType type,
            long value) {
    }

    Map<String, List<ConnectionQueryServicesMetric>> getConnectionQueryServicesMetrics() {
        return Collections.emptyMap();
    }

    Map<String, List<HistogramDistribution>> getHistogramsForConnectionQueryServices() {
        return Collections.emptyMap();
    }

    void clearConnectionQueryServiceMetrics() {

    }

    ConnectionQueryServicesMetrics getConnectionQueryServiceMetricsInstance(
            String connectionQueryServiceName) {
        return null;
    }
}
