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

import static org.apache.phoenix.monitoring.MetricType.OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.PHOENIX_CONNECTIONS_THROTTLED_COUNTER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.monitoring.ConnectionQueryServicesMetric;
import org.apache.phoenix.monitoring.ConnectionQueryServicesMetricImpl;
import org.apache.phoenix.monitoring.MetricType;

/**
 * Class for Connection Query Service Metrics.
 */
public class ConnectionQueryServicesMetrics {
    /**
     * List Metrics tracked in Connection Query Service Metrics
     */
    public enum QueryServiceMetrics {
        CONNECTION_QUERY_SERVICE_OPEN_PHOENIX_CONNECTIONS_COUNTER(OPEN_PHOENIX_CONNECTIONS_COUNTER),
        CONNECTION_QUERY_SERVICE_OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER(
                OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER),
        CONNECTION_QUERY_SERVICE_PHOENIX_CONNECTIONS_THROTTLED_COUNTER(
                PHOENIX_CONNECTIONS_THROTTLED_COUNTER);

        private MetricType metricType;
        private ConnectionQueryServicesMetric metric;

        QueryServiceMetrics(MetricType metricType) {
            this.metricType = metricType;
        }
    }

    private final String connectionQueryServiceName;
    private Map<MetricType, ConnectionQueryServicesMetric> metricRegister;
    private ConnectionQueryServicesMetricsHistograms connectionQueryServiceMetricsHistograms;

    public ConnectionQueryServicesMetrics(final String connectionQueryServiceName,
            Configuration conf) {
        this.connectionQueryServiceName = connectionQueryServiceName;
        metricRegister = new HashMap<>();
        for (QueryServiceMetrics connectionQueryServiceMetric
                : QueryServiceMetrics.values()) {
            connectionQueryServiceMetric.metric =
                    new ConnectionQueryServicesMetricImpl(connectionQueryServiceMetric.metricType);
            metricRegister.put(connectionQueryServiceMetric.metricType,
                    connectionQueryServiceMetric.metric);
        }
        connectionQueryServiceMetricsHistograms =
                new ConnectionQueryServicesMetricsHistograms(connectionQueryServiceName, conf);
    }

    /**
     * This function is used to update the value of Metric
     * In case of counter val will be passed as 1.
     *
     * @param type metric type
     * @param val update value. In case of counters, this will be 1
     */
    public void setMetricValue(MetricType type, long val) {
        if (!metricRegister.containsKey(type)) {
            return;
        }
        ConnectionQueryServicesMetric metric = metricRegister.get(type);
        metric.set(val);
    }

    /**
     * This function is used to get the value of Metric.
     *
     * @param type metric type
     * @return val current value of metric.
     */
    public long getMetricValue(MetricType type) {
        if (!metricRegister.containsKey(type)) {
            return 0;
        }
        ConnectionQueryServicesMetric metric = metricRegister.get(type);
        return metric.getValue();
    }

    public String getConnectionQueryServiceName() {
        return connectionQueryServiceName;
    }

    /**
     * This method is called to aggregate all the Metrics across all Connection Query Service.
     *
     * @return map of Connection Query Service name -> list of ConnectionQueryServicesMetric.
     */
    public List<ConnectionQueryServicesMetric> getAllMetrics() {
        return new ArrayList<>(metricRegister.values());
    }

    public ConnectionQueryServicesMetricsHistograms getConnectionQueryServiceHistograms() {
        return connectionQueryServiceMetricsHistograms;
    }
}
