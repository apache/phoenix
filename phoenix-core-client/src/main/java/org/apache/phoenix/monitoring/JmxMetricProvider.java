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

import org.apache.hadoop.hbase.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;

import java.util.Map;

/**
 * This class implements the JMX based default Metric publishing
 * of Metrics to JMX end point.
 * This class is defined in phoenix/phoenix-core/src/main/resources/META-INF/services/org.apache.phoenix.monitoring.MetricPublisherSupplierFactory
 */
public class JmxMetricProvider implements MetricPublisherSupplierFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxMetricProvider.class);
    private static final String metricsName = "PHOENIX-TableLevel";
    private static final String metricsDesc = "Phoenix Client Metrics";
    private static final String metricsjmxContext = "phoenixTableLevel";
    private static final String metricsContext = "Phoenix,sub=CLIENT";

    private MetricRegistry metricRegistry;

    @Override public void registerMetricProvider() {
        metricRegistry = createMetricRegistry();
        GlobalMetricRegistriesAdapter.getInstance().registerMetricRegistry(metricRegistry);
    }

    @Override public void unregisterMetricProvider() {

    }

    private  MetricRegistry createMetricRegistry() {
        LOGGER.info("Creating Metric Registry for Phoenix Table Level Metrics");
        MetricRegistryInfo registryInfo =
                new MetricRegistryInfo(metricsName, metricsDesc,
                        metricsjmxContext, metricsContext, true);
        return MetricRegistries.global().create(registryInfo);
    }

    private static class PhoenixMetricGauge implements Gauge<Long> {
        private final PhoenixTableMetric metric;

        public PhoenixMetricGauge(PhoenixTableMetric metric) {
            this.metric = metric;
        }

        @Override public Long getValue() {
            return metric.getValue();
        }
    }

    private String getMetricNameFromMetricType(MetricType type, String tableName) {
        return tableName + "_table_" + type;
    }

    @Override public void registerMetrics(TableClientMetrics tInstance) {
        for (Map.Entry<MetricType, PhoenixTableMetric> entry : tInstance.getMetricRegistry()
                .entrySet()) {
            metricRegistry
                    .register(getMetricNameFromMetricType(entry.getKey(), tInstance.getTableName()),
                            new PhoenixMetricGauge(entry.getValue()));
        }
    }

    @Override public void unRegisterMetrics(TableClientMetrics tInstance) {
        for (Map.Entry<MetricType, PhoenixTableMetric> entry : tInstance.getMetricRegistry()
                .entrySet()) {
            metricRegistry
                    .remove(getMetricNameFromMetricType(entry.getKey(), tInstance.getTableName()));
        }
    }

}
