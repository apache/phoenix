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
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompatGlobalClientMetricsRegistry {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CompatGlobalClientMetricsRegistry.class);

    protected static MetricRegistry metricRegistry;

    protected static void createRegistry() {
        LOGGER.info("Creating Metric Registry for Phoenix Global Metrics");
        MetricRegistryInfo registryInfo =
                new MetricRegistryInfo("PHOENIX", "Phoenix Client Metrics", "phoenix",
                        "Phoenix,sub=CLIENT", true);
        metricRegistry = MetricRegistries.global().create(registryInfo);
    }

    protected static void registerMetricToRegistry(final String name,
            final ValueProvider valueProvider) {
        metricRegistry.register(name, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return valueProvider.getValue();
            }
        });
    }

    protected static void registerMetricsAdapter(String metricTag) {
        GlobalMetricRegistriesAdapter.getInstance().registerMetricRegistry(metricRegistry,
            metricTag);
    }

    // Glue interface to break dependency on org.apache.hadoop.hbase.metrics.Gauge
    protected interface ValueProvider {
        public Long getValue();
    }

}
