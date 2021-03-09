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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.phoenix.query.QueryServicesOptions;

public class JmxMetricProvider implements MetricPublisherSupplierFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxMetricProvider.class);
    static MetricRegistry metricRegistry;

    @Override public void registerMetricProvider() {
        metricRegistry = createMetricRegistry();
        GlobalMetricRegistriesAdapter.getInstance().registerMetricRegistry(metricRegistry,QueryServicesOptions.withDefaults().getClientMetricTag());
    }


    public static  MetricRegistry getMetricRegistryInstance() {
        return metricRegistry;
    }

    private  MetricRegistry createMetricRegistry() {
        LOGGER.info("Creating Metric Registry for Phoenix Table Level Metrics");
        MetricRegistryInfo registryInfo = new MetricRegistryInfo("PHOENIX-TableLevel", "Phoenix Client Metrics",
                "phoenixTableLevel", "Phoenix,sub=CLIENT", true);
        return MetricRegistries.global().create(registryInfo);
    }

    @Override public void unregisterMetricProvider() {

    }
}
