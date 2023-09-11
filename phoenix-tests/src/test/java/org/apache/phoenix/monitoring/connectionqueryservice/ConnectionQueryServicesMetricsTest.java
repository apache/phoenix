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

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.monitoring.ConnectionQueryServicesMetric;
import org.apache.phoenix.monitoring.MetricType;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesNameMetricsTest.openInternalPhoenixConnCounter;
import static org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesNameMetricsTest.openPhoenixConnCounter;
import static org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesNameMetricsTest.connectionQueryServiceNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectionQueryServicesMetricsTest {
    static Map<String, ConnectionQueryServicesMetrics> phoenixConnectionQueryServiceSet =
            new HashMap<>();

    public boolean verifyConnectionQueryServiceName() {

        if (phoenixConnectionQueryServiceSet.isEmpty()) {
            return false;
        }
        for (String connectionQueryServiceName : connectionQueryServiceNames) {
            ConnectionQueryServicesMetrics instance =
                    phoenixConnectionQueryServiceSet.get(connectionQueryServiceName);
            if (!instance.getConnectionQueryServiceName().equals(connectionQueryServiceName)) {
                return false;
            }
        }
        return true;
    }

    public void verifyMetricsFromPhoenixConnectionQueryServiceMetrics() {
        assertFalse(phoenixConnectionQueryServiceSet.isEmpty());
        for (int i = 0; i < connectionQueryServiceNames.length; i++) {
            ConnectionQueryServicesMetrics instance =
                    phoenixConnectionQueryServiceSet.get(connectionQueryServiceNames[i]);
            assertEquals(instance.getConnectionQueryServiceName(), connectionQueryServiceNames[i]);
            List<ConnectionQueryServicesMetric> metricList = instance.getAllMetrics();
            for (ConnectionQueryServicesMetric metric : metricList) {

                if (metric.getMetricType()
                        .equals(MetricType.OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER)) {
                    assertEquals(openInternalPhoenixConnCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER)) {
                    assertEquals(openPhoenixConnCounter[i], metric.getValue());
                }
            }
        }
    }

    @Test
    public void testPhoenixConnectionQueryServiceMetricsForPhoenixConnectionQueryServiceName() {
        Configuration conf = new Configuration();
        for (int i = 0; i < connectionQueryServiceNames.length; i++) {
            ConnectionQueryServicesMetrics instance =
                    new ConnectionQueryServicesMetrics(connectionQueryServiceNames[i], conf);
            phoenixConnectionQueryServiceSet.put(connectionQueryServiceNames[i], instance);
        }
        assertTrue(verifyConnectionQueryServiceName());
    }

    /**
     * This test is for changeMetricValue() Method and getMetricMap()
     */
    @Test
    public void testPhoenixConnectionQueryServiceMetrics() {
        Configuration conf = new Configuration();
        for (int i = 0; i < connectionQueryServiceNames.length; i++) {
            ConnectionQueryServicesMetrics instance =
                    new ConnectionQueryServicesMetrics(connectionQueryServiceNames[i], conf);
            phoenixConnectionQueryServiceSet.put(connectionQueryServiceNames[i], instance);

            instance.setMetricValue(MetricType.OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER,
                    openInternalPhoenixConnCounter[i]);
            instance.setMetricValue(
                    MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER, openPhoenixConnCounter[i]);
        }
        verifyMetricsFromPhoenixConnectionQueryServiceMetrics();
        phoenixConnectionQueryServiceSet.clear();
    }
}
