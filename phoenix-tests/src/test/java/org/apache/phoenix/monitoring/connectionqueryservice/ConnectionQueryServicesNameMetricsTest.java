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

import org.apache.phoenix.monitoring.ConnectionQueryServicesMetric;
import org.apache.phoenix.monitoring.MetricType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.monitoring.MetricType.OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.PHOENIX_CONNECTIONS_THROTTLED_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class is used primarily to populate data and
 * verification methods
 */

public class ConnectionQueryServicesNameMetricsTest {

    public static final String[] connectionQueryServiceNames =
            { "USE_CASE_1", "USE_CASE_2", "USE_CASE_3" };
    public static Map<String, Map<MetricType, Long>>[] connectionQueryServiceNameMetricMap =
            new Map[connectionQueryServiceNames.length];
    public static final long[] openPhoenixConnCounter = { 1, 1, 1 };
    public static final long[] openInternalPhoenixConnCounter = { 1, 1, 1 };
    public static final long[] phoenixConnThrottledCounter = { 1, 2, 3 };


    public void populateMetrics() {
        for (int i = 0; i < connectionQueryServiceNameMetricMap.length; i++) {
            connectionQueryServiceNameMetricMap[i] = new HashMap<>();
        }
        for (int i = 0; i < connectionQueryServiceNames.length; i++) {
            Map<MetricType, Long> metrics = new HashMap<>();
            metrics.put(OPEN_PHOENIX_CONNECTIONS_COUNTER, openPhoenixConnCounter[i]);
            metrics.put(
                    OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER, openInternalPhoenixConnCounter[i]);
            metrics.put(PHOENIX_CONNECTIONS_THROTTLED_COUNTER, phoenixConnThrottledCounter[i]);

            connectionQueryServiceNameMetricMap[i].put(connectionQueryServiceNames[i], metrics);
        }
    }

    public void verfiyCountOfConnectionQueryServices(int noOfConnectionQueryServiceName) {
        Map<String, List<ConnectionQueryServicesMetric>> map =
                ConnectionQueryServicesMetricsManager.getAllConnectionQueryServicesMetrics();
        assertFalse(map == null || map.isEmpty());
        for (int i = 0; i < noOfConnectionQueryServiceName; i++) {
            assertTrue(map.containsKey(connectionQueryServiceNames[i]));
            List<ConnectionQueryServicesMetric> connectionQueryServiceNameMetric =
                    map.get(connectionQueryServiceNames[i]);
            for (ConnectionQueryServicesMetric metric : connectionQueryServiceNameMetric) {
                if (metric.getMetricType().equals(OPEN_PHOENIX_CONNECTIONS_COUNTER)) {
                    assertEquals(openPhoenixConnCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER)) {
                    assertEquals(openInternalPhoenixConnCounter[i], metric.getValue());
                }
                if (metric.getMetricType().equals(PHOENIX_CONNECTIONS_THROTTLED_COUNTER)) {
                    assertEquals(phoenixConnThrottledCounter[i], metric.getValue());
                }
            }
        }
    }

}
