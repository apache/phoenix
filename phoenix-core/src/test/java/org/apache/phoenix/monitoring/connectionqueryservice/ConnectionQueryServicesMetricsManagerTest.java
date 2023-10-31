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
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.apache.phoenix.monitoring.MetricType.OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.PHOENIX_CONNECTIONS_THROTTLED_COUNTER;
import static org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesNameMetricsTest.connectionQueryServiceNames;
import static org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesNameMetricsTest.openInternalPhoenixConnCounter;
import static org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesNameMetricsTest.openPhoenixConnCounter;
import static org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesNameMetricsTest.phoenixConnThrottledCounter;
import static org.junit.Assert.assertTrue;

public class ConnectionQueryServicesMetricsManagerTest {
    public boolean verifyMetricsReset(){
        Map<String, List<ConnectionQueryServicesMetric>> map =
                ConnectionQueryServicesMetricsManager.getAllConnectionQueryServicesMetrics();
        return map != null && map.isEmpty();
    }

    public boolean verifyConnectionQueryServiceNamesExists(String connectionQueryServiceName){
        Map<String,List<ConnectionQueryServicesMetric>>map =
                ConnectionQueryServicesMetricsManager.getAllConnectionQueryServicesMetrics();
        return map != null && map.containsKey(connectionQueryServiceName);
    }

    @Test
    public void testConnectionQueryServiceMetricsForUpdateMetricsMethod() {

        QueryServicesOptions options = QueryServicesOptions.withDefaults();
        options.setConnectionQueryServiceMetricsEnabled();
        ConnectionQueryServicesMetricsManager connectionQueryServicesMetricsManager =
                new ConnectionQueryServicesMetricsManager(options);
        ConnectionQueryServicesMetricsManager.setInstance(connectionQueryServicesMetricsManager);

        ConnectionQueryServicesNameMetricsTest
                testData = new ConnectionQueryServicesNameMetricsTest();
        testData.populateMetrics();
        for(int i = 0; i < connectionQueryServiceNames.length; i++) {
            ConnectionQueryServicesMetricsManager.updateMetrics(connectionQueryServiceNames[i],
                    OPEN_PHOENIX_CONNECTIONS_COUNTER, openPhoenixConnCounter[i]);
            ConnectionQueryServicesMetricsManager.updateMetrics(connectionQueryServiceNames[i],
                    OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER, openInternalPhoenixConnCounter[i]);
            ConnectionQueryServicesMetricsManager.updateMetrics(connectionQueryServiceNames[i],
                    PHOENIX_CONNECTIONS_THROTTLED_COUNTER, phoenixConnThrottledCounter[i]);
        }
        testData.verfiyCountOfConnectionQueryServices(connectionQueryServiceNames.length);
        ConnectionQueryServicesMetricsManager.clearAllConnectionQueryServiceMetrics();
        assertTrue(verifyMetricsReset());
    }

    @Test
    public void testHistogramMetricsForOpenPhoenixConnectionCounter() {
        String connectionQueryServiceName = "USE_CASE_1";
        Configuration conf = new Configuration();
        conf.set(QueryServices.CONNECTION_QUERY_SERVICE_HISTOGRAM_SIZE_RANGES, "3, 6, 9");

        QueryServicesOptions mockOptions = Mockito.mock(QueryServicesOptions.class);
        Mockito.doReturn(true).when(mockOptions)
                .isConnectionQueryServiceMetricsEnabled();
        Mockito.doReturn(conf).when(mockOptions).getConfiguration();
        ConnectionQueryServicesMetricsManager connectionQueryServicesMetricsManager =
                new ConnectionQueryServicesMetricsManager(mockOptions);
        ConnectionQueryServicesMetricsManager.setInstance(connectionQueryServicesMetricsManager);
        for (int i=0; i<9; i++) {
            updateMetricsAndHistogram(i+1, connectionQueryServiceName);
        }


        // Generate distribution map from histogram snapshots.
        ConnectionQueryServicesHistogram connectionQueryServicesHistogram =
                ConnectionQueryServicesMetricsManager.getConnectionQueryServiceOpenConnectionHistogram(connectionQueryServiceName);

        Map<String, Long> openPhoenixConnMap = connectionQueryServicesHistogram.getRangeHistogramDistribution().getRangeDistributionMap();
        for (Long count: openPhoenixConnMap.values()) {
            Assert.assertEquals(new Long(3), count);
        }
    }

    private void updateMetricsAndHistogram (long counter, String connectionQueryServiceName) {
        ConnectionQueryServicesMetricsManager.updateMetrics(connectionQueryServiceName,
                OPEN_PHOENIX_CONNECTIONS_COUNTER, counter);
        ConnectionQueryServicesMetricsManager.updateConnectionQueryServiceOpenConnectionHistogram(counter,
                connectionQueryServiceName);
    }
}
