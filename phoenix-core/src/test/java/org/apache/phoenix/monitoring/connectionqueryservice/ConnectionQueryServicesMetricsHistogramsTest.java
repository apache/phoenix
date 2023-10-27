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
import org.junit.Assert;
import org.junit.Test;

public class ConnectionQueryServicesMetricsHistogramsTest {
    @Test
    public void testConnectionQueryServiceMetricsHistograms() {
        String connectionQueryServiceName = "USE_CASE_1";
        Configuration conf = new Configuration();
        ConnectionQueryServicesMetricsHistograms
                connectionQueryServiceMetricsHistograms = new ConnectionQueryServicesMetricsHistograms(connectionQueryServiceName, conf);
        Assert.assertEquals(connectionQueryServiceName, connectionQueryServiceMetricsHistograms.getConnectionQueryServicesName());
        Assert.assertNotNull(connectionQueryServiceMetricsHistograms.getConnectionQueryServicesOpenConnHisto());
        Assert.assertNotNull(connectionQueryServiceMetricsHistograms.getConnectionQueryServicesInternalOpenConnHisto());

        Assert.assertEquals(2, connectionQueryServiceMetricsHistograms.getConnectionQueryServicesHistogramsDistribution().size());
    }
}
