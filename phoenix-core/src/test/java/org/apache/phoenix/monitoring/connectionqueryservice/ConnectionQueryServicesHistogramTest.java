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
import org.apache.phoenix.query.QueryServices;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ConnectionQueryServicesHistogramTest {

    @Test
    public void testConnectionQueryServiceHistogramRangeOverride() {
        String histoName = "PhoenixInternalOpenConn";
        Configuration conf = new Configuration();
        conf.set(QueryServices.CONNECTION_QUERY_SERVICE_HISTOGRAM_SIZE_RANGES, "2, 5, 8");
        ConnectionQueryServicesHistogram histogram = new ConnectionQueryServicesHistogram(histoName,
                "histogram for Number of open internal phoenix connections", conf);
        Assert.assertEquals(histoName, histogram.getName());
        long[] ranges = histogram.getRanges();
        Assert.assertNotNull(ranges);
        long[] expectRanges = {2,5,8};
        Assert.assertArrayEquals(expectRanges, ranges);
    }

    @Test
    public void testEveryRangeInDefaultRange() {
        //1, 3, 7, 9, 15, 30, 120, 600
        Configuration conf = new Configuration();
        String histoName = "PhoenixInternalOpenConn";
        conf.unset(QueryServices.CONNECTION_QUERY_SERVICE_HISTOGRAM_SIZE_RANGES);
        ConnectionQueryServicesHistogram histogram = new ConnectionQueryServicesHistogram(histoName,
                "histogram for Number of open internal phoenix connections", conf);
        Assert.assertEquals(histoName, histogram.getName());
        Assert.assertEquals(ConnectionQueryServicesHistogram.DEFAULT_RANGE, histogram.getRanges());

        histogram.add(1);
        histogram.add(3);
        histogram.add(7);
        histogram.add(9);
        histogram.add(15);
        histogram.add(30);
        histogram.add(120);
        histogram.add(600);

        Map<String, Long> distribution = histogram.getRangeHistogramDistribution().getRangeDistributionMap();
        Map<String, Long> expectedMap = new HashMap<>();
        expectedMap.put("0,1", 1l);
        expectedMap.put("1,10", 3l);
        expectedMap.put("10,100", 2l);
        expectedMap.put("100,500", 1l);
        expectedMap.put("500,1000", 1l);
        Assert.assertEquals(expectedMap, distribution);
    }
}
