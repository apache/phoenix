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

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.junit.Assert;
import org.junit.Test;

/**
 Test for {@link LatencyHistogram}
 **/
public class LatencyHistogramTest {

    @Test
    public void testLatencyHistogramRangeOverride() {
        String histoName = "PhoenixGetLatencyHisto";
        Configuration conf = new Configuration();
        conf.set(QueryServices.PHOENIX_HISTOGRAM_LATENCY_RANGES, "2, 5, 8");
        LatencyHistogram histogram = new LatencyHistogram(histoName,
                "histogram for GET operation latency", conf);
        Assert.assertEquals(histoName, histogram.getName());
        long[] ranges = histogram.getRanges();
        Assert.assertNotNull(ranges);
        Assert.assertEquals(3, ranges.length);
        Assert.assertEquals(2, ranges[0]);
        Assert.assertEquals(5, ranges[1]);
        Assert.assertEquals(8, ranges[2]);
    }

    @Test
    public void testEveryRangeInDefaultRange() {
        //1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 60000, 120000, 300000, 600000
        Configuration conf = new Configuration();
        String histoName = "PhoenixGetLatencyHisto";
        conf.unset(QueryServices.PHOENIX_HISTOGRAM_LATENCY_RANGES);
        LatencyHistogram histogram = new LatencyHistogram(histoName,
                "histogram for GET operation latency", conf);
        Assert.assertEquals(histoName, histogram.getName());
        Assert.assertEquals(LatencyHistogram.DEFAULT_RANGE, histogram.getRanges());

        histogram.add(1);
        histogram.add(2);
        histogram.add(3);
        histogram.add(5);
        histogram.add(20);
        histogram.add(60);
        histogram.add(200);
        histogram.add(600);
        histogram.add(2000);
        histogram.add(6000);
        histogram.add(20000);
        histogram.add(45000);
        histogram.add(90000);
        histogram.add(200000);
        histogram.add(450000);
        histogram.add(900000);

        Map<String, Long> distribution = histogram.getRangeHistogramDistribution().getRangeDistributionMap();
        Map<String, Long> expectedMap = new HashMap<>();
        expectedMap.put("0,1", 1l);
        expectedMap.put("1,3", 2l);
        expectedMap.put("3,10", 1l);
        expectedMap.put("10,30", 1l);
        expectedMap.put("30,100", 1l);
        expectedMap.put("100,300", 1l);
        expectedMap.put("300,1000", 1l);
        expectedMap.put("1000,3000", 1l);
        expectedMap.put("3000,10000", 1l);
        expectedMap.put("10000,30000", 1l);
        expectedMap.put("30000,60000", 1l);
        expectedMap.put("60000,120000", 1l);
        expectedMap.put("120000,300000", 1l);
        expectedMap.put("300000,600000", 1l);
        Assert.assertEquals(expectedMap, distribution);
    }

}