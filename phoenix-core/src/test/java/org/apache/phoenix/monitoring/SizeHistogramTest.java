/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.MutableSizeHistogram;
import org.apache.phoenix.query.QueryServices;
import org.junit.Assert;
import org.junit.Test;

/**
 Test for {@link SizeHistogram}
 **/
public class SizeHistogramTest {

    @Test
    public void testSizeHistogramRangeOverride() {
        Configuration conf = new Configuration();
        conf.set(QueryServices.PHOENIX_HISTOGRAM_SIZE_RANGES, "1, 100, 1000");
        SizeHistogram histogram = new SizeHistogram("PhoenixReadBytesHisto",
                "histogram for read bytes", conf);
        long[] ranges = histogram.getRanges();
        Assert.assertNotNull(ranges);
        Assert.assertEquals(3, ranges.length);
        Assert.assertEquals(1, ranges[0]);
        Assert.assertEquals(100, ranges[1]);
        Assert.assertEquals(1000, ranges[2]);
    }

    @Test
    public void testEveryRangeInDefaultRange() {
        // {10,100,1000,10000,100000,1000000,10000000,100000000};
        Configuration conf = new Configuration();
        String histoName = "PhoenixReadBytesHisto";
        conf.unset(QueryServices.PHOENIX_HISTOGRAM_SIZE_RANGES);
        SizeHistogram histogram = new SizeHistogram(histoName,
                "histogram for read bytes", conf);
        Assert.assertEquals(histoName, histogram.getName());
        Assert.assertEquals(SizeHistogram.DEFAULT_RANGE, histogram.getRanges());

        histogram.add(5);
        histogram.add(50);
        histogram.add(500);
        histogram.add(5000);
        histogram.add(50000);
        histogram.add(500000);
        histogram.add(5000000);
        histogram.add(50000000);
        Map<String, Long>
                distribution = histogram.getRangeHistogramDistribution().getRangeDistributionMap();
        Map<String, Long> expectedMap = new HashMap<>();
        expectedMap.put("0,10", 1l);
        expectedMap.put("10,100", 1l);
        expectedMap.put("100,1000", 1l);
        expectedMap.put("1000,10000", 1l);
        expectedMap.put("10000,100000", 1l);
        expectedMap.put("100000,1000000", 1l);
        expectedMap.put("1000000,10000000", 1l);
        expectedMap.put("10000000,100000000", 1l);
        Assert.assertEquals(expectedMap, distribution);
    }

}