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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;


import java.util.HashMap;
import java.util.Map;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
    Creates a histogram with the specified range.
 */
public class RangeHistogram {
    private Histogram histogram;
    private long[] ranges;
    private String name;
    private String desc;
    private static final Logger LOGGER = LoggerFactory.getLogger(RangeHistogram.class);

    public RangeHistogram(long[] ranges, String name, String description) {
        Preconditions.checkNotNull(ranges);
        Preconditions.checkArgument(ranges.length != 0);
        this.ranges = ranges; // the ranges are static or either provided by user
        this.name = name;
        this.desc = description;
        /*
            Below is the memory footprint per precision as of hdrhistogram version 2.1.12
            Histogram#getEstimatedFootprintInBytes provide a (conservatively high) estimate
            of the Histogram's total footprint in bytes.
            |-----------------------------------------|
            |PRECISION |  ERROR RATE |  SIZE IN BYTES |
            |    1     |     10%     |       3,584    |
            |    2     |     1%      |      22,016    |
            |    3     |     0.1%    |     147,968    |
            |    4     |     0.01%   |   1,835,520    |
            |    5     |     0.001%  |  11,534,848    |
            |-----------------------------------------|
         */
        // highestTrackable value is the last value in the provided range.
        this.histogram = new ConcurrentHistogram(this.ranges[this.ranges.length-1], 2);
    }

    public void add(long value) {
        if (value > histogram.getHighestTrackableValue()) {
            // Ignoring recording value more than maximum trackable value.
            LOGGER.warn("Histogram recording higher value than maximum. Ignoring it.");
            return;
        }
        histogram.recordValue(value);
    }

    public Histogram getHistogram() {
        return histogram;
    }

    public long[] getRanges() {
        return ranges;
    }

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }

    public HistogramDistribution getRangeHistogramDistribution() {
        // Generate distribution from the snapshot.
        Histogram snapshot = histogram.copy();
        HistogramDistributionImpl
                distribution =
                new HistogramDistributionImpl(name, snapshot.getMinValue(), snapshot.getMaxValue(),
                        snapshot.getTotalCount(), generateDistributionMap(snapshot));
        return distribution;
    }

    private Map<String, Long> generateDistributionMap(Histogram snapshot) {
        long priorRange = 0;
        Map<String, Long> map = new HashMap<>();
        for (int i = 0; i < ranges.length; i++) {
            // We get the next non equivalent range to avoid double counting.
            // getCountBetweenValues is inclusive of both values but since we are getting
            // next non equivalent value from the lower bound it will be more than priorRange.
            long nextNonEquivalentRange = histogram.nextNonEquivalentValue(priorRange);
            // lower exclusive upper inclusive
            long val = snapshot.getCountBetweenValues(nextNonEquivalentRange, ranges[i]);
            map.put(priorRange + "," + ranges[i], val);
            priorRange = ranges[i];
        }
        return map;
    }

}