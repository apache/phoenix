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

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a histogram with specified max possible value that can be recorded and exposes
 * percentile distribution of the recorded values along with the count of no. of recorded values.
 * Internally, uses {@link org.HdrHistogram.Histogram} for capturing the recorded values.
 * <br/><br/>
 * Supports capturing additional metadata about the values being recorded as key/value pairs a.k
 * .a tags.
 */
public abstract class PercentileHistogram {
    private static final Logger LOGGER = LoggerFactory.getLogger(PercentileHistogram.class);

    // Strings used to create metrics names.
    public static final String NUM_OPS_METRIC_NAME = "_num_ops";
    public static final String MIN_METRIC_NAME = "_min";
    public static final String MAX_METRIC_NAME = "_max";
    public static final String MEDIAN_METRIC_NAME = "_median";
    public static final String TWENTY_FIFTH_PERCENTILE_METRIC_NAME = "_25th_percentile";
    public static final String SEVENTY_FIFTH_PERCENTILE_METRIC_NAME = "_75th_percentile";
    public static final String NINETIETH_PERCENTILE_METRIC_NAME = "_90th_percentile";
    public static final String NINETY_FIFTH_PERCENTILE_METRIC_NAME = "_95th_percentile";

    private Histogram prevHistogram = null;
    private final Recorder recorder;
    private final String name;
    private final long maxUtil;
    private Map<String, String> tags = null;

    PercentileHistogram(long maxUtil, String name) {
        this.name = name;
        this.maxUtil = maxUtil;
        this.recorder = new Recorder(maxUtil, 2);
    }

    public void addValue(long value) {
        if (value > maxUtil) {
            // Ignoring recording value more than maximum trackable value.
            LOGGER.warn("Histogram recording higher value than maximum. Ignoring it.");
            return;
        }
        recorder.recordValue(value);
    }

    public HistogramDistribution getPercentileHistogramDistribution() {
        Histogram histogram = this.recorder.getIntervalHistogram(prevHistogram);
        HistogramDistribution distribution;
        if (tags == null) {
            distribution = new PercentileHistogramDistribution(name, histogram.getMinValue(),
                    histogram.getMaxValue(), histogram.getTotalCount(),
                    generateDistributionMap(histogram));
        }
        else {
            distribution = new PercentileHistogramDistribution(name, histogram.getMinValue(),
                    histogram.getMaxValue(), histogram.getTotalCount(),
                    generateDistributionMap(histogram), tags);
        }
        this.prevHistogram = histogram;
        return distribution;
    }

    public void addTag(String key, String value) {
        if (tags == null) {
            tags = new HashMap<>();
        }
        tags.put(key, value);
    }

    protected abstract Map<String, Long> generateDistributionMap(Histogram snapshot);
}
