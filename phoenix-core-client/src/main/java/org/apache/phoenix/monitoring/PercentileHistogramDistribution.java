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

import java.util.Collections;
import java.util.Map;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Exposes percentiles captured using {@link PercentileHistogram} to the consumers w/o exposing
 * underlying {@link org.HdrHistogram.Histogram} instances.
 * <br/><br/>
 * Call {@link #getPercentileDistributionMap()} to get percentile distribution from the histogram
 * . Call {@link #getTags()} to get the tags attached to the histogram.
 */
public class PercentileHistogramDistribution extends HistogramDistributionImpl {
    private final Map<String, Long> percentileDistributionMap;
    private Map<String, String> tags = null;

    public PercentileHistogramDistribution(String histoName, long min, long max, long count,
                                           Map<String, Long> percentileDistributionMap) {
        super(histoName, min, max, count, Collections.emptyMap());
        this.percentileDistributionMap = ImmutableMap.copyOf(percentileDistributionMap);
    }

    public PercentileHistogramDistribution(String histoName, long min, long max, long count,
                                           Map<String, Long> percentileDistributionMap,
                                           Map<String, String> tags) {
        this(histoName, min, max, count, percentileDistributionMap);
        this.tags = ImmutableMap.copyOf(tags);
    }

    public Map<String, Long> getPercentileDistributionMap() {
        return percentileDistributionMap;
    }

    public Map<String, Long> getRangeDistributionMap() {
        throw new UnsupportedOperationException("Range Histogram Distribution is not supported!!");
    }

    public Map<String, String> getTags() {
        return tags == null ? Collections.emptyMap() : tags;
    }
}
