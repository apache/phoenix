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
 * <b>External User-Facing API</b>
 * <p>
 * An immutable snapshot of percentile distribution data captured by a {@link PercentileHistogram}.
 * This class provides a consumer-friendly interface to access histogram statistics without exposing
 * the underlying {@link org.HdrHistogram.Histogram} instances. <br/>
 * <br/>
 * This class contains:
 * <ul>
 * <li>Percentile distribution map with percentile names as keys and their values</li>
 * <li>Basic statistics (minimum, maximum, total count)</li>
 * <li>Optional tags for additional metadata</li>
 * </ul>
 * <br/>
 * Use {@link PercentileHistogram#getPercentileHistogramDistribution()} to get the percentile
 * distribution captured by the histogram. Use {@link #getTags()} to access any tags attached to the
 * histogram. <br/>
 * <br/>
 * All data in this class is immutable after construction, making it safe for concurrent access.
 */
public class PercentileHistogramDistribution extends HistogramDistributionImpl {
    private final ImmutableMap<String, Long> percentileDistributionMap;
    private ImmutableMap<String, String> tags = null;

    PercentileHistogramDistribution(String histoName, long min, long max, long count,
                                           Map<String, Long> percentileDistributionMap) {
        super(histoName, min, max, count, Collections.emptyMap());
        this.percentileDistributionMap = ImmutableMap.copyOf(percentileDistributionMap);
    }

    PercentileHistogramDistribution(String histoName, long min, long max, long count,
                                           Map<String, Long> percentileDistributionMap,
                                           Map<String, String> tags) {
        this(histoName, min, max, count, percentileDistributionMap);
        this.tags = ImmutableMap.copyOf(tags);
    }

    /**
     * Returns an immutable map containing the percentile distribution and statistical data captured
     * by the histogram. The map contains metric names as keys and their corresponding values. <br/>
     * <br/>
     * The map includes:
     * <ul>
     * <li>Percentile values (e.g., "_90th_percentile", "_95th_percentile", "_median")</li>
     * <li>Statistical metrics (e.g., "_min", "_max", "_num_ops")</li>
     * </ul>
     * <br/>
     * This is the primary method for accessing percentile analysis results. The specific
     * percentiles and statistics included depend on the concrete {@link PercentileHistogram}
     * implementation that generated this distribution. <br/>
     * <br/>
     * The returned map is immutable and safe for concurrent access.
     * @return an immutable map of metric names to their calculated values
     */
    public ImmutableMap<String, Long> getPercentileDistributionMap() {
        return percentileDistributionMap;
    }

    public Map<String, Long> getRangeDistributionMap() {
        throw new UnsupportedOperationException("Range Histogram Distribution is not supported!!");
    }

    /**
     * Returns the metadata tags associated with this histogram distribution. Tags provide
     * additional context about the histogram data and are commonly used for dimensional monitoring,
     * allowing metrics to be filtered and grouped by tag names. <br/>
     * <br/>
     * Tags are attached to the histogram using {@link PercentileHistogram#addTag(String, String)}
     * before generating the distribution snapshot. <br/>
     * <br/>
     * The returned map is immutable and safe for concurrent access.
     * @return an immutable map of tag key-value pairs, or an empty map if no tags were attached
     */
    public ImmutableMap<String, String> getTags() {
        return tags == null ? ImmutableMap.of() : tags;
    }
}
