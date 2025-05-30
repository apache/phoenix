package org.apache.phoenix.monitoring;

import java.util.Collections;
import java.util.Map;

/**
 * Exposes percentiles captured using {@link PercentileHistogram} to the consumers w/o exposing
 * underlying {@link org.HdrHistogram.Histogram} instances.
 */
public class PercentileHistogramDistribution extends HistogramDistributionImpl {
    private final Map<String, Long> percentileDistributionMap;
    private Map<String, String> tags = null;

    public PercentileHistogramDistribution(String histoName, long min, long max, long count,
                                           Map<String, Long> percentileDistributionMap) {
        super(histoName, min, max, count, Collections.emptyMap());
        this.percentileDistributionMap = percentileDistributionMap;
    }

    public PercentileHistogramDistribution(String histoName, long min, long max, long count,
                                           Map<String, Long> percentileDistributionMap,
                                           Map<String, String> tags) {
        this(histoName, min, max, count, percentileDistributionMap);
        this.tags = tags;
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
