package org.apache.phoenix.monitoring;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class PercentileHistogram {
    private static final Logger LOGGER = LoggerFactory.getLogger(PercentileHistogram.class);

    // Strings used to create metrics names.
    String NUM_OPS_METRIC_NAME = "_num_ops";
    String MIN_METRIC_NAME = "_min";
    String MAX_METRIC_NAME = "_max";
    String MEDIAN_METRIC_NAME = "_median";
    String TWENTY_FIFTH_PERCENTILE_METRIC_NAME = "_25th_percentile";
    String SEVENTY_FIFTH_PERCENTILE_METRIC_NAME = "_75th_percentile";
    String NINETIETH_PERCENTILE_METRIC_NAME = "_90th_percentile";
    String NINETY_FIFTH_PERCENTILE_METRIC_NAME = "_95th_percentile";

    private Histogram histogram;
    final private String name;
    final private long maxUtil;
    private Map<String, String> tags = null;

    PercentileHistogram(long maxUtil, String name) {
        this.name = name;
        this.maxUtil = maxUtil;
        // Keeping same precision as {@link RangeHistogram}
        this.histogram = new ConcurrentHistogram(maxUtil, 2);
    }

    private void init() {
        this.histogram = new ConcurrentHistogram(maxUtil, 2);
    }

    public void addValue(long value) {
        if (value > histogram.getHighestTrackableValue()) {
            // Ignoring recording value more than maximum trackable value.
            LOGGER.warn("Histogram recording higher value than maximum. Ignoring it.");
            return;
        }
        histogram.recordValue(value);
    }

    private Histogram getSnapshotAndReset() {
        Histogram snapshot = histogram;
        init();
        return snapshot;
    }

    public HistogramDistribution getPercentileHistogramDistribution() {
        Histogram percentileHistogram = getSnapshotAndReset();
        HistogramDistribution distribution =
                new PercentileHistogramDistribution(name, histogram.getMinValue(),
                        histogram.getMaxValue(), histogram.getTotalCount(),
                        generateDistributionMap(percentileHistogram), ImmutableMap.copyOf(tags));
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
