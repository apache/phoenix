package org.apache.phoenix.monitoring;

import org.HdrHistogram.Histogram;

import java.util.HashMap;
import java.util.Map;

public class UtilizationHistogram extends PercentileHistogram {

    public UtilizationHistogram(long maxUtil, String name) {
        super(maxUtil, name);
    }

    protected Map<String, Long> generateDistributionMap(Histogram snapshot) {
        Map<String, Long> metrics = new HashMap<>();
        metrics.put(NUM_OPS_METRIC_NAME, snapshot.getTotalCount());
        metrics.put(MIN_METRIC_NAME, snapshot.getMinValue());
        metrics.put(MAX_METRIC_NAME, snapshot.getMaxValue());
        metrics.put(TWENTY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.getValueAtPercentile(25));
        metrics.put(MEDIAN_METRIC_NAME, snapshot.getValueAtPercentile(50));
        metrics.put(SEVENTY_FIFTH_PERCENTILE_METRIC_NAME,
                snapshot.getValueAtPercentile(75));
        metrics.put(NINETIETH_PERCENTILE_METRIC_NAME, snapshot.getValueAtPercentile(90));
        metrics.put(NINETY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.getValueAtPercentile(95));
        return metrics;
    }
}
