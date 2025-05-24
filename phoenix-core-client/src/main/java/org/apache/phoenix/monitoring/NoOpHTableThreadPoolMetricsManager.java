package org.apache.phoenix.monitoring;

import java.util.function.Supplier;

public class NoOpHTableThreadPoolMetricsManager extends HTableThreadPoolMetricsManager {
    public static final NoOpHTableThreadPoolMetricsManager noOpHTableThreadPoolMetricManager =
            new NoOpHTableThreadPoolMetricsManager();

    private NoOpHTableThreadPoolMetricsManager() {
        super();
    }

    @Override
    public void updateActiveThreads(String threadPoolName, int activeThreads,
                                    Supplier<HTableThreadPoolHistograms> supplier) {
    }

    @Override
    public void updateQueueSize(String threadPoolName, int queueSize,
                                Supplier<HTableThreadPoolHistograms> supplier) {
    }
}
