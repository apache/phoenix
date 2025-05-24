package org.apache.phoenix.monitoring;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HTableThreadPoolHistograms {
    public enum Tag {
        server,
        connectionProfile,
    }

    final private UtilizationHistogram activeThreadsHisto;
    final private UtilizationHistogram queuedSizeHisto;

    public HTableThreadPoolHistograms(long maxThreadPoolSize, long maxQueueSize) {
        activeThreadsHisto = new UtilizationHistogram(maxThreadPoolSize, "ActiveThreadsCount");
        queuedSizeHisto = new UtilizationHistogram(maxQueueSize, "QueueSize");
    }

    public void updateActiveThreads(long activeThreads) {
        activeThreadsHisto.addValue(activeThreads);
    }

    public void updateQueuedSize(long queuedSize) {
        queuedSizeHisto.addValue(queuedSize);
    }

    public void addServerTag(String value) {
        addTag(Tag.server.name(), value);
    }

    public void addConnectionProfileTag(String value) {
        addTag(Tag.connectionProfile.name(), value);
    }

    public void addTag(String key, String value) {
        activeThreadsHisto.addTag(key, value);
        queuedSizeHisto.addTag(key, value);
    }

    public List<HistogramDistribution> getThreadPoolHistogramsDistribution() {
        return ImmutableList.of(activeThreadsHisto.getPercentileHistogramDistribution(),
                queuedSizeHisto.getPercentileHistogramDistribution());
    }
}
