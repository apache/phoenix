package org.apache.phoenix.monitoring;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThreadPoolHistograms {
    public enum Tag {
        server,
        connectionProfile,
    }

    final private Map<String, String> tags;
    final private UtilizationHistogram activeThreadsHisto;
    final private UtilizationHistogram queuedSizeHisto;

    public ThreadPoolHistograms(long maxThreadPoolSize, long maxQueueSize) {
        activeThreadsHisto = new UtilizationHistogram(maxThreadPoolSize, "ActiveThreadsCount");
        queuedSizeHisto = new UtilizationHistogram(maxQueueSize, "QueueSize");
        tags = new HashMap<>();
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
