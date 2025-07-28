package org.apache.phoenix.replication.metrics;

public class MetricsReplicationLogReplayFileTrackerImpl extends MetricsReplicationLogFileTrackerImpl {

    private static final String METRICS_NAME = "ReplicationLogReplayFileTracker";
    private static final String METRICS_DESCRIPTION = "Metrics about Replication Log Replay File Tracker for an HA Group";
    private static final String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    public MetricsReplicationLogReplayFileTrackerImpl(final String haGroupName) {
        super(MetricsReplicationLogReplayFileTrackerImpl.METRICS_NAME, MetricsReplicationLogReplayFileTrackerImpl.METRICS_DESCRIPTION, MetricsReplicationLogFileTracker.METRICS_CONTEXT,
                MetricsReplicationLogReplayFileTrackerImpl.METRICS_JMX_CONTEXT + ",haGroup=" + haGroupName);
        super.groupMetricsContext = MetricsReplicationLogReplayFileTrackerImpl.METRICS_JMX_CONTEXT + ",haGroup=" + haGroupName;
    }

}
