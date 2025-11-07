package org.apache.phoenix.replication.metrics;

import java.util.concurrent.ConcurrentHashMap;



/**
 * Factory class for creating log forwarder metrics
 */
public class MetricsReplicationLogForwarderSourceFactory {
    /** Cache of ReplicationLogTrackerForwarderImpl instances by HA Group ID */
    private static final ConcurrentHashMap<String, MetricsReplicationLogTrackerForwarderImpl> TRACKER_INSTANCES =
            new ConcurrentHashMap<>();

    /** Cache of ReplicationLogDiscoveryForwarderImpl instances by HA Group ID */
    private static final ConcurrentHashMap<String, MetricsReplicationLogDiscoveryForwarderImpl> DISCOVERY_INSTANCES =
            new ConcurrentHashMap<>();

    public static MetricsReplicationLogTrackerForwarderImpl getInstanceForTracker(String haGroupName) {
        return TRACKER_INSTANCES.computeIfAbsent(haGroupName, k ->
                new MetricsReplicationLogTrackerForwarderImpl(haGroupName));
    }

    public static MetricsReplicationLogDiscoveryForwarderImpl getInstanceForDiscovery(String haGroupName) {
        return DISCOVERY_INSTANCES.computeIfAbsent(haGroupName, k ->
                new MetricsReplicationLogDiscoveryForwarderImpl(haGroupName));
    }
}
