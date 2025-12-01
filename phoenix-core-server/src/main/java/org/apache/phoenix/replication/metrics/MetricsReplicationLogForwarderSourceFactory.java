/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.metrics;

import java.util.concurrent.ConcurrentHashMap;


/**
 * Factory class for creating log forwarder metrics
 */
public class MetricsReplicationLogForwarderSourceFactory {

    private MetricsReplicationLogForwarderSourceFactory() {
        // Utility class, no instantiation
    }

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
