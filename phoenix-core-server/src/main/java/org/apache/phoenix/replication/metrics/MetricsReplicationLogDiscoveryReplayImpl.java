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

/** Implementation of metrics source for ReplicationReplayLogFileDiscovery operations. */
public class MetricsReplicationLogDiscoveryReplayImpl
    extends MetricsReplicationLogDiscoveryImpl {

    private static final String METRICS_NAME = "ReplicationLogDiscoveryReplay";
    private static final String METRICS_DESCRIPTION =
        "Metrics about Replication Replay Log Discovery for an HA Group";
    private static final String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    public MetricsReplicationLogDiscoveryReplayImpl(final String haGroupName) {
        super(MetricsReplicationLogDiscoveryReplayImpl.METRICS_NAME,
            MetricsReplicationLogDiscoveryReplayImpl.METRICS_DESCRIPTION,
            MetricsReplicationLogDiscoveryImpl.METRICS_CONTEXT,
            MetricsReplicationLogDiscoveryReplayImpl.METRICS_JMX_CONTEXT
                    + ",haGroup=" + haGroupName);
        super.groupMetricsContext =
            MetricsReplicationLogDiscoveryReplayImpl.METRICS_JMX_CONTEXT
                    + ",haGroup=" + haGroupName;
    }
}
