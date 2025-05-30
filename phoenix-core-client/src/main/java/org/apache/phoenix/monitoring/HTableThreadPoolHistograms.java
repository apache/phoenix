/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Creates a collection of histograms for capturing multiple stats related to HTable thread pool
 * utilization and contention.
 * <br/><br/>
 * Supports capturing additional metadata about the stats in the form of key/value pairs a.k.a. tags
 * .  By default supports two tags i.e. servers and connectionProfile. "servers" tag specified
 * the quorum string used in URL for establishing Phoenix connection. This can ZK quorum,
 * master quorum, etc., based on the HBase connection registry. "connectionProfile" identifies
 * the principal used in URL to create separate CQSI instances for different principals.
 * <br/>
 * Custom tags can also be specified as String key/value pairs using
 * {@link #addTag(String, String)}.
 * <br/><br/>
 * Internally this class uses {@link org.HdrHistogram.Histogram}. At the time of instantiation of
 * an instance of this class HdrHistograms get initialized for each of the stats being collected.
 * <br/><br/>
 * To view list of the stats being collected please refer {@link HistogramName}.
 */
public class HTableThreadPoolHistograms {
    public enum Tag {
        servers,
        connectionProfile,
    }

    public enum HistogramName {
        ActiveThreadsCount,
        QueueSize,
    }

    final private UtilizationHistogram activeThreadsHisto;
    final private UtilizationHistogram queuedSizeHisto;

    public HTableThreadPoolHistograms(long maxThreadPoolSize, long maxQueueSize) {
        activeThreadsHisto = new UtilizationHistogram(maxThreadPoolSize,
                HistogramName.ActiveThreadsCount.name());
        queuedSizeHisto = new UtilizationHistogram(maxQueueSize, HistogramName.QueueSize.name());
    }

    public void updateActiveThreads(long activeThreads) {
        activeThreadsHisto.addValue(activeThreads);
    }

    public void updateQueuedSize(long queuedSize) {
        queuedSizeHisto.addValue(queuedSize);
    }

    public void addServerTag(String value) {
        addTag(Tag.servers.name(), value);
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
