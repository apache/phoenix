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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;

/** Implementation of metrics source for ReplicationLogDiscovery operations. */
public class MetricsReplicationLogDiscoveryImpl extends BaseSourceImpl
        implements MetricsReplicationLogDiscovery {

    protected String groupMetricsContext;
    protected final MutableFastCounter numRoundsProcessed;
    protected final MutableFastCounter numInProgressDirectoryProcessed;
    protected final MutableHistogram timeToProcessNewFiles;
    protected final MutableHistogram timeToProcessInProgressFiles;

    public MetricsReplicationLogDiscoveryImpl(String metricsName, String metricsDescription,
            String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
        numRoundsProcessed = getMetricsRegistry().newCounter(NUM_ROUNDS_PROCESSED,
            NUM_ROUNDS_PROCESSED_DESC, 0L);
        numInProgressDirectoryProcessed = getMetricsRegistry()
            .newCounter(NUM_IN_PROGRESS_DIRECTORY_PROCESSED,
                NUM_IN_PROGRESS_DIRECTORY_PROCESSED_DESC, 0L);
        timeToProcessNewFiles = getMetricsRegistry().newHistogram(TIME_TO_PROCESS_NEW_FILES,
            TIME_TO_PROCESS_NEW_FILES_DESC);
        timeToProcessInProgressFiles = getMetricsRegistry()
            .newHistogram(TIME_TO_PROCESS_IN_PROGRESS_FILES,
                TIME_TO_PROCESS_IN_PROGRESS_FILES_DESC);
    }

    @Override
    public void incrementNumRoundsProcessed() {
        numRoundsProcessed.incr();
    }

    @Override
    public void incrementNumInProgressDirectoryProcessed() {
        numInProgressDirectoryProcessed.incr();
    }

    @Override
    public void updateTimeToProcessNewFiles(long timeMs) {
        timeToProcessNewFiles.add(timeMs);
    }

    @Override
    public void updateTimeToProcessInProgressFiles(long timeMs) {
        timeToProcessInProgressFiles.add(timeMs);
    }

    @Override
    public void close() {
        // Unregister this metrics source
        DefaultMetricsSystem.instance().unregisterSource(groupMetricsContext);
    }

    @Override
    public ReplicationLogDiscoveryMetricValues getCurrentMetricValues() {
        return new ReplicationLogDiscoveryMetricValues(
            numRoundsProcessed.value(),
            numInProgressDirectoryProcessed.value(),
            timeToProcessNewFiles.getMax(),
            timeToProcessInProgressFiles.getMax()
        );
    }

    @Override
    public String getMetricsContext() {
        return groupMetricsContext;
    }
}
