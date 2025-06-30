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

/** Implementation of metrics source for ReplicationLogProcessor operations. */
public class MetricsReplicationLogProcessorImpl extends BaseSourceImpl 
        implements MetricsReplicationLogProcessor {

    private String groupMetricsContext;
    private final MutableFastCounter failedMutationsCount;
    private final MutableFastCounter failedBatchCount;
    private final MutableFastCounter logFileReplayFailureCount;
    private final MutableFastCounter logFileReplaySuccessCount;
    private final MutableHistogram batchReplayTime;
    private final MutableHistogram logFileReplayTime;

    public MetricsReplicationLogProcessorImpl(final String haGroupId) {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, 
            METRICS_JMX_CONTEXT + ",haGroup=" + haGroupId);
        groupMetricsContext = METRICS_JMX_CONTEXT + ",haGroup=" + haGroupId;
    }

    public MetricsReplicationLogProcessorImpl(String metricsName, String metricsDescription, 
            String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
        failedMutationsCount = getMetricsRegistry().newCounter(FAILED_MUTATIONS_COUNT,
            FAILED_MUTATIONS_COUNT_DESC, 0L);
        failedBatchCount = getMetricsRegistry().newCounter(FAILED_BATCH_COUNT,
            FAILED_BATCH_COUNT_DESC, 0L);
        logFileReplayFailureCount = getMetricsRegistry().newCounter(LOG_FILE_REPLAY_FAILURE_COUNT,
            LOG_FILE_REPLAY_FAILURE_COUNT_DESC, 0L);
        logFileReplaySuccessCount = getMetricsRegistry().newCounter(LOG_FILE_REPLAY_SUCCESS_COUNT,
            LOG_FILE_REPLAY_SUCCESS_COUNT_DESC, 0L);
        batchReplayTime = getMetricsRegistry().newHistogram(BATCH_REPLAY_TIME, 
            BATCH_REPLAY_TIME_DESC);
        logFileReplayTime = getMetricsRegistry().newHistogram(LOG_FILE_REPLAY_TIME, 
            LOG_FILE_REPLAY_TIME_DESC);
    }

    @Override
    public void incrementFailedMutationsCount(long delta) {
        failedMutationsCount.incr(delta);
    }

    @Override
    public void incrementFailedBatchCount() {
        failedBatchCount.incr();
    }

    @Override
    public void incrementLogFileReplayFailureCount() {
        logFileReplayFailureCount.incr();
    }

    @Override
    public void incrementLogFileReplaySuccessCount() {
        logFileReplaySuccessCount.incr();
    }

    @Override
    public void updateBatchReplayTime(long timeMs) {
        batchReplayTime.add(timeMs);
    }

    @Override
    public void updateLogFileReplayTime(long timeMs) {
        logFileReplayTime.add(timeMs);
    }

    @Override
    public void close() {
        // Unregister this metrics source
        DefaultMetricsSystem.instance().unregisterSource(groupMetricsContext);
    }

    @Override
    public ReplicationLogProcessorMetricValues getCurrentMetricValues() {
        return new ReplicationLogProcessorMetricValues(
            failedMutationsCount.value(),
            logFileReplayFailureCount.value(),
            logFileReplaySuccessCount.value(),
            logFileReplayTime.getMax(),
            batchReplayTime.getMax()
        );
    }

    @Override
    public String getMetricsContext() {
        return groupMetricsContext;
    }
}
