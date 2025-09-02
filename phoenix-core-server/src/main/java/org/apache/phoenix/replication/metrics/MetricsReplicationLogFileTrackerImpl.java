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

/** Implementation of metrics source for ReplicationLogFileTracker operations. */
public class MetricsReplicationLogFileTrackerImpl extends BaseSourceImpl
        implements MetricsReplicationLogFileTracker {

    protected String groupMetricsContext;
    protected final MutableFastCounter markFileInProgressRequestCount;
    protected final MutableFastCounter markFileCompletedRequestCount;
    protected final MutableFastCounter markFileFailedRequestCount;
    protected final MutableFastCounter markFileCompletedRequestFailedCount;
    protected final MutableHistogram markFileInProgressTime;
    protected final MutableHistogram markFileCompletedTime;
    protected final MutableHistogram markFileFailedTime;

    public MetricsReplicationLogFileTrackerImpl(String metricsName, String metricsDescription,
            String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
        markFileInProgressRequestCount = getMetricsRegistry()
            .newCounter(MARK_FILE_IN_PROGRESS_REQUEST_COUNT,
                MARK_FILE_IN_PROGRESS_REQUEST_COUNT_DESC, 0L);
        markFileCompletedRequestCount = getMetricsRegistry()
            .newCounter(MARK_FILE_COMPLETED_REQUEST_COUNT,
                MARK_FILE_COMPLETED_REQUEST_COUNT_DESC, 0L);
        markFileFailedRequestCount = getMetricsRegistry().newCounter(MARK_FILE_FAILED_REQUEST_COUNT,
            MARK_FILE_FAILED_REQUEST_COUNT_DESC, 0L);
        markFileCompletedRequestFailedCount = getMetricsRegistry()
            .newCounter(MARK_FILE_COMPLETED_REQUEST_FAILED_COUNT,
                MARK_FILE_COMPLETED_REQUEST_FAILED_COUNT_DESC, 0L);
        markFileInProgressTime = getMetricsRegistry().newHistogram(MARK_FILE_IN_PROGRESS_TIME,
            MARK_FILE_IN_PROGRESS_TIME_DESC);
        markFileCompletedTime = getMetricsRegistry().newHistogram(MARK_FILE_COMPLETED_TIME,
            MARK_FILE_COMPLETED_TIME_DESC);
        markFileFailedTime = getMetricsRegistry().newHistogram(MARK_FILE_FAILED_TIME,
            MARK_FILE_FAILED_TIME_DESC);
    }

    @Override
    public void incrementMarkFileInProgressRequestCount() {
        markFileInProgressRequestCount.incr();
    }

    @Override
    public void incrementMarkFileCompletedRequestCount() {
        markFileCompletedRequestCount.incr();
    }

    @Override
    public void incrementMarkFileFailedRequestCount() {
        markFileFailedRequestCount.incr();
    }

    @Override
    public void incrementMarkFileCompletedRequestFailedCount() {
        markFileCompletedRequestFailedCount.incr();
    }

    @Override
    public void updateMarkFileInProgressTime(long timeMs) {
        markFileInProgressTime.add(timeMs);
    }

    @Override
    public void updateMarkFileCompletedTime(long timeMs) {
        markFileCompletedTime.add(timeMs);
    }

    @Override
    public void updateMarkFileFailedTime(long timeMs) {
        markFileFailedTime.add(timeMs);
    }

    @Override
    public void close() {
        // Unregister this metrics source
        DefaultMetricsSystem.instance().unregisterSource(groupMetricsContext);
    }

    @Override
    public ReplicationLogFileTrackerMetricValues getCurrentMetricValues() {
        return new ReplicationLogFileTrackerMetricValues(
            markFileInProgressRequestCount.value(),
            markFileCompletedRequestCount.value(),
            markFileFailedRequestCount.value(),
            markFileCompletedRequestFailedCount.value(),
            markFileInProgressTime.getMax(),
            markFileCompletedTime.getMax(),
            markFileFailedTime.getMax()
        );
    }

    @Override
    public String getMetricsContext() {
        return groupMetricsContext;
    }
}
