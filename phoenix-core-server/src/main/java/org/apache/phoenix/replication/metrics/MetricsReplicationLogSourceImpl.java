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
package org.apache.phoenix.replication.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;

/** Implementation of metrics source for ReplicationLog operations. */
public class MetricsReplicationLogSourceImpl extends BaseSourceImpl
        implements MetricsReplicationLogSource {

    private final MutableFastCounter timeBasedRotationCount;
    private final MutableFastCounter sizeBasedRotationCount;
    private final MutableFastCounter errorBasedRotationCount;
    private final MutableFastCounter rotationCount;
    private final MutableFastCounter rotationFailuresCount;
    private final MutableHistogram appendTime;
    private final MutableHistogram syncTime;
    private final MutableHistogram rotationTime;
    private final MutableHistogram ringBufferTime;

    public MetricsReplicationLogSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public MetricsReplicationLogSourceImpl(String metricsName, String metricsDescription,
        String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
        timeBasedRotationCount = getMetricsRegistry().newCounter(TIME_BASED_ROTATION_COUNT,
            TIME_BASED_ROTATION_COUNT_DESC, 0L);
        sizeBasedRotationCount = getMetricsRegistry().newCounter(SIZE_BASED_ROTATION_COUNT,
            SIZE_BASED_ROTATION_COUNT_DESC, 0L);
        errorBasedRotationCount = getMetricsRegistry().newCounter(ERROR_BASED_ROTATION_COUNT,
            ERROR_BASED_ROTATION_COUNT_DESC, 0L);
        rotationCount = getMetricsRegistry().newCounter(ROTATION_COUNT, ROTATION_COUNT_DESC, 0L);
        rotationFailuresCount = getMetricsRegistry().newCounter(ROTATION_FAILURES,
            ROTATION_FAILURES_DESC, 0L);
        appendTime = getMetricsRegistry().newHistogram(APPEND_TIME, APPEND_TIME_DESC);
        syncTime = getMetricsRegistry().newHistogram(SYNC_TIME, SYNC_TIME_DESC);
        rotationTime = getMetricsRegistry().newHistogram(ROTATION_TIME, ROTATION_TIME_DESC);
        ringBufferTime = getMetricsRegistry().newHistogram(RING_BUFFER_TIME,
            RING_BUFFER_TIME_DESC);
    }

    @Override
    public void incrementTimeBasedRotationCount() {
        timeBasedRotationCount.incr();
    }

    @Override
    public void incrementSizeBasedRotationCount() {
        sizeBasedRotationCount.incr();
    }

    @Override
    public void incrementErrorBasedRotationCount() {
        errorBasedRotationCount.incr();
    }

    @Override
    public void incrementRotationCount() {
        rotationCount.incr();
    }

    @Override
    public void incrementRotationFailureCount() {
        rotationFailuresCount.incr();
    }

    @Override
    public void updateAppendTime(long timeNs) {
        appendTime.add(timeNs);
    }

    @Override
    public void updateSyncTime(long timeNs) {
        syncTime.add(timeNs);
    }

    @Override
    public void updateRotationTime(long timeNs) {
        rotationTime.add(timeNs);
    }

    @Override
    public void updateRingBufferTime(long timeNs) {
        ringBufferTime.add(timeNs);
    }

    @Override
    public ReplicationLogMetricValues getCurrentMetricValues() {
        return new ReplicationLogMetricValues(
            timeBasedRotationCount.value(),
            sizeBasedRotationCount.value(),
            errorBasedRotationCount.value(),
            rotationCount.value(),
            rotationFailuresCount.value(),
            appendTime.getMax(),
            syncTime.getMax(),
            rotationTime.getMax(),
            ringBufferTime.getMax()
        );
    }

    @Override
    public String getMetricsName() {
        return METRICS_NAME;
    }

    @Override
    public String getMetricsDescription() {
        return METRICS_DESCRIPTION;
    }

    @Override
    public String getMetricsContext() {
        return METRICS_CONTEXT;
    }

    @Override
    public String getMetricsJmxContext() {
        return METRICS_JMX_CONTEXT;
    }

}
