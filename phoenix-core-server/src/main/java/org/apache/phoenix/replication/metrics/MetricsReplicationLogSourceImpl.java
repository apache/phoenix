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
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/** Implementation of metrics source for ReplicationLog operations. */
public class MetricsReplicationLogSourceImpl extends BaseSourceImpl
        implements MetricsReplicationLogSource {

    private final MutableFastCounter timeBasedRotationCount;
    private final MutableFastCounter sizeBasedRotationCount;
    private final MutableFastCounter errorBasedRotationCount;
    private final MutableFastCounter totalRotationCount;
    private final MetricHistogram appendTime;
    private final MetricHistogram syncTime;
    private final MetricHistogram rotationTime;
    private final MetricHistogram ringBufferTime;

    public MetricsReplicationLogSourceImpl() {
        super(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
        timeBasedRotationCount = getMetricsRegistry().newCounter(TIME_BASED_ROTATION_COUNTER,
            TIME_BASED_ROTATION_COUNTER_DESC, 0L);
        sizeBasedRotationCount = getMetricsRegistry().newCounter(SIZE_BASED_ROTATION_COUNTER,
            SIZE_BASED_ROTATION_COUNTER_DESC, 0L);
        errorBasedRotationCount = getMetricsRegistry().newCounter(ERROR_BASED_ROTATION_COUNTER,
            ERROR_BASED_ROTATION_COUNTER_DESC, 0L);
        totalRotationCount = getMetricsRegistry().newCounter(TOTAL_ROTATION_COUNTER,
            TOTAL_ROTATION_COUNTER_DESC, 0L);
        appendTime = getMetricsRegistry().newHistogram(APPEND_TIME, APPEND_TIME_DESC);
        syncTime = getMetricsRegistry().newHistogram(SYNC_TIME, SYNC_TIME_DESC);
        rotationTime = getMetricsRegistry().newHistogram(ROTATION_TIME, ROTATION_TIME_DESC);
        ringBufferTime = getMetricsRegistry().newHistogram(RING_BUFFER_TIME,
            RING_BUFFER_TIME_DESC);
    }

    @Override
    public void incrementTimeBasedRotationCounter() {
        timeBasedRotationCount.incr();
    }

    @Override
    public void incrementSizeBasedRotationCounter() {
        sizeBasedRotationCount.incr();
    }

    @Override
    public void incrementErrorBasedRotationCounter() {
        errorBasedRotationCount.incr();
    }

    @Override
    public void incrementTotalRotationCounter() {
        totalRotationCount.incr();
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
    // Visible for testing
    public ReplicationLogMetricValues getCurrentMetricValues() {
        return new ReplicationLogMetricValues.Builder()
            .setTimeBasedRotationCount(timeBasedRotationCount.value())
            .setSizeBasedRotationCount(sizeBasedRotationCount.value())
            .setErrorBasedRotationCount(errorBasedRotationCount.value())
            .setTotalRotationCount(totalRotationCount.value())
            .setAppendTime(appendTime.getCount())
            .setSyncTime(syncTime.getCount())
            .setRotationTime(rotationTime.getCount())
            .setRingBufferTime(ringBufferTime.getCount())
            .build();
    }

}
