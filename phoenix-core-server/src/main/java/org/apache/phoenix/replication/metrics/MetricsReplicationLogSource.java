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

import org.apache.hadoop.hbase.metrics.BaseSource;

/** Interface for metrics related to ReplicationLog operations. */
public interface MetricsReplicationLogSource extends BaseSource {

    String METRICS_NAME = "ReplicationLog";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about Phoenix Replication Log Operations";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    String TIME_BASED_ROTATION_COUNTER = "timeBasedRotationCounter";
    String TIME_BASED_ROTATION_COUNTER_DESC = "Number of time-based log rotations";

    String SIZE_BASED_ROTATION_COUNTER = "sizeBasedRotationCounter";
    String SIZE_BASED_ROTATION_COUNTER_DESC = "Number of size-based log rotations";

    String ERROR_BASED_ROTATION_COUNTER = "errorBasedRotationCounter";
    String ERROR_BASED_ROTATION_COUNTER_DESC =
        "Number of times rotateLog was called due to errors";

    String TOTAL_ROTATION_COUNTER = "totalRotationCounter";
    String TOTAL_ROTATION_COUNTER_DESC = "Total number of times rotateLog was called";

    String APPEND_TIME = "appendTimeMs";
    String APPEND_TIME_DESC = "Histogram of time taken for append operations in nanoseconds";

    String SYNC_TIME = "syncTimeMs";
    String SYNC_TIME_DESC = "Histogram of time taken for sync operations in nanoseconds";

    String ROTATION_TIME = "rotationTimeMs";
    String ROTATION_TIME_DESC = "Histogram of time taken for log rotations in nanoseconds";

    String RING_BUFFER_TIME = "ringBufferTime";
    String RING_BUFFER_TIME_DESC = "Time events spend in the ring buffer";

    /**
     * Increments the counter for time-based log rotations.
     * This counter tracks the number of times the log was rotated due to time threshold.
     */
    void incrementTimeBasedRotationCounter();

    /**
     * Increments the counter for size-based log rotations.
     * This counter tracks the number of times the log was rotated due to size threshold.
     */
    void incrementSizeBasedRotationCounter();

    /**
     * Increments the counter for error-based log rotations.
     * This counter tracks the number of times the log was rotated due to errors.
     */
    void incrementErrorBasedRotationCounter();

    /**
     * Increments the counter for total log rotations.
     * This counter tracks the total number of times the log was rotated, regardless of reason.
     */
    void incrementTotalRotationCounter();

    /**
     * Update the time taken for an append operation in nanoseconds.
     * @param timeNs Time taken in nanoseconds
     */
    void updateAppendTime(long timeNs);

    /**
     * Update the time taken for a sync operation in nanoseconds.
     * @param timeNs Time taken in nanoseconds
     */
    void updateSyncTime(long timeNs);

    /**
     * Update the time taken for a rotation operation in nanoseconds.
     * @param timeNs Time taken in nanoseconds
     */
    void updateRotationTime(long timeNs);

    /**
     * Update the time an event spent in the ring buffer in nanoseconds.
     * @param timeNs Time spent in nanoseconds
     */
    void updateRingBufferTime(long timeNs);

    // Get current values for testing
    ReplicationLogMetricValues getCurrentMetricValues();

}
