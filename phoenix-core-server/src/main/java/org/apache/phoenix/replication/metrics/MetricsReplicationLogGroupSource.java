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

import org.apache.hadoop.hbase.metrics.BaseSource;

/** Interface for metrics related to ReplicationLogGroup operations. */
public interface MetricsReplicationLogGroupSource extends BaseSource {

  String METRICS_NAME = "ReplicationLogGroup";
  String METRICS_CONTEXT = "phoenix";
  String METRICS_DESCRIPTION = "Metrics about Replication Log Operations for an HA Group";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String ROTATION_COUNT = "rotationCount";
  String ROTATION_COUNT_DESC = "Total number of times rotateLog was called";

  String ROTATION_FAILURES = "rotationFailures";

  String ROTATION_FAILURES_DESC = "Number of times log rotation has failed";
  String APPEND_TIME = "appendTimeMs";
  String APPEND_TIME_DESC = "Histogram of time taken for append operations in nanoseconds";

  String SYNC_TIME = "syncTimeMs";
  String SYNC_TIME_DESC = "Histogram of time taken for sync operations in nanoseconds";

  String ROTATION_TIME = "rotationTimeMs";
  String ROTATION_TIME_DESC = "Histogram of time taken for log rotations in nanoseconds";

  String RING_BUFFER_TIME = "ringBufferTime";
  String RING_BUFFER_TIME_DESC = "Time events spend in the ring buffer";

  String SYNC_TO_SAF_TRANSITIONS = "syncToSafTransitions";
  String SYNC_TO_SAF_TRANSITIONS_DESC = "Number of SYNC to STORE_AND_FORWARD mode transitions";

  /**
   * Increments the counter for total log rotations. This counter tracks the total number of times
   * the log was rotated, regardless of reason.
   */
  void incrementRotationCount();

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

  /**
   * Increments the counter for log rotation failures. This counter tracks the number of times log
   * rotation has failed.
   */
  void incrementRotationFailureCount();

  /** Increment the SYNC to STORE_AND_FORWARD transition counter. */
  void incrementSyncToSafTransitions();

  /**
   * Unregister this metrics source.
   */
  void close();

  // Get current values for testing
  ReplicationLogMetricValues getCurrentMetricValues();
}
