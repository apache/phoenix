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

  String ROTATION_COUNT = "phoenixWALRotationCount";
  String ROTATION_COUNT_DESC = "Total number of times rotateLog was called";

  String ROTATION_FAILURES = "phoenixWALRotationFailures";
  String ROTATION_FAILURES_DESC = "Number of times log rotation has failed";

  // Time histograms encode the unit in the name suffix (Ms or Ns) so consumers cannot misinterpret.
  String APPEND_TIME = "phoenixWALAppendTimeNs";
  String APPEND_TIME_DESC = "Histogram of time taken for append operations in nanoseconds";

  String SYNC_TIME = "phoenixWALSyncTimeMs";
  String SYNC_TIME_DESC = "Histogram of time taken for sync operations in milliseconds";

  String ROTATION_TIME = "phoenixWALRotationTimeMs";
  String ROTATION_TIME_DESC = "Histogram of time taken for log rotations in milliseconds";

  String RING_BUFFER_TIME = "phoenixWALSyncRingBufferTimeNs";
  String RING_BUFFER_TIME_DESC =
    "Time SYNC events spend in the ring buffer (queue + drain ahead) in nanoseconds";

  String FS_SYNC_TIME = "phoenixWALFsSyncTimeMs";
  String FS_SYNC_TIME_DESC =
    "Histogram of time taken for the underlying filesystem sync (fsync) in milliseconds";

  String BATCH_SIZE = "phoenixWALBatchSize";
  String BATCH_SIZE_DESC = "Histogram of number of events drained per Disruptor batch";

  String PENDING_SYNC_COUNT = "phoenixWALPendingSyncCount";
  String PENDING_SYNC_COUNT_DESC = "Histogram of pending sync futures coalesced into one fsync";

  String PENDING_SYNC_WAIT_TIME = "phoenixWALPendingSyncWaitTimeNs";
  String PENDING_SYNC_WAIT_TIME_DESC =
    "Time a SYNC event waits between consumer pickup and fsync start, in nanoseconds";

  String SYNC_TO_SAF_TRANSITIONS = "SyncToSafTransitions";
  String SYNC_TO_SAF_TRANSITIONS_DESC = "Number of SYNC to STORE_AND_FORWARD mode transitions";

  /**
   * Increments the counter for total log rotations. This counter tracks the total number of times
   * the log was rotated, regardless of reason.
   */
  void incrementRotationCount();

  /**
   * Update the time taken for an append operation. Recorded into histogram in nanoseconds.
   * @param timeNs Time taken in nanoseconds
   */
  void updateAppendTime(long timeNs);

  /**
   * Update the time taken for a sync operation. Recorded into histogram in milliseconds.
   * @param timeNs Time taken in nanoseconds
   */
  void updateSyncTime(long timeNs);

  /**
   * Update the time taken for a rotation operation. Recorded into histogram in milliseconds.
   * @param timeNs Time taken in nanoseconds
   */
  void updateRotationTime(long timeNs);

  /**
   * Update the time a SYNC event spent in the ring buffer (queue + drain ahead). Recorded into
   * histogram in nanoseconds.
   * @param timeNs Time spent in nanoseconds
   */
  void updateRingBufferTime(long timeNs);

  /**
   * Update the time taken for the underlying filesystem sync (fsync). Recorded into histogram in
   * milliseconds.
   * @param timeNs Time taken in nanoseconds
   */
  void updateFsSyncTime(long timeNs);

  /**
   * Update the number of events drained in a single Disruptor batch.
   * @param size Number of events in the batch
   */
  void updateBatchSize(long size);

  /**
   * Update the number of pending sync futures coalesced into one fsync.
   * @param count Number of sync futures
   */
  void updatePendingSyncCount(long count);

  /**
   * Update the time a SYNC event waited between consumer pickup and fsync start.
   * @param timeNs Time in nanoseconds
   */
  void updatePendingSyncWaitTime(long timeNs);

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
