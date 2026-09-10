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

/** Interface for metrics related to ReplicationLogProcessor operations. */
public interface MetricsReplicationLogProcessor extends BaseSource {

  String METRICS_NAME = "ReplicationLogProcessor";
  String METRICS_CONTEXT = "phoenix";
  String METRICS_DESCRIPTION = "Metrics about Replication Log Processor for an HA Group";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String FAILED_MUTATIONS_COUNT = "failedMutationsCount";
  String FAILED_MUTATIONS_COUNT_DESC = "Number of failed mutations";
  String FAILED_BATCH_COUNT = "failedBatchCount";
  String FAILED_BATCH_COUNT_DESC = "Number of failed batches";
  String LOG_FILE_REPLAY_FAILURE_COUNT = "logFileReplayFailureCount";
  String LOG_FILE_REPLAY_FAILURE_COUNT_DESC = "Number of files failed to replay";
  String LOG_FILE_REPLAY_SUCCESS_COUNT = "logFileReplaySuccessCount";
  String LOG_FILE_REPLAY_SUCCESS_COUNT_DESC = "Number of files successfully to replayed";
  String BATCH_REPLAY_TIME = "batchReplayTimeMs";
  String BATCH_REPLAY_TIME_DESC =
    "Histogram of time taken for replaying a batch of log file in milliseconds";
  String LOG_FILE_REPLAY_TIME = "logFileReplayTimeMs";
  String LOG_FILE_REPLAY_TIME_DESC =
    "Histogram of time taken for replaying a log file in milliseconds";
  String SUCCESSFUL_FILE_MUTATIONS_REPLAYED_COUNT = "successfulFileMutationsReplayedCount";
  String SUCCESSFUL_FILE_MUTATIONS_REPLAYED_COUNT_DESC =
    "Total number of mutations replayed across log files that completed successfully; mutations "
      + "already applied by a file that then fails mid-replay are not counted";
  String MUTATIONS_PER_FILE = "mutationsPerFile";
  String MUTATIONS_PER_FILE_DESC =
    "Histogram of the number of mutations replayed per log file (files with mutations only)";

  /**
   * Increments the counter for failed mutations. This counter tracks the number of mutations that
   * failed during processing.
   */
  void incrementFailedMutationsCount(long delta);

  /**
   * Increments the counter for log file replay failures. This counter tracks the number of log
   * files that failed to replay.
   */
  void incrementFailedBatchCount();

  /**
   * Increments the counter for log file replay failures. This counter tracks the number of log
   * files that failed to replay.
   */
  void incrementLogFileReplayFailureCount();

  /**
   * Increments the counter for log file replay successes. This counter tracks the number of log
   * files that were successfully replayed.
   */
  void incrementLogFileReplaySuccessCount();

  /**
   * Update the time taken for replaying a batch of mutations in milliseconds.
   * @param timeMs Time taken in milliseconds
   */
  void updateBatchReplayTime(long timeMs);

  /**
   * Update the time taken for replaying a log file in milliseconds.
   * @param timeMs Time taken in milliseconds
   */
  void updateLogFileReplayTime(long timeMs);

  /**
   * Increments the count of mutations replayed by files that completed successfully. Called once
   * per file after its replay returns without error, so mutations already applied by a file that
   * then fails mid-replay are not counted. Tracks replay throughput across fully-successful files.
   * @param delta the number of mutations replayed for a successfully completed log file
   */
  void incrementSuccessfulFileMutationsReplayedCount(long delta);

  /**
   * Records a sample into the per-file mutation-count histogram. Called only for files that
   * replayed at least one mutation, so rotation-only (zero-mutation) files do not skew the
   * distribution toward zero.
   * @param mutationsCount the number of mutations replayed for a single log file
   */
  void updateMutationsPerFile(long mutationsCount);

  /**
   * Unregister this metrics source.
   */
  void close();

  // Get current values for testing
  ReplicationLogProcessorMetricValues getCurrentMetricValues();
}
