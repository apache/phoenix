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
package org.apache.phoenix.hbase.index.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * IndexCDCConsumer metrics for eventually consistent index updates.
 */
public interface MetricsIndexCDCConsumerSource extends BaseSource {

  String METRICS_NAME = "IndexCDCConsumer";
  String METRICS_CONTEXT = "phoenix";
  String METRICS_DESCRIPTION = "Metrics about the Phoenix Index CDC Consumer";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String CDC_BATCH_PROCESS_TIME = "cdcBatchProcessTime";
  String CDC_BATCH_PROCESS_TIME_DESC =
    "Histogram for the end-to-end time in milliseconds for processing one CDC batch";

  String CDC_MUTATION_GENERATE_TIME = "cdcMutationGenerateTime";
  String CDC_MUTATION_GENERATE_TIME_DESC =
    "Histogram for the time in milliseconds to generate index mutations from data row states";

  String CDC_MUTATION_APPLY_TIME = "cdcMutationApplyTime";
  String CDC_MUTATION_APPLY_TIME_DESC =
    "Histogram for the time in milliseconds to apply index mutations to index tables";

  String CDC_BATCH_COUNT = "cdcBatchCount";
  String CDC_BATCH_COUNT_DESC = "The number of CDC batches processed";

  String CDC_MUTATION_COUNT = "cdcMutationCount";
  String CDC_MUTATION_COUNT_DESC = "The number of individual index mutations applied through CDC";

  String CDC_BATCH_FAILURE_COUNT = "cdcBatchFailureCount";
  String CDC_BATCH_FAILURE_COUNT_DESC = "The number of CDC batch processing failures";

  String CDC_EVENT_SKIPPED_COUNT = "cdcEventSkippedCount";
  String CDC_EVENT_SKIPPED_COUNT_DESC =
    "The number of times the consumer permanently advanced past CDC events whose data table "
      + "row state could not be read within phoenix.index.cdc.consumer.max.data.visibility.retries"
      + " attempts. Each increment represents one or more CDC events that will never be applied "
      + "to the eventually consistent index \u2014 typically caused by failed or aborted data "
      + "table mutations upstream. Non-zero values indicate silent data divergence between the "
      + "data table and its EC indexes.";

  String CDC_PARENT_REPLAY_ACTIVE_REGIONS = "cdcParentReplayActiveRegions";
  String CDC_PARENT_REPLAY_ACTIVE_REGIONS_DESC =
    "Gauge of regions currently in the parent-region replay phase (post-split / post-merge "
      + "catch-up before steady-state own-partition processing begins). Per-table value = number "
      + "of regions of this table on this RegionServer currently replaying ancestor partitions. "
      + "The lag histogram inflates during this phase by design (parent-replay timestamps do not "
      + "advance the child's own-partition freshness watermark) \u2014 this gauge lets operators "
      + "distinguish 'normal post-split catch-up' from 'consumer is broken'.";

  String CDC_PARENT_REPLAY_DURATION = "cdcParentReplayDuration";
  String CDC_PARENT_REPLAY_DURATION_DESC =
    "Histogram (milliseconds) of how long it took to fully replay one ancestor partition during "
      + "post-split / post-merge catch-up. One sample is emitted per parent partition when this "
      + "consumer either marks it COMPLETE or observes another consumer marking it COMPLETE. "
      + "Ancestors that were already COMPLETE when discovered emit no sample.";

  String CDC_CONSUMER_ACTIVE_REGIONS = "cdcConsumerActiveRegions";
  String CDC_CONSUMER_ACTIVE_REGIONS_DESC =
    "Gauge of regions whose IndexCDCConsumer is currently in steady-state own-partition "
      + "processing for this table on this RegionServer. Incremented immediately before entering "
      + "the main poll loop (after startup wait, EC-index discovery, CDC_STREAM wait, tracker "
      + "lookup, and any parent-region replay have all completed) and decremented when the loop "
      + "exits. A gauge of 0 where >0 is expected indicates the consumer either never reached "
      + "steady state (cold start, missing EC index, missing CDC_STREAM entry) or exited "
      + "(stopped, crashed, or region moved away). Combine with cdcParentReplayActiveRegions to "
      + "tell 'in catch-up' apart from 'in steady state' apart from 'not running at all'.";

  String CDC_INDEX_UPDATE_LAG = "cdcIndexUpdateLag";
  String CDC_INDEX_UPDATE_LAG_DESC =
    "Histogram of current time minus the consumer's effective freshness watermark, in "
      + "milliseconds. The watermark advances on successful own-partition batches AND on empty "
      + "polls (which prove caught-up to queryStart - timestampBufferMs). Idle steady state is "
      + "≈ timestampBufferMs; grows during sustained failure, parent-region replay, or cold "
      + "start (where it is floored at now - consumerStartTime).";

  /**
   * Updates the CDC batch processing time histogram.
   * @param dataTableName physical data table name
   * @param t             time taken in milliseconds
   */
  void updateCdcBatchProcessTime(String dataTableName, long t);

  /**
   * Updates the CDC mutation generation time histogram.
   * @param dataTableName physical data table name
   * @param t             time taken in milliseconds
   */
  void updateCdcMutationGenerateTime(String dataTableName, long t);

  /**
   * Updates the CDC mutation apply time histogram.
   * @param dataTableName physical data table name
   * @param t             time taken in milliseconds
   */
  void updateCdcMutationApplyTime(String dataTableName, long t);

  /**
   * Increments the CDC batch count.
   * @param dataTableName physical data table name
   */
  void incrementCdcBatchCount(String dataTableName);

  /**
   * Increments the CDC mutation count by the given amount.
   * @param dataTableName physical data table name
   * @param count         number of mutations applied
   */
  void incrementCdcMutationCount(String dataTableName, long count);

  /**
   * Increments the CDC batch failure count.
   * @param dataTableName physical data table name
   */
  void incrementCdcBatchFailureCount(String dataTableName);

  /**
   * Increments the count of CDC events permanently skipped after exhausting data-visibility
   * retries. See {@link #CDC_EVENT_SKIPPED_COUNT_DESC}.
   * @param dataTableName physical data table name
   */
  void incrementCdcEventSkippedCount(String dataTableName);

  /**
   * Increments the parent-region replay active gauge by 1. Must be paired with a corresponding
   * {@link #decrementCdcParentReplayActiveRegions(String)} in a {@code finally} block.
   * @param dataTableName physical data table name
   */
  void incrementCdcParentReplayActiveRegions(String dataTableName);

  /**
   * Decrements the parent-region replay active gauge by 1. Must be invoked in a {@code finally}
   * block paired with {@link #incrementCdcParentReplayActiveRegions(String)}.
   * @param dataTableName physical data table name
   */
  void decrementCdcParentReplayActiveRegions(String dataTableName);

  /**
   * Adds a sample to the parent-region replay duration histogram. Called once per ancestor
   * partition after its replay reaches a terminal state.
   * @param dataTableName physical data table name
   * @param durationMs    wall-clock time spent replaying this ancestor partition
   */
  void updateCdcParentReplayDuration(String dataTableName, long durationMs);

  /**
   * Increments the steady-state active-regions gauge by 1. Must be paired with a corresponding
   * {@link #decrementCdcConsumerActiveRegions(String)} in a {@code finally} block.
   * @param dataTableName physical data table name
   */
  void incrementCdcConsumerActiveRegions(String dataTableName);

  /**
   * Decrements the steady-state active-regions gauge by 1. Must be invoked in a {@code finally}
   * block paired with {@link #incrementCdcConsumerActiveRegions(String)}.
   * @param dataTableName physical data table name
   */
  void decrementCdcConsumerActiveRegions(String dataTableName);

  /**
   * Updates the CDC lag histogram.
   * @param dataTableName physical data table name
   * @param lag           lag in milliseconds between current time and last processed event
   */
  void updateCdcLag(String dataTableName, long lag);
}
