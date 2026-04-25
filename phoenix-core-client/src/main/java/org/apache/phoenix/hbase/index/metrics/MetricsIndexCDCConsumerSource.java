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

  String CDC_INDEX_UPDATE_LAG = "cdcIndexUpdateLag";
  String CDC_INDEX_UPDATE_LAG_DESC =
    "Histogram for the lag in milliseconds between current time and the last processed CDC event";

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
   * Updates the CDC lag histogram.
   * @param dataTableName physical data table name
   * @param lag           lag in milliseconds between current time and last processed event
   */
  void updateCdcLag(String dataTableName, long lag);
}
