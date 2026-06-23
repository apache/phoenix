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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Implementation for tracking IndexCDCConsumer metrics.
 */
public class MetricsIndexCDCConsumerSourceImpl extends BaseSourceImpl
  implements MetricsIndexCDCConsumerSource {

  private final MetricHistogram cdcBatchProcessTimeHisto;
  private final MetricHistogram cdcMutationGenerateTimeHisto;
  private final MetricHistogram cdcMutationApplyTimeHisto;
  private final MutableFastCounter cdcBatchCounter;
  private final MutableFastCounter cdcMutationCounter;
  private final MutableFastCounter cdcBatchFailureCounter;
  private final MutableFastCounter cdcEventSkippedCounter;
  private final MutableGaugeLong cdcParentReplayActiveRegionsGauge;
  private final MetricHistogram cdcParentReplayDurationHisto;
  private final MutableGaugeLong cdcConsumerActiveRegionsGauge;
  private final MetricHistogram cdcIndexUpdateLagHisto;

  public MetricsIndexCDCConsumerSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsIndexCDCConsumerSourceImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    cdcBatchProcessTimeHisto =
      getMetricsRegistry().newHistogram(CDC_BATCH_PROCESS_TIME, CDC_BATCH_PROCESS_TIME_DESC);
    cdcMutationGenerateTimeHisto = getMetricsRegistry().newHistogram(CDC_MUTATION_GENERATE_TIME,
      CDC_MUTATION_GENERATE_TIME_DESC);
    cdcMutationApplyTimeHisto =
      getMetricsRegistry().newHistogram(CDC_MUTATION_APPLY_TIME, CDC_MUTATION_APPLY_TIME_DESC);
    cdcBatchCounter = getMetricsRegistry().newCounter(CDC_BATCH_COUNT, CDC_BATCH_COUNT_DESC, 0L);
    cdcMutationCounter =
      getMetricsRegistry().newCounter(CDC_MUTATION_COUNT, CDC_MUTATION_COUNT_DESC, 0L);
    cdcBatchFailureCounter =
      getMetricsRegistry().newCounter(CDC_BATCH_FAILURE_COUNT, CDC_BATCH_FAILURE_COUNT_DESC, 0L);
    cdcEventSkippedCounter =
      getMetricsRegistry().newCounter(CDC_EVENT_SKIPPED_COUNT, CDC_EVENT_SKIPPED_COUNT_DESC, 0L);
    cdcParentReplayActiveRegionsGauge = getMetricsRegistry()
      .newGauge(CDC_PARENT_REPLAY_ACTIVE_REGIONS, CDC_PARENT_REPLAY_ACTIVE_REGIONS_DESC, 0L);
    cdcParentReplayDurationHisto = getMetricsRegistry().newHistogram(CDC_PARENT_REPLAY_DURATION,
      CDC_PARENT_REPLAY_DURATION_DESC);
    cdcConsumerActiveRegionsGauge = getMetricsRegistry().newGauge(CDC_CONSUMER_ACTIVE_REGIONS,
      CDC_CONSUMER_ACTIVE_REGIONS_DESC, 0L);
    cdcIndexUpdateLagHisto =
      getMetricsRegistry().newHistogram(CDC_INDEX_UPDATE_LAG, CDC_INDEX_UPDATE_LAG_DESC);
  }

  @Override
  public void updateCdcBatchProcessTime(String dataTableName, long t) {
    incrementTableSpecificHistogram(CDC_BATCH_PROCESS_TIME, dataTableName, t);
    cdcBatchProcessTimeHisto.add(t);
  }

  @Override
  public void updateCdcMutationGenerateTime(String dataTableName, long t) {
    incrementTableSpecificHistogram(CDC_MUTATION_GENERATE_TIME, dataTableName, t);
    cdcMutationGenerateTimeHisto.add(t);
  }

  @Override
  public void updateCdcMutationApplyTime(String dataTableName, long t) {
    incrementTableSpecificHistogram(CDC_MUTATION_APPLY_TIME, dataTableName, t);
    cdcMutationApplyTimeHisto.add(t);
  }

  @Override
  public void incrementCdcBatchCount(String dataTableName) {
    incrementTableSpecificCounter(CDC_BATCH_COUNT, dataTableName);
    cdcBatchCounter.incr();
  }

  @Override
  public void incrementCdcMutationCount(String dataTableName, long count) {
    MutableFastCounter tableCounter =
      getMetricsRegistry().getCounter(getMetricName(CDC_MUTATION_COUNT, dataTableName), 0);
    tableCounter.incr(count);
    cdcMutationCounter.incr(count);
  }

  @Override
  public void incrementCdcBatchFailureCount(String dataTableName) {
    incrementTableSpecificCounter(CDC_BATCH_FAILURE_COUNT, dataTableName);
    cdcBatchFailureCounter.incr();
  }

  @Override
  public void incrementCdcEventSkippedCount(String dataTableName, long count) {
    MutableFastCounter tableCounter =
      getMetricsRegistry().getCounter(getMetricName(CDC_EVENT_SKIPPED_COUNT, dataTableName), 0);
    tableCounter.incr(count);
    cdcEventSkippedCounter.incr(count);
  }

  @Override
  public void incrementCdcParentReplayActiveRegions(String dataTableName) {
    incrementTableSpecificGauge(CDC_PARENT_REPLAY_ACTIVE_REGIONS, dataTableName);
    cdcParentReplayActiveRegionsGauge.incr();
  }

  @Override
  public void decrementCdcParentReplayActiveRegions(String dataTableName) {
    decrementTableSpecificGauge(CDC_PARENT_REPLAY_ACTIVE_REGIONS, dataTableName);
    cdcParentReplayActiveRegionsGauge.decr();
  }

  @Override
  public void updateCdcParentReplayDuration(String dataTableName, long durationMs) {
    incrementTableSpecificHistogram(CDC_PARENT_REPLAY_DURATION, dataTableName, durationMs);
    cdcParentReplayDurationHisto.add(durationMs);
  }

  @Override
  public void incrementCdcConsumerActiveRegions(String dataTableName) {
    incrementTableSpecificGauge(CDC_CONSUMER_ACTIVE_REGIONS, dataTableName);
    cdcConsumerActiveRegionsGauge.incr();
  }

  @Override
  public void decrementCdcConsumerActiveRegions(String dataTableName) {
    decrementTableSpecificGauge(CDC_CONSUMER_ACTIVE_REGIONS, dataTableName);
    cdcConsumerActiveRegionsGauge.decr();
  }

  @Override
  public void updateCdcLag(String dataTableName, long lag) {
    incrementTableSpecificHistogram(CDC_INDEX_UPDATE_LAG, dataTableName, lag);
    cdcIndexUpdateLagHisto.add(lag);
  }

  private void incrementTableSpecificCounter(String baseName, String tableName) {
    MutableFastCounter tableCounter =
      getMetricsRegistry().getCounter(getMetricName(baseName, tableName), 0);
    tableCounter.incr();
  }

  private void incrementTableSpecificHistogram(String baseName, String tableName, long t) {
    MetricHistogram tableHistogram =
      getMetricsRegistry().getHistogram(getMetricName(baseName, tableName));
    tableHistogram.add(t);
  }

  private void incrementTableSpecificGauge(String baseName, String tableName) {
    MutableGaugeLong tableGauge =
      getMetricsRegistry().getGauge(getMetricName(baseName, tableName), 0);
    tableGauge.incr();
  }

  private void decrementTableSpecificGauge(String baseName, String tableName) {
    MutableGaugeLong tableGauge =
      getMetricsRegistry().getGauge(getMetricName(baseName, tableName), 0);
    tableGauge.decr();
  }

  private String getMetricName(String baseName, String tableName) {
    return baseName + "." + tableName;
  }
}
