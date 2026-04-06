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

  private String getMetricName(String baseName, String tableName) {
    return baseName + "." + tableName;
  }
}
