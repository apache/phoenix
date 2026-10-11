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
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.lib.MutableMetric;

public class MetricsVectorIndexSourceImpl extends BaseSourceImpl
  implements MetricsVectorIndexSource {

  private final MutableFastCounter vectorCentroidAssignments;
  private final MutableFastCounter vectorCentroidReassignments;
  private final MetricHistogram vectorScorecardFlushTimeHisto;
  private final MetricHistogram vectorScorecardReconcileTimeHisto;
  // The server values of the HNSW metrics, by ordinal
  private final MutableMetric[] hnswMetrics = new MutableMetric[HnswMetric.values().length];

  public MetricsVectorIndexSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsVectorIndexSourceImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    vectorCentroidAssignments = getMetricsRegistry().newCounter(VECTOR_CENTROID_ASSIGNMENTS,
      VECTOR_CENTROID_ASSIGNMENTS_DESC, 0L);
    vectorCentroidReassignments = getMetricsRegistry().newCounter(VECTOR_CENTROID_REASSIGNMENTS,
      VECTOR_CENTROID_REASSIGNMENTS_DESC, 0L);

    vectorScorecardFlushTimeHisto = getMetricsRegistry().newHistogram(VECTOR_SCORECARD_FLUSH_TIME,
      VECTOR_SCORECARD_FLUSH_TIME_DESC);
    vectorScorecardReconcileTimeHisto = getMetricsRegistry()
      .newHistogram(VECTOR_SCORECARD_RECONCILE_TIME, VECTOR_SCORECARD_RECONCILE_TIME_DESC);

    for (HnswMetric metric : HnswMetric.values()) {
      switch (metric.kind()) {
        case COUNTER:
          hnswMetrics[metric.ordinal()] =
            getMetricsRegistry().newCounter(metric.metricName(), metric.description(), 0L);
          break;
        case GAUGE:
          hnswMetrics[metric.ordinal()] =
            getMetricsRegistry().newGauge(metric.metricName(), metric.description(), 0L);
          break;
        default:
          hnswMetrics[metric.ordinal()] =
            getMetricsRegistry().newHistogram(metric.metricName(), metric.description());
      }
    }
  }

  @Override
  public void incrementVectorCentroidAssignments(String indexName, long count) {
    incrementIndexSpecificCounter(VECTOR_CENTROID_ASSIGNMENTS, indexName, count);
    vectorCentroidAssignments.incr(count);
  }

  @Override
  public void incrementVectorCentroidReassignments(String indexName, long count) {
    incrementIndexSpecificCounter(VECTOR_CENTROID_REASSIGNMENTS, indexName, count);
    vectorCentroidReassignments.incr(count);
  }

  @Override
  public void updateVectorScorecardFlushTime(String indexName, long timeMs) {
    incrementIndexSpecificHistogram(VECTOR_SCORECARD_FLUSH_TIME, indexName, timeMs);
    vectorScorecardFlushTimeHisto.add(timeMs);
  }

  @Override
  public void updateVectorScorecardReconcileTime(String indexName, long timeMs) {
    incrementIndexSpecificHistogram(VECTOR_SCORECARD_RECONCILE_TIME, indexName, timeMs);
    vectorScorecardReconcileTimeHisto.add(timeMs);
  }

  @Override
  public HnswIndexMetrics getHnswIndexMetrics(String indexName) {
    MutableMetric[] index = new MutableMetric[hnswMetrics.length];
    for (HnswMetric metric : HnswMetric.values()) {
      if (metric.serverOnly()) {
        continue;
      }
      String name = getCounterName(metric.metricName(), indexName);
      switch (metric.kind()) {
        case COUNTER:
          index[metric.ordinal()] = getMetricsRegistry().getCounter(name, 0L);
          break;
        case GAUGE:
          index[metric.ordinal()] = getMetricsRegistry().getGauge(name, 0L);
          break;
        default:
          index[metric.ordinal()] = getMetricsRegistry().getHistogram(name);
      }
    }
    return (metric, value) -> {
      MutableMetric m = index[metric.ordinal()];
      if (m != null) {
        updateHnsw(m, metric.kind(), value);
      }
      updateHnsw(hnswMetrics[metric.ordinal()], metric.kind(), value);
    };
  }

  @Override
  public void updateHnsw(HnswMetric metric, long value) {
    updateHnsw(hnswMetrics[metric.ordinal()], metric.kind(), value);
  }

  private static void updateHnsw(MutableMetric m, HnswMetricKind kind, long value) {
    switch (kind) {
      case COUNTER:
        ((MutableFastCounter) m).incr(value);
        break;
      case GAUGE:
        ((MutableGaugeLong) m).incr(value);
        break;
      default:
        ((MutableHistogram) m).add(value);
    }
  }

  private void incrementIndexSpecificCounter(String baseCounterName, String indexName, long count) {
    MutableFastCounter indexSpecificCounter =
      getMetricsRegistry().getCounter(getCounterName(baseCounterName, indexName), 0L);
    indexSpecificCounter.incr(count);
  }

  private void incrementIndexSpecificHistogram(String baseCounterName, String indexName, long t) {
    MetricHistogram indexSpecificHistogram =
      getMetricsRegistry().getHistogram(getCounterName(baseCounterName, indexName));
    indexSpecificHistogram.add(t);
  }

  private String getCounterName(String baseCounterName, String indexName) {
    return baseCounterName + "." + indexName;
  }
}
