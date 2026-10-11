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

import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource.HnswMetric;

/** Reads the values of HNSW metrics for tests. */
public final class HnswMetricValues {
  private HnswMetricValues() {
  }

  /** Returns the metrics source of the server. */
  public static MetricsVectorIndexSourceImpl server() {
    return (MetricsVectorIndexSourceImpl) MetricsIndexerSourceFactory.getInstance()
      .getMetricsVectorIndexSource();
  }

  /** Returns the server value of a metric in the source of the server. */
  public static long value(HnswMetric metric) {
    return value(server(), metric, null);
  }

  /** Returns the value of a metric for an index in the source of the server. */
  public static long value(HnswMetric metric, String index) {
    return value(server(), metric, index);
  }

  /**
   * Returns the value of a counter or a gauge, or the number of values of a histogram. A metric
   * that is not registered has the value 0.
   * @param index the index, or null for the server value
   */
  public static long value(MetricsVectorIndexSourceImpl source, HnswMetric metric, String index) {
    MutableMetric m = source.getMetricsRegistry()
      .get(index == null ? metric.metricName() : metric.metricName() + "." + index);
    if (m == null) {
      return 0;
    }
    switch (metric.kind()) {
      case COUNTER:
        return ((MutableFastCounter) m).value();
      case GAUGE:
        return ((MutableGaugeLong) m).value();
      default:
        return ((MutableHistogram) m).getCount();
    }
  }
}
