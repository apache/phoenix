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
package org.apache.phoenix.monitoring;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.phoenix.compat.hbase.CompatScanMetrics;

public class SlowestScanReadMetricsQueue {

  public static final SlowestScanReadMetricsQueue NOOP_SLOWEST_SCAN_READ_METRICS_QUEUE =
    new SlowestScanReadMetricsQueue() {
      @Override
      public void add(String tableName, Map<String, Long> scanMetrics) {
      }

      @Override
      public Map<String, Map<MetricType, Long>> aggregate() {
        return Collections.emptyMap();
      }
    };

  private final Deque<Map<String, Map<MetricType, Long>>> slowestScanReadMetricsQueue;

  public SlowestScanReadMetricsQueue() {
    this.slowestScanReadMetricsQueue = new ConcurrentLinkedDeque<>();
  }

  public void add(String tableName, Map<String, Long> scanMetrics) {
    Map<String, Map<MetricType, Long>> processedTableScanMetrics = new HashMap<>();
    Map<MetricType, Long> processedScanMetrics = new HashMap<>();
    processedTableScanMetrics.put(tableName, processedScanMetrics);
    for (Map.Entry<String, Long> entry : scanMetrics.entrySet()) {
      switch (entry.getKey()) {
        case ScanMetrics.RPC_CALLS_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_RPC_CALLS, entry.getValue());
          break;
        case ScanMetrics.REMOTE_RPC_CALLS_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_REMOTE_RPC_CALLS, entry.getValue());
          break;
        case ScanMetrics.MILLIS_BETWEEN_NEXTS_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_MILLS_BETWEEN_NEXTS, entry.getValue());
          break;
        case ScanMetrics.NOT_SERVING_REGION_EXCEPTION_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_NOT_SERVING_REGION_EXCEPTION, entry.getValue());
          break;
        case ScanMetrics.BYTES_IN_RESULTS_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_BYTES_REGION_SERVER_RESULTS, entry.getValue());
          break;
        case ScanMetrics.BYTES_IN_REMOTE_RESULTS_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_BYTES_IN_REMOTE_RESULTS, entry.getValue());
          break;
        case ScanMetrics.REGIONS_SCANNED_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_SCANNED_REGIONS, entry.getValue());
          break;
        case ScanMetrics.RPC_RETRIES_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_RPC_RETRIES, entry.getValue());
          break;
        case ScanMetrics.REMOTE_RPC_RETRIES_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_REMOTE_RPC_RETRIES, entry.getValue());
          break;
        case ScanMetrics.COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_ROWS_SCANNED, entry.getValue());
          break;
        case ScanMetrics.COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME:
          processedScanMetrics.put(MetricType.COUNT_ROWS_FILTERED, entry.getValue());
          break;
        case CompatScanMetrics.FS_READ_TIME_METRIC_NAME:
          processedScanMetrics.put(MetricType.FS_READ_TIME, entry.getValue());
          break;
        case CompatScanMetrics.BYTES_READ_FROM_FS_METRIC_NAME:
          processedScanMetrics.put(MetricType.BYTES_READ_FROM_FS, entry.getValue());
          break;
        case CompatScanMetrics.BYTES_READ_FROM_MEMSTORE_METRIC_NAME:
          processedScanMetrics.put(MetricType.BYTES_READ_FROM_MEMSTORE, entry.getValue());
          break;
        case CompatScanMetrics.BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME:
          processedScanMetrics.put(MetricType.BYTES_READ_FROM_BLOCKCACHE, entry.getValue());
          break;
        case CompatScanMetrics.BLOCK_READ_OPS_COUNT_METRIC_NAME:
          processedScanMetrics.put(MetricType.BLOCK_READ_OPS_COUNT, entry.getValue());
          break;
        case CompatScanMetrics.RPC_SCAN_PROCESSING_TIME_METRIC_NAME:
          processedScanMetrics.put(MetricType.RPC_SCAN_PROCESSING_TIME, entry.getValue());
          break;
        case CompatScanMetrics.RPC_SCAN_QUEUE_WAIT_TIME_METRIC_NAME:
          processedScanMetrics.put(MetricType.RPC_SCAN_QUEUE_WAIT_TIME, entry.getValue());
          break;
      }
    }
    this.slowestScanReadMetricsQueue.add(processedTableScanMetrics);
  }

  public Map<String, Map<MetricType, Long>> aggregate() {
    Map<String, Map<MetricType, Long>> slowestScanReadMetrics = null;
    long maxMillisBetweenNexts = Long.MIN_VALUE;

    // Optimized: Single pass through the queue to find the maximum value
    while (!slowestScanReadMetricsQueue.isEmpty()) {
      Map<String, Map<MetricType, Long>> scanReadMetrics = slowestScanReadMetricsQueue.poll();

      // Extract the millis between nexts value directly for comparison
      long currentMillisBetweenNexts = extractMillisBetweenNexts(scanReadMetrics);

      if (slowestScanReadMetrics == null || currentMillisBetweenNexts > maxMillisBetweenNexts) {
        slowestScanReadMetrics = scanReadMetrics;
        maxMillisBetweenNexts = currentMillisBetweenNexts;
      }
    }

    if (slowestScanReadMetrics == null) {
      return Collections.emptyMap();
    }
    return slowestScanReadMetrics;
  }

  public static Map<String, Map<MetricType, Long>> getSlowest(Map<String, Map<MetricType, Long>> o1,
    Map<String, Map<MetricType, Long>> o2) {
    // Optimized: Direct comparison without using COMPARATOR
    long value1 = extractMillisBetweenNexts(o1);
    long value2 = extractMillisBetweenNexts(o2);
    return value1 >= value2 ? o1 : o2;
  }

  public static long extractMillisBetweenNexts(Map<String, Map<MetricType, Long>> input) {
    Map<MetricType,
      Long> metrics = input.values().iterator().hasNext()
        ? input.values().iterator().next()
        : Collections.emptyMap();
    return metrics.getOrDefault(MetricType.COUNT_MILLS_BETWEEN_NEXTS, Long.MIN_VALUE);
  }

  public static class SlowestScanMetrics {
    private long sumOfMillisSecBetweenNexts;
    private List<Map<String, Map<MetricType, Long>>> scanMetrics;

    public SlowestScanMetrics(long sumOfMillisSecBetweenNexts,
      List<Map<String, Map<MetricType, Long>>> scanMetrics) {
      this.sumOfMillisSecBetweenNexts = sumOfMillisSecBetweenNexts;
      this.scanMetrics = scanMetrics;
    }

    public long getSumOfMillisSecBetweenNexts() {
      return sumOfMillisSecBetweenNexts;
    }

    public void setSumOfMillisSecBetweenNexts(long sumOfMillisSecBetweenNexts) {
      this.sumOfMillisSecBetweenNexts = sumOfMillisSecBetweenNexts;
    }

    public List<Map<String, Map<MetricType, Long>>> getScanMetrics() {
      return scanMetrics;
    }

    public void setScanMetrics(List<Map<String, Map<MetricType, Long>>> scanMetrics) {
      this.scanMetrics = scanMetrics;
    }
  }
}
