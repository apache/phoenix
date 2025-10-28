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
import java.util.Map;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;
import org.apache.phoenix.compat.hbase.CompatScanMetrics;

import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

public class ScanMetricsGroup {

  public static final ScanMetricsGroup EMPTY_SCAN_METRICS_GROUP =
    new ScanMetricsGroup("", Collections.emptyMap()) {
      @Override
      public Long getSumOfMillisSecBetweenNexts() {
        return Long.MIN_VALUE;
      }
    };

  private final String tableName;
  private Map<String, Long> scanMetrics = null;
  private Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion = null;
  private final Long sumOfMillisSecBetweenNexts;

  public ScanMetricsGroup(String tableName, Map<String, Long> scanMetrics) {
    this.tableName = tableName;
    this.scanMetrics = scanMetrics;
    this.sumOfMillisSecBetweenNexts = scanMetrics.get(ScanMetrics.MILLIS_BETWEEN_NEXTS_METRIC_NAME);
  }

  public ScanMetricsGroup(String tableName,
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion,
    Map<String, Long> globalScanMetrics) {
    this.tableName = tableName;
    this.scanMetricsByRegion = scanMetricsByRegion;
    this.sumOfMillisSecBetweenNexts =
      globalScanMetrics.get(ScanMetrics.MILLIS_BETWEEN_NEXTS_METRIC_NAME);
  }

  private MetricType getMetricType(String metricName) {
    switch (metricName) {
      case ScanMetrics.RPC_CALLS_METRIC_NAME:
        return MetricType.COUNT_RPC_CALLS;
      case ScanMetrics.REMOTE_RPC_CALLS_METRIC_NAME:
        return MetricType.COUNT_REMOTE_RPC_CALLS;
      case ScanMetrics.MILLIS_BETWEEN_NEXTS_METRIC_NAME:
        return MetricType.COUNT_MILLS_BETWEEN_NEXTS;
      case ScanMetrics.NOT_SERVING_REGION_EXCEPTION_METRIC_NAME:
        return MetricType.COUNT_NOT_SERVING_REGION_EXCEPTION;
      case ScanMetrics.BYTES_IN_RESULTS_METRIC_NAME:
        return MetricType.COUNT_BYTES_REGION_SERVER_RESULTS;
      case ScanMetrics.BYTES_IN_REMOTE_RESULTS_METRIC_NAME:
        return MetricType.COUNT_BYTES_IN_REMOTE_RESULTS;
      case ScanMetrics.REGIONS_SCANNED_METRIC_NAME:
        return MetricType.COUNT_SCANNED_REGIONS;
      case ScanMetrics.RPC_RETRIES_METRIC_NAME:
        return MetricType.COUNT_RPC_RETRIES;
      case ScanMetrics.REMOTE_RPC_RETRIES_METRIC_NAME:
        return MetricType.COUNT_REMOTE_RPC_RETRIES;
      case ScanMetrics.COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME:
        return MetricType.COUNT_ROWS_SCANNED;
      case ScanMetrics.COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME:
        return MetricType.COUNT_ROWS_FILTERED;
      case CompatScanMetrics.FS_READ_TIME_METRIC_NAME:
        return MetricType.FS_READ_TIME;
      case CompatScanMetrics.BYTES_READ_FROM_FS_METRIC_NAME:
        return MetricType.BYTES_READ_FROM_FS;
      case CompatScanMetrics.BYTES_READ_FROM_MEMSTORE_METRIC_NAME:
        return MetricType.BYTES_READ_FROM_MEMSTORE;
      case CompatScanMetrics.BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME:
        return MetricType.BYTES_READ_FROM_BLOCKCACHE;
      case CompatScanMetrics.BLOCK_READ_OPS_COUNT_METRIC_NAME:
        return MetricType.BLOCK_READ_OPS_COUNT;
      case CompatScanMetrics.RPC_SCAN_PROCESSING_TIME_METRIC_NAME:
        return MetricType.RPC_SCAN_PROCESSING_TIME;
      case CompatScanMetrics.RPC_SCAN_QUEUE_WAIT_TIME_METRIC_NAME:
        return MetricType.RPC_SCAN_QUEUE_WAIT_TIME;
      default:
        return null;
    }
  }

  public String getTableName() {
    return tableName;
  }

  public Long getSumOfMillisSecBetweenNexts() {
    return sumOfMillisSecBetweenNexts;
  }

  public JsonObject toJson() {
    JsonObject tableJson = new JsonObject();
    if (
      (scanMetrics == null || scanMetrics.isEmpty())
        && (scanMetricsByRegion == null || scanMetricsByRegion.isEmpty())
    ) {
      return tableJson;
    }
    if (scanMetricsByRegion != null) {
      tableJson.addProperty("table", tableName);
      JsonArray regionMetrics = new JsonArray();
      for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
        .entrySet()) {
        JsonObject regionJson = new JsonObject();
        ScanMetricsRegionInfo regionInfo = entry.getKey();
        regionJson.addProperty("region", regionInfo.getEncodedRegionName());
        regionJson.addProperty("server", regionInfo.getServerName().toString());
        for (Map.Entry<String, Long> scanMetricEntry : entry.getValue().entrySet()) {
          MetricType metricType = getMetricType(scanMetricEntry.getKey());
          if (metricType == null) {
            continue;
          }
          regionJson.addProperty(metricType.shortName(), scanMetricEntry.getValue());
        }
        regionMetrics.add(regionJson);
      }
      tableJson.add("regions", regionMetrics);
    } else {
      tableJson.addProperty("table", tableName);
      for (Map.Entry<String, Long> scanMetricEntry : scanMetrics.entrySet()) {
        MetricType metricType = getMetricType(scanMetricEntry.getKey());
        if (metricType == null) {
          continue;
        }
        tableJson.addProperty(metricType.shortName(), scanMetricEntry.getValue());
      }
    }
    return tableJson;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }
}
