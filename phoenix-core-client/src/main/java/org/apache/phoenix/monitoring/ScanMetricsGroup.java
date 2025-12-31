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

import java.util.Map;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;

import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

/**
 * Captures scan metrics at both aggregate and region levels for HBase scans created by Phoenix
 * clients.
 * <p>
 * Instances of this class are used to track and identify the slowest-performing scans. The slowness
 * of a scan is measured by the total time spent between consecutive calls to
 * {@link org.apache.hadoop.hbase.client.ResultScanner#next()} in the HBase client, which represents
 * the actual data retrieval latency experienced by the client.
 * <p>
 * This class supports two modes of metric collection:
 * <ul>
 * <li>Aggregate metrics: Overall scan metrics across all regions</li>
 * <li>Region-level metrics: Detailed metrics broken down by individual HBase regions</li>
 * </ul>
 * <p>
 * Use {@link #toJson()} to obtain a JSON object representation of the scan metrics for
 * serialization or reporting purposes.
 */
public class ScanMetricsGroup {

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
          MetricType metricType = MetricType.getMetricType(scanMetricEntry.getKey());
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
        MetricType metricType = MetricType.getMetricType(scanMetricEntry.getKey());
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
