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

import static org.apache.phoenix.monitoring.MetricType.BLOCK_READ_OPS_COUNT;
import static org.apache.phoenix.monitoring.MetricType.BYTES_READ_FROM_BLOCKCACHE;
import static org.apache.phoenix.monitoring.MetricType.BYTES_READ_FROM_FS;
import static org.apache.phoenix.monitoring.MetricType.BYTES_READ_FROM_MEMSTORE;
import static org.apache.phoenix.monitoring.MetricType.COUNT_BYTES_IN_REMOTE_RESULTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_BYTES_REGION_SERVER_RESULTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_MILLS_BETWEEN_NEXTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_NOT_SERVING_REGION_EXCEPTION;
import static org.apache.phoenix.monitoring.MetricType.COUNT_REMOTE_RPC_CALLS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_REMOTE_RPC_RETRIES;
import static org.apache.phoenix.monitoring.MetricType.COUNT_ROWS_FILTERED;
import static org.apache.phoenix.monitoring.MetricType.COUNT_ROWS_SCANNED;
import static org.apache.phoenix.monitoring.MetricType.COUNT_RPC_CALLS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_RPC_RETRIES;
import static org.apache.phoenix.monitoring.MetricType.COUNT_SCANNED_REGIONS;
import static org.apache.phoenix.monitoring.MetricType.FS_READ_TIME;
import static org.apache.phoenix.monitoring.MetricType.PAGED_ROWS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.RPC_SCAN_PROCESSING_TIME;
import static org.apache.phoenix.monitoring.MetricType.RPC_SCAN_QUEUE_WAIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.SCAN_BYTES;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;
import org.apache.hadoop.hbase.util.JsonMapper;
import org.apache.phoenix.log.LogLevel;

import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

public class ScanMetricsHolder {

  private Map<String, Long> scanMetricMap;
  private Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion;
  private Object scan;
  private final String tableName;
  private final Map<MetricType, CombinableMetric> metricTypeToMetric;

  private static final ScanMetricsHolder NO_OP_INSTANCE =
    new ScanMetricsHolder(new ReadMetricQueue(false, LogLevel.OFF), "", null, false);

  public static ScanMetricsHolder getInstance(ReadMetricQueue readMetrics, String tableName,
    Scan scan, LogLevel connectionLogLevel) {
    return ScanMetricsHolder.getInstance(readMetrics, tableName, scan, connectionLogLevel, false);
  }

  public static ScanMetricsHolder getInstance(ReadMetricQueue readMetrics, String tableName,
    Scan scan, LogLevel connectionLogLevel, boolean isScanMetricsByRegionEnabled) {
    if (connectionLogLevel == LogLevel.OFF && !readMetrics.isRequestMetricsEnabled()) {
      return NO_OP_INSTANCE;
    }
    return new ScanMetricsHolder(readMetrics, tableName, scan, isScanMetricsByRegionEnabled);
  }

  private ScanMetricsHolder(ReadMetricQueue readMetrics, String tableName, Scan scan,
    boolean isScanMetricsByRegionEnabled) {
    readMetrics.addScanHolder(this);
    this.tableName = tableName;
    this.scan = scan;
    if (scan != null) {
      scan.setScanMetricsEnabled(true);
      scan.setEnableScanMetricsByRegion(isScanMetricsByRegionEnabled);
    }
    metricTypeToMetric = new HashMap<>();
    metricTypeToMetric.put(COUNT_RPC_CALLS, readMetrics.allotMetric(COUNT_RPC_CALLS, tableName));
    metricTypeToMetric.put(COUNT_REMOTE_RPC_CALLS,
      readMetrics.allotMetric(COUNT_REMOTE_RPC_CALLS, tableName));
    metricTypeToMetric.put(COUNT_MILLS_BETWEEN_NEXTS,
      readMetrics.allotMetric(COUNT_MILLS_BETWEEN_NEXTS, tableName));
    metricTypeToMetric.put(COUNT_NOT_SERVING_REGION_EXCEPTION,
      readMetrics.allotMetric(COUNT_NOT_SERVING_REGION_EXCEPTION, tableName));
    metricTypeToMetric.put(COUNT_BYTES_REGION_SERVER_RESULTS,
      readMetrics.allotMetric(COUNT_BYTES_REGION_SERVER_RESULTS, tableName));
    metricTypeToMetric.put(COUNT_BYTES_IN_REMOTE_RESULTS,
      readMetrics.allotMetric(COUNT_BYTES_IN_REMOTE_RESULTS, tableName));
    metricTypeToMetric.put(COUNT_SCANNED_REGIONS,
      readMetrics.allotMetric(COUNT_SCANNED_REGIONS, tableName));
    metricTypeToMetric.put(COUNT_RPC_RETRIES,
      readMetrics.allotMetric(COUNT_RPC_RETRIES, tableName));
    metricTypeToMetric.put(COUNT_REMOTE_RPC_RETRIES,
      readMetrics.allotMetric(COUNT_REMOTE_RPC_RETRIES, tableName));
    metricTypeToMetric.put(COUNT_ROWS_SCANNED,
      readMetrics.allotMetric(COUNT_ROWS_SCANNED, tableName));
    metricTypeToMetric.put(COUNT_ROWS_FILTERED,
      readMetrics.allotMetric(COUNT_ROWS_FILTERED, tableName));
    metricTypeToMetric.put(SCAN_BYTES, readMetrics.allotMetric(SCAN_BYTES, tableName));
    metricTypeToMetric.put(PAGED_ROWS_COUNTER,
      readMetrics.allotMetric(PAGED_ROWS_COUNTER, tableName));
    metricTypeToMetric.put(FS_READ_TIME, readMetrics.allotMetric(FS_READ_TIME, tableName));
    metricTypeToMetric.put(BYTES_READ_FROM_FS,
      readMetrics.allotMetric(BYTES_READ_FROM_FS, tableName));
    metricTypeToMetric.put(BYTES_READ_FROM_MEMSTORE,
      readMetrics.allotMetric(BYTES_READ_FROM_MEMSTORE, tableName));
    metricTypeToMetric.put(BYTES_READ_FROM_BLOCKCACHE,
      readMetrics.allotMetric(BYTES_READ_FROM_BLOCKCACHE, tableName));
    metricTypeToMetric.put(BLOCK_READ_OPS_COUNT,
      readMetrics.allotMetric(BLOCK_READ_OPS_COUNT, tableName));
    metricTypeToMetric.put(RPC_SCAN_PROCESSING_TIME,
      readMetrics.allotMetric(RPC_SCAN_PROCESSING_TIME, tableName));
    metricTypeToMetric.put(RPC_SCAN_QUEUE_WAIT_TIME,
      readMetrics.allotMetric(RPC_SCAN_QUEUE_WAIT_TIME, tableName));
  }

  public CombinableMetric getCountOfRemoteRPCcalls() {
    return metricTypeToMetric.get(COUNT_REMOTE_RPC_CALLS);
  }

  public CombinableMetric getSumOfMillisSecBetweenNexts() {
    return metricTypeToMetric.get(COUNT_MILLS_BETWEEN_NEXTS);
  }

  public CombinableMetric getCountOfNSRE() {
    return metricTypeToMetric.get(COUNT_NOT_SERVING_REGION_EXCEPTION);
  }

  public CombinableMetric getCountOfBytesInRemoteResults() {
    return metricTypeToMetric.get(COUNT_BYTES_IN_REMOTE_RESULTS);
  }

  public CombinableMetric getCountOfRegions() {
    return metricTypeToMetric.get(COUNT_SCANNED_REGIONS);
  }

  public CombinableMetric getCountOfRPCRetries() {
    return metricTypeToMetric.get(COUNT_RPC_RETRIES);
  }

  public CombinableMetric getCountOfRemoteRPCRetries() {
    return metricTypeToMetric.get(COUNT_REMOTE_RPC_RETRIES);
  }

  public CombinableMetric getCountOfRowsFiltered() {
    return metricTypeToMetric.get(COUNT_ROWS_FILTERED);
  }

  public CombinableMetric getCountOfRPCcalls() {
    return metricTypeToMetric.get(COUNT_RPC_CALLS);
  }

  public CombinableMetric getCountOfBytesInResults() {
    return metricTypeToMetric.get(COUNT_BYTES_REGION_SERVER_RESULTS);
  }

  public CombinableMetric getCountOfRowsScanned() {
    return metricTypeToMetric.get(COUNT_ROWS_SCANNED);
  }

  public Map<String, Long> getScanMetricMap() {
    return scanMetricMap;
  }

  public CombinableMetric getCountOfBytesScanned() {
    return metricTypeToMetric.get(SCAN_BYTES);
  }

  public CombinableMetric getCountOfRowsPaged() {
    return metricTypeToMetric.get(PAGED_ROWS_COUNTER);
  }

  public CombinableMetric getFsReadTime() {
    return metricTypeToMetric.get(FS_READ_TIME);
  }

  public CombinableMetric getCountOfBytesReadFromFS() {
    return metricTypeToMetric.get(BYTES_READ_FROM_FS);
  }

  public CombinableMetric getCountOfBytesReadFromMemstore() {
    return metricTypeToMetric.get(BYTES_READ_FROM_MEMSTORE);
  }

  public CombinableMetric getCountOfBytesReadFromBlockcache() {
    return metricTypeToMetric.get(BYTES_READ_FROM_BLOCKCACHE);
  }

  public CombinableMetric getCountOfBlockReadOps() {
    return metricTypeToMetric.get(BLOCK_READ_OPS_COUNT);
  }

  public CombinableMetric getRpcScanProcessingTime() {
    return metricTypeToMetric.get(RPC_SCAN_PROCESSING_TIME);
  }

  public CombinableMetric getRpcScanQueueWaitTime() {
    return metricTypeToMetric.get(RPC_SCAN_QUEUE_WAIT_TIME);
  }

  public void setScanMetricMap(Map<String, Long> scanMetricMap) {
    this.scanMetricMap = scanMetricMap;
  }

  public void
    setScanMetricsByRegion(Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion) {
    this.scanMetricsByRegion = scanMetricsByRegion;
  }

  public void populateMetrics(Long dummyRowCounter) {
    for (Map.Entry<MetricType, CombinableMetric> entry : metricTypeToMetric.entrySet()) {
      String hbaseMetricName = entry.getKey().getHBaseMetricName();
      CombinableMetric metric = entry.getValue();
      if (metric.getMetricType() == MetricType.PAGED_ROWS_COUNTER) {
        changeMetric(metric, dummyRowCounter);
      } else if (hbaseMetricName != null && !hbaseMetricName.isEmpty()) {
        changeMetric(metric, scanMetricMap.get(hbaseMetricName));
      }
    }
  }

  private void changeMetric(CombinableMetric metric, Long value) {
    if (value != null) {
      metric.change(value);
    }
  }

  public Long getCostOfScan() {
    if (scanMetricMap == null || scanMetricMap.isEmpty()) {
      return Long.MAX_VALUE;
    }
    return scanMetricMap.get(ScanMetrics.MILLIS_BETWEEN_NEXTS_METRIC_NAME);
  }

  public JsonObject toJson() {
    JsonObject tableJson = new JsonObject();
    if (scanMetricMap == null || scanMetricMap.isEmpty()) {
      return tableJson;
    }
    if (scanMetricsByRegion != null && !scanMetricsByRegion.isEmpty()) {
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
      for (Map.Entry<String, Long> scanMetricEntry : scanMetricMap.entrySet()) {
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
    try {
      return "{\"scan\":" + scan + ", \"scanMetrics\":"
        + JsonMapper.writeObjectAsString(scanMetricMap) + "}";
    } catch (IOException e) {
      return "{\"Exception while converting scan metrics to Json\":\"" + e.getMessage() + "\"}";
    }
  }
}
