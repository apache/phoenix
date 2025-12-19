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
import java.util.Map;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.JsonMapper;
import org.apache.phoenix.log.LogLevel;

public class ScanMetricsHolder {

  private final CombinableMetric countOfRPCcalls;
  private final CombinableMetric countOfRemoteRPCcalls;
  private final CombinableMetric sumOfMillisSecBetweenNexts;
  private final CombinableMetric countOfNSRE;
  private final CombinableMetric countOfBytesInResults;
  private final CombinableMetric countOfBytesInRemoteResults;
  private final CombinableMetric countOfRegions;
  private final CombinableMetric countOfRPCRetries;
  private final CombinableMetric countOfRemoteRPCRetries;
  private final CombinableMetric countOfRowsScanned;
  private final CombinableMetric countOfRowsFiltered;
  private final CombinableMetric countOfBytesScanned;
  private final CombinableMetric countOfRowsPaged;
  private final CombinableMetric fsReadTime;
  private final CombinableMetric countOfBytesReadFromFS;
  private final CombinableMetric countOfBytesReadFromMemstore;
  private final CombinableMetric countOfBytesReadFromBlockcache;
  private final CombinableMetric countOfBlockReadOps;
  private final CombinableMetric rpcScanProcessingTime;
  private final CombinableMetric rpcScanQueueWaitTime;
  private Map<String, Long> scanMetricMap;
  private Object scan;

  private static final ScanMetricsHolder NO_OP_INSTANCE =
    new ScanMetricsHolder(new ReadMetricQueue(false, LogLevel.OFF), "", null);

  public static ScanMetricsHolder getInstance(ReadMetricQueue readMetrics, String tableName,
    Scan scan, LogLevel connectionLogLevel) {
    if (connectionLogLevel == LogLevel.OFF && !readMetrics.isRequestMetricsEnabled()) {
      return NO_OP_INSTANCE;
    }
    scan.setScanMetricsEnabled(true);
    return new ScanMetricsHolder(readMetrics, tableName, scan);
  }

  private ScanMetricsHolder(ReadMetricQueue readMetrics, String tableName, Scan scan) {
    readMetrics.addScanHolder(this);
    this.scan = scan;
    countOfRPCcalls = readMetrics.allotMetric(COUNT_RPC_CALLS, tableName);
    countOfRemoteRPCcalls = readMetrics.allotMetric(COUNT_REMOTE_RPC_CALLS, tableName);
    sumOfMillisSecBetweenNexts = readMetrics.allotMetric(COUNT_MILLS_BETWEEN_NEXTS, tableName);
    countOfNSRE = readMetrics.allotMetric(COUNT_NOT_SERVING_REGION_EXCEPTION, tableName);
    countOfBytesInResults = readMetrics.allotMetric(COUNT_BYTES_REGION_SERVER_RESULTS, tableName);
    countOfBytesInRemoteResults = readMetrics.allotMetric(COUNT_BYTES_IN_REMOTE_RESULTS, tableName);
    countOfRegions = readMetrics.allotMetric(COUNT_SCANNED_REGIONS, tableName);
    countOfRPCRetries = readMetrics.allotMetric(COUNT_RPC_RETRIES, tableName);
    countOfRemoteRPCRetries = readMetrics.allotMetric(COUNT_REMOTE_RPC_RETRIES, tableName);
    countOfRowsScanned = readMetrics.allotMetric(COUNT_ROWS_SCANNED, tableName);
    countOfRowsFiltered = readMetrics.allotMetric(COUNT_ROWS_FILTERED, tableName);
    countOfBytesScanned = readMetrics.allotMetric(SCAN_BYTES, tableName);
    countOfRowsPaged = readMetrics.allotMetric(PAGED_ROWS_COUNTER, tableName);
    fsReadTime = readMetrics.allotMetric(FS_READ_TIME, tableName);
    countOfBytesReadFromFS = readMetrics.allotMetric(BYTES_READ_FROM_FS, tableName);
    countOfBytesReadFromMemstore = readMetrics.allotMetric(BYTES_READ_FROM_MEMSTORE, tableName);
    countOfBytesReadFromBlockcache = readMetrics.allotMetric(BYTES_READ_FROM_BLOCKCACHE, tableName);
    countOfBlockReadOps = readMetrics.allotMetric(BLOCK_READ_OPS_COUNT, tableName);
    rpcScanProcessingTime = readMetrics.allotMetric(RPC_SCAN_PROCESSING_TIME, tableName);
    rpcScanQueueWaitTime = readMetrics.allotMetric(RPC_SCAN_QUEUE_WAIT_TIME, tableName);
  }

  public CombinableMetric getCountOfRemoteRPCcalls() {
    return countOfRemoteRPCcalls;
  }

  public CombinableMetric getSumOfMillisSecBetweenNexts() {
    return sumOfMillisSecBetweenNexts;
  }

  public CombinableMetric getCountOfNSRE() {
    return countOfNSRE;
  }

  public CombinableMetric getCountOfBytesInRemoteResults() {
    return countOfBytesInRemoteResults;
  }

  public CombinableMetric getCountOfRegions() {
    return countOfRegions;
  }

  public CombinableMetric getCountOfRPCRetries() {
    return countOfRPCRetries;
  }

  public CombinableMetric getCountOfRemoteRPCRetries() {
    return countOfRemoteRPCRetries;
  }

  public CombinableMetric getCountOfRowsFiltered() {
    return countOfRowsFiltered;
  }

  public CombinableMetric getCountOfRPCcalls() {
    return countOfRPCcalls;
  }

  public CombinableMetric getCountOfBytesInResults() {
    return countOfBytesInResults;
  }

  public CombinableMetric getCountOfRowsScanned() {
    return countOfRowsScanned;
  }

  public Map<String, Long> getScanMetricMap() {
    return scanMetricMap;
  }

  public CombinableMetric getCountOfBytesScanned() {
    return countOfBytesScanned;
  }

  public CombinableMetric getCountOfRowsPaged() {
    return countOfRowsPaged;
  }

  public CombinableMetric getFsReadTime() {
    return fsReadTime;
  }

  public CombinableMetric getCountOfBytesReadFromFS() {
    return countOfBytesReadFromFS;
  }

  public CombinableMetric getCountOfBytesReadFromMemstore() {
    return countOfBytesReadFromMemstore;
  }

  public CombinableMetric getCountOfBytesReadFromBlockcache() {
    return countOfBytesReadFromBlockcache;
  }

  public CombinableMetric getCountOfBlockReadOps() {
    return countOfBlockReadOps;
  }

  public CombinableMetric getRpcScanProcessingTime() {
    return rpcScanProcessingTime;
  }

  public CombinableMetric getRpcScanQueueWaitTime() {
    return rpcScanQueueWaitTime;
  }

  public void setScanMetricMap(Map<String, Long> scanMetricMap) {
    this.scanMetricMap = scanMetricMap;
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
