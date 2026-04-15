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
package org.apache.phoenix.compat.hbase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;

public class CompatScanMetrics {

  public static class RegionMetricsInfo {
    private final String encodedRegionName;
    private final String serverName;
    private final Map<String, Long> metrics;

    public RegionMetricsInfo(String encodedRegionName, String serverName,
      Map<String, Long> metrics) {
      this.encodedRegionName = encodedRegionName;
      this.serverName = serverName;
      this.metrics = metrics;
    }

    public String getEncodedRegionName() {
      return encodedRegionName;
    }

    public String getServerName() {
      return serverName;
    }

    public Map<String, Long> getMetrics() {
      return metrics;
    }
  }

  public static final String FS_READ_TIME_METRIC_NAME =
    ServerSideScanMetrics.FS_READ_TIME_METRIC_NAME;
  public static final String BYTES_READ_FROM_FS_METRIC_NAME =
    ServerSideScanMetrics.BYTES_READ_FROM_FS_METRIC_NAME;
  public static final String BYTES_READ_FROM_MEMSTORE_METRIC_NAME =
    ServerSideScanMetrics.BYTES_READ_FROM_MEMSTORE_METRIC_NAME;
  public static final String BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME =
    ServerSideScanMetrics.BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME;
  public static final String BLOCK_READ_OPS_COUNT_METRIC_NAME =
    ServerSideScanMetrics.BLOCK_READ_OPS_COUNT_METRIC_NAME;
  public static final String RPC_SCAN_PROCESSING_TIME_METRIC_NAME =
    ServerSideScanMetrics.RPC_SCAN_PROCESSING_TIME_METRIC_NAME;
  public static final String RPC_SCAN_QUEUE_WAIT_TIME_METRIC_NAME =
    ServerSideScanMetrics.RPC_SCAN_QUEUE_WAIT_TIME_METRIC_NAME;

  private CompatScanMetrics() {
    // Not to be instantiated
  }

  public static boolean supportsFineGrainedReadMetrics() {
    return true;
  }

  public static Long getFsReadTime(ScanMetrics scanMetrics) {
    return getCounterValue(scanMetrics, ServerSideScanMetrics.FS_READ_TIME_METRIC_NAME);
  }

  public static Long getBytesReadFromFs(ScanMetrics scanMetrics) {
    return getCounterValue(scanMetrics, ServerSideScanMetrics.BYTES_READ_FROM_FS_METRIC_NAME);
  }

  public static Long getBytesReadFromMemstore(ScanMetrics scanMetrics) {
    return getCounterValue(scanMetrics, ServerSideScanMetrics.BYTES_READ_FROM_MEMSTORE_METRIC_NAME);
  }

  public static Long getBytesReadFromBlockCache(ScanMetrics scanMetrics) {
    return getCounterValue(scanMetrics,
      ServerSideScanMetrics.BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME);
  }

  public static Long getBlockReadOpsCount(ScanMetrics scanMetrics) {
    return getCounterValue(scanMetrics, ServerSideScanMetrics.BLOCK_READ_OPS_COUNT_METRIC_NAME);
  }

  private static Long getCounterValue(ScanMetrics scanMetrics, String metricName) {
    AtomicLong counter = scanMetrics.getCounter(metricName);
    return counter != null ? counter.get() : 0L;
  }

  public static boolean supportsScanMetricsByRegion() {
    return true;
  }

  public static void enableScanMetricsByRegion(Scan scan, boolean enabled) {
    scan.setEnableScanMetricsByRegion(enabled);
  }

  public static List<RegionMetricsInfo> collectRegionMetrics(ScanMetrics scanMetrics) {
    Map<ScanMetricsRegionInfo, Map<String, Long>> byRegion = scanMetrics.collectMetricsByRegion();
    if (byRegion == null || byRegion.isEmpty()) {
      return Collections.emptyList();
    }
    List<RegionMetricsInfo> result = new ArrayList<>();
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : byRegion.entrySet()) {
      ScanMetricsRegionInfo regionInfo = entry.getKey();
      result.add(new RegionMetricsInfo(regionInfo.getEncodedRegionName(),
        regionInfo.getServerName().toString(), entry.getValue()));
    }
    return result;
  }
}
