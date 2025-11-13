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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScannerContext has all methods package visible. To properly update the context progress for our
 * scanners we need this helper
 */
public class PhoenixScannerContext extends ScannerContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixScannerContext.class);

  // tracks the start time of the rpc on the server for server paging
  private final long startTime;

  /**
   * The scanner remains open on the server during the course of multiple scan rpc requests. We need
   * a way to determine during the next() call if it is a new scan rpc request on the same scanner.
   * This is needed so that we can reset the start time for server paging. Every scan rpc request
   * creates a new ScannerContext which has the lastPeekedCell set to null in the beginning.
   * Subsequent next() calls will set this field in the ScannerContext.
   */
  public static boolean isNewScanRpcRequest(ScannerContext scannerContext) {
    return scannerContext != null && scannerContext.getLastPeekedCell() == null;
  }

  public PhoenixScannerContext(ScannerContext hbaseContext) {
    // set limits to null to create no limit context
    super(Objects.requireNonNull(hbaseContext).keepProgress, null,
      Objects.requireNonNull(hbaseContext).isTrackingMetrics());
    startTime = EnvironmentEdgeManager.currentTimeMillis();
  }

  public PhoenixScannerContext(boolean trackMetrics) {
    super(false, null, trackMetrics);
    startTime = EnvironmentEdgeManager.currentTimeMillis();
  }

  public long getStartTime() {
    return startTime;
  }

  public void incrementSizeProgress(List<Cell> cells) {
    for (Cell cell : cells) {
      super.incrementSizeProgress(PrivateCellUtil.estimatedSerializedSizeOf(cell), cell.heapSize());
    }
  }

  /**
   * returnImmediately is a private field in ScannerContext and there is no getter API on it But the
   * checkTimeLimit API on the ScannerContext will return true if returnImmediately is set
   */
  public boolean isReturnImmediately() {
    return checkTimeLimit(ScannerContext.LimitScope.BETWEEN_ROWS);
  }

  /**
   * Update the scanner context created by RSRpcServices so that it can act accordingly
   * @param dst    hbase scanner context created on every new scan rpc request
   * @param result list of cells to be returned to the client as scan rpc response
   */
  public void updateHBaseScannerContext(ScannerContext dst, List<Cell> result) {
    if (dst == null) {
      return;
    }
    // update last peeked cell
    dst.setLastPeekedCell(getLastPeekedCell());
    // update return immediately
    if (isDummy(result) || isReturnImmediately()) {
      // when a dummy row is returned by a lower layer, set returnImmediately
      // on the ScannerContext to force HBase to return a response to the client
      dst.returnImmediately();
    }
    // update metrics
    if (isTrackingMetrics() && dst.isTrackingMetrics()) {
      // getMetricsMap call resets the metrics internally
      for (Map.Entry<String, Long> entry : getMetrics().getMetricsMap().entrySet()) {
        dst.metrics.addToCounter(entry.getKey(), entry.getValue());
      }
    }
    // update progress
    dst.setProgress(getBatchProgress(), getDataSizeProgress(), getHeapSizeProgress());
  }

  public static boolean isTimedOut(ScannerContext context, long pageSizeMs) {
    if (context == null || !(context instanceof PhoenixScannerContext)) {
      return false;
    }
    PhoenixScannerContext phoenixScannerContext = (PhoenixScannerContext) context;
    return EnvironmentEdgeManager.currentTimeMillis() - phoenixScannerContext.startTime
        > pageSizeMs;
  }

  /**
   * Set returnImmediately on the ScannerContext to true, it will have the same behavior as reaching
   * the time limit. Use this to make RSRpcService.scan return immediately.
   */
  public static void setReturnImmediately(ScannerContext context) {
    if (context == null || !(context instanceof PhoenixScannerContext)) {
      return;
    }
    PhoenixScannerContext phoenixScannerContext = (PhoenixScannerContext) context;
    phoenixScannerContext.returnImmediately();
  }

  public static boolean isReturnImmediately(ScannerContext context) {
    if (context == null || !(context instanceof PhoenixScannerContext)) {
      return false;
    }
    PhoenixScannerContext phoenixScannerContext = (PhoenixScannerContext) context;
    return phoenixScannerContext.isReturnImmediately();
  }
}
