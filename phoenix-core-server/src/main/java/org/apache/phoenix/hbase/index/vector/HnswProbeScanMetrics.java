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
package org.apache.phoenix.hbase.index.vector;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.DelegateRegionScanner;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;

/**
 * Adds the rows that the candidate probe of an HNSW search reads to the scan metrics of the client
 * scan.
 * <p>
 * <ul>
 * <li><b>Collection:</b> The probe runs in preScannerOpen. HBase makes the scanner context of the
 * RPC after this hook. Thus the probe scanners use a probe context that counts the rows that they
 * read and filter.</li>
 * <li><b>Carrier:</b> A server-only scan attribute carries these counts to postScannerOpen. HBase
 * gives the same scan object to both hooks. The search removes a client value of the attribute
 * before the probe copies the scan.</li>
 * <li><b>Transfer:</b> The Phoenix scan observer puts a wrapper above all its scanners. The wrapper
 * adds the counts to the first scanner context that tracks metrics. It adds them only once. Phoenix
 * scanners below it can close or replace their delegates, and they do not change the wrapper.</li>
 * <li><b>Scope:</b> Only the row counters use this path. The probe runs on the RPC handler thread,
 * so HBase already counts its byte and block reads in its thread-local scan metrics.</li>
 * </ul>
 * If the client does not track scan metrics, the probe uses no context and the scan gets no
 * wrapper.
 */
public final class HnswProbeScanMetrics {
  // The row counts of the probe, as two longs: the rows scanned and the rows filtered
  static final String ATTRIBUTE = "_HnswProbeScanMetrics";

  private HnswProbeScanMetrics() {
  }

  /**
   * Removes the carrier attribute from the client scan, and returns a probe context.
   * @param scan the client scan of the search
   * @return a context that counts the rows that the probe reads, or null if the client does not
   *         track scan metrics
   */
  static ScannerContext probeContext(Scan scan) {
    scan.setAttribute(ATTRIBUTE, null);
    if (!scan.isScanMetricsEnabled()) {
      return null;
    }
    // The batch limit is the same as the limit of the default context of a region scanner
    return ScannerContext.newBuilder().setBatchLimit(scan.getBatch()).setTrackMetrics(true).build();
  }

  /**
   * Stores the row counts of the probe context on the client scan, if the probe read rows.
   * @param scan    the client scan of the search
   * @param context the probe context, or null if the client does not track scan metrics
   */
  static void carry(Scan scan, ScannerContext context) {
    if (context == null) {
      return;
    }
    ServerSideScanMetrics metrics = context.getMetrics();
    long scanned = metrics.countOfRowsScanned.get();
    long filtered = metrics.countOfRowsFiltered.get();
    if (scanned > 0 || filtered > 0) {
      scan.setAttribute(ATTRIBUTE, Bytes.add(Bytes.toBytes(scanned), Bytes.toBytes(filtered)));
    }
  }

  /**
   * Returns a scanner that adds the probe row counts of the scan to the scan metrics of the client.
   * Removes the carrier attribute from the scan. Returns the given scanner if the scan carries no
   * counts. The caller gives the top Phoenix scanner of the scan.
   * <p>
   * Only the HNSW search of the default replica removes a client value of the attribute. Thus the
   * method ignores the attribute on other scans and on other replicas.
   * @param env     the coprocessor environment of the region
   * @param scan    the client scan
   * @param scanner the top Phoenix scanner of the scan
   * @return the wrapper, or the given scanner
   */
  public static RegionScanner wrap(RegionCoprocessorEnvironment env, Scan scan,
    RegionScanner scanner) {
    byte[] counts = scan.getAttribute(ATTRIBUTE);
    if (counts == null) {
      return scanner;
    }
    scan.setAttribute(ATTRIBUTE, null);
    if (
      counts.length != 2 * Bytes.SIZEOF_LONG
        || scan.getAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX) == null
        || !RegionReplicaUtil.isDefaultReplica(env.getRegionInfo())
    ) {
      return scanner;
    }
    return new ProbeMetricsScanner(scanner, Bytes.toLong(counts, 0),
      Bytes.toLong(counts, Bytes.SIZEOF_LONG));
  }

  // Adds the probe row counts once, to the first context that tracks metrics. A call without a
  // context, or with a context that does not track metrics, leaves the counts for a later call. The
  // RPC calls the scanner from one thread at a time, so the flag needs no synchronization. A new
  // scanner of the same scan gets no counts
  private static final class ProbeMetricsScanner extends DelegateRegionScanner {
    private final long scanned;
    private final long filtered;
    private boolean added;

    private ProbeMetricsScanner(RegionScanner scanner, long scanned, long filtered) {
      super(scanner);
      this.scanned = scanned;
      this.filtered = filtered;
    }

    private void addTo(ScannerContext context) {
      if (added || context == null || !context.isTrackingMetrics()) {
        return;
      }
      ServerSideScanMetrics metrics = context.getMetrics();
      metrics.addToCounter(ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME, scanned);
      metrics.addToCounter(ServerSideScanMetrics.COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME, filtered);
      added = true;
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
      addTo(scannerContext);
      return super.next(result, scannerContext);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
      addTo(scannerContext);
      return super.nextRaw(result, scannerContext);
    }
  }
}
