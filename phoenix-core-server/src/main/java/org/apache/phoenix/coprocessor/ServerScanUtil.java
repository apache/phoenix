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
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.filter.PagingFilter;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.util.ScanUtil;

/**
 * Utilities for internal server-side region scans that must honor Phoenix TTL exactly like a client
 * read. The client normally sets the empty-column and TTL scan attributes
 * ({@link org.apache.phoenix.util.ScanUtil#setScanAttributesForPhoenixTTL}) and the coprocessor
 * hook {@code BaseScannerRegionObserver.postScannerOpen} wraps the scan in a
 * {@link TTLRegionScanner}. Internal scans opened directly via {@code region.getScanner(scan)}
 * bypass that hook, so they set no attributes and are never TTL-masked. These helpers reproduce
 * both steps for server-side callers (e.g. {@code IndexRegionObserver} current-row reads) so an
 * internal scan masks identically to a client scan.
 * <p>
 * This class lives in the {@code org.apache.phoenix.coprocessor} package so it can reference the
 * server-only {@link TTLRegionScanner} and {@link PagingRegionScanner}; the client-side
 * {@code ScanUtil} cannot.
 */
public class ServerScanUtil {

  private ServerScanUtil() {
  }

  /**
   * Sets the Phoenix TTL and paging scan attributes on an internal data-table scan so it behaves
   * exactly like a client read.
   * <p>
   * TTL masking attributes ({@link TTLRegionScanner} reads these):
   * <ul>
   * <li>the empty-column CF/CQ, supplied by the caller from the bytes the client threaded on the
   * mutation ({@link org.apache.phoenix.util.ScanUtil#annotateMutationWithLiteralTTL}) — the single
   * source for every path, secondary-index and no-index (atomic / ON DUPLICATE KEY /
   * {@code returnResult} / row-delete) alike;</li>
   * <li>{@code IS_STRICT_TTL=false} when {@code isStrictTTL == false}, so a non-strict table is not
   * masked (absence of the attribute defaults to strict, matching the read path);</li>
   * <li>the view's literal TTL as the standard {@code _TTL} scan attribute when
   * {@code literalTTLForScan != null}. A base table's literal TTL is left unset so
   * {@link TTLRegionScanner}'s CF-descriptor fallback derives it.</li>
   * </ul>
   */
  public static void setInternalScanAttributes(Configuration conf, Scan scan, byte[] emptyCF,
    byte[] emptyCQ, byte[] literalTTLForScan, boolean isStrictTTL) {
    scan.setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME, emptyCF);
    scan.setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME, emptyCQ);
    if (!isStrictTTL) {
      // Absence of the attribute defaults to strict-true (ScanUtil.isStrictTTL), so only set it
      // when the table/view is non-strict, mirroring setScanAttributesForPhoenixTTL.
      scan.setAttribute(BaseScannerRegionObserverConstants.IS_STRICT_TTL,
        PBoolean.INSTANCE.toBytes(false));
    }
    if (literalTTLForScan != null) {
      // Only views carry a literal TTL here; a base table relies on the CF-descriptor fallback.
      scan.setAttribute(BaseScannerRegionObserverConstants.TTL, literalTTLForScan);
    }
    setInternalScanAttributesForPaging(conf, scan);
  }

  /**
   * Reproduces the client read path's server-paging setup for an internal scan. On the client the
   * {@code SERVER_PAGE_SIZE_MS} attribute is set by
   * {@code ScanUtil.setScanAttributeForPaging(Scan, PhoenixConnection)} and the scan filter is
   * later wrapped in a {@link PagingFilter} by {@code BaseScannerRegionObserver.preScannerOpen}.
   * Internal scans opened directly via {@code region.getScanner(scan)} bypass both, so this method
   * performs both steps up-front. The region-server {@link Configuration} is the source of the
   * paging props here, standing in for the client's {@code PhoenixConnection} props.
   * <p>
   * Ordering matters: {@code PagingRegionScanner}'s constructor reads the {@link PagingFilter} and
   * the page size off the scan, so this must run before
   * {@link #openRegionScanner(RegionCoprocessorEnvironment, Region, Scan)} builds the scanner.
   */
  public static void setInternalScanAttributesForPaging(Configuration conf, Scan scan) {
    if (
      !conf.getBoolean(QueryServices.PHOENIX_SERVER_PAGING_ENABLED_ATTRIB,
        QueryServicesOptions.DEFAULT_PHOENIX_SERVER_PAGING_ENABLED)
    ) {
      return;
    }
    long pageSizeMs = conf.getInt(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, -1);
    if (pageSizeMs == -1) {
      // Use half of the HBase RPC timeout value as the server page size, mirroring the client
      // ScanUtil.setScanAttributeForPaging fallback.
      pageSizeMs =
        (long) (conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT)
          * 0.5);
    }
    scan.setAttribute(BaseScannerRegionObserverConstants.SERVER_PAGE_SIZE_MS,
      Bytes.toBytes(Long.valueOf(pageSizeMs)));
    // Wrap the scan filter in a PagingFilter as the top-level filter, matching
    // BaseScannerRegionObserver.preScannerOpen. PagingRegionScanner then detects when PagingFilter
    // has paged the scan out and returns a dummy result; readDataTableRows skips those dummies.
    if (!(scan.getFilter() instanceof PagingFilter)) {
      scan.setFilter(new PagingFilter(scan.getFilter(), ScanUtil.getPageSizeMsForFilter(scan)));
    }
  }

  /**
   * Opens a region scanner wrapped exactly as {@code BaseScannerRegionObserver.postScannerOpen}
   * wraps a client scan, so TTL masking is applied. This is always safe:
   * {@link TTLRegionScanner#isMaskingEnabled} no-ops the masking when Phoenix compaction is
   * disabled, the empty-column attributes are absent, the TTL is FOREVER, or the scan is non-strict
   * — so wrapping a non-TTL scan changes no behavior.
   */
  public static RegionScanner openRegionScanner(RegionCoprocessorEnvironment env, Region region,
    Scan scan) throws IOException {
    return new TTLRegionScanner(env, scan,
      new PagingRegionScanner(region, region.getScanner(scan), scan));
  }
}
