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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.schema.types.PBoolean;

/**
 * Utilities for internal server-side region scans that must honor Phoenix TTL exactly like a client
 * read. The client normally sets the empty-column and TTL scan attributes
 * ({@link org.apache.phoenix.util.ScanUtil#setScanAttributesForPhoenixTTL}) and the coprocessor
 * hook {@code BaseScannerRegionObserver.postScannerOpen} wraps the scan in a
 * {@link TTLRegionScanner}. Internal scans opened directly via {@code region.getScanner(scan)}
 * bypass that hook, so they set no attributes and are never TTL-masked. These helpers reproduce the
 * TTL-masking step for server-side callers (e.g. {@code IndexRegionObserver} current-row reads) so
 * an internal scan masks identically to a client scan.
 * <p>
 * Note: server paging is intentionally <b>not</b> reproduced here. Paging exists to bound the work
 * a single RPC handler thread does before yielding; these internal scans are region-local (opened
 * directly on the {@link Region}, not through the RPC scan path), so they consume no handler thread
 * and there is nothing for paging to protect.
 */
public class ServerScanUtil {

  private ServerScanUtil() {
  }

  /**
   * Sets the Phoenix TTL scan attributes on an internal data-table scan so it masks exactly like a
   * client read.
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
  public static void setInternalScanAttributes(Scan scan, byte[] emptyCF, byte[] emptyCQ,
    byte[] literalTTLForScan, boolean isStrictTTL) {
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
  }

  /**
   * Opens a region scanner wrapped in a {@link TTLRegionScanner} exactly as
   * {@code BaseScannerRegionObserver.postScannerOpen} wraps a client scan, so TTL masking is
   * applied. This is always safe: {@link TTLRegionScanner#isMaskingEnabled} no-ops the masking when
   * Phoenix compaction is disabled, the empty-column attributes are absent, the TTL is FOREVER, or
   * the scan is non-strict — so wrapping a non-TTL scan changes no behavior.
   * <p>
   * Unlike the client read path this does not wrap in a {@code PagingRegionScanner}: the scan is
   * region-local (opened directly on the {@link Region}, off the RPC path), so it holds no handler
   * thread and has nothing for paging to protect.
   */
  public static RegionScanner openRegionScanner(RegionCoprocessorEnvironment env, Region region,
    Scan scan) throws IOException {
    return new TTLRegionScanner(env, scan, region.getScanner(scan));
  }
}
