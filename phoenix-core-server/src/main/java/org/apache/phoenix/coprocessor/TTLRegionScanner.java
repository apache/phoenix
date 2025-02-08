/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.IS_PHOENIX_TTL_SCAN_TABLE_SYSTEM;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.isPhoenixTableTTLEnabled;

/**
 *  TTLRegionScanner masks expired rows using the empty column cell timestamp
 */
public class TTLRegionScanner extends BaseRegionScanner {
    private static final Logger LOG =
            LoggerFactory.getLogger(TTLRegionScanner.class);
    private final boolean isMaskingEnabled;
    private final RegionCoprocessorEnvironment env;
    private Scan scan;
    private long rowCount = 0;
    private long maxRowCount = Long.MAX_VALUE;
    private long pageSizeMs;
    long ttl;
    long ttlWindowStart;
    byte[] emptyCQ;
    byte[] emptyCF;
    private boolean initialized = false;

    public TTLRegionScanner(final RegionCoprocessorEnvironment env, final Scan scan,
            final RegionScanner s) {
        super(s);
        this.env = env;
        this.scan = scan;
        this.pageSizeMs = ScanUtil.getPageSizeMsForRegionScanner(scan);
        emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
        emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
        long currentTime = scan.getTimeRange().getMax() == HConstants.LATEST_TIMESTAMP ?
                EnvironmentEdgeManager.currentTimeMillis() : scan.getTimeRange().getMax();
        byte[] isSystemTable = scan.getAttribute(IS_PHOENIX_TTL_SCAN_TABLE_SYSTEM);
        if (isPhoenixTableTTLEnabled(env.getConfiguration()) && (isSystemTable == null
                || !Bytes.toBoolean(isSystemTable))) {
            ttl = ScanUtil.getTTL(this.scan);
        } else {
            ttl = env.getRegion().getTableDescriptor().getColumnFamilies()[0].getTimeToLive();
        }
        // Regardless if the Phoenix Table TTL feature is disabled cluster wide or the client is
        // an older client and does not supply the empty column parameters, the masking should not
        // be done here. We also disable masking when TTL is HConstants.FOREVER.
        isMaskingEnabled = emptyCF != null && emptyCQ != null && ttl != HConstants.FOREVER
                && (isPhoenixTableTTLEnabled(env.getConfiguration()) && (isSystemTable == null
                || !Bytes.toBoolean(isSystemTable)));

        ttlWindowStart = ttl == HConstants.FOREVER ? 1 : currentTime - ttl * 1000;
        ttl *= 1000;
    }

    private void init() throws IOException {
        // HBase PageFilter will also count the expired rows.
        // Instead of using PageFilter for counting, we will count returned row here.
        PageFilter pageFilter = ScanUtil.removePageFilter(scan);
        if (pageFilter != null) {
            maxRowCount = pageFilter.getPageSize();
            delegate.close();
            delegate = ((DelegateRegionScanner)delegate).getNewRegionScanner(scan);
        }
    }

    private boolean isExpired(List<Cell> result) throws IOException {
        long maxTimestamp = 0;
        long minTimestamp = Long.MAX_VALUE;
        long ts;
        boolean found = false;
        for (Cell c : result) {
            ts = c.getTimestamp();
            if (!found && ScanUtil.isEmptyColumn(c, emptyCF, emptyCQ)) {
                if (ts < ttlWindowStart) {
                    return true;
                }
                found = true;
            }
            if (maxTimestamp < ts) {
                maxTimestamp = ts;
            }
            if (minTimestamp > ts) {
                minTimestamp = ts;
            }
        }
        if (!found) {
            LOG.warn("No empty column cell " + env.getRegion().getRegionInfo().getTable());
        }
        if (maxTimestamp - minTimestamp <= ttl) {
            return false;
        }

        // We need to check if the gap between two consecutive cell timestamps is more than ttl
        // and if so trim the cells beyond the gap. The gap analysis works by doing a scan in a
        // sliding time range window of ttl width. This scan reads the latest version of the row in
        // that time range. If we find a version, then in that time range there is no gap. We find
        // the timestamp at which the update happened and then slide the window past that
        // timestamp. If no version is returned, then we have found a gap.
        // On a gap, all the cells below the current sliding window's end time
        // can be trimmed from the result. We slide the window past the current end time to find
        // any more gaps so that we can find the largest timestamp in the
        // [minTimestamp, maxTimestamp] window below which all the cells can be trimmed.
        // This algorithm doesn't read all the row versions into the memory since the
        // number of row versions can be unbounded and reading all of them at once can cause GC
        // issues. In practice, ttl windows are in days or months so the entire
        // [minTimestamp, maxTimestamp] range shouldn't span more than 2-3 ttl windows.
        // We know that an update happened at minTimestamp so initialize the sliding window
        // to [minTimestamp + 1, minTimestamp + ttl] which means the scan range should be
        // [minTimestamp + 1, minTimestamp + ttl + 1).
        long wndStartTS = minTimestamp + 1;
        long wndEndTS = wndStartTS + ttl;
        // any cell in the scan result list having a timestamp below trimTimestamp will be
        // removed from the list and not returned back to the client. Initially, it is equal to
        // the minTimestamp.
        long trimTimestamp = minTimestamp;
        List<Cell> row = new ArrayList<>();
        LOG.debug("Doing gap analysis for {} min = {}, max = {}",
                env.getRegionInfo().getRegionNameAsString(), minTimestamp, maxTimestamp);
        while (wndEndTS <= maxTimestamp) {
            LOG.debug("WndStart = {}, WndEnd = {}, trim = {}", wndStartTS, wndEndTS, trimTimestamp);
            row.clear(); // reset the row on every iteration
            Scan singleRowScan = new Scan();
            singleRowScan.setTimeRange(wndStartTS, wndEndTS);
            byte[] rowKey = CellUtil.cloneRow(result.get(0));
            singleRowScan.withStartRow(rowKey, true);
            singleRowScan.withStopRow(rowKey, true);
            RegionScanner scanner =
                    ((DelegateRegionScanner) delegate).getNewRegionScanner(singleRowScan);
            scanner.next(row);
            scanner.close();
            if (row.isEmpty()) {
                // no update in this window, we found a gap and the row expired
                trimTimestamp = wndEndTS - 1;
                LOG.debug("Found gap at {}", trimTimestamp);
                // next window will start at wndEndTS. Scan timeranges are half-open [min, max)
                wndStartTS = wndEndTS;
            } else {
                // we found an update within the ttl
                long lastUpdateTS = 0;
                for (Cell cell : row) {
                    lastUpdateTS = Math.max(lastUpdateTS, cell.getTimestamp());
                }
                // slide the window 1 past the lastUpdateTS
                LOG.debug("lastUpdateTS = {}", lastUpdateTS);
                wndStartTS = lastUpdateTS + 1;
            }
            wndEndTS = wndStartTS + ttl;
        }
        Iterator<Cell> iterator = result.iterator();
        while(iterator.hasNext()) {
            if (iterator.next().getTimestamp() < trimTimestamp) {
                iterator.remove();
            }
        }
        return false;
    }

    private boolean skipExpired(List<Cell> result, boolean raw, boolean hasMore) throws IOException {
        boolean expired = isExpired(result);
        if (!expired) {
            return hasMore;
        }
        result.clear();
        if (!hasMore) {
            return false;
        }
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        do {
            hasMore = raw ? delegate.nextRaw(result) : delegate.next(result);
            if (result.isEmpty() || ScanUtil.isDummy(result)) {
                return hasMore;
            }
            if (!isExpired(result)) {
                return hasMore;
            }
            Cell cell = result.get(0);
            result.clear();
            if (EnvironmentEdgeManager.currentTimeMillis() - startTime > pageSizeMs) {
                ScanUtil.getDummyResult(CellUtil.cloneRow(cell), result);
                return hasMore;
            }
        } while (hasMore);
        return false;
    }

    private boolean next(List<Cell> result, boolean raw, ScannerContext scannerContext)
            throws IOException {
        boolean hasMore;
        if (!isMaskingEnabled) {
            if (scannerContext != null) {
                hasMore = raw
                        ? delegate.nextRaw(result, scannerContext)
                        : delegate.next(result, scannerContext);
            } else {
                hasMore = raw ? delegate.nextRaw(result) : delegate.next(result);
            }
            return hasMore;
        }
        if (!initialized) {
            init();
            initialized = true;
        }

        if (scannerContext != null) {
            hasMore = raw
                    ? delegate.nextRaw(result, scannerContext)
                    : delegate.next(result, scannerContext);
        } else {
            hasMore = raw ? delegate.nextRaw(result) : delegate.next(result);
        }

        if (result.isEmpty() || ScanUtil.isDummy(result)) {
            return hasMore;
        }
        hasMore = skipExpired(result, raw, hasMore);
        if (result.isEmpty() || ScanUtil.isDummy(result)) {
            return hasMore;
        }
        rowCount++;
        if (rowCount >= maxRowCount) {
            return false;
        }
        return hasMore;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return next(results, false, null);
    }

    @Override
    public boolean nextRaw(List<Cell> results) throws IOException {
        return next(results, true, null);
    }

    @Override
    public boolean next(List<Cell> results, ScannerContext scannerContext) throws IOException {
        return next(results, false, scannerContext);
    }

    @Override
    public boolean nextRaw(List<Cell> results, ScannerContext scannerContext) throws IOException {
        return next(results, true, scannerContext);
    }

    @Override
    public RegionScanner getNewRegionScanner(Scan scan) throws IOException {
        try {
            return new TTLRegionScanner(env, scan,
                    ((DelegateRegionScanner)delegate).getNewRegionScanner(scan));
        } catch (ClassCastException e) {
            throw new DoNotRetryIOException(e);
        }
    }
}
