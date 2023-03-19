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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME;

/**
 *  TTLRegionScanner masks expired rows.
 */
public class TTLRegionScanner extends BaseRegionScanner {
    private static final Logger LOG =
            LoggerFactory.getLogger(TTLRegionScanner.class);
    private final boolean isPhoenixTableTTLEnabled;
    private final RegionCoprocessorEnvironment env;
    private Scan scan;
    private long rowCount = 0;
    private long pageSize = Long.MAX_VALUE;
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
        emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
        emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        ttl = env.getRegion().getTableDescriptor().getColumnFamilies()[0].getTimeToLive();
        ttlWindowStart = ttl == HConstants.FOREVER ? 1 : currentTime - ttl * 1000;
        ttl *= 1000;
        isPhoenixTableTTLEnabled =
                env.getConfiguration().getBoolean(QueryServices.PHOENIX_TABLE_TTL_ENABLED,
                        QueryServicesOptions.DEFAULT_PHOENIX_TABLE_TTL_ENABLED);;
	}

    private void init() throws IOException {
        // HBase PageFilter will also count the expired rows.
        // Instead of using PageFilter for counting, we will count returned row here.
        PageFilter pageFilter = ScanUtil.removePageFilter(scan);
        if (pageFilter != null) {
            pageSize = pageFilter.getPageSize();
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
            ts = c.getTimestamp();
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
        if (maxTimestamp == HConstants.LATEST_TIMESTAMP) {
            return false;
        }
        if (maxTimestamp - minTimestamp <= ttl) {
            return false;
        }
        // We need check if the gap between two consecutive cell timestamps is more than ttl
        // and if so trim the cells beyond the gap
        Scan singleRowScan = new Scan();
        byte[] rowKey = CellUtil.cloneRow(result.get(0));
        singleRowScan.withStartRow(rowKey, true);
        singleRowScan.withStopRow(rowKey, true);
        RegionScanner scanner = ((DelegateRegionScanner)delegate).getNewRegionScanner(singleRowScan);
        List<Cell> row = new ArrayList<>();
        scanner.next(row);
        scanner.close();
        if (row.isEmpty()) {
            return true;
        }

        List<Long> tsList = new ArrayList<>(row.size());
        for (Cell c : row) {
            tsList.add(c.getTimestamp());
        }
        Collections.sort(tsList);
        long previous = minTimestamp;
        for (Long timestamp : tsList) {
            if (timestamp - previous > ttl) {
                minTimestamp = timestamp;
            }
            previous = timestamp;
        }
        Iterator<Cell> iterator = result.iterator();
        while(iterator.hasNext()) {
            if (iterator.next().getTimestamp() < minTimestamp) {
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
            if (EnvironmentEdgeManager.currentTimeMillis() - startTime > pageSize) {
                ScanUtil.getDummyResult(CellUtil.cloneRow(cell), result);
                return hasMore;
            }
        } while (hasMore);
        return false;
    }

    private boolean next(List<Cell> result, boolean raw) throws IOException {
        if (!isPhoenixTableTTLEnabled) {
            return raw ? delegate.nextRaw(result) : delegate.next(result);
        }
        if (!initialized) {
            init();
            initialized = true;
        }
        boolean hasMore = raw ? delegate.nextRaw(result) : delegate.next(result);
        if (result.isEmpty() || ScanUtil.isDummy(result)) {
            return hasMore;
        }
        hasMore = skipExpired(result, raw, hasMore);
        if (result.isEmpty() || ScanUtil.isDummy(result)) {
            return hasMore;
        }
        rowCount++;
        if (rowCount >= pageSize) {
            return false;
        }
        return hasMore;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
	   return next(results, false);
    }

    @Override
    public boolean nextRaw(List<Cell> results) throws IOException {
        return next(results, true);
    }

    @Override
    public RegionScanner getNewRegionScanner(Scan scan) throws IOException {
        return new TTLRegionScanner(env, scan,
                ((DelegateRegionScanner)delegate).getNewRegionScanner(scan));
    }
}
