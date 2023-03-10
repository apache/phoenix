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
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME;
import static org.apache.phoenix.util.ScanUtil.*;

/**
 *  TTLRegionScanner masks expired rows.
 */
public class TTLRegionScanner extends BaseRegionScanner {
    private static final Logger LOG =
            LoggerFactory.getLogger(TTLRegionScanner.class);
    private final RegionCoprocessorEnvironment env;
    private Scan scan;
    private long rowCount = 0;
    private long pageSize = Long.MAX_VALUE;
    long ttlWindowStart = 0;
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
        int ttl = env.getRegion().getTableDescriptor().getColumnFamilies()[0].getTimeToLive();
        ttlWindowStart = ttl == HConstants.FOREVER ? 1 : currentTime - ttl * 1000;
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

    private boolean isExpired(List<Cell> result) {
        boolean found = false;
        for (Cell c : result) {
            if (ScanUtil.isEmptyColumn(c, emptyCF, emptyCQ)) {
                found = true;
                if (c.getTimestamp() < ttlWindowStart) {
                    return true;
                }
                break;
            }
        }

        if (!found) {
            LOG.warn("No empty column cell " + env.getRegion().getRegionInfo().getTable());
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
            if (result.isEmpty() || isDummy(result)) {
                return hasMore;
            }
            if (!isExpired(result)) {
                return hasMore;
            }
            Cell cell = result.get(0);
            result.clear();
            if (EnvironmentEdgeManager.currentTimeMillis() - startTime > pageSize) {
                getDummyResult(CellUtil.cloneRow(cell), result);
                return hasMore;
            }
        } while (hasMore);
        return false;
    }

    private boolean next(List<Cell> result, boolean raw) throws IOException {
        if (!initialized) {
            init();
            initialized = true;
        }
        boolean hasMore = raw ? delegate.nextRaw(result) : delegate.next(result);
        if (result.isEmpty() || isDummy(result)) {
            return hasMore;
        }
        hasMore = skipExpired(result, raw, hasMore);
        if (result.isEmpty() || isDummy(result)) {
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
