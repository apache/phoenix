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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.filter.PagingFilter;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  PagingRegionScanner works with PagingFilter to make sure that the time between two rows
 *  returned by the HBase region scanner should not exceed the configured page size in ms
 *  (on PagingFilter). When the page size is reached (because there are too many cells/rows
 *  to be filtered out), PagingFilter stops the HBase region scanner and sets its state
 *  to STOPPED. In this case, the HBase region scanner next() returns false and
 *  PagingFilter#isStopped() returns true. PagingRegionScanner is responsible for detecting
 *  PagingFilter has stopped the scanner, and then closing the current HBase region scanner,
 *  starting a new one to resume the scan operation and returning a dummy result to signal to
 *  Phoenix client to resume the scan operation by skipping this dummy result and calling
 *  ResultScanner#next().
 */
public class PagingRegionScanner extends BaseRegionScanner {
    private Region region;
    private Scan scan;
    private PagingFilter pagingFilter;

    private static final Logger LOGGER = LoggerFactory.getLogger(PagingRegionScanner.class);

	public PagingRegionScanner(Region region, RegionScanner scanner, Scan scan) {
	    super(scanner);
	    this.region = region;
	    this.scan = scan;
        pagingFilter = ScanUtil.getPhoenixPagingFilter(scan);
        if (pagingFilter != null) {
            pagingFilter.init();
        }
    }

    private boolean next(List<Cell> results, boolean raw) throws IOException {
        try {
            if (pagingFilter != null) {
                pagingFilter.init();
            }
            boolean hasMore = raw ? delegate.nextRaw(results) : delegate.next(results);
            if (pagingFilter == null) {
                return hasMore;
            }
            if (!hasMore) {
                // There is no more row from the HBase region scanner. We need to check if PageFilter
                // has stopped the region scanner
                if (pagingFilter.isStopped()) {
                    // Close the current region scanner, start a new one and return a dummy result
                    delegate.close();
                    byte[] rowKey = pagingFilter.getRowKeyAtStop();
                    boolean isInclusive = pagingFilter.isNextRowInclusive();
                    scan.withStartRow(rowKey, isInclusive);
                    delegate = region.getScanner(scan);
                    if (results.isEmpty()) {
                        LOGGER.info("Page filter stopped, generating dummy key {} inclusive={}",
                                Bytes.toStringBinary(rowKey), isInclusive);
                        ScanUtil.getDummyResult(rowKey, results);
                    }
                    return true;
                }
                return false;
            } else {
                // We got a row from the HBase scanner within the configured time (i.e., the page size). We need to
                // start a new page on the next next() call.
                return true;
            }
        } catch (Exception e) {
            if (pagingFilter != null) {
                pagingFilter.init();
            }
            throw e;
        }
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
        return new PagingRegionScanner(region, region.getScanner(scan), scan);
    }
}
