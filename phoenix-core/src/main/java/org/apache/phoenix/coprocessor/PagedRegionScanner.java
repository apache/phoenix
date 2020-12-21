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
import org.apache.phoenix.filter.PagedFilter;

import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPhoenixPagedFilter;

public class PagedRegionScanner extends BaseRegionScanner {
    protected Region region;
    protected Scan scan;
    protected PagedFilter pageFilter;
	public PagedRegionScanner(Region region, RegionScanner scanner, Scan scan) {
	    super(scanner);
	    this.region = region;
	    this.scan = scan;
	    pageFilter = getPhoenixPagedFilter(scan);
	    if (pageFilter != null) {
	        pageFilter.init();
        }
	}

    private boolean next(List<Cell> results, boolean raw) throws IOException {
	    try {
            boolean hasMore = raw ? delegate.nextRaw(results) : delegate.next(results);
            if (pageFilter == null) {
                return hasMore;
            }
            if (!hasMore) {
                if (pageFilter.isStopped()) {
                    delegate.close();
                    byte[] rowKey = pageFilter.getRowKeyAtStop();
                    scan.withStartRow(rowKey, true);
                    delegate = region.getScanner(scan);
                    if (results.isEmpty()) {
                        getDummyResult(rowKey, results);
                    }
                    pageFilter.init();
                    return true;
                }
                return false;
            } else {
                pageFilter.resetStartTime();
                return true;
            }
        } catch (Exception e) {
            pageFilter.init();
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
        return new PagedRegionScanner(region, region.getScanner(scan), scan);
    }
}
