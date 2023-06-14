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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;

public abstract class ReadRepairScanner extends BaseRegionScanner {

    public Logger LOGGER;
    public RegionScanner scanner;
    public Scan scan;
    public RegionCoprocessorEnvironment env;
    public byte[] emptyCF;
    public byte[] emptyCQ;
    public Region region;
    public boolean hasMore;
    public long pageSizeMs;
    public long pageSize = Long.MAX_VALUE;
    public long rowCount = 0;
    public long maxTimestamp;
    public long ageThreshold;
    public boolean initialized = false;

    public ReadRepairScanner(RegionCoprocessorEnvironment env, Scan scan, RegionScanner scanner) {
        super(scanner);
        LOGGER = LoggerFactory.getLogger(this.getClass());
        this.env = env;
        this.scan = scan;
        this.scanner = scanner;
        region = env.getRegion();
        emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
        emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
        pageSizeMs = getPageSizeMsForRegionScanner(scan);
        maxTimestamp = scan.getTimeRange().getMax();
    }

    private void init() throws IOException {
        if (!initialized) {
            PageFilter pageFilter = ScanUtil.removePageFilter(scan);
            if (pageFilter != null) {
                System.out.println("page filter removed, page size set");
                pageSize = pageFilter.getPageSize();
                scanner.close();
                scanner = region.getScanner(scan);
            }
            initialized = true;
        }
    }

    /*
    Method which checks whether a row is VERIFIED (i.e. does not need repair).
     */
    abstract boolean verifyRow(List<Cell> row);

    /*
    Method which repairs the given row
     */
    abstract void repairRow(List<Cell> row) throws IOException;

    public boolean next(List<Cell> result, boolean raw) throws IOException {
        try {
            init();
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            do {
                if (raw) {
                    hasMore = scanner.nextRaw(result);
                } else {
                    hasMore = scanner.next(result);
                }
                if (result.isEmpty()) {
                    return hasMore;
                }
                if (isDummy(result)) {
                    return true;
                }
                Cell cell = result.get(0);
                if (verifyRowAndRepairIfNecessary(result)) {
                    break;
                }
                if (hasMore && (EnvironmentEdgeManager.currentTimeMillis() - startTime) >= pageSizeMs) {
                    byte[] rowKey = CellUtil.cloneRow(cell);
                    result.clear();
                    getDummyResult(rowKey, result);
                    return true;
                }
                // skip this row as it is invalid
                // if there is no more row, then result will be an empty list
            } while (hasMore);
            rowCount++;
            if (rowCount == pageSize) {
                return false;
            }
            return hasMore;
        } catch (Throwable t) {
            ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
            return false; // impossible
        }
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        return next(result, false);
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return next(result, true);
    }

    /**
     * Helper method to verifies and repairs the row if necessary.
     * @param cellList
     * @return true if the row is already VERIFIED, false if the row needed repair
     * @throws IOException
     */
    private boolean verifyRowAndRepairIfNecessary(List<Cell> cellList) throws IOException {
        // check if row is VERIFIED
        if (verifyRow(cellList)) {
            return true;
        }
        else {
            try {
                repairRow(cellList);
            } catch (IOException e) {
                LOGGER.warn("Row Repair failure on region {}.",
                        env.getRegionInfo().getRegionNameAsString());
                throw e;
            }

            if (cellList.isEmpty()) {
                return false;
            }
            return true;
        }
    }

    public boolean isEmptyColumn(Cell cell) {
        return Bytes.compareTo(
                cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                emptyCF, 0, emptyCF.length) == 0 &&
                Bytes.compareTo(
                    cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                    emptyCQ, 0, emptyCQ.length) == 0;
    }
}
