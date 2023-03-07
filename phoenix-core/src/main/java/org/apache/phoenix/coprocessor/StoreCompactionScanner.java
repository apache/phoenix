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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The store scanner that implements Phoenix TTL and Max Lookback
 */
public class StoreCompactionScanner implements InternalScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(StoreCompactionScanner.class);
    private final InternalScanner storeScanner;
    private final Region region;
    private final Store store;
    private final Configuration config;
    private final RegionCoprocessorEnvironment env;
    private long maxLookbackWindowStart;
    private long ttlWindowStart;
    private int minVersion;
    private int maxVersion;
    private final boolean firstStore;
    private KeepDeletedCells keepDeletedCells;
    private long compactionTime;

    public StoreCompactionScanner(RegionCoprocessorEnvironment env,
                                Store store,
                                InternalScanner storeScanner,
                                long maxLookbackInMs) {
        this.storeScanner = storeScanner;
        this.region = env.getRegion();
        this.store = store;
        this.env = env;
        this.config = env.getConfiguration();
        compactionTime = EnvironmentEdgeManager.currentTimeMillis();
        this.maxLookbackWindowStart = compactionTime - maxLookbackInMs;
        ColumnFamilyDescriptor cfd = store.getColumnFamilyDescriptor();
        long ttl = cfd.getTimeToLive();
        this.ttlWindowStart = ttl == HConstants.FOREVER ? 1 : compactionTime - ttl * 1000;
        this.maxLookbackWindowStart = Math.max(ttlWindowStart, maxLookbackWindowStart);
        this.minVersion = cfd.getMinVersions();
        this.maxVersion = cfd.getMaxVersions();
        this.keepDeletedCells = cfd.getKeepDeletedCells();
        firstStore = region.getStores().get(0).getColumnFamilyName().
                equals(store.getColumnFamilyName());
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        boolean hasMore = storeScanner.next(result);
        filter(result, false);
        Collections.sort(result, CellComparator.getInstance());
        return hasMore;
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }

    @Override
    public void close() throws IOException {
        storeScanner.close();
    }
    private void formColumns(List<Cell> result, List<List<Cell>> columns,
            List<Cell> deleteMarkers) {
        Cell currentColumnCell = null;
        List<Cell> currentColumn = null;
        for (Cell cell : result) {
            if (cell.getType() != Cell.Type.Put) {
                deleteMarkers.add(cell);
            }
            if (currentColumnCell == null) {
                currentColumn = new ArrayList<>();
                currentColumnCell = cell;
                currentColumn.add(cell);
            }
            else if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
                    cell.getQualifierLength(),
                    currentColumnCell.getQualifierArray(), currentColumnCell.getQualifierOffset(),
                    currentColumnCell.getQualifierLength()) != 0) {
                columns.add(currentColumn);
                currentColumn = new ArrayList<>();
                currentColumnCell = cell;
                currentColumn.add(cell);
            }
            else {
                currentColumn.add(cell);
            }
        }
        if (currentColumn != null) {
            columns.add(currentColumn);
        }
    }

    /**
     * A row version that does not share a cell with any other row version is called a
     * compaction row version.
     * The latest live or deleted row version at the compaction time (compactionTime) is the first
     * compaction row version. The next row version which does not share a cell with the
     * first compaction row version is the next compaction row version.
     *
     * The first compaction row version is a valid row version (i.e., a row version at a given
     * time). The subsequent compactions row versions may not represent a valid row version if
     * the rows are updated partially.
     *
     * Compaction row versions are used for compaction purposes to determine which row versions to
     * retain.
     */
    class CompactionRowVersion {
        // Cells included in the row version
        List<Cell> cells = new ArrayList<>();
        // The timestamp of the row version
        long ts = 0;
        // The version of a row version. It is the minimum of the versions of the cells included
        // in the row version
        int version = 0;
    }

    class RowContext {
        Cell familyDeleteMarker = null;
        Cell familyVersionDeleteMarker = null;
        List<Cell> columnDeleteMarkers = null;
        int version = 0;
        private void addColumnDeleteMarker(Cell deleteMarker) {
            if (columnDeleteMarkers == null) {
                columnDeleteMarkers = new ArrayList<>();
            }
            columnDeleteMarkers.add(deleteMarker);
        }
    }

    private long getNextRowVersionTimestamp(List<List<Cell>> columns, RowContext rowContext) {
        long ts = 0;
        for (List<Cell> column : columns) {
            Cell firstCell = column.get(0);
            if (firstCell.getType() == Cell.Type.Put) {
                if (ts < firstCell.getTimestamp()) {
                    ts = firstCell.getTimestamp();
                }
            }
        }
        return ts;
    }
    /**
     * Decide if compaction row versions outside the max lookback window but inside the TTL window
     * should be retained. The retention rules are as follows.
     * 1. Live rows whose version is less than the max version are retained at the region
     * level compaction or if the store is the first store in the region (i.e., for the first colum
     * family).
     * 2. If the store is not the first store, compaction has to be done at the region level when
     * a live row versions exist. This is because we cannot calculate the actual row versions.
     * 3. All deleted rows are retained if KeepDeletedCells is TTL
     * 4. When KeepDeletedCells is TRUE, deleted rows whose version is less than max version are
     * retained at the region level or if the store is the first store in the region (i.e., for
     * the first column family).
     * 5. If the store is not the first store and compaction is done at the store level,
     * compaction has to be done at the region level when KeepDeletedCells is TRUE. This is because
     * we cannot calculate the actual row versions.
     *
     */
    private boolean retainOutsideMaxLookbackButInsideTTLWindow(List<Cell> result,
            CompactionRowVersion rowVersion, RowContext rowContext, boolean regionLevel) {
        if (firstStore || regionLevel) {
            if (rowContext.familyDeleteMarker == null &&
                    rowContext.familyVersionDeleteMarker == null) {
                // The compaction row version is alive
                if (rowVersion.version < maxVersion) {
                    // Rule 1
                    result.addAll(rowVersion.cells);
                }
            }
            else {
                // Deleted rows
                if ((rowVersion.version < maxVersion && keepDeletedCells == KeepDeletedCells.TRUE)
                        || keepDeletedCells == KeepDeletedCells.TTL) {
                    // Retain based on rule 3 or 4
                    result.addAll(rowVersion.cells);
                }
            }
        }
        else {
            // Store level compaction for the store that is not the first store
            if (rowContext.familyDeleteMarker == null &&
                    rowContext.familyVersionDeleteMarker == null) {
                // Rule 2
                return false;
            }
            if (keepDeletedCells == KeepDeletedCells.TTL) {
                // Retain base on rule 3
                result.addAll(rowVersion.cells);
            }
            else if (keepDeletedCells == KeepDeletedCells.TRUE) {
                // Rule 5
                return false;
            }
        }
        if (rowContext.familyVersionDeleteMarker != null) {
            // Set it to null so it will be used once
            rowContext.familyVersionDeleteMarker = null;
        }
        return true;
    }

    /**
     * Decide if compaction row versions outside the TTL window should be retained.
     * 1. Live rows whose version less than the min version are retained if The store level is
     * the first store in the region (i.e., for the first colum family)
     * 2. For the store that is not the first store, we cannot determine if live rows should be
     * retained at the store level. This is because we cannot calculate the actual row versions.
     * The calculated versions can be lower than the actual versions. In this case, the compaction
     * should be redone at the region level. The region level rules are the same as the first
     * store level rules.
     * 3. Delete rows whose delete markers are inside the TTL window and KeepDeletedCells is TTL
     * are retained regardless if the compaction is at the store or region level
     *
     */
    private boolean retainOutsideTTLWindow(List<Cell> result,
            CompactionRowVersion rowVersion, RowContext rowContext, boolean regionLevel) {
        if (firstStore || regionLevel) {
            if (rowContext.familyDeleteMarker == null &&
                    rowContext.familyVersionDeleteMarker == null) {
                // Live rows
                if (rowVersion.version < minVersion) {
                    // Rule 1
                    result.addAll(rowVersion.cells);
                }
            }
            else {
                // Delete rows
                if (keepDeletedCells == KeepDeletedCells.TTL) {
                    // Rule 2
                    result.addAll(rowVersion.cells);
                }
            }
        } else {
            if (rowContext.familyDeleteMarker == null &&
                    rowContext.familyVersionDeleteMarker == null) {
                // Rule 2
                return false;
            }
            if (keepDeletedCells == KeepDeletedCells.TTL) {
                // Rule 3
                result.addAll(rowVersion.cells);
            }
        }
        if (rowContext.familyVersionDeleteMarker != null) {
            // Set it to null so it will be used once
            rowContext.familyVersionDeleteMarker = null;
        }
        return true;
    }

    private boolean prepareResults(List<Cell> result, CompactionRowVersion rowVersion,
            RowContext rowContext, boolean regionLevel) {
        if (rowVersion.ts >= maxLookbackWindowStart) {
            // All rows within the max lookback window are retained
            result.addAll(rowVersion.cells);
            return true;
        }
        else if (rowVersion.ts >= ttlWindowStart) {
            return retainOutsideMaxLookbackButInsideTTLWindow(result, rowVersion, rowContext,
                    regionLevel);
        } else {
            return retainOutsideTTLWindow(result, rowVersion, rowContext, regionLevel);
        }
    }

    /**
     * Form the next compaction row version by picking the first cell from each column if the cell
     * is not a delete marker (Type.Delete or Type.DeleteColumn) until the first delete family or
     * delete family version column marker is visited
     * @param columns
     * @param rowContext
     * @return
     */
    private CompactionRowVersion formNextCompactionRowVersion(List<List<Cell>> columns,
            RowContext rowContext) {
        CompactionRowVersion rowVersion = null;
        long ts = getNextRowVersionTimestamp(columns, rowContext);
        boolean firstColumn = true;
        for (List<Cell> column : columns) {
            if (firstColumn) {
                firstColumn = false;
                Cell cell = column.get(0);
                if (cell.getType() == Cell.Type.DeleteFamily) {
                    if (cell.getTimestamp() >= ts) {
                        rowContext.familyDeleteMarker = cell;
                        column.remove(0);
                        continue;
                    }
                }
                else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
                    if (cell.getTimestamp() >= ts) {
                        rowContext.familyVersionDeleteMarker = cell;
                        column.remove(0);
                        continue;
                    }
                }
            }
            Cell firstCell = column.remove(0);
            if (firstCell.getType() == Cell.Type.DeleteColumn ||
                    firstCell.getType() == Cell.Type.Delete) {
                rowContext.addColumnDeleteMarker(firstCell);
            }
            else if (rowVersion == null) {
                rowVersion = new CompactionRowVersion();
                rowVersion.ts = ts;
                rowVersion.cells.add(firstCell);
                rowVersion.version = rowContext.version++;
            } else {
                rowVersion.cells.add(firstCell);
            }
        }
        return rowVersion;
    }

    private boolean formCompactionRowVersions(List<List<Cell>> columns,
            List<Cell> result,
            boolean regionLevel) {
        RowContext rowContext = new RowContext();
        while (!columns.isEmpty()) {
            CompactionRowVersion compactionRowVersion =
                    formNextCompactionRowVersion(columns, rowContext);
            if (compactionRowVersion != null) {
                if (!prepareResults(result, compactionRowVersion, rowContext, regionLevel)) {
                    return false;
                }
            }
            // Remove the columns that are empty
            int columnIndex = 0;
            while (!columns.isEmpty() && columnIndex < columns.size()) {
                if (columns.get(columnIndex).isEmpty()) {
                    columns.remove(columnIndex);
                } else {
                    columnIndex++;
                }
            }
        }
        return true;
    }
    /**
     * Filter {@link Cell}s from the underlying store scanner
     *
     * @param result
     *            next batch of {@link Cell}s
     * @throws IOException 
     */
    private void filter(List<Cell> result, boolean regionLevel) throws IOException {
        if (result.isEmpty()) {
            return;
        }
        Cell firstCell = result.get(0);
        byte[] rowKey = Bytes.copy(firstCell.getRowArray(), firstCell.getRowOffset(),
                firstCell.getRowLength());
        List<List<Cell>> columns = new ArrayList<>();
        List<Cell> deleteMarkers = new ArrayList<>();
        formColumns(result, columns, deleteMarkers);
        result.clear();
        if (!formCompactionRowVersions(columns, result, regionLevel)) {
            filterRegionLevel(result, rowKey);
        }
        for (Cell cell : deleteMarkers) {
            if (cell.getTimestamp() >= maxLookbackWindowStart) {
                result.add(cell);
            }
            else if (cell.getTimestamp() >= ttlWindowStart) {
                if (keepDeletedCells != KeepDeletedCells.FALSE) {
                    result.add(cell);
                }
            }
        }
    }

    private void filterRegionLevel(List<Cell> result, byte[] rowKey) throws IOException {
        Scan scan = new Scan();
        scan.setRaw(true);
        scan.readAllVersions();
        scan.setTimeRange(0, compactionTime);
        scan.withStartRow(rowKey, true);
        scan.withStopRow(rowKey, true);
        result.clear();
        RegionScanner scanner = region.getScanner(scan);
        List<Cell> regionResults = new ArrayList<>(result.size());
        scanner.next(regionResults);
        scanner.close();
        filter(regionResults, true);
        byte[] familyName = store.getColumnFamilyDescriptor().getName();
        for (Cell cell : regionResults) {
            if (Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(),
                    cell.getFamilyLength(),
                    familyName, 0, familyName.length) == 0) {
                result.add(cell);
            }
        }
    }
}
