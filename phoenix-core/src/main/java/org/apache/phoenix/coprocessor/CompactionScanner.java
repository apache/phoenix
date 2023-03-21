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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.query.QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX;

/**
 * The store scanner that implements Phoenix TTL and Max Lookback. Phoenix overrides the
 * implementation data retention policies in HBase which is built at the cell and implements
 * its row level data retention within this store scanner.
 */
public class CompactionScanner implements InternalScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionScanner.class);
    public static final String SEPARATOR = ":";
    private final InternalScanner storeScanner;
    private final Region region;
    private final Store store;
    private final Configuration config;
    private final RegionCoprocessorEnvironment env;
    private long maxLookbackWindowStart;
    private long ttlWindowStart;
    private long ttl;
    private int minVersion;
    private int maxVersion;
    private final boolean emptyCFStore;
    private KeepDeletedCells keepDeletedCells;
    private long compactionTime;
    private static Map<String, Long> maxLookbackMap = new ConcurrentHashMap<>();

    public CompactionScanner(RegionCoprocessorEnvironment env,
                                Store store,
                                InternalScanner storeScanner,
                                long maxLookbackInMillis,
                                byte[] emptyCF) {
        this.storeScanner = storeScanner;
        this.region = env.getRegion();
        this.store = store;
        this.env = env;
        this.config = env.getConfiguration();
        compactionTime = EnvironmentEdgeManager.currentTimeMillis();
        String columnFamilyName = store.getColumnFamilyName();
        String tableName = region.getRegionInfo().getTable().getNameAsString();
        Long overriddenMaxLookback =
                maxLookbackMap.remove(tableName + SEPARATOR + columnFamilyName);
        this.maxLookbackWindowStart = compactionTime - (overriddenMaxLookback == null ?
                maxLookbackInMillis : Math.max(maxLookbackInMillis, overriddenMaxLookback));
        ColumnFamilyDescriptor cfd = store.getColumnFamilyDescriptor();
        ttl = cfd.getTimeToLive();
        this.ttlWindowStart = ttl == HConstants.FOREVER ? 1 : compactionTime - ttl * 1000;
        ttl *= 1000;
        this.maxLookbackWindowStart = Math.max(ttlWindowStart, maxLookbackWindowStart);
        this.minVersion = cfd.getMinVersions();
        this.maxVersion = cfd.getMaxVersions();
        this.keepDeletedCells = cfd.getKeepDeletedCells();
        emptyCFStore = region.getTableDescriptor().getColumnFamilies().length == 1 ||
                columnFamilyName.equals(Bytes.toString(emptyCF)) ||
                columnFamilyName.startsWith(LOCAL_INDEX_COLUMN_FAMILY_PREFIX);
    }

    /**
     * Any coprocessors within a JVM can extend the max lookback window for a column family
     * by calling this static method.
     */
    public static void overrideMaxLookback(String tableName, String columnFamilyName,
            long maxLookbackInMillis) {
        if (tableName == null || columnFamilyName == null) {
            return;
        }
        Long old = maxLookbackMap.putIfAbsent(tableName + SEPARATOR + columnFamilyName,
                maxLookbackInMillis);
        if (old == null || old < maxLookbackInMillis) {
            maxLookbackMap.put(columnFamilyName, maxLookbackInMillis);
        }
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        boolean hasMore = storeScanner.next(result);
        if (!result.isEmpty()) {
            filter(result, false);
        }
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

    /**
     * The cells of row (i.e., result) read from HBase store are lexographically ordered for user
     * tables using the key part of the cells which includes row, family, qualifier,
     * timestamp and type. The cells belong of a column are ordered from the latest to the oldest.
     * The method leverages this ordering and groups the cells into their columns.
     * columns.
     */
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
            else if (!CellUtil.matchingQualifier(cell, currentColumnCell)) {
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
     * A compaction row version includes the latest put cell versions from each column such that
     * the cell versions do not cross delete family markers. In other words, the compaction row
     * versions are built from cell versions that are all either before or after the next delete
     * family or delete family version maker if family delete markers exist. Also, when the cell
     * timestamps are ordered for a given row version, the difference between two subsequent
     * timestamps has to be less than the ttl value.
     *
     * Compaction row versions are disjoint sets. A compaction row version does not share a cell
     * version with the next compaction row version. A compaction row version includes at most
     * one cell version from a column.
     *
     * After creating the first compaction row version, we form the next compaction row version
     * from the remaining cell versions.
     *
     * Compaction row versions are used for compaction purposes to determine which row versions
     * to retain. With the compaction row version concept, we can apply HBase data retention
     * parameters to the compaction process at the Phoenix level.
     */
    class CompactionRowVersion {
        // Cells included in the row version
        List<Cell> cells = new ArrayList<>();
        // The timestamp of the row version
        long ts = 0;
        // The version of a row version. It is the minimum of the versions of the cells included
        // in the row version
        int version = 0;
        @Override
        public String toString() {
            StringBuilder output = new StringBuilder();
            output.append("Cell count: " + cells.size() + "\n");
            for (Cell cell : cells) {
                output.append(cell + "\n");
            }
            output.append("ts:" + ts + " v:" + version);
            return output.toString();
        }
    }

    /**
     * The context for a given row during compaction. A row may have multiple compaction row
     * versions. CompactionScanner uses the same row context for these versions.
     */
    class RowContext {
        Cell familyDeleteMarker = null;
        Cell familyVersionDeleteMarker = null;
        List<Cell> columnDeleteMarkers = null;
        int version = 0;
        long maxTimestamp;
        long minTimestamp;
        private void addColumnDeleteMarker(Cell deleteMarker) {
            if (columnDeleteMarkers == null) {
                columnDeleteMarkers = new ArrayList<>();
            }
            columnDeleteMarkers.add(deleteMarker);
        }
    }

    /**
     * This method finds out the maximum and minimum timestamp of the cells of the next row
     * version.
     *
     * @param columns
     * @param rowContext
     */
    private void getNextRowVersionTimestamp(List<List<Cell>> columns, RowContext rowContext) {
        rowContext.maxTimestamp = 0;
        rowContext.minTimestamp = Long.MAX_VALUE;
        long ts;
        long currentDeleteFamilyTimestamp = 0;
        long nextDeleteFamilyTimestamp = 0;
        int columnIndex = 0;
        for (List<Cell> column : columns) {
            Cell firstCell = column.get(0);
            ts = firstCell.getTimestamp();
            if (ts <= nextDeleteFamilyTimestamp) {
                continue;
            }
            if (firstCell.getType() == Cell.Type.DeleteFamily ||
                    firstCell.getType() == Cell.Type.DeleteFamilyVersion) {
                if (columnIndex == 0) {
                    // Family delete markers are always found in the first column of a column family
                    // When Phoenix deletes a row, it places a family delete marker in each column
                    // family with the same timestamp. We just need to process the delete column
                    // family markers of the first column family, which would be in the column
                    // with columnIndex=0.
                    currentDeleteFamilyTimestamp = firstCell.getTimestamp();
                    // We need to check if the next delete family marker exits. If so, we need
                    // to record its timestamp as by definition a compaction row version cannot
                    // cross a family delete marker
                    if (column.size() > 1) {
                        nextDeleteFamilyTimestamp = column.get(1).getTimestamp();
                    } else {
                        nextDeleteFamilyTimestamp = 0;
                    }
                }
            } if (firstCell.getType() == Cell.Type.Put) {
                // Compaction row versions are constructed from put cells. So, we use only
                // put cell timestamps to find the time range for a compaction row version
                if (rowContext.maxTimestamp < ts) {
                    rowContext.maxTimestamp = ts;
                }
                if (currentDeleteFamilyTimestamp != 0 && currentDeleteFamilyTimestamp < rowContext.maxTimestamp) {
                    // A compaction row version do not cross a family delete marker. This means
                    // min timestamp cannot be lower than currentDeleteFamilyTimestamp
                    rowContext.minTimestamp = currentDeleteFamilyTimestamp + 1;
                } else if (rowContext.minTimestamp > ts) {
                    rowContext.minTimestamp = ts;
                }
            }
            columnIndex++;
        }

        // The gap between two back to back mutations if more than ttl then we know that the row
        // is expired before the newer mutation. If the length of the time range of a compaction
        // row version is not more than ttl, then we know the mutations covered by the next
        // compaction row version are not apart from each other more than ttl.
        if (rowContext.maxTimestamp - rowContext.minTimestamp <= ttl) {
            return;
        }
        // The quick time range check did not pass. We need sort the timestamps and check
        // the gaps one by one and determine which cells should be covered by the next
        // compaction row version
        List<Long> tsList = new ArrayList<>(columnIndex);
        for (List<Cell> column : columns) {
            Cell firstCell = column.get(0);
            if (firstCell.getType() == Cell.Type.Put) {
                tsList.add(firstCell.getTimestamp());
            }
        }
        Collections.sort(tsList);
        long previous = rowContext.minTimestamp;
        for (Long timestamp : tsList) {
            if (timestamp - previous > ttl) {
                rowContext.minTimestamp = timestamp;
            }
            previous = timestamp;
        }
    }
    /**
     * Decide if compaction row versions outside the max lookback window but inside the TTL window
     * should be retained. The rows that are retained are added to result.
     *
     * If the store is not the empty column family store and one of the following conditions holds,
     * then we need to do region level compaction.  This is because we cannot calculate the actual
     * row versions (we cannot determine if these store level row versions are part of which
     * region level row versions). In this case, this method returns false.
     * 1. The compaction row version is alive
     * 2. The compaction row version is deleted and KeepDeletedCells is not TTL
     *
     * If the compaction can be done at store level (because none of the above conditions hold or
     * the compaction is done at the region level), and one of the following conditions holds then
     * the cells of the compaction row version are retained
     * 3 The compaction row version is alive and its version is less than VERSIONS
     * 4. The compaction row version is deleted and KeepDeletedCells is TTL
     * 5. The compaction row version is deleted, its version is less than MIN_VERSIONS and
     * KeepDeletedCells is TRUE
     * 6. The compaction row version is deleted, its version is zero, and the delete marker is
     *    inside the max lookback window. (Note this is the first compaction row version outside
     *    the max lookback window. This version should be visible through the scn connection with
     *    the timestamp >= the delete marker's timestamp
     *
     */
    private boolean retainOutsideMaxLookbackButInsideTTLWindow(List<Cell> result,
            CompactionRowVersion rowVersion, RowContext rowContext, boolean regionLevel) {
        // Decide if the compaction should be done at the region level
        if (!emptyCFStore && !regionLevel) {
            if (rowContext.familyDeleteMarker == null
                    && rowContext.familyVersionDeleteMarker == null) {
                // Rule 1
                return false;
            }
            if (keepDeletedCells != KeepDeletedCells.TTL) {
                // Rule 2
                return false;
            }
        }
        // We can do compaction at the store level
        if (rowContext.familyDeleteMarker == null && rowContext.familyVersionDeleteMarker == null) {
            // The compaction row version is alive
            if (rowVersion.version < maxVersion) {
                // Rule 3
                result.addAll(rowVersion.cells);
            }
        } else {
            // Deleted
            if ((rowVersion.version < maxVersion && keepDeletedCells == KeepDeletedCells.TRUE) ||
                    keepDeletedCells == KeepDeletedCells.TTL) {
                // Retain based on rule 4 or 5
                result.addAll(rowVersion.cells);
            } else if (maxLookbackWindowStart != compactionTime && rowVersion.version == 0 &&
                    (rowContext.familyVersionDeleteMarker != null &&
                    rowContext.familyVersionDeleteMarker.getTimestamp() > maxLookbackWindowStart) ||
                    (rowContext.familyDeleteMarker != null &&
                            rowContext.familyDeleteMarker.getTimestamp() > maxLookbackWindowStart)) {
                // Retained based on rule 6
                result.addAll(rowVersion.cells);
            }
        }
        if (rowContext.familyVersionDeleteMarker != null) {
            // Set it to null so it will be used once
            rowContext.familyVersionDeleteMarker = null;
        }
        return true;
    }

    /**
     * Decide if compaction row versions outside the TTL window should be retained. The rows that
     * are retained are added to result.
     *
     * If the store is not the empty column family store and one of the following conditions holds,
     * then we need to do region level compaction.  This is because we cannot calculate the actual
     * row versions (we cannot determine if these store level row versions are part of which
     * region level row versions). In this case, this method returns false.
     *
     * 1. The compaction row version is alive
     * 2. The compaction row version is deleted, KeepDeletedCells is TRUE, and the delete family
     *    marker is not outside the TTL window
     *
     * If the compaction can be done at store level because none of the above conditions hold or
     * the compaction is done at the region level, and one of the following conditions holds then
     * the cells of the compaction row version are retained
     *
     * 3. Live row versions less than MIN_VERSIONS are retained
     * 4. Delete row versions whose delete markers are inside the TTL window and
     *    KeepDeletedCells is TTL are retained
     * 5. The compaction row version is deleted, its version is zero, and the delete marker is
     *    inside the max lookback window. (Note this is the first compaction row version outside
     *    the max lookback window. This version should be visible through the scn connection with
     *    the timestamp >= the delete marker's timestamp
     */
    private boolean retainOutsideTTLWindow(List<Cell> result,
            CompactionRowVersion rowVersion, RowContext rowContext, boolean regionLevel) {
        // Decide if the compaction should be done at the region level
        if (!emptyCFStore && !regionLevel) {
            if (rowContext.familyDeleteMarker == null
                    && rowContext.familyVersionDeleteMarker == null) {
                // Rule 1
                return false;
            }
            if (keepDeletedCells == KeepDeletedCells.TRUE && (
                    (rowContext.familyVersionDeleteMarker != null &&
                            rowContext.familyVersionDeleteMarker.getTimestamp() >= ttlWindowStart) ||
                            (rowContext.familyDeleteMarker != null &&
                                    rowContext.familyDeleteMarker.getTimestamp() >= ttlWindowStart)
            )) {
                // Rule 2
                return false;
            }
        }
        // We can do compaction at the store level
        if (rowContext.familyDeleteMarker == null
                && rowContext.familyVersionDeleteMarker == null) {
            // Live compaction row version
            if (rowVersion.version < minVersion) {
                // Rule 3
                result.addAll(rowVersion.cells);
            }
        } else {
            // Deleted compaction row version
            if (keepDeletedCells == KeepDeletedCells.TTL && (
                    (rowContext.familyVersionDeleteMarker != null &&
                    rowContext.familyVersionDeleteMarker.getTimestamp() > ttlWindowStart) ||
                            (rowContext.familyDeleteMarker != null &&
                                    rowContext.familyDeleteMarker.getTimestamp() > ttlWindowStart)
            )) {
                // Rule 4
                result.addAll(rowVersion.cells);
            } else if (maxLookbackWindowStart != compactionTime && rowVersion.version == 0 &&
                    (rowContext.familyVersionDeleteMarker != null &&
                            rowContext.familyVersionDeleteMarker.getTimestamp() > maxLookbackWindowStart) ||
                    (rowContext.familyDeleteMarker != null &&
                            rowContext.familyDeleteMarker.getTimestamp() > maxLookbackWindowStart)) {
                // Rule 6
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
            if (rowContext.familyVersionDeleteMarker != null) {
                // Set it to null so it will be used once
                rowContext.familyVersionDeleteMarker = null;
            }
            return true;
        }
        else if (rowVersion.ts >= ttlWindowStart) {
            return retainOutsideMaxLookbackButInsideTTLWindow(result, rowVersion, rowContext,
                    regionLevel);
        } else {
            return retainOutsideTTLWindow(result, rowVersion, rowContext, regionLevel);
        }
    }

    private boolean shouldRetainCell(RowContext rowContext, Cell cell, boolean regionLevel) {
        if (rowContext.columnDeleteMarkers == null) {
            return true;
        }
        int i = 0;
        for (Cell dm : rowContext.columnDeleteMarkers) {
            if (cell.getTimestamp() > dm.getTimestamp()) {
                continue;
            }
            if ((!regionLevel || CellUtil.matchingFamily(cell, dm)) &&
                    CellUtil.matchingQualifier(cell, dm)){
                if (dm.getType() == Cell.Type.Delete) {
                    // Delete is for deleting a specific cell version. Thus, it can be used
                    // to delete only one cell.
                    rowContext.columnDeleteMarkers.remove(i);
                }
                if (rowContext.maxTimestamp >= maxLookbackWindowStart) {
                    // Inside the max lookback window
                    return true;
                }
                if (rowContext.maxTimestamp >= ttlWindowStart) {
                    // Outside the max lookback window but inside the TTL window
                    if (keepDeletedCells == KeepDeletedCells.FALSE &&
                            dm.getTimestamp() < maxLookbackWindowStart ) {
                        return false;
                    }
                }
                if (keepDeletedCells == KeepDeletedCells.TTL &&
                        dm.getTimestamp() >= ttlWindowStart) {
                    return true;
                }
                if (maxLookbackWindowStart != compactionTime &&
                        dm.getTimestamp() > maxLookbackWindowStart && rowContext.version == 0) {
                    // The delete marker is inside the max lookback window and this is the first
                    // cell version outside the max lookback window. This cell should be
                    // visible through the scn connection with the timestamp >= the delete
                    // marker's timestamp
                    return true;
                }
                return false;
            }
            i++;
        }
        return true;
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
            RowContext rowContext, boolean regionLevel) {
        CompactionRowVersion rowVersion = null;
        getNextRowVersionTimestamp(columns, rowContext);
        for (List<Cell> column : columns) {
            Cell cell = column.get(0);
            if (cell.getType() == Cell.Type.DeleteFamily) {
                if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                    rowContext.familyDeleteMarker = cell;
                    column.remove(0);
                }
                continue;
            }
            else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
                if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                    rowContext.familyVersionDeleteMarker = cell;
                    column.remove(0);
                }
                continue;
            }
            if (rowContext.maxTimestamp != 0 &&
                    column.get(0).getTimestamp() < rowContext.minTimestamp) {
                continue;
            }
            column.remove(0);
            if (cell.getType() == Cell.Type.DeleteColumn ||
                    cell.getType() == Cell.Type.Delete) {
                rowContext.addColumnDeleteMarker(cell);
                continue;
            }
            if (!shouldRetainCell(rowContext, cell, regionLevel)) {
                continue;
            }
            if (rowVersion == null) {
                rowVersion = new CompactionRowVersion();
                rowVersion.ts = rowContext.maxTimestamp;
                rowVersion.cells.add(cell);
                if (rowVersion.ts >= maxLookbackWindowStart) {
                    rowVersion.version = 0;
                } else {
                    rowVersion.version = rowContext.version++;
                }

            } else {
                rowVersion.cells.add(cell);
            }
        }
        return rowVersion;
    }

    private boolean formCompactionRowVersions(RowContext rowContext, List<List<Cell>> columns,
            List<Cell> result, boolean regionLevel) {
        while (!columns.isEmpty()) {
            CompactionRowVersion compactionRowVersion =
                    formNextCompactionRowVersion(columns, rowContext, regionLevel);
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
        List<List<Cell>> columns = new ArrayList<>();
        List<Cell> deleteMarkers = new ArrayList<>();
        formColumns(result, columns, deleteMarkers);
        List<Cell> input = new ArrayList<>(result);
        result.clear();
        RowContext rowContext = new RowContext();
        if (!formCompactionRowVersions(rowContext, columns, result, regionLevel)) {
            filterRegionLevel(input, result);
        }
        // Filter delete markers
        if (!deleteMarkers.isEmpty()) {
            int version = 0;
            Cell last = deleteMarkers.get(0);
            for (Cell cell : deleteMarkers) {
                if (cell.getType() != last.getType() || !CellUtil.matchingColumn(cell, last)) {
                    version = 0;
                    last = cell;
                }
                if (cell.getTimestamp() >= maxLookbackWindowStart) {
                    result.add(cell);
                } else if (cell.getTimestamp() >= ttlWindowStart) {
                    if (keepDeletedCells == KeepDeletedCells.TTL) {
                        result.add(cell);
                    } else if (keepDeletedCells == KeepDeletedCells.TRUE) {
                        if (version < maxVersion) {
                            version++;
                            result.add(cell);
                        }
                    } else if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                        result.add(cell);
                    }
                }
            }
        }
        Collections.sort(result, CellComparator.getInstance());
    }

    private static int compareQualifiers(Cell a, Cell b) {
        return Bytes.compareTo(a.getQualifierArray(), a.getQualifierOffset(),
                a.getQualifierLength(),
                b.getQualifierArray(), b.getQualifierOffset(), a.getQualifierLength());
    }

    private static int compareTypes(Cell a, Cell b) {
        Cell.Type aType = a.getType();
        Cell.Type bType = b.getType();

        if (aType == bType) {
            return 0;
        }
        if (aType == Cell.Type.DeleteFamily) {
            return -1;
        }
        if (bType == Cell.Type.DeleteFamily) {
            return 1;
        }
        if (aType == Cell.Type.DeleteFamilyVersion) {
            return -1;
        }
        if (bType == Cell.Type.DeleteFamilyVersion) {
            return 1;
        }
        if (aType == Cell.Type.DeleteColumn) {
            return -1;
        }
        return 1;
    }

    private void trimRegionResult(List<Cell> regionResult, List<Cell> input, List<Cell> result) {
        int index = 0;
        int size = regionResult.size();
        int compare = 0;

        for (Cell originalCell : input) {
            if (index == size) {
                break;
            }
            Cell regionCell = regionResult.get(index);
            while (!CellUtil.matchingFamily(originalCell, regionCell)) {
                index++;
                if (index == size) {
                    break;
                }
                regionCell = regionResult.get(index);
            }
            if (index == size) {
                break;
            }
            compare = compareTypes(originalCell, regionCell);
            while  (compare > 0) {
                index++;
                if (index == size) {
                    break;
                }
                regionCell = regionResult.get(index);
                if (!CellUtil.matchingFamily(originalCell, regionCell)) {
                    break;
                }
                compare = compareTypes(originalCell, regionCell);
            }
            if (index == size || !CellUtil.matchingFamily(originalCell, regionCell)) {
                break;
            }
            if (compare != 0) {
                continue;
            }
            compare = compareQualifiers(originalCell, regionCell);
            while  (compare > 0) {
                index++;
                if (index == size) {
                    break;
                }
                regionCell = regionResult.get(index);
                if (!CellUtil.matchingFamily(originalCell, regionCell)) {
                    break;
                }
                compare = compareQualifiers(originalCell, regionCell);
            }
            if (index == size || !CellUtil.matchingFamily(originalCell, regionCell)) {
                break;
            }
            if (compare != 0 || originalCell.getTimestamp() > regionCell.getTimestamp()) {
                continue;
            }
            while (originalCell.getTimestamp() < regionCell.getTimestamp()) {
                index++;
                if (index == size) {
                    break;
                }
                regionCell = regionResult.get(index);
                if (!CellUtil.matchingColumn(originalCell, regionCell)) {
                    break;
                }
            }
            if (index == size) {
                break;
            }
            if (!CellUtil.matchingColumn(originalCell, regionCell) ||
                    originalCell.getTimestamp() != regionCell.getTimestamp()) {
                continue;
            }
            result.add(originalCell);
            index++;
        }
    }
    private void filterRegionLevel(List<Cell> input, List<Cell> result) throws IOException {
        byte[] rowKey = CellUtil.cloneRow(input.get(0));
        Scan scan = new Scan();
        scan.setRaw(true);
        scan.readAllVersions();
        scan.setTimeRange(0, compactionTime);
        scan.withStartRow(rowKey, true);
        scan.withStopRow(rowKey, true);
        RegionScanner scanner = region.getScanner(scan);
        List<Cell> regionResult = new ArrayList<>(result.size());
        scanner.next(regionResult);
        scanner.close();
        filter(regionResult, true);
        result.clear();
        trimRegionResult(regionResult, input, result);

    }
}
