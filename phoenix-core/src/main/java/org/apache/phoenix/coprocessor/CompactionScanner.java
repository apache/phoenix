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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
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
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.query.QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX;

/**
 * The store scanner that implements Phoenix TTL and Max Lookback. Phoenix overrides the
 * HBase implementation of data retention policies which is built at the cell level, and implements
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
    private final long maxLookbackInMillis;
    private int minVersion;
    private int maxVersion;
    private final boolean emptyCFStore;
    private final boolean localIndex;
    private final int familyCount;
    private KeepDeletedCells keepDeletedCells;
    private long compactionTime;
    private final byte[] emptyCF;
    private final byte[] emptyCQ;
    private final byte[] storeColumnFamily;
    private static Map<String, Long> maxLookbackMap = new ConcurrentHashMap<>();
    private PhoenixLevelRowCompactor phoenixLevelRowCompactor;
    private HBaseLevelRowCompactor hBaseLevelRowCompactor;

    public CompactionScanner(RegionCoprocessorEnvironment env,
            Store store,
            InternalScanner storeScanner,
            long maxLookbackInMillis,
            byte[] emptyCF,
            byte[] emptyCQ,
            int phoenixTTL,
            boolean isSystemTable) {
        this.storeScanner = storeScanner;
        this.region = env.getRegion();
        this.store = store;
        this.env = env;
        this.emptyCF = emptyCF;
        this.emptyCQ = emptyCQ;
        this.config = env.getConfiguration();
        compactionTime = EnvironmentEdgeManager.currentTimeMillis();
        this.maxLookbackInMillis = maxLookbackInMillis;
        String columnFamilyName = store.getColumnFamilyName();
        storeColumnFamily = columnFamilyName.getBytes();
        String tableName = region.getRegionInfo().getTable().getNameAsString();
        Long overriddenMaxLookback =
                maxLookbackMap.remove(tableName + SEPARATOR + columnFamilyName);
        maxLookbackInMillis = overriddenMaxLookback == null ?
                maxLookbackInMillis : Math.max(maxLookbackInMillis, overriddenMaxLookback);
        // The oldest scn is current time - maxLookbackInMillis. Phoenix sets the scan time range
        // for scn queries [0, scn). This means that the maxlookback size should be
        // maxLookbackInMillis + 1 so that the oldest scn does not return empty row
        this.maxLookbackWindowStart = maxLookbackInMillis == 0 ?
                compactionTime : compactionTime - (maxLookbackInMillis + 1);
        ColumnFamilyDescriptor cfd = store.getColumnFamilyDescriptor();
        this.ttl = isSystemTable ? cfd.getTimeToLive() : phoenixTTL;
        this.ttlWindowStart = ttl == HConstants.FOREVER ? 1 : compactionTime - ttl * 1000;
        ttl *= 1000;
        this.maxLookbackWindowStart = Math.max(ttlWindowStart, maxLookbackWindowStart);
        this.minVersion = cfd.getMinVersions();
        this.maxVersion = cfd.getMaxVersions();
        this.keepDeletedCells = cfd.getKeepDeletedCells();
        familyCount = region.getTableDescriptor().getColumnFamilies().length;
        localIndex = columnFamilyName.startsWith(LOCAL_INDEX_COLUMN_FAMILY_PREFIX);
        emptyCFStore = familyCount == 1 || columnFamilyName.equals(Bytes.toString(emptyCF))
                        || localIndex;
        phoenixLevelRowCompactor = new PhoenixLevelRowCompactor();
        hBaseLevelRowCompactor = new HBaseLevelRowCompactor();
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
        if (old != null && old < maxLookbackInMillis) {
            maxLookbackMap.put(tableName + SEPARATOR + columnFamilyName, maxLookbackInMillis);
        }
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        boolean hasMore = storeScanner.next(result);
        if (!result.isEmpty()) {
            phoenixLevelRowCompactor.compact(result, false);
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
     * The context for a given row during compaction. A row may have multiple compaction row
     * versions. CompactionScanner uses the same row context for these versions.
     */
    static class RowContext {
        Cell familyDeleteMarker = null;
        Cell familyVersionDeleteMarker = null;
        List<Cell> columnDeleteMarkers = null;
        int version = 0;
        long maxTimestamp;
        long minTimestamp;
        private void addColumnDeleteMarker(Cell deleteMarker) {
            if (columnDeleteMarkers == null) {
                columnDeleteMarkers = new ArrayList<>();
                columnDeleteMarkers.add(deleteMarker);
                return;
            }
            int i = 0;
            // Replace the existing delete marker for the same column
            for (Cell cell : columnDeleteMarkers) {
                if (cell.getType() == deleteMarker.getType() &&
                        CellUtil.matchingColumn(cell, deleteMarker)) {
                    columnDeleteMarkers.remove(i);
                    break;
                }
                i++;
            }
            columnDeleteMarkers.add(deleteMarker);
        }

        private void retainFamilyDeleteMarker(List<Cell> retainedCells) {
            if (familyVersionDeleteMarker != null) {
                retainedCells.add(familyVersionDeleteMarker);
                // Set it to null so it will be used once
                familyVersionDeleteMarker = null;
            } else {
                retainedCells.add(familyDeleteMarker);
            }
        }
        /**
         * Based on the column delete markers decide if the cells should be retained. If a
         * deleted cell is retained, the delete marker is also retained.
         */
        private void retainCell(Cell cell, List<Cell> retainedCells,
                KeepDeletedCells keepDeletedCells, long ttlWindowStart) {
            int i = 0;
            for (Cell dm : columnDeleteMarkers) {
                if (cell.getTimestamp() > dm.getTimestamp()) {
                    continue;
                }
                if ((CellUtil.matchingFamily(cell, dm)) &&
                        CellUtil.matchingQualifier(cell, dm)) {
                    if (dm.getType() == Cell.Type.Delete) {
                        // Delete is for deleting a specific cell version. Thus, it can be used
                        // to delete only one cell.
                        columnDeleteMarkers.remove(i);
                    }
                    if (maxTimestamp >= ttlWindowStart) {
                        // Inside the TTL window
                        if (keepDeletedCells != KeepDeletedCells.FALSE ) {
                            retainedCells.add(cell);
                            retainedCells.add(dm);
                        }
                    } else if (keepDeletedCells == KeepDeletedCells.TTL &&
                            dm.getTimestamp() >= ttlWindowStart) {
                        retainedCells.add(cell);
                        retainedCells.add(dm);
                    }
                    return;
                }
                i++;
            }
            // No delete marker for this cell
            retainedCells.add(cell);
        }
        /**
         * This method finds out the maximum and minimum timestamp of the cells of the next row
         * version. Cells are organized into columns based on the pair of family name and column
         * qualifier. This means that the delete family markers for a column family will have their
         * own column. However, the delete column markers will be packed with the put cells. The cells
         * within a column are ordered in descending timestamps.
         */
        private void getNextRowVersionTimestamps(LinkedList<LinkedList<Cell>> columns,
                byte[] columnFamily) {
            maxTimestamp = 0;
            minTimestamp = Long.MAX_VALUE;
            Cell firstCell;
            LinkedList<Cell> deleteColumn = null;
            long ts;
            for (LinkedList<Cell> column : columns) {
                firstCell = column.getFirst();
                ts = firstCell.getTimestamp();
                if ((firstCell.getType() == Cell.Type.DeleteFamily ||
                        firstCell.getType() == Cell.Type.DeleteFamilyVersion) &&
                        CellUtil.matchingFamily(firstCell, columnFamily)) {
                    deleteColumn = column;
                }
                if (maxTimestamp < ts) {
                    maxTimestamp = ts;
                }
                if (minTimestamp > ts) {
                    minTimestamp = ts;
                }
            }
            if (deleteColumn != null) {
                // A row version do not cross a family delete marker. This means
                // min timestamp cannot be lower than the delete markers timestamp
                for (Cell cell : deleteColumn) {
                    ts = cell.getTimestamp();
                    if (ts < maxTimestamp) {
                        minTimestamp = ts + 1;
                        break;
                    }
                }
            }
        }
    }

    /**
     * HBaseLevelRowCompactor ensures that the cells of a given row are retained according to the
     * HBase data retention rules.
     *
     */
    class HBaseLevelRowCompactor {
        /**
         * A compaction row version includes the latest put cell versions from each column such that
         * the cell versions do not cross delete family markers. In other words, the compaction row
         * versions are built from cell versions that are all either before or after the next delete
         * family or delete family version maker if family delete markers exist. Also, when the cell
         * timestamps are ordered for a given row version, the difference between two subsequent
         * timestamps has to be less than the ttl value. This is taken care before calling
         * HBaseLevelRowCompactor#compact().
         *
         * Compaction row versions are disjoint sets. A compaction row version does not share a cell
         * version with the next compaction row version. A compaction row version includes at most
         * one cell version from a column.
         *
         * After creating the first compaction row version, we form the next compaction row version
         * from the remaining cell versions.
         *
         * Compaction row versions are used for compaction purposes to efficiently determine which
         * cell versions to retain based on the HBase data retention parameters.
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
         * Decide if compaction row versions inside the TTL window should be retained. The
         * versions are retained if one of the following conditions holds
         * 1. The compaction row version is alive and its version is less than VERSIONS
         * 2. The compaction row version is deleted and KeepDeletedCells is TTL
         * 3. The compaction row version is deleted, its version is less than MIN_VERSIONS and
         * KeepDeletedCells is TRUE
         *
         */
        private void retainInsideTTLWindow(CompactionRowVersion rowVersion, RowContext rowContext,
                List<Cell> retainedCells) {
            if (rowContext.familyDeleteMarker == null && rowContext.familyVersionDeleteMarker == null) {
                // The compaction row version is alive
                if (rowVersion.version < maxVersion) {
                    // Rule 1
                    retainCells(rowVersion, rowContext, retainedCells);
                }
            } else {
                // Deleted
                if ((rowVersion.version < maxVersion && keepDeletedCells == KeepDeletedCells.TRUE) ||
                        keepDeletedCells == KeepDeletedCells.TTL) {
                    // Retain based on rule 2 or 3
                    retainCells(rowVersion, rowContext, retainedCells);
                    rowContext.retainFamilyDeleteMarker(retainedCells);
                }
            }
        }

        /**
         * Decide if compaction row versions outside the TTL window should be retained. The
         * versions are retained if one of the following conditions holds
         *
         * 1. Live row versions less than MIN_VERSIONS are retained
         * 2. Delete row versions whose delete markers are inside the TTL window and
         *    KeepDeletedCells is TTL are retained
         */
        private void retainOutsideTTLWindow(CompactionRowVersion rowVersion, RowContext rowContext,
                List<Cell> retainedCells) {
            if (rowContext.familyDeleteMarker == null
                    && rowContext.familyVersionDeleteMarker == null) {
                // Live compaction row version
                if (rowVersion.version < minVersion) {
                    // Rule 1
                    retainCells(rowVersion, rowContext, retainedCells);
                }
            } else {
                // Deleted compaction row version
                if (keepDeletedCells == KeepDeletedCells.TTL && (
                        (rowContext.familyVersionDeleteMarker != null &&
                                rowContext.familyVersionDeleteMarker.getTimestamp() > ttlWindowStart) ||
                                (rowContext.familyDeleteMarker != null &&
                                        rowContext.familyDeleteMarker.getTimestamp() > ttlWindowStart)
                )) {
                    // Rule 2
                    retainCells(rowVersion, rowContext, retainedCells);
                    rowContext.retainFamilyDeleteMarker(retainedCells);
                }
            }
        }

        private void retainCells(CompactionRowVersion rowVersion, RowContext rowContext,
                List<Cell> retainedCells) {
            if (rowContext.columnDeleteMarkers == null) {
                retainedCells.addAll(rowVersion.cells);
                return;
            }
            for (Cell cell : rowVersion.cells) {
                rowContext.retainCell(cell, retainedCells, keepDeletedCells, ttlWindowStart);
            }
        }

        /**
         * Form the next compaction row version by picking (removing) the first cell from each
         * column. Put cells are used to form the next compaction row version. Delete markers
         * are added to the row context which are processed to decide which row versions
         * or cell version to delete.
         */
        private void formNextCompactionRowVersion(LinkedList<LinkedList<Cell>> columns,
                RowContext rowContext, List<Cell> retainedCells) {
            CompactionRowVersion rowVersion = new CompactionRowVersion();
            rowContext.getNextRowVersionTimestamps(columns, storeColumnFamily);
            rowVersion.ts = rowContext.maxTimestamp;
            rowVersion.version = rowContext.version++;
            for (LinkedList<Cell> column : columns) {
                Cell cell = column.getFirst();
                if (column.getFirst().getTimestamp() < rowContext.minTimestamp) {
                    continue;
                }
                if (cell.getType() == Cell.Type.DeleteFamily) {
                    if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                        rowContext.familyDeleteMarker = cell;
                        column.removeFirst();
                    }
                    continue;
                }
                else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
                    if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                        rowContext.familyVersionDeleteMarker = cell;
                        column.removeFirst();
                    }
                    continue;
                }
                column.removeFirst();
                if (cell.getType() == Cell.Type.DeleteColumn ||
                        cell.getType() == Cell.Type.Delete) {
                    rowContext.addColumnDeleteMarker(cell);
                    continue;
                }
                rowVersion.cells.add(cell);
            }
            if (rowVersion.cells.isEmpty()) {
                return;
            }
            if (rowVersion.ts >= ttlWindowStart) {
                retainInsideTTLWindow(rowVersion, rowContext, retainedCells);
            } else {
                retainOutsideTTLWindow(rowVersion, rowContext, retainedCells);
            }
        }

        private void formCompactionRowVersions(LinkedList<LinkedList<Cell>> columns,
                List<Cell> result) {
            RowContext rowContext = new RowContext();
            while (!columns.isEmpty()) {
                formNextCompactionRowVersion(columns, rowContext, result);
                // Remove the columns that are empty
                Iterator<LinkedList<Cell>> iterator = columns.iterator();
                while (iterator.hasNext()) {
                    LinkedList<Cell> column = iterator.next();
                    if (column.isEmpty()) {
                        iterator.remove();
                    }
                }
            }
        }

        /**
         * Group the cells that are ordered lexicographically into columns based on
         * the pair of family name and column qualifier. While doing that also add the delete
         * markers to a separate list.
         */
        private void formColumns(List<Cell> result, LinkedList<LinkedList<Cell>> columns,
                List<Cell> deleteMarkers) {
            Cell currentColumnCell = null;
            LinkedList<Cell> currentColumn = null;
            for (Cell cell : result) {
                if (cell.getType() != Cell.Type.Put) {
                    deleteMarkers.add(cell);
                }
                if (currentColumnCell == null) {
                    currentColumn = new LinkedList<>();
                    currentColumnCell = cell;
                    currentColumn.add(cell);
                } else if (!CellUtil.matchingColumn(cell, currentColumnCell)) {
                    columns.add(currentColumn);
                    currentColumn = new LinkedList<>();
                    currentColumnCell = cell;
                    currentColumn.add(cell);
                } else {
                    currentColumn.add(cell);
                }
            }
            if (currentColumn != null) {
                columns.add(currentColumn);
            }
        }

        /**
         * Compacts a single row at the HBase level. The result parameter is the input row and
         * modified to be the output of the compaction.
         */
        private void compact(List<Cell> result) {
            if (result.isEmpty()) {
                return;
            }
            LinkedList<LinkedList<Cell>> columns = new LinkedList<>();
            List<Cell> deleteMarkers = new ArrayList<>();
            formColumns(result, columns, deleteMarkers);
            result.clear();
            formCompactionRowVersions(columns, result);
        }
    }

    /**
     * PhoenixLevelRowCompactor ensures that the cells of the latest row version and the
     * row versions that are visible through the max lookback window are retained including delete
     * markers placed after these cells. This is the complete set of cells that Phoenix
     * needs for its queries. Beyond these cells, HBase retention rules may require more
     * cells to be retained. These cells are identified by the HBase level compaction implemented
     * by HBaseLevelRowCompactor.
     *
     */
    class PhoenixLevelRowCompactor {

        /**
         * The cells of the row (i.e., result) read from HBase store are lexicographically ordered
         * for tables using the key part of the cells which includes row, family, qualifier,
         * timestamp and type. The cells belong of a column are ordered from the latest to
         * the oldest. The method leverages this ordering and groups the cells into their columns
         * based on the pair of family name and column qualifier.
         *
         * The cells within the max lookback window except the once at the lower edge of the
         * max lookback window are retained immediately and not included in the constructed columns.
         */
        private void getColumns(List<Cell> result, LinkedList<LinkedList<Cell>> columns,
                List<Cell> retainedCells) {
            Cell currentColumnCell = null;
            LinkedList<Cell> currentColumn = null;
            for (Cell cell : result) {
                if (cell.getTimestamp() > maxLookbackWindowStart) {
                    retainedCells.add(cell);
                    continue;
                }
                if (currentColumnCell == null) {
                    currentColumn = new LinkedList<>();
                    currentColumnCell = cell;
                    currentColumn.add(cell);
                } else if (!CellUtil.matchingColumn(cell, currentColumnCell)) {
                    columns.add(currentColumn);
                    currentColumn = new LinkedList<>();
                    currentColumnCell = cell;
                    currentColumn.add(cell);
                } else {
                    currentColumn.add(cell);
                }
            }
            if (currentColumn != null) {
                columns.add(currentColumn);
            }
        }

        /**
         * Close the gap between the two timestamps, max and min, with the minimum number of cells
         * from the input list such that the timestamp difference between two cells should
         * not more than ttl. The cells that are used to close the gap are added to the output
         * list.
         */
        private void closeGap(long max, long min, List<Cell> input, List<Cell> output) {
            int  previous = -1;
            long ts;
            for (Cell cell : input) {
                ts = cell.getTimestamp();
                if (ts >= max) {
                    previous++;
                    continue;
                }
                if (previous == -1) {
                    break;
                }
                if (max - ts > ttl) {
                    max = input.get(previous).getTimestamp();
                    output.add(input.remove(previous));
                    if (max - min > ttl) {
                        closeGap(max, min, input, output);
                    }
                    return;
                }
                previous++;
            }
        }

        /**
         * Retain the last row version visible through the max lookback window
         */
        private void retainCellsOfLastRowVersion(LinkedList<LinkedList<Cell>> columns,
                List<Cell> retainedCells) {
            if (columns.isEmpty()) {
                return;
            }
            RowContext rowContext = new RowContext();
            rowContext.getNextRowVersionTimestamps(columns, storeColumnFamily);
            List<Cell> retainedPutCells = new ArrayList<>();
            for (LinkedList<Cell> column : columns) {
                Cell cell = column.getFirst();
                if (cell.getTimestamp() < rowContext.minTimestamp) {
                    continue;
                }
                if (cell.getType() == Cell.Type.Put) {
                    retainedPutCells.add(cell);
                } else if (cell.getType() == Cell.Type.DeleteFamily ||
                        cell.getType() == Cell.Type.DeleteFamilyVersion) {
                    if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                        // This means that the row version outside the max lookback window is
                        // deleted and thus should not be visible to the scn queries
                        if (cell.getTimestamp() == maxLookbackWindowStart) {
                            // Include delete markers at maxLookbackWindowStart
                            retainedCells.add(cell);
                        }
                        return;
                    }
                } else if (cell.getTimestamp() == maxLookbackWindowStart) {
                    // Include delete markers at maxLookbackWindowStart
                    retainedCells.add(cell);
                }
            }
            if (compactionTime - rowContext.maxTimestamp > maxLookbackInMillis + ttl) {
                // The row version should not be visible via the max lookback window. Nothing to do
                return;
            }
            retainedCells.addAll(retainedPutCells);
            // If the gap between two back to back mutations is more than ttl then the older
            // mutation will be considered expired and masked. If the length of the time range of
            // a row version is not more than ttl, then we know the cells covered by the row
            // version are not apart from each other more than ttl and will not be masked.
            if (rowContext.maxTimestamp - rowContext.minTimestamp <= ttl) {
                return;
            }
            // The quick time range check did not pass. We need get at least one empty cell to cover
            // the gap so that the row version will not be masked by PhoenixTTLRegionScanner.
            List<Cell> emptyCellColumn = null;
            for (LinkedList<Cell> column : columns) {
                if (ScanUtil.isEmptyColumn(column.getFirst(), emptyCF, emptyCQ)) {
                    emptyCellColumn = column;
                    break;
                }
            }
            if (emptyCellColumn == null) {
                return;
            }
            int size = retainedPutCells.size();
            long tsArray[] = new long[size];
            int i = 0;
            for (Cell cell : retainedPutCells) {
                tsArray[i++] = cell.getTimestamp();
            }
            Arrays.sort(tsArray);
            for (i = size - 1; i > 0; i--) {
                if (tsArray[i] - tsArray[i - 1] > ttl) {
                    closeGap(tsArray[i], tsArray[i - 1], emptyCellColumn, retainedCells);
                }
            }
        }

        private boolean retainCellsForMaxLookback(List<Cell> result, boolean regionLevel,
                List<Cell> retainedCells) {
            LinkedList<LinkedList<Cell>> columns = new LinkedList<>();
            getColumns(result, columns, retainedCells);
            long maxTimestamp = 0;
            long minTimestamp = Long.MAX_VALUE;
            long ts;
            for (LinkedList<Cell> column : columns) {
                ts = column.getFirst().getTimestamp();
                if (ts > maxTimestamp) {
                    maxTimestamp = ts;
                }
                ts = column.getLast().getTimestamp();
                if (ts < minTimestamp) {
                    minTimestamp = ts;
                }
            }
            if (compactionTime - maxTimestamp > maxLookbackInMillis + ttl) {
                if (!emptyCFStore && !regionLevel) {
                    // The row version is more than maxLookbackInMillis + ttl old. We cannot decide
                    // if we should retain it with the store level compaction when the current
                    // store is not the empty column family store.
                    return false;
                }
                return true;
            }
            // If the time gap between two back to back mutations is more than ttl then we know
            // that the row is expired within the time gap.
            if (maxTimestamp - minTimestamp > ttl) {
                if ((familyCount > 1 && !regionLevel && !localIndex)) {
                    // When there are more than one column family for a given table and a row
                    // version constructed at the store level covers a time span larger than ttl,
                    // we need region level compaction to see if the other stores have more cells
                    // for any of these large time gaps. A store level compaction may incorrectly
                    // remove some cells due to a large time gap which may not there at the region
                    // level.
                    return false;
                }
                // We either have one column family or are doing region level compaction. In both
                // case, we can safely trim the cells beyond the first time gap larger ttl
                int size = result.size();
                long tsArray[] = new long[size];
                int i = 0;
                for (Cell cell : result) {
                    tsArray[i++] = cell.getTimestamp();
                }
                Arrays.sort(tsArray);
                boolean gapFound = false;
                // Since timestamps are sorted in ascending order, traverse them in reverse order
                for (i = size - 1; i > 0; i--) {
                    if (tsArray[i] - tsArray[i - 1] > ttl) {
                        minTimestamp = tsArray[i];
                        gapFound = true;
                        break;
                    }
                }
                if (gapFound) {
                    List<Cell> trimmedResult = new ArrayList<>(size - i);
                    for (Cell cell : result) {
                        if (cell.getTimestamp() >= minTimestamp) {
                            trimmedResult.add(cell);
                        }
                    }
                    columns.clear();
                    retainedCells.clear();
                    getColumns(trimmedResult, columns, retainedCells);
                }
            }
            retainCellsOfLastRowVersion(columns, retainedCells);
            return true;
        }
        /**
         * Compacts a single row at the Phoenix level. The result parameter is the input row and
         * modified to be the output of the compaction process.
         */
        private void compact(List<Cell> result, boolean regionLevel) throws IOException {
            if (result.isEmpty()) {
                return;
            }
            List<Cell> phoenixResult = new ArrayList<>(result.size());
            if (!retainCellsForMaxLookback(result, regionLevel, phoenixResult)) {
                if (familyCount == 1 || regionLevel) {
                    throw new RuntimeException("UNEXPECTED");
                }
                phoenixResult.clear();
                compactRegionLevel(result, phoenixResult);
            }
            if (maxVersion == 1 &&  minVersion == 0 && keepDeletedCells == KeepDeletedCells.FALSE) {
                // We need to Phoenix level compaction only
                Collections.sort(phoenixResult, CellComparator.getInstance());
                result.clear();
                result.addAll(phoenixResult);
                return;
            }
            // We may need to do retain more cells, and so we need to run HBase level compaction
            // too. The result of two compactions will be merged and duplicate cells are removed.
            int phoenixResultSize = phoenixResult.size();
            List<Cell> hbaseResult = new ArrayList<>(result);
            hBaseLevelRowCompactor.compact(hbaseResult);
            phoenixResult.addAll(hbaseResult);
            Collections.sort(phoenixResult, CellComparator.getInstance());
            result.clear();
            Cell previousCell = null;
            // Eliminate duplicates
            for (Cell cell : phoenixResult) {
                if (previousCell == null ||
                        cell.getTimestamp() != previousCell.getTimestamp() ||
                        cell.getType() != previousCell.getType() ||
                        !CellUtil.matchingColumn(cell, previousCell)) {
                    result.add(cell);
                }
                previousCell = cell;
            }

            if (result.size() > phoenixResultSize) {
                LOGGER.debug("HBase level compaction retained " +
                        (result.size() - phoenixResultSize) + " more cells");
            }
        }

        private int compareTypes(Cell a, Cell b) {
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

        private int compare(Cell a, Cell b) {
            int result;
            result = Bytes.compareTo(a.getFamilyArray(), a.getFamilyOffset(),
                    a.getFamilyLength(),
                    b.getFamilyArray(), b.getFamilyOffset(), b.getFamilyLength());
            if (result != 0) {
                return result;
            }
            result = Bytes.compareTo(a.getQualifierArray(), a.getQualifierOffset(),
                    a.getQualifierLength(),
                    b.getQualifierArray(), b.getQualifierOffset(), b.getQualifierLength());
            if (result != 0) {
                return result;
            }
            if (a.getTimestamp() > b.getTimestamp()) {
                return -1;
            }
            if (a.getTimestamp() < b.getTimestamp()) {
                return 1;
            }
            return compareTypes(a, b);
        }

        /**
         * The generates the intersection of regionResult and input. The result is the resulting
         * intersection.
         */
        private void trimRegionResult(List<Cell> regionResult, List<Cell> input,
                List<Cell> result) {
            if (regionResult.isEmpty()) {
                return;
            }
            int index = 0;
            int size = regionResult.size();
            int compare;
            for (Cell originalCell : input) {
                Cell regionCell = regionResult.get(index);
                compare = compare(originalCell, regionCell);
                while (compare > 0) {
                    index++;
                    if (index == size) {
                        break;
                    }
                    regionCell = regionResult.get(index);
                    compare = compare(originalCell, regionCell);
                }
                if (compare == 0) {
                    result.add(originalCell);
                    index++;
                }
                if (index == size) {
                    break;
                }
            }
        }

        /**
         * This is used only when the Phoenix level compaction cannot be done at the store level.
         */
        private void compactRegionLevel(List<Cell> input, List<Cell> result) throws IOException {
            byte[] rowKey = CellUtil.cloneRow(input.get(0));
            Scan scan = new Scan();
            scan.setRaw(true);
            scan.readAllVersions();
            // compaction + 1 because the upper limit of the time range is not inclusive
            scan.setTimeRange(0, compactionTime + 1);
            scan.withStartRow(rowKey, true);
            scan.withStopRow(rowKey, true);
            RegionScanner scanner = region.getScanner(scan);
            List<Cell> regionResult = new ArrayList<>(result.size());
            scanner.next(regionResult);
            scanner.close();
            Collections.sort(regionResult, CellComparator.getInstance());
            compact(regionResult, true);
            result.clear();
            trimRegionResult(regionResult, input, result);
        }
    }
}
