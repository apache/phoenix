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
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;
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
        this.minVersion = cfd.getMinVersions();
        this.maxVersion = cfd.getMaxVersions();
        this.keepDeletedCells = cfd.getKeepDeletedCells();
        firstStore = region.getStores().get(0).getColumnFamilyName().
                equals(store.getColumnFamilyName());
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        synchronized (storeScanner) {
            boolean hasMore = storeScanner.next(result);
            filter(result, true);
            Collections.sort(result, CellComparator.getInstance());
            return hasMore;
        }
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
        // The delete marker deleting this row version
        Cell deleteFamilyMarker = null;
        // Delete or DeleteColumn markers deleting a cell of this version
        List<Cell> columnDeleteMarkers = null;
        // The timestamp of the row version
        long ts = 0;
        // The version of a row version. It is the minimum of the versions of the cells included
        // in the row version
        int version = 0;
        private void addColumnDeleteMarker(Cell deleteMarker) {
            if (columnDeleteMarkers == null) {
                columnDeleteMarkers = new ArrayList<>();
            }
            columnDeleteMarkers.add(deleteMarker);
        }
    }

    private boolean isCellDeleted(List<Cell> deleteMarkers, CompactionRowVersion rowVersion,
            Cell cell, boolean storeLevel) {
        int i = 0;
        for (Cell dm : deleteMarkers) {
            if ((storeLevel ||
                    Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(),
                    cell.getFamilyLength(),
                    dm.getFamilyArray(), dm.getFamilyOffset(), dm.getFamilyLength()) == 0) &&
                    Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
                            cell.getQualifierLength(),
                            dm.getQualifierArray(), dm.getQualifierOffset(),
                            dm.getQualifierLength()) == 0) {
                if (dm.getType() == Cell.Type.Delete) {
                    deleteMarkers.remove(i);
                    if (rowVersion.columnDeleteMarkers == null) {
                        rowVersion.columnDeleteMarkers = new ArrayList<>();
                    }
                    rowVersion.columnDeleteMarkers.add(dm);
                }
                return true;
            }
            i++;
        }
        return false;
    }

    private long getNextRowVersionTimestamp(List<List<Cell>> columns) {
        long ts = 0;
        for (List<Cell> column : columns) {
            Cell firstCell = column.get(0);
            if (firstCell.getType() == Cell.Type.DeleteFamily ||
                    firstCell.getType() == Cell.Type.DeleteFamilyVersion) {
                break;
            }
            if (firstCell.getType() == Cell.Type.DeleteColumn ||
                    firstCell.getType() == Cell.Type.Delete) {
                continue;
            }
            if (ts < firstCell.getTimestamp()) {
                ts = firstCell.getTimestamp();
            }
        }
        return ts;
    }

    private boolean formRowVersions(List<List<Cell>> columns,
                                    List<CompactionRowVersion> rowVersions,
                                    boolean storeLevel) {
        Cell lastDeleteFamilyMarker = null;
        Cell lastDeleteFamilyVersionMarker = null;
        List<Cell> columnDeleteMarkers = null;
        int version = 0;
        while (!columns.isEmpty()) {
            long ts = getNextRowVersionTimestamp(columns);
            CompactionRowVersion rowVersion = null;
            // Form the next row version by picking the first cell from each column if the cell
            // is not masked by a delete marker
            for (List<Cell> column : columns) {
                Cell firstCell = column.remove(0);
                if (firstCell.getType() == Cell.Type.DeleteFamily ||
                        firstCell.getType() == Cell.Type.DeleteFamilyVersion) {
                    if (firstCell.getType() == Cell.Type.DeleteFamily) {
                        if (firstCell.getTimestamp() >= ttlWindowStart &&
                                keepDeletedCells == KeepDeletedCells.FALSE) {
                            // This family delete marker deletes the rest of the row versions.
                            // There is nothing left to do here
                            return true;
                        }
                        lastDeleteFamilyMarker = firstCell;
                    } else {
                        lastDeleteFamilyVersionMarker = firstCell;
                    }
                    break;
                }
                if (firstCell.getType() == Cell.Type.DeleteColumn ||
                        firstCell.getType() == Cell.Type.Delete) {
                    if (columnDeleteMarkers == null) {
                        columnDeleteMarkers = new ArrayList<>();
                    }
                    columnDeleteMarkers.add(firstCell);
                    if (rowVersion != null) {
                        rowVersion.addColumnDeleteMarker(firstCell);
                    }
                }
                else if (rowVersion == null) {
                    if (storeLevel && !firstStore && ts < ttlWindowStart &&
                            version < maxVersion - 1 && lastDeleteFamilyMarker == null) {
                        // For the stores that are not the first (or only) store of their region,
                        // we cannot determine if a live row version is outside the TTL window
                        // should be retained if the version of the row version is less than the max
                        // version. We need to do a row scan to at the region level in this case.
                        return false;
                    }
                    rowVersion = new CompactionRowVersion();
                    rowVersions.add(rowVersion);
                    rowVersion.ts = ts;
                    if (lastDeleteFamilyVersionMarker != null) {
                        rowVersion.deleteFamilyMarker = lastDeleteFamilyVersionMarker;
                        // The delete family version delete marker is consumed, so set it to null
                        lastDeleteFamilyVersionMarker = null;
                    } else if (lastDeleteFamilyMarker != null) {
                        rowVersion.deleteFamilyMarker = lastDeleteFamilyMarker;
                    }
                    if (columnDeleteMarkers != null) {
                        rowVersion.columnDeleteMarkers = new ArrayList<>(columnDeleteMarkers);
                    }
                    rowVersion.cells.add(firstCell);
                    rowVersion.version = version++;
                } else if (firstCell.getTimestamp() == ts) {
                    rowVersion.cells.add(firstCell);
                } else if (columnDeleteMarkers == null ||
                        !isCellDeleted(columnDeleteMarkers, rowVersion, firstCell, storeLevel)) {
                    rowVersion.cells.add(firstCell);
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

    private void addCells(CompactionRowVersion rowVersion, List<Cell> result) {
        if (rowVersion.columnDeleteMarkers == null) {
            result.addAll(rowVersion.cells);
            return;
        }
        for (Cell cell : rowVersion.cells) {
            if (rowVersion.columnDeleteMarkers.isEmpty()) {
                result.add(cell);
            }
            else {
                Cell dm = rowVersion.columnDeleteMarkers.get(0);
                if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
                        cell.getQualifierLength(), dm.getQualifierArray(), dm.getQualifierOffset(),
                        dm.getQualifierLength()) == 0) {
                    rowVersion.columnDeleteMarkers.remove(0);
                    continue;
                }
                else {
                    result.add(cell);
                }
            }
        }
    }
    private void updateResultsWithRowOutsideMaxLookbackButInsideTTLWindow(List<Cell> result,
            CompactionRowVersion rowVersion) {
        // The rows that are outside the max lookback window but inside the TTL window
        if (rowVersion.deleteFamilyMarker == null) {
            if (rowVersion.version < maxVersion) {
                // Live rows whose version is less than max version are retained inside
                // the TTL window
                addCells(rowVersion, result);
            }
        }
        else if (keepDeletedCells == KeepDeletedCells.TTL) {
            // Deleted rows inside the TTL window with KeepDeletedCells.TTL are retained
            addCells(rowVersion, result);
        }
        else if (keepDeletedCells == KeepDeletedCells.TRUE) {
            if (rowVersion.version < maxVersion) {
                // Deleted rows inside the TTL window with KeepDeletedCells.TRUE are
                // retained if their version is less than max version
                addCells(rowVersion, result);
            }
        }
    }

    private void updateResultsWithRowOutsideTTLWindow(List<Cell> result,
            CompactionRowVersion rowVersion) {
        if (rowVersion.deleteFamilyMarker == null) {
            if (firstStore) {
                if (rowVersion.version < minVersion) {
                    // Live rows whose version is less than min version are retained outside
                    // the TTL window
                    addCells(rowVersion, result);
                }
            } else if (rowVersion.version < maxVersion) {
                // check if this row needs to be retained
            } else if (keepDeletedCells == KeepDeletedCells.TTL) {
                if (rowVersion.deleteFamilyMarker.getTimestamp() >= ttlWindowStart) {
                    addCells(rowVersion, result);
                }
            }
        }
    }
    private void prepareResults(List<Cell> result, List<CompactionRowVersion> rows,
            List<Cell> deleteMarkers) {
        for (CompactionRowVersion row : rows) {
            if (row.ts >= maxLookbackWindowStart) {
                // All rows within the max lookback window are retained
                result.addAll(row.cells);
            }
            else if (row.ts >= ttlWindowStart) {
                updateResultsWithRowOutsideMaxLookbackButInsideTTLWindow(result, row);
            } else {
                updateResultsWithRowOutsideTTLWindow(result, row);
            }
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
    /**
     * Filter {@link Cell}s from the underlying store scanner
     *
     * @param result
     *            next batch of {@link Cell}s
     * @throws IOException 
     */
    private void filter(List<Cell> result, boolean storeLevel) throws IOException {
        if (result.isEmpty()) {
            return;
        }
        List<List<Cell>> columns = new ArrayList<>();
        List<Cell> deleteMarkers = new ArrayList<>();
        formColumns(result, columns, deleteMarkers);
        List<CompactionRowVersion> rows = new ArrayList<>();
        if (formRowVersions(columns, rows, storeLevel)) {
            result.clear();
            prepareResults(result, rows, deleteMarkers);
        } else {
            filterRegionLevel(result);
        }
    }

    private void filterRegionLevel(List<Cell> result) throws IOException {
        Scan scan = new Scan();
        scan.setRaw(true);;
        scan.readAllVersions();
        scan.setTimeRange(0, compactionTime);
        byte[] rowKey = result.get(0).getRowArray();
        scan.withStartRow(rowKey, true);
        scan.withStartRow(rowKey, true);
        result.clear();
        RegionScanner scanner = region.getScanner(scan);
        List<Cell> regionResults = new ArrayList<>(result.size());
        scanner.next(regionResults);
        scanner.close();
        filter(regionResults, false);
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
