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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.coprocessor.generated.CDCInfoProtos;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.CDCTableInfo;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.CDCChangeBuilder;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.JacksonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.CDC_DATA_TABLE_DEF;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

/**
 * CDC (Change Data Capture) enabled region scanner for global indexes that processes
 * uncovered CDC index queries by reconstructing CDC events from index and data table rows.
 *
 * <h3>Purpose</h3>
 * This scanner extends {@link UncoveredGlobalIndexRegionScanner} to handle CDC index queries
 * where the CDC index doesn't contain all the columns needed to satisfy the query. It bridges
 * the gap between CDC index rows and the original data table to reconstruct complete CDC events.
 *
 * <h3>CDC Event Processing</h3>
 * The scanner processes two types of CDC events:
 * <ul>
 *   <li><b>Regular CDC Events:</b> Requires data table scan to build CDC event JSON from
 *       current/historical row state</li>
 *   <li><b>Pre-Image CDC Events:</b> Contains embedded CDC data (e.g., TTL delete events)
 *       that can be returned directly without data table scan</li>
 * </ul>
 *
 * <h3>CDC Event Structure</h3>
 * The scanner produces CDC events in JSON format containing:
 * <ul>
 *   <li><b>event_type:</b> "upsert", "delete", or "ttl_delete"</li>
 *   <li><b>pre_image:</b> Row state before the change (for updates/deletes)</li>
 *   <li><b>post_image:</b> Row state after the change (for inserts/updates)</li>
 * </ul>
 *
 * @see UncoveredGlobalIndexRegionScanner
 * @see CDCChangeBuilder
 * @see CDCTableInfo
 */
public class CDCGlobalIndexRegionScanner extends UncoveredGlobalIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CDCGlobalIndexRegionScanner.class);
    private CDCTableInfo cdcDataTableInfo;
    private CDCChangeBuilder changeBuilder;

    public CDCGlobalIndexRegionScanner(final RegionScanner innerScanner,
                                       final Region region,
                                       final Scan scan,
                                       final RegionCoprocessorEnvironment env,
                                       final Scan dataTableScan,
                                       final TupleProjector tupleProjector,
                                       final IndexMaintainer indexMaintainer,
                                       final byte[][] viewConstants,
                                       final ImmutableBytesWritable ptr,
                                       final long pageSizeMs,
                                       final long queryLimit) throws IOException {
        super(innerScanner, region, scan, env, dataTableScan, tupleProjector, indexMaintainer,
                viewConstants, ptr, pageSizeMs, queryLimit);
        CDCUtil.setupScanForCDC(dataTableScan);
        cdcDataTableInfo = CDCTableInfo.createFromProto(CDCInfoProtos.CDCTableDef
                .parseFrom(scan.getAttribute(CDC_DATA_TABLE_DEF)));
        changeBuilder = new CDCChangeBuilder(cdcDataTableInfo);
    }

    @Override
    protected Scan prepareDataTableScan(Collection<byte[]> dataRowKeys) throws IOException {
        //TODO: Get Timerange from the start row and end row of the index scan object
        // and set it in the datatable scan object.
//        if (scan.getStartRow().length == 8) {
//            startTimeRange = PLong.INSTANCE.getCodec().decodeLong(
//              scan.getStartRow(), 0, SortOrder.getDefault());
//        }
//        if (scan.getStopRow().length == 8) {
//            stopTimeRange = PLong.INSTANCE.getCodec().decodeLong(
//              scan.getStopRow(), 0, SortOrder.getDefault());
//        }
        return CDCUtil.setupScanForCDC(prepareDataTableScan(dataRowKeys, true));
    }

    protected boolean getNextCoveredIndexRow(List<Cell> result) throws IOException {
        if (indexRowIterator.hasNext()) {
            List<Cell> indexRow = indexRowIterator.next();
            Cell indexCell = indexRow.get(0);
            byte[] indexRowKey = ImmutableBytesPtr.cloneCellRowIfNecessary(indexCell);
            if (indexRow.size() > 1) {
                boolean success = handlePreImageCDCEvent(indexRow, indexRowKey, indexCell, result);
                if (success) {
                    return true;
                }
            }
            ImmutableBytesPtr dataRowKey = new ImmutableBytesPtr(
                    indexToDataRowKeyMap.get(indexRowKey));
            Result dataRow = dataRows.get(dataRowKey);
            long changeTS = indexCell.getTimestamp();
            TupleProjector dataTableProjector = cdcDataTableInfo.getDataTableProjector();
            Expression[] expressions = dataTableProjector != null ?
                    dataTableProjector.getExpressions() : null;
            boolean isSingleCell = dataTableProjector != null;
            byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(
                    cdcDataTableInfo.getQualifierEncodingScheme()).getFirst();
            changeBuilder.initChange(changeTS);
            try {
                if (dataRow != null) {
                    int curColumnNum = 0;
                    List<CDCTableInfo.CDCColumnInfo> cdcColumnInfoList =
                            this.cdcDataTableInfo.getColumnInfoList();
                    cellLoop:
                    for (Cell cell : dataRow.rawCells()) {
                        if (! changeBuilder.isChangeRelevant(cell)) {
                            continue;
                        }
                        byte[] cellFam = ImmutableBytesPtr.cloneCellFamilyIfNecessary(cell);
                        byte[] cellQual = ImmutableBytesPtr.cloneCellQualifierIfNecessary(cell);
                        if (cell.getType() == Cell.Type.DeleteFamily) {
                            if (changeTS == cell.getTimestamp()) {
                                changeBuilder.markAsDeletionEvent();
                            } else if (changeTS > cell.getTimestamp()
                                    && changeBuilder.getLastDeletedTimestamp() == 0L) {
                                // Cells with timestamp less than the lowerBoundTsForPreImage
                                // can not be part of the PreImage as there is a Delete Family
                                // marker after that.
                                changeBuilder.setLastDeletedTimestamp(cell.getTimestamp());
                            }
                        } else if ((cell.getType() == Cell.Type.DeleteColumn
                                || cell.getType() == Cell.Type.Put)
                                && !Arrays.equals(cellQual, emptyCQ)) {
                            if (! changeBuilder.isChangeRelevant(cell)) {
                                // We don't need to build the change image, just skip it.
                                continue;
                            }
                            // In this case, cell is the row, meaning we loop over rows..
                            if (isSingleCell) {
                                while (curColumnNum < cdcColumnInfoList.size()) {
                                    boolean hasValue = dataTableProjector.getSchema().extractValue(
                                            cell, (SingleCellColumnExpression)
                                                    expressions[curColumnNum], ptr);
                                    if (hasValue) {
                                        Object cellValue = getColumnValue(ptr.get(),
                                                ptr.getOffset(), ptr.getLength(),
                                                cdcColumnInfoList.get(curColumnNum).getColumnType());
                                        changeBuilder.registerChange(cell, curColumnNum, cellValue);
                                    }
                                    ++curColumnNum;
                                }
                                break cellLoop;
                            }
                            while (true) {
                                CDCTableInfo.CDCColumnInfo currentColumnInfo =
                                        cdcColumnInfoList.get(curColumnNum);
                                int columnComparisonResult = CDCUtil.compareCellFamilyAndQualifier(
                                                cellFam, cellQual,
                                                currentColumnInfo.getColumnFamily(),
                                                currentColumnInfo.getColumnQualifier());
                                if (columnComparisonResult > 0) {
                                    if (++curColumnNum >= cdcColumnInfoList.size()) {
                                        // Have no more column definitions, so the rest of the cells
                                        // must be for dropped columns and so can be ignored.
                                        break cellLoop;
                                    }
                                    // Continue looking for the right column definition
                                    // for this cell.
                                    continue;
                                } else if (columnComparisonResult < 0) {
                                    // We didn't find a column definition for this cell, ignore the
                                    // current cell but continue working on the rest of the cells.
                                    continue cellLoop;
                                }

                                // else, found the column definition.
                                Object cellValue = cell.getType() == Cell.Type.DeleteColumn ? null
                                        : getColumnValue(cell, cdcColumnInfoList.get(curColumnNum)
                                                .getColumnType());
                                changeBuilder.registerChange(cell, curColumnNum, cellValue);
                                // Done processing the current cell, check the next cell.
                                break;
                            }
                        }
                    }
                    if (changeBuilder.isNonEmptyEvent()) {
                        Result cdcRow = getCDCImage(indexRowKey, indexCell);
                        if (cdcRow != null && tupleProjector != null) {
                            if (indexCell.getType() == Cell.Type.DeleteFamily) {
                                // result is of type EncodedColumnQualiferCellsList for queries with
                                // Order by clause. It fails when Delete Family cell is added to it
                                // as it expects column qualifier bytes which is not available.
                                // Adding empty PUT cell as a placeholder.
                                result.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                                        .setRow(indexRowKey)
                                        .setFamily(ImmutableBytesPtr.cloneCellFamilyIfNecessary(
                                                indexCell))
                                        .setQualifier(indexMaintainer.getEmptyKeyValueQualifier())
                                        .setTimestamp(indexCell.getTimestamp())
                                        .setType(Cell.Type.Put)
                                        .setValue(EMPTY_BYTE_ARRAY).build());
                            } else {
                                result.add(indexCell);
                            }
                            IndexUtil.addTupleAsOneCell(result, new ResultTuple(cdcRow),
                                    tupleProjector, ptr);
                        } else {
                            result.clear();
                        }
                    } else {
                        result.clear();
                    }
                } else {
                    result.clear();
                }

                return true;
            } catch (Throwable e) {
                LOGGER.error("Exception in UncoveredIndexRegionScanner for region "
                        + region.getRegionInfo().getRegionNameAsString(), e);
                throw e;
            }
        }
        return false;
    }

    private Result getCDCImage(byte[] indexRowKey, Cell firstCell) throws JsonProcessingException {
        byte[] value = JacksonUtil.getObjectWriter(HashMap.class).writeValueAsBytes(
                changeBuilder.buildCDCEvent());
        return createCDCResult(indexRowKey, firstCell, changeBuilder.getChangeTimestamp(), value);
    }

    /**
     * Generates the Result object for the CDC event.
     *
     * @param indexRowKey The CDC index row key
     * @param firstCell   The first cell
     * @param timestamp   The timestamp for the CDC event
     * @param value       The CDC event JSON bytes
     * @return Result containing the CDC data
     */
    private Result createCDCResult(byte[] indexRowKey, Cell firstCell, long timestamp,
                                   byte[] value) {
        CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
        return Result.create(Collections.singletonList(builder
                .setRow(indexRowKey)
                .setFamily(ImmutableBytesPtr.cloneCellFamilyIfNecessary(firstCell))
                .setQualifier(cdcDataTableInfo.getCdcJsonColQualBytes())
                .setTimestamp(timestamp)
                .setValue(value)
                .setType(Cell.Type.Put)
                .build()));
    }

    private Object getColumnValue(Cell cell, PDataType dataType) {
        return getColumnValue(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                dataType);
    }

    private Object getColumnValue(byte[] cellValue, int offset, int length, PDataType dataType) {
        Object value;
        if (CDCUtil.isBinaryType(dataType)) {
            value = ImmutableBytesPtr.copyBytesIfNecessary(cellValue, offset, length);
        } else {
            value = dataType.toObject(cellValue, offset, length);
        }
        return CDCUtil.getColumnEncodedValue(value, dataType);
    }

    /**
     * Handles CDC events that already contain pre-image data, avoiding data table scan.
     * Supports both the new CDC_IMAGE_CQ column and traditional CDC JSON column.
     *
     * @param indexRow    The CDC index row cells
     * @param indexRowKey The CDC index row key
     * @param indexCell   The primary index cell
     * @param result      The result list to populate
     * @return true if event was processed successfully
     */
    private boolean handlePreImageCDCEvent(List<Cell> indexRow, byte[] indexRowKey,
                                           Cell indexCell, List<Cell> result) {
        Cell cdcDataCell = null;
        for (Cell cell : indexRow) {
            if (Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
                    cell.getQualifierLength(),
                    QueryConstants.CDC_IMAGE_CQ_BYTES, 0,
                    QueryConstants.CDC_IMAGE_CQ_BYTES.length)) {
                cdcDataCell = cell;
                break;
            }
        }
        if (cdcDataCell == null) {
            return false;
        }
        byte[] cdcEventBytes = CellUtil.cloneValue(cdcDataCell);
        Result cdcRow = createCDCResult(indexRowKey, indexCell, cdcDataCell.getTimestamp(),
                cdcEventBytes);

        if (tupleProjector != null) {
            result.add(indexCell);
            IndexUtil.addTupleAsOneCell(result, new ResultTuple(cdcRow), tupleProjector, ptr);
        } else {
            result.clear();
        }
        LOGGER.debug("Processed CDC event with embedded data, skipped data table scan for"
                + " row key: {}", Bytes.toStringBinary(indexRowKey));
        return true;
    }
}
