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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.coprocessor.generated.CDCInfoProtos;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.CDCTableInfo;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CDC_DATA_TABLE_DEF;
import static org.apache.phoenix.query.QueryConstants.CDC_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.NAME_SEPARATOR;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

public class CDCGlobalIndexRegionScanner extends UncoveredGlobalIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CDCGlobalIndexRegionScanner.class);
    private CDCTableInfo cdcDataTableInfo;

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
        CDCUtil.initForRawScan(dataTableScan);
        cdcDataTableInfo = CDCTableInfo.createFromProto(CDCInfoProtos.CDCTableDef
                .parseFrom(scan.getAttribute(CDC_DATA_TABLE_DEF)));
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
        return CDCUtil.initForRawScan(prepareDataTableScan(dataRowKeys, true));
    }

    protected boolean getNextCoveredIndexRow(List<Cell> result) throws IOException {
        if (indexRowIterator.hasNext()) {
            List<Cell> indexRow = indexRowIterator.next();
            // firstCell: Picking the earliest cell in the index row so that
            // timestamp of the cell and the row will be same.
            Cell firstIndexCell = indexRow.get(indexRow.size() - 1);
            byte[] indexRowKey = ImmutableBytesPtr.cloneCellRowIfNecessary(firstIndexCell);
            ImmutableBytesPtr dataRowKey = new ImmutableBytesPtr(
                    indexToDataRowKeyMap.get(indexRowKey));
            Result dataRow = dataRows.get(dataRowKey);
            Long indexCellTS = firstIndexCell.getTimestamp();
            Map<String, Object> preImageObj = null;
            Map<String, Object> changeImageObj = null;
            // FIXME: The below boolean flags should probably be translated to util methods on
            //  the Enum class itself.
            boolean isChangeImageInScope = this.cdcDataTableInfo.getIncludeScopes().size() == 0
                    || (this.cdcDataTableInfo.getIncludeScopes().contains(PTable.CDCChangeScope.CHANGE));
            boolean isPreImageInScope =
                    this.cdcDataTableInfo.getIncludeScopes().contains(PTable.CDCChangeScope.PRE);
            boolean isPostImageInScope =
                    this.cdcDataTableInfo.getIncludeScopes().contains(PTable.CDCChangeScope.POST);
            if (isPreImageInScope || isPostImageInScope) {
                preImageObj = new HashMap<>();
            }
            if (isChangeImageInScope || isPostImageInScope) {
                changeImageObj = new HashMap<>();
            }
            Long lowerBoundTsForPreImage = 0L;
            boolean foundDataTableCellForUpsert = false;
            boolean foundIndexCellForRowDelete = false;
            byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(
                    cdcDataTableInfo.getQualifierEncodingScheme()).getFirst();
            try {
                if (dataRow != null) {
                    int columnListIndex = 0;
                    List<CDCTableInfo.CDCColumnInfo> cdcColumnInfoList =
                            this.cdcDataTableInfo.getColumnInfoList();
                    cellLoop:
                    for (Cell cell : dataRow.rawCells()) {
                        byte[] cellFam = ImmutableBytesPtr.cloneCellFamilyIfNecessary(cell);
                        byte[] cellQual = ImmutableBytesPtr.cloneCellQualifierIfNecessary(cell);
                        if (cell.getType() == Cell.Type.DeleteFamily) {
                            if (indexCellTS == cell.getTimestamp()) {
                                foundIndexCellForRowDelete = true;
                            } else if (indexCellTS > cell.getTimestamp()
                                    && lowerBoundTsForPreImage == 0L) {
                                // Cells with timestamp less than the lowerBoundTsForPreImage
                                // can not be part of the PreImage as there is a Delete Family
                                // marker after that.
                                lowerBoundTsForPreImage = cell.getTimestamp();
                            }
                        } else if ((cell.getType() == Cell.Type.DeleteColumn
                                || cell.getType() == Cell.Type.Put)
                                && !Arrays.equals(cellQual, emptyCQ)) {
                            while (true) {
                                CDCTableInfo.CDCColumnInfo currentColumnInfo =
                                        cdcColumnInfoList.get(columnListIndex);
                                int columnComparisonResult = CDCUtil.compareCellFamilyAndQualifier(
                                        cellFam, cellQual,
                                        currentColumnInfo.getColumnFamily(),
                                        currentColumnInfo.getColumnQualifier());
                                if (columnComparisonResult > 0) {
                                    if (++columnListIndex >= cdcColumnInfoList.size()) {
                                        // Have no more column definitions, so the rest of the cells
                                        // must be for dropped columns and so can be ignored.
                                        break cellLoop;
                                    }
                                    // Continue looking for the right column definition for this cell.
                                    continue;
                                } else if (columnComparisonResult < 0) {
                                    // We didn't find a column definition for this cell, ignore the
                                    // current cell but continue working on the rest of the cells.
                                    continue cellLoop;
                                }

                                // else, found the column definition.
                                if (cell.getTimestamp() < indexCellTS
                                        && cell.getTimestamp() > lowerBoundTsForPreImage) {
                                    if (isPreImageInScope || isPostImageInScope) {
                                        String cdcColumnName = getColumnDisplayName(currentColumnInfo);
                                        if (preImageObj.containsKey(cdcColumnName)) {
                                            // Ignore current cell, continue with the rest.
                                            continue cellLoop;
                                        }
                                        preImageObj.put(cdcColumnName,
                                                this.getColumnValue(cell, cdcColumnInfoList
                                                        .get(columnListIndex).getColumnType()));
                                    }
                                } else if (cell.getTimestamp() == indexCellTS) {
                                    foundDataTableCellForUpsert = true;
                                    if (isChangeImageInScope || isPostImageInScope) {
                                        changeImageObj.put(getColumnDisplayName(currentColumnInfo),
                                                this.getColumnValue(cell, cdcColumnInfoList
                                                        .get(columnListIndex).getColumnType()));
                                    }
                                }
                                // Done processing the current column, look for other columns.
                                break;
                            }
                        }
                    }
                    if (foundDataTableCellForUpsert || foundIndexCellForRowDelete) {
                        Result cdcRow = getCDCImage(indexRowKey, preImageObj, changeImageObj,
                                foundIndexCellForRowDelete, indexCellTS, firstIndexCell,
                                isChangeImageInScope, isPreImageInScope, isPostImageInScope);
                        if (cdcRow != null && tupleProjector != null) {
                            if (firstIndexCell.getType() == Cell.Type.DeleteFamily) {
                                // result is of type EncodedColumnQualiferCellsList for queries with
                                // Order by clause. It fails when Delete Family cell is added to it
                                // as it expects column qualifier bytes which is not available.
                                // Adding empty PUT cell as a placeholder.
                                result.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                                        .setRow(indexRowKey)
                                        .setFamily(ImmutableBytesPtr.cloneCellFamilyIfNecessary(
                                                firstIndexCell))
                                        .setQualifier(indexMaintainer.getEmptyKeyValueQualifier())
                                        .setTimestamp(firstIndexCell.getTimestamp())
                                        .setType(Cell.Type.Put)
                                        .setValue(EMPTY_BYTE_ARRAY).build());
                            } else {
                                result.add(firstIndexCell);
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

    private String getColumnDisplayName(CDCTableInfo.CDCColumnInfo currentColumnInfo) {
        String cdcColumnName;
        // Don't include Column Family if it is a default column Family
        if (Arrays.equals(currentColumnInfo.getColumnFamily(),
                cdcDataTableInfo.getDefaultColumnFamily())) {
            cdcColumnName = currentColumnInfo.getColumnName();
        }
        else {
            cdcColumnName = currentColumnInfo.getColumnFamilyName() +
                    NAME_SEPARATOR + currentColumnInfo.getColumnName();
        }
        return cdcColumnName;
    }

    private Result getCDCImage(byte[] indexRowKey, Map<String, Object> preImageObj,
                               Map<String, Object> changeImageObj, boolean isIndexCellDeleteRow, Long indexCellTS,
                               Cell firstCell, boolean isChangeImageInScope, boolean isPreImageInScope,
                               boolean isPostImageInScope) {
        Map<String, Object> rowValueMap = new HashMap<>();

        if (isPreImageInScope) {
            rowValueMap.put(CDC_PRE_IMAGE, preImageObj);
        }

        if (isChangeImageInScope) {
            rowValueMap.put(CDC_CHANGE_IMAGE, changeImageObj);
        }

        if (isPostImageInScope) {
            Map<String, Object> postImageObj = new HashMap<>();
            if (!isIndexCellDeleteRow) {
                postImageObj.putAll(preImageObj);
                postImageObj.putAll(changeImageObj);
            }
            rowValueMap.put(CDC_POST_IMAGE, postImageObj);
        }

        if (isIndexCellDeleteRow) {
            rowValueMap.put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        } else {
            rowValueMap.put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }
        Gson gson = new GsonBuilder().serializeNulls().create();
        byte[] value =
                gson.toJson(rowValueMap).getBytes(StandardCharsets.UTF_8);
        CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
        Result cdcRow = Result.create(Arrays.asList(builder
                .setRow(indexRowKey)
                .setFamily(ImmutableBytesPtr.cloneCellFamilyIfNecessary(firstCell))
                .setQualifier(cdcDataTableInfo.getCdcJsonColQualBytes())
                .setTimestamp(indexCellTS)
                .setValue(value)
                .setType(Cell.Type.Put)
                .build()));

        return cdcRow;
    }

    private Object getColumnValue(Cell cell, PDataType dataType) {
        byte[] cellValue = ImmutableBytesPtr.cloneCellValueIfNecessary(cell);
        if (dataType.getSqlType() == Types.BINARY) {
            return Base64.getEncoder().encodeToString(cellValue);
        } else if (dataType.getSqlType() == Types.DATE
                || dataType.getSqlType() == Types.TIMESTAMP
                || dataType.getSqlType() == Types.TIME
                || dataType.getSqlType() == Types.TIME_WITH_TIMEZONE
                || dataType.getSqlType() == Types.TIMESTAMP_WITH_TIMEZONE) {
            return dataType.toObject(cellValue).toString();
        } else {
            return dataType.toObject(cellValue);
        }
    }
}
