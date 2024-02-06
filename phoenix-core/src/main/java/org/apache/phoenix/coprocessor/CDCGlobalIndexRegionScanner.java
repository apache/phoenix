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
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.phoenix.util.IndexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
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
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CDC_INCLUDE_SCOPES;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CDC_JSON_COL_QUALIFIER;
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
    private Set<PTable.CDCChangeScope> cdcChangeScopeSet;
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
        Charset utf8Charset = StandardCharsets.UTF_8;
        String cdcChangeScopeStr = utf8Charset.decode(
                ByteBuffer.wrap(scan.getAttribute(CDC_INCLUDE_SCOPES))).toString();
        try {
            cdcChangeScopeSet = CDCUtil.makeChangeScopeEnumsFromString(cdcChangeScopeStr);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
            Cell firstCell = indexRow.get(indexRow.size() - 1);
            ImmutableBytesPtr dataRowKey = new ImmutableBytesPtr(
                    indexToDataRowKeyMap.get(CellUtil.cloneRow(firstCell)));
            Result dataRow = dataRows.get(dataRowKey);
            Long indexCellTS = firstCell.getTimestamp();
            Map<String, Object> preImageObj = null;
            Map<String, Object> changeImageObj = null;
            boolean isChangeImageInScope = this.cdcChangeScopeSet.size() == 0
                    || (this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE));
            boolean isPreImageInScope =
                    this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE);
            boolean isPostImageInScope =
                    this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST);
            if (isPreImageInScope || isPostImageInScope) {
                preImageObj = new HashMap<>();
            }
            if (isChangeImageInScope || isPostImageInScope) {
                changeImageObj = new HashMap<>();
            }
            Long lowerBoundTsForPreImage = 0L;
            boolean isIndexCellDeleteRow = false;
            byte[] emptyCQ = indexMaintainer.getEmptyKeyValueQualifier();
            try {
                int columnListIndex = 0;
                List<CDCTableInfo.CDCColumnInfo> cdcColumnInfoList =
                        this.cdcDataTableInfo.getColumnInfoList();
                CDCTableInfo.CDCColumnInfo currentColumnInfo =
                        cdcColumnInfoList.get(columnListIndex);
                for (Cell cell : dataRow.rawCells()) {
                    if (cell.getType() == Cell.Type.DeleteFamily) {
                        if (columnListIndex > 0) {
                            continue;
                        }
                        if (indexCellTS == cell.getTimestamp()) {
                            isIndexCellDeleteRow = true;
                        } else if (indexCellTS > cell.getTimestamp()
                                && lowerBoundTsForPreImage == 0L) {
                            lowerBoundTsForPreImage = cell.getTimestamp();
                        }
                    } else if ((cell.getType() == Cell.Type.DeleteColumn
                            || cell.getType() == Cell.Type.Put)
                            && !Arrays.equals(cell.getQualifierArray(), emptyCQ)
                            && columnListIndex < cdcColumnInfoList.size()) {
                        int cellColumnComparator = CDCUtil.compareCellFamilyAndQualifier(
                                cell.getFamilyArray(), cell.getQualifierArray(),
                                currentColumnInfo.getColumnFamily(),
                                currentColumnInfo.getColumnQualifier());
                        while (cellColumnComparator > 0) {
                            columnListIndex += 1;
                            if (columnListIndex >= cdcColumnInfoList.size()) {
                                break;
                            }
                            currentColumnInfo = cdcColumnInfoList.get(columnListIndex);
                            cellColumnComparator = CDCUtil.compareCellFamilyAndQualifier(
                                    cell.getFamilyArray(), cell.getQualifierArray(),
                                    currentColumnInfo.getColumnFamily(),
                                    currentColumnInfo.getColumnQualifier());
                        }
                        if (columnListIndex >= cdcColumnInfoList.size()) {
                            break;
                        }
                        if (cellColumnComparator < 0) {
                            continue;
                        }
                        if (cellColumnComparator == 0) {
                            String columnFamily = StandardCharsets.UTF_8
                                    .decode(ByteBuffer.wrap(currentColumnInfo
                                            .getColumnFamily())).toString();
                            String cdcColumnName = columnFamily +
                                    NAME_SEPARATOR + currentColumnInfo.getColumnName();
                            if (Arrays.equals(
                                    currentColumnInfo.getColumnFamily(),
                                    cdcDataTableInfo.getDefaultColumnFamily())) {
                                cdcColumnName = currentColumnInfo.getColumnName();
                            }
                            if (cell.getTimestamp() < indexCellTS
                                    && cell.getTimestamp() > lowerBoundTsForPreImage) {
                                if (isPreImageInScope || isPostImageInScope) {
                                    if (preImageObj.containsKey(cdcColumnName)) {
                                        continue;
                                    }
                                    preImageObj.put(cdcColumnName,
                                            this.getColumnValue(cell, cdcColumnInfoList
                                                    .get(columnListIndex).getColumnType()));
                                }
                            } else if (cell.getTimestamp() == indexCellTS) {
                                if (isChangeImageInScope || isPostImageInScope) {
                                    changeImageObj.put(cdcColumnName,
                                            this.getColumnValue(cell, cdcColumnInfoList
                                                    .get(columnListIndex).getColumnType()));
                                }
                            }
                        }
                    }
                }
                Result cdcRow = getCDCImage(preImageObj, changeImageObj, isIndexCellDeleteRow,
                        indexCellTS, firstCell, isChangeImageInScope, isPreImageInScope,
                        isPostImageInScope);
                if (cdcRow != null && tupleProjector != null) {
                    if (firstCell.getType() == Cell.Type.DeleteFamily) {
                        result.add(CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                                .setRow(firstCell.getRowArray()).
                                setFamily(firstCell.getFamilyArray())
                                .setQualifier(indexMaintainer.getEmptyKeyValueQualifier()).
                                setTimestamp(firstCell.getTimestamp())
                                .setType(Cell.Type.Put).
                                setValue(EMPTY_BYTE_ARRAY).build());
                    } else {
                        result.add(firstCell);
                    }
                    IndexUtil.addTupleAsOneCell(result, new ResultTuple(cdcRow),
                            tupleProjector, ptr);
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

    private Result getCDCImage(
            Map<String, Object> preImageObj, Map<String, Object> changeImageObj,
            boolean isIndexCellDeleteRow, Long indexCellTS, Cell firstCell,
            boolean isChangeImageInScope, boolean isPreImageInScope, boolean isPostImageInScope) {
        Map<String, Object> rowValueMap = new HashMap<>();

        if (isPreImageInScope) {
            rowValueMap.put(CDC_PRE_IMAGE, preImageObj);
        }

        if (isChangeImageInScope) {
            rowValueMap.put(CDC_CHANGE_IMAGE, changeImageObj);
        }

        Map<String, Object> postImageObj = new HashMap<>();
        if (isPostImageInScope) {
            if (!isIndexCellDeleteRow) {
                for (Map.Entry<String, Object> preImageObjCol : preImageObj.entrySet()) {
                    postImageObj.put(preImageObjCol.getKey(), preImageObjCol.getValue());
                }
                for (Map.Entry<String, Object> changeImageObjCol : changeImageObj.entrySet()) {
                    postImageObj.put(changeImageObjCol.getKey(), changeImageObjCol.getValue());
                }
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
                .setRow(indexToDataRowKeyMap.get(CellUtil.cloneRow(firstCell)))
                .setFamily(firstCell.getFamilyArray())
                .setQualifier(scan.getAttribute(CDC_JSON_COL_QUALIFIER))
                .setTimestamp(indexCellTS)
                .setValue(value)
                .setType(Cell.Type.Put)
                .build()));

        return cdcRow;
    }

    private Object getColumnValue(Cell cell, PDataType dataType) {
        if (dataType.getSqlType() == Types.BINARY) {
            return Base64.getEncoder().encodeToString(cell.getValueArray());
        } else if (dataType.getSqlType() == Types.DATE) {
            return ((Date) dataType.toObject(cell.getValueArray())).getTime();
        } else {
            return dataType.toObject(cell.getValueArray());
        }
    }

}
