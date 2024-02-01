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
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.IndexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.*;
import static org.apache.phoenix.query.QueryConstants.DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY_STR;
import static org.apache.phoenix.query.QueryConstants.UPSERT_EVENT_TYPE;
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
        String cdcChangeScopeStr = utf8Charset.decode(ByteBuffer.wrap(scan.getAttribute(CDC_INCLUDE_SCOPES))).toString();
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
            Cell firstCell = indexRow.get(indexRow.size() - 1);
            byte[] indexRowKey = new ImmutableBytesPtr(firstCell.getRowArray(),
                    firstCell.getRowOffset(), firstCell.getRowLength())
                    .copyBytesIfNecessary();
            ImmutableBytesPtr dataRowKey = new ImmutableBytesPtr(
                    indexToDataRowKeyMap.get(indexRowKey));
            Result dataRow = dataRows.get(dataRowKey);
            Long indexCellTS = firstCell.getTimestamp();
            Map<String, Map<String,Object>> preImageObj = new HashMap<>();
            Map<String, Map<String,Object>> changeImageObj = new HashMap<>();
            Long lowerBoundForPreImage = 0l;
            boolean isIndexCellDeleteRow = false;
            byte[] emptyCQ = indexMaintainer.getEmptyKeyValueQualifier();
            try {
                int columnListIndex = 0;
                List<CDCTableInfo.CDCColumnInfo> cdcColumnInfoList = this.cdcDataTableInfo.getColumnInfoList();
                for (Cell cell : dataRow.rawCells()) {
                    if (cell.getType() == Cell.Type.DeleteFamily) {
                        if (columnListIndex > 0) {
                            continue;
                        }
                        if (indexCellTS == cell.getTimestamp()) {
                            isIndexCellDeleteRow = true;
                        } else if (indexCellTS > cell.getTimestamp()) {
                            lowerBoundForPreImage = cell.getTimestamp();
                        }
                    } else if (cell.getType() == Cell.Type.DeleteColumn
                            || cell.getType() == Cell.Type.Put) {
                        if (!Arrays.equals(cell.getQualifierArray(), emptyCQ)
                                && CDCUtil.compareCellFamilyAndQualifier(
                                        cell.getFamilyArray(),
                                        cell.getQualifierArray(),
                                        cdcColumnInfoList.get(columnListIndex).getColumnFamily(),
                                        cdcColumnInfoList.get(columnListIndex).getColumnQualifier()) > 0) {
                            while (columnListIndex <= cdcColumnInfoList.size()
                                    && CDCUtil.compareCellFamilyAndQualifier(
                                    cell.getFamilyArray(),
                                    cell.getQualifierArray(),
                                    cdcColumnInfoList.get(columnListIndex).getColumnFamily(),
                                    cdcColumnInfoList.get(columnListIndex).getColumnQualifier()) > 0) {
                                columnListIndex += 1;
                            }
                            if (columnListIndex >= cdcColumnInfoList.size()) {
                                break;
                            }
                        }
                        if (CDCUtil.compareCellFamilyAndQualifier(
                                cell.getFamilyArray(),
                                cell.getQualifierArray(),
                                cdcColumnInfoList.get(columnListIndex).getColumnFamily(),
                                cdcColumnInfoList.get(columnListIndex).getColumnQualifier()) < 0) {
                            continue;
                        }
                        if (CDCUtil.compareCellFamilyAndQualifier(
                                cell.getFamilyArray(),
                                cell.getQualifierArray(),
                                cdcColumnInfoList.get(columnListIndex).getColumnFamily(),
                                cdcColumnInfoList.get(columnListIndex).getColumnQualifier()) == 0) {
                            String columnFamily = StandardCharsets.UTF_8
                                    .decode(ByteBuffer.wrap(cdcColumnInfoList.get(columnListIndex).getColumnFamily())).toString();
                            String columnQualifier = cdcColumnInfoList.get(columnListIndex).getColumnName();
                            if (Arrays.equals(cdcColumnInfoList.get(columnListIndex).getColumnFamily(),
                                    cdcDataTableInfo.getDefaultColumnFamily())) {
                                columnFamily = DEFAULT_COLUMN_FAMILY_STR;
                            }
                            if (cell.getTimestamp() < indexCellTS
                                    && cell.getTimestamp() > lowerBoundForPreImage) {
                                if (!preImageObj.containsKey(columnFamily)) {
                                    preImageObj.put(columnFamily, new HashMap<>());
                                }
                                if (preImageObj.get(columnFamily).containsKey(columnQualifier)) {
                                    continue;
                                }
                                preImageObj.get(columnFamily).put(columnQualifier,
                                        cdcColumnInfoList.get(columnListIndex).getColumnType().toObject(
                                        cell.getValueArray()));
                            } else if (cell.getTimestamp() == indexCellTS) {
                                if (!changeImageObj.containsKey(columnFamily)) {
                                    changeImageObj.put(columnFamily, new HashMap<>());
                                }
                                changeImageObj.get(columnFamily).put(columnQualifier,
                                        cdcColumnInfoList.get(columnListIndex).getColumnType().toObject(
                                        cell.getValueArray()));
                            }
                        }
                    }
                }
                Result cdcRow = getCDCImage(
                        preImageObj, changeImageObj, isIndexCellDeleteRow, indexCellTS, firstCell);
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
            Map<String, Map<String, Object>> preImageObj,
            Map<String, Map<String, Object>> changeImageObj,
            boolean isIndexCellDeleteRow, Long indexCellTS, Cell firstCell) {
        Map<String, Object> rowValueMap = new HashMap<>();

        if (this.cdcChangeScopeSet.size() == 0
                || (this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE))) {
            rowValueMap.put(PRE_IMAGE, preImageObj);
        }

        if (this.cdcChangeScopeSet.size() == 0
                || (this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE))) {
            rowValueMap.put(CHANGE_IMAGE, changeImageObj);
        }

        Map<String, Map<String, Object>> postImageObj = new HashMap<>();
        if (this.cdcChangeScopeSet.size() == 0
                || (this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST))) {
            if (!isIndexCellDeleteRow) {
                for (Map.Entry<String, Map<String, Object>> preImageObjFamily
                        : preImageObj.entrySet()) {
                    String columnFamily = preImageObjFamily.getKey();
                    postImageObj.put(columnFamily, new HashMap<>());
                    for (Map.Entry<String, Object> preImageColQual : preImageObjFamily.getValue().entrySet()) {
                        postImageObj.get(columnFamily).put(preImageColQual.getKey(), preImageColQual.getValue());
                    }
                }
                for (Map.Entry<String, Map<String, Object>> changeImageObjFamily
                        : changeImageObj.entrySet()) {
                    String columnFamily = changeImageObjFamily.getKey();
                    if (!postImageObj.containsKey(columnFamily)) {
                        postImageObj.put(columnFamily, new HashMap<>());
                    }
                    for (Map.Entry<String, Object> changeImageColQual : changeImageObjFamily.getValue().entrySet()) {
                        postImageObj.get(columnFamily).put(changeImageColQual.getKey(), changeImageColQual.getValue());
                    }
                }
            }
            rowValueMap.put(POST_IMAGE, postImageObj);
        }

        if (isIndexCellDeleteRow) {
            rowValueMap.put(EVENT_TYPE, DELETE_EVENT_TYPE);
        } else {
            rowValueMap.put(EVENT_TYPE, UPSERT_EVENT_TYPE);
        }
        Gson gson = new GsonBuilder().serializeNulls().create();

        byte[] value =
                gson.toJson(rowValueMap).getBytes(StandardCharsets.UTF_8);
        CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
        Result cdcRow = Result.create(Arrays.asList(builder.
                setRow(indexToDataRowKeyMap.get(new ImmutableBytesPtr(firstCell.getRowArray(),
                        firstCell.getRowOffset(), firstCell.getRowLength())
                        .copyBytesIfNecessary())).
                setFamily(firstCell.getFamilyArray()).
                setQualifier(scan.getAttribute(CDC_JSON_COL_QUALIFIER)).
                setTimestamp(indexCellTS).
                setValue(value).
                setType(Cell.Type.Put).
                build()));

        return cdcRow;
    }
}
