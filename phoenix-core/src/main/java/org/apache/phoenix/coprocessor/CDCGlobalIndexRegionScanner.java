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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.*;

public class CDCGlobalIndexRegionScanner extends UncoveredGlobalIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CDCGlobalIndexRegionScanner.class);

    private Map<ImmutableBytesPtr, String> dataColQualNameMap;
    private Map<ImmutableBytesPtr, PDataType> dataColQualTypeMap;
    // Map<dataRowKey: Map<TS: Map<qualifier: Cell>>>
    private Set<PTable.CDCChangeScope> cdcChangeScopeSet;


    private final static String EVENT_TYPE = "event_type";
    private final static String PRE_IMAGE = "pre_image";
    private final static String POST_IMAGE = "post_image";
    private final static String CHANGE_IMAGE = "change_image";
    private final static String UPSERT_EVENT_TYPE = "upsert";
    private final static String DELETE_EVENT_TYPE = "delete";

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
        dataColQualNameMap = ScanUtil.deserializeColumnQualifierToNameMap(
                scan.getAttribute(DATA_COL_QUALIFIER_TO_NAME_MAP));
        dataColQualTypeMap = ScanUtil.deserializeColumnQualifierToTypeMap(
                scan.getAttribute(DATA_COL_QUALIFIER_TO_TYPE_MAP));
        cdcChangeScopeSet = CDCUtil.makeChangeScopeEnumsFromString(
                new String(scan.getAttribute(CDC_INCLUDE_SCOPES), StandardCharsets.UTF_8));
    }

    @Override
    protected Scan prepareDataTableScan(Collection<byte[]> dataRowKeys) throws IOException {
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
            Long indexCellTs = firstCell.getTimestamp();
            Cell.Type indexCellType = firstCell.getType();

            Map<ImmutableBytesPtr, Cell> preImageObj = new HashMap<>();
            Map<ImmutableBytesPtr, Cell> changeImageObj = new HashMap<>();
            List<Cell> resultCells = Arrays.asList(dataRow.rawCells());
            Collections.sort(resultCells, CellComparator.getInstance().reversed());

            boolean isIndexCellDeleteRow = false;
            boolean isIndexCellDeleteColumn = false;
            try {
                for (Cell cell : resultCells) {
                    if (cell.getType() == Cell.Type.DeleteColumn) {
                        // DDL is not supported in CDC
                        if (cell.getTimestamp() == indexCellTs) {
                            isIndexCellDeleteColumn = true;
                            break;
                        }
                    } else if (cell.getType() == Cell.Type.Put) {
                        if (cell.getTimestamp() < indexCellTs) {
                            preImageObj.put(new ImmutableBytesPtr(
                                    cell.getQualifierArray(),
                                    cell.getQualifierOffset(),
                                    cell.getQualifierLength()), cell);
                        } else if (cell.getTimestamp() == indexCellTs) {
                            changeImageObj.put(new ImmutableBytesPtr(
                                    cell.getQualifierArray(),
                                    cell.getQualifierOffset(),
                                    cell.getQualifierLength()), cell);
                        }
                    } else if (cell.getType() == Cell.Type.DeleteFamily) {
                        if (indexCellType == Cell.Type.DeleteFamily
                            && indexCellTs == cell.getTimestamp()){
                            isIndexCellDeleteRow = true;
                            break;
                        }
                        // Removing the Cells which are upserted before this DeleteFamily Cell
                        // as current index Cell ts is greater than the DeleteFamily Cell
                        if (indexCellTs > cell.getTimestamp()) {
                            Iterator<Map.Entry<ImmutableBytesPtr, Cell>> iterator = preImageObj.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<ImmutableBytesPtr, Cell> entry = iterator.next();
                                if (entry.getValue().getTimestamp() < cell.getTimestamp()) {
                                    iterator.remove();
                                }
                            }
                        }
                    }
                }
                if ((indexCellType == Cell.Type.DeleteFamily && isIndexCellDeleteRow == false)
                        || isIndexCellDeleteColumn == true) {
                    result.clear();
                } else {
                    Result cdcRow = getCDCImage(preImageObj, changeImageObj, isIndexCellDeleteRow, indexCellTs, firstCell);
                    result.add(firstCell);
                    if (cdcRow != null && tupleProjector != null) {
                        IndexUtil.addTupleAsOneCell(result, new ResultTuple(cdcRow),
                                tupleProjector, ptr);
                    } else {
                        result.clear();
                    }
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

    private Result getCDCImage (
            Map<ImmutableBytesPtr, Cell> preImageObj,
            Map<ImmutableBytesPtr, Cell> changeImageObj,
            boolean isIndexCellDeleteRow, Long indexCellTs, Cell firstCell) {
        Map<String, Object> rowValueMap = new HashMap<>();

        Map<String, Object> preImage = new HashMap<>();
        if (this.cdcChangeScopeSet.size() == 0 ||
                (this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE))) {
            for (Map.Entry<ImmutableBytesPtr, Cell> preImageObjCell : preImageObj.entrySet()) {
                if (dataColQualNameMap.get(preImageObjCell.getKey()) != null) {
                    preImage.put(dataColQualNameMap.get(preImageObjCell.getKey()),
                            dataColQualTypeMap.get(preImageObjCell.getKey()).toObject(
                                    preImageObjCell.getValue().getValueArray()));
                }
            }
            rowValueMap.put(PRE_IMAGE, preImage);
        }

        Map<String, Object> changeImage = new HashMap<>();
        if (this.cdcChangeScopeSet.size() == 0 ||
                (this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE))) {
            for (Map.Entry<ImmutableBytesPtr, Cell> changeImageObjCell : changeImageObj.entrySet()) {
                if (dataColQualNameMap.get(changeImageObjCell.getKey()) != null) {
                    changeImage.put(dataColQualNameMap.get(changeImageObjCell.getKey()),
                            dataColQualTypeMap.get(changeImageObjCell.getKey()).toObject(
                                    changeImageObjCell.getValue().getValueArray()));
                }
            }
            rowValueMap.put(CHANGE_IMAGE, changeImage);
        }

        Map<String, Object> postImage = new HashMap<>();
        if (this.cdcChangeScopeSet.size() == 0 ||
                (this.cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST))){
            if (isIndexCellDeleteRow == false) {
                for (Map.Entry<ImmutableBytesPtr, Cell> changeImageObjCell : changeImageObj.entrySet()) {
                    if(dataColQualNameMap.get(changeImageObjCell.getKey()) != null) {
                        postImage.put(dataColQualNameMap.get(changeImageObjCell.getKey()),
                                dataColQualTypeMap.get(changeImageObjCell.getKey()).toObject(
                                        changeImageObjCell.getValue().getValueArray()));
                    }
                }
                for (Map.Entry<ImmutableBytesPtr, Cell> preImageObjCell : preImageObj.entrySet()) {
                    if(dataColQualNameMap.get(preImageObjCell.getKey()) != null
                            && postImage.get(dataColQualNameMap.get(preImageObjCell.getKey())) == null) {
                        postImage.put(dataColQualNameMap.get(preImageObjCell.getKey()),
                                dataColQualTypeMap.get(preImageObjCell.getKey()).toObject(
                                        preImageObjCell.getValue().getValueArray()));
                    }
                }
            }
            rowValueMap.put(POST_IMAGE, postImage);
        }


        if (isIndexCellDeleteRow) {
            rowValueMap.put(EVENT_TYPE, DELETE_EVENT_TYPE);
        } else {
            rowValueMap.put(EVENT_TYPE, UPSERT_EVENT_TYPE);
        }

        byte[] value =
                new Gson().toJson(rowValueMap).getBytes(StandardCharsets.UTF_8);
        CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
        Result cdcRow = Result.create(Arrays.asList(builder.
                setRow(indexToDataRowKeyMap.get(new ImmutableBytesPtr(firstCell.getRowArray(),
                        firstCell.getRowOffset(), firstCell.getRowLength())
                        .copyBytesIfNecessary())).
                setFamily(firstCell.getFamilyArray()).
                setQualifier(scan.getAttribute((CDC_JSON_COL_QUALIFIER))).
                setTimestamp(indexCellTs).
                setValue(value).
                setType(Cell.Type.Put).
                build()));

        return cdcRow;
    }

    @Override
    protected void scanDataTableRows(long startTime) throws IOException {
        super.scanDataTableRows(startTime);
        List<List<Cell>> indexRowList = new ArrayList<>();
        // Creating new Index Rows for Delete Row events
        for (int rowIndex = 0; rowIndex < indexRows.size(); rowIndex++) {
            List<Cell> indexRow = indexRows.get(rowIndex);
            indexRowList.add(indexRow);
            if (indexRow.size() > 1) {
                List<Cell> deleteRow = new ArrayList<>();
                for (int cellIndex = indexRow.size() - 1; cellIndex >= 0; cellIndex--) {
                    Cell cell = indexRow.get(cellIndex);
                    if (cell.getType() == Cell.Type.DeleteFamily) {
                        byte[] indexRowKey = new ImmutableBytesPtr(cell.getRowArray(),
                                cell.getRowOffset(), cell.getRowLength())
                                .copyBytesIfNecessary();
                        ImmutableBytesPtr dataRowKey = new ImmutableBytesPtr(
                                indexToDataRowKeyMap.get(indexRowKey));
                        Result dataRow = dataRows.get(dataRowKey);
                        List<Cell> resultCells = Arrays.asList(dataRow.rawCells());
                        for (Cell dataRowCell : resultCells) {
                            if (dataRowCell.getType() == Cell.Type.DeleteFamily && dataRowCell.getTimestamp() == cell.getTimestamp()) {
                                deleteRow.add(cell);
                                indexRowList.add(deleteRow);
                                break;
                            }
                        }
                    }
                    if (deleteRow.size() > 0) {
                        break;
                    }
                }
            }
        }
        this.indexRows = indexRowList;
    }
}
