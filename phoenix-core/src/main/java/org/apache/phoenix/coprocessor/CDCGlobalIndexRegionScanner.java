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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CDC_JSON_COL_QUALIFIER;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.DATA_COL_QUALIFIER_TO_NAME_MAP;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.DATA_COL_QUALIFIER_TO_TYPE_MAP;

public class CDCGlobalIndexRegionScanner extends UncoveredGlobalIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CDCGlobalIndexRegionScanner.class);

    private Map<byte[], String> dataColQualNameMap;
    private Map<byte[], PDataType> dataColQualTypeMap;
    // Map<dataRowKey: Map<TS: Map<qualifier: Cell>>>
    private Map<byte[], Map<Long, Map<byte[], Cell>>> dataRowChanges =
            Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

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
    }

    @Override
    protected Scan prepareDataTableScan(Collection<byte[]> dataRowKeys) throws IOException {
        return CDCUtil.initForRawScan(prepareDataTableScan(dataRowKeys, true));
    }

    protected void scanDataRows(Collection<byte[]> dataRowKeys, long startTime) throws IOException {
        Scan dataScan = prepareDataTableScan(dataRowKeys);
        if (dataScan == null) {
            return;
        }
        try (ResultScanner resultScanner = dataHTable.getScanner(dataScan)) {
            for (Result result = resultScanner.next(); (result != null);
                 result = resultScanner.next()) {
                if (ScanUtil.isDummy(result)) {
                    state = State.SCANNING_DATA_INTERRUPTED;
                    break;
                }

                List<Cell> resultCells = Arrays.asList(result.rawCells());
                Collections.sort(resultCells, CellComparator.getInstance().reversed());
                List<Cell> deleteMarkers = new ArrayList<>();
                List<List<Cell>> columns = new LinkedList<>();
                Cell currentColumnCell = null;
                Pair<byte[], byte[]> emptyKV = EncodedColumnsUtil.getEmptyKeyValueInfo(
                        EncodedColumnsUtil.getQualifierEncodingScheme(scan));
                List<Cell> currentColumn = null;
                Set<Long> uniqueTimeStamps = new HashSet<>();
                for (Cell cell : resultCells) {
                    uniqueTimeStamps.add(cell.getTimestamp());
                    if (cell.getType() != Cell.Type.Put) {
                        deleteMarkers.add(cell);
                    }
                    if (CellUtil.matchingColumn(cell, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                            emptyKV.getFirst())) {
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
                List<Long> sortedTimestamps = uniqueTimeStamps.stream().sorted().collect(
                        Collectors.toList());
                Map<byte[], Cell> rollingRow = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                int[] columnPointers = new int[columns.size()];
                Map<Long, Map<byte[], Cell>> changeTimeline = dataRowChanges.get(result.getRow());
                if (changeTimeline == null) {
                    changeTimeline = new TreeMap();
                    dataRowChanges.put(result.getRow(), changeTimeline);
                }
                for (Long ts: sortedTimestamps) {
                    for (int i = 0; i < columns.size(); ++i) {
                        Cell cell = columns.get(i).get(columnPointers[i]);
                        if (cell.getTimestamp() == ts) {
                            rollingRow.put(cell.getQualifierArray(), cell);
                            ++columnPointers[i];
                        }
                    }
                    Map rowOfCells = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                    rowOfCells.putAll(rollingRow);
                    changeTimeline.put(ts, rowOfCells);
                }

                if ((EnvironmentEdgeManager.currentTimeMillis() - startTime) >= pageSizeMs) {
                    state = State.SCANNING_DATA_INTERRUPTED;
                    break;
                }
            }
            if (state == State.SCANNING_DATA_INTERRUPTED) {
                LOGGER.info("One of the scan tasks in UncoveredGlobalIndexRegionScanner"
                        + " for region " + region.getRegionInfo().getRegionNameAsString()
                        + " could not complete on time (in " + pageSizeMs + " ms) and"
                        + " will be resubmitted");
            }
        } catch (Throwable t) {
            exceptionMessage = "scanDataRows fails for at least one task";
            ServerUtil.throwIOException(dataHTable.getName().toString(), t);
        }
    }

    protected boolean getNextCoveredIndexRow(List<Cell> result) throws IOException {
        if (indexRowIterator.hasNext()) {
            List<Cell> indexRow = indexRowIterator.next();
            for (Cell c: indexRow) {
                if (c.getType() == Cell.Type.Put) {
                    result.add(c);
                }
            }
            try {
                byte[] indexRowKey = CellUtil.cloneRow(indexRow.get(0));
                Long indexRowTs = result.get(0).getTimestamp();
                Map<Long, Map<byte[], Cell>> changeTimeline = dataRowChanges.get(
                        indexToDataRowKeyMap.get(indexRowKey));
                Map<byte[], Cell> mapOfCells = changeTimeline != null ? changeTimeline.get(indexRowTs) : null;
                Result dataRow = null;
                if (mapOfCells != null) {
                    Map <String, Object> rowValueMap = new HashMap<>(mapOfCells.size());
                    for (Map.Entry<byte[], Cell> entry: mapOfCells.entrySet()) {
                        String colName = dataColQualNameMap.get(entry.getKey());
                        Object colVal = dataColQualTypeMap.get(entry.getKey()).toObject(
                                entry.getValue().getValueArray());
                        rowValueMap.put(colName, colVal);
                    }
                    Cell firstCell = result.get(0);
                    byte[] value =
                            new Gson().toJson(rowValueMap).getBytes(StandardCharsets.UTF_8);
                    CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
                    dataRow = Result.create(Arrays.asList(builder.
                            setRow(indexToDataRowKeyMap.get(indexRowKey)).
                            setFamily(firstCell.getFamilyArray()).
                            setQualifier(scan.getAttribute((CDC_JSON_COL_QUALIFIER))).
                            setTimestamp(indexRow.get(0).getTimestamp()).
                            setValue(value).
                            setType(Cell.Type.Put).
                            build()));
                }
                if (dataRow != null && tupleProjector != null) {
                    IndexUtil.addTupleAsOneCell(result, new ResultTuple(dataRow),
                            tupleProjector, ptr);
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
}
