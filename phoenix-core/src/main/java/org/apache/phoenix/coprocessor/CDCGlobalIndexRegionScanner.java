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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.COL_QUALIFIER_TO_NAME_MAP;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.PK_COL_EXPRESSIONS;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.PK_COL_NAMES_AND_TYPES;
import static org.apache.phoenix.util.ScanUtil.deserializePKColExpressions;

public class CDCGlobalIndexRegionScanner extends UncoveredGlobalIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CDCGlobalIndexRegionScanner.class);

    private Map<byte[], String> colQualNameMap;
    private List<Pair<String, PDataType>> pkColNamesAndTypes;
    private final List<RowKeyColumnExpression> pkColExpressions;

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
        colQualNameMap = ScanUtil.deserializeColumnQualifierToNameMap(
                scan.getAttribute(COL_QUALIFIER_TO_NAME_MAP));
        //pkColNamesAndTypes = ScanUtil.deserializePKColNamesAndTypes(
        //        scan.getAttribute(PK_COL_NAMES_AND_TYPES));
        pkColExpressions = deserializePKColExpressions(scan.getAttribute(PK_COL_EXPRESSIONS));
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
                LinkedList<Cell> currentColumn = null;
                for (Cell cell : resultCells) {
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

                    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                    Tuple tuple = new ResultTuple(result);
                    tuple.getKey(ptr);
                    for (RowKeyColumnExpression expr: pkColExpressions) {
                        expr.evaluate(tuple, ptr);
                        if (ptr.getLength() == 0) {
                            continue;
                        }
                        Object o = expr.getDataType().toObject(ptr, expr.getDataType(), expr.getSortOrder(),
                                expr.getMaxLength(), expr.getScale());
                    }
                }
                if (currentColumn != null) {
                    columns.add(currentColumn);
                }

                dataRows.put(new ImmutableBytesPtr(result.getRow()), result);
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
}
