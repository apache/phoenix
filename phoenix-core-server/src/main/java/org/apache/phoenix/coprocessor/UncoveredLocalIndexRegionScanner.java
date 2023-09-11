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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerIndexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class UncoveredLocalIndexRegionScanner extends UncoveredIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UncoveredLocalIndexRegionScanner.class);
    final int offset;
    final byte[] actualStartKey;

    public UncoveredLocalIndexRegionScanner(final RegionScanner innerScanner,
                                            final Region region,
                                            final Scan scan,
                                            final RegionCoprocessorEnvironment env,
                                            final Scan dataTableScan,
                                            final TupleProjector tupleProjector,
                                            final IndexMaintainer indexMaintainer,
                                            final byte[][] viewConstants,
                                            final ImmutableBytesWritable ptr,
                                            final long pageSizeMs,
                                            final int offset,
                                            final byte[] actualStartKey,
                                            final long queryLimit) {
        super(innerScanner, region, scan, env, dataTableScan, tupleProjector, indexMaintainer,
                viewConstants, ptr, pageSizeMs, queryLimit);
        this.offset = offset;
        this.actualStartKey = actualStartKey;
    }

    protected void scanDataRows(Collection<byte[]> dataRowKeys, long startTime) throws IOException {
        Scan dataScan = prepareDataTableScan(dataRowKeys);
        if (dataScan == null) {
            return;
        }
        try (RegionScanner regionScanner = region.getScanner(dataScan)) {
            boolean hasMore;
            do {
                List<Cell> row = new ArrayList<Cell>();
                hasMore = regionScanner.nextRaw(row);

                if (!row.isEmpty()) {
                    if (ScanUtil.isDummy(row)) {
                        state = State.SCANNING_DATA_INTERRUPTED;
                        break;
                    }
                    Cell firstCell = row.get(0);
                    dataRows.put(new ImmutableBytesPtr(CellUtil.cloneRow(firstCell)),
                            Result.create(row));
                    if (hasMore &&
                            (EnvironmentEdgeManager.currentTimeMillis() - startTime) >=
                                    pageSizeMs) {
                        state = State.SCANNING_DATA_INTERRUPTED;
                        break;
                    }
                }
            } while (hasMore);
            if (state == State.SCANNING_DATA_INTERRUPTED) {
                LOGGER.info("Data table scan is interrupted in "
                        + "UncoveredLocalIndexRegionScanner for region "
                        + region.getRegionInfo().getRegionNameAsString()
                        + " as it could not complete on time (in " + pageSizeMs + " ms), and"
                        + " it will be resubmitted");
            }
        }
    }

    @Override
    protected void scanDataTableRows(long startTime)
            throws IOException {
        if (indexToDataRowKeyMap.size() == 0) {
            state = State.READY;
            return;
        }
        scanDataRows(indexToDataRowKeyMap.values(), startTime);
        if (state == State.SCANNING_DATA_INTERRUPTED) {
            state = State.SCANNING_DATA;
        } else {
            state = State.READY;
        }
    }

    @Override
    protected boolean scanIndexTableRows(List<Cell> result,
                                         final long startTime) throws IOException {
        return scanIndexTableRows(result, startTime, actualStartKey, offset);
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        boolean hasMore = super.next(result);
        ServerIndexUtil.wrapResultUsingOffset(result, offset);
        return hasMore;
    }
}
