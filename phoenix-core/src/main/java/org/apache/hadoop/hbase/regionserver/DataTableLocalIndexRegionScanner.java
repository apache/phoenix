/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.coprocessor.DelegateRegionScanner;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;

public class DataTableLocalIndexRegionScanner extends DelegateRegionScanner {
    MultiKeyValueTuple result = new MultiKeyValueTuple();
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    KeyValueBuilder kvBuilder = GenericKeyValueBuilder.INSTANCE;
    private List<IndexMaintainer> indexMaintainers;
    private byte[] startKey;
    private byte[] endKey;
    private byte[] localIndexFamily;

    public DataTableLocalIndexRegionScanner(RegionScanner scanner, Region region,
            List<IndexMaintainer> indexMaintainers, byte[] localIndexFamily) throws IOException {
        super(scanner);
        this.indexMaintainers = indexMaintainers;
        this.startKey = region.getRegionInfo().getStartKey();
        this.endKey = region.getRegionInfo().getEndKey();
        this.localIndexFamily = localIndexFamily;
    }

    @Override
    public boolean next(List<Cell> outResult, ScannerContext scannerContext) throws IOException {
        List<Cell> dataTableResults = new ArrayList<Cell>();
        boolean next = super.next(dataTableResults, scannerContext);
        getLocalIndexCellsFromDataTable(dataTableResults, outResult);
        return next;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        List<Cell> dataTableResults = new ArrayList<Cell>();
        boolean next = super.next(dataTableResults);
        getLocalIndexCellsFromDataTable(dataTableResults, results);
        return next;
    }

    private void getLocalIndexCellsFromDataTable(List<Cell> dataTableResults, List<Cell> localIndexResults)
            throws IOException {
        if (!dataTableResults.isEmpty()) {
            result.setKeyValues(dataTableResults);
            for (IndexMaintainer maintainer : indexMaintainers) {
                result.getKey(ptr);
                ValueGetter valueGetter = maintainer
                        .createGetterFromKeyValues(ImmutableBytesPtr.copyBytesIfNecessary(ptr), dataTableResults);
                List<Cell> list = maintainer.buildUpdateMutation(kvBuilder, valueGetter, ptr,
                        dataTableResults.get(0).getTimestamp(), startKey, endKey).getFamilyCellMap()
                        .get(localIndexFamily);
                if (list != null) {
                    localIndexResults.addAll(list);
                }
            }
        }
    }

}
