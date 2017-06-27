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

import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.coprocessor.DelegateRegionScanner;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver.MutationList;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.util.ServerUtil;
/*
 * Scanner to read data store and regenerate the local index data
 */
public class DataTableLocalIndexRegionScanner extends DelegateRegionScanner {
    MultiKeyValueTuple result = new MultiKeyValueTuple();
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    KeyValueBuilder kvBuilder = GenericKeyValueBuilder.INSTANCE;
    private List<IndexMaintainer> indexMaintainers;
    private byte[] startKey;
    private byte[] endKey;
    private byte[] localIndexFamily;
    private Region region;
    long maxBatchSizeBytes;
    int maxBatchSize;
    private MutationList mutationList;
    
    
    /**
     * @param scanner Scanner for data table stores 
     * @param region 
     * @param indexMaintainers Maintainer of local Indexes which needs to built
     * @param localIndexFamily LocalIndex family needs to be built.
     * @param conf
     * @throws IOException
     */
    public DataTableLocalIndexRegionScanner(RegionScanner scanner, Region region,
            List<IndexMaintainer> indexMaintainers, byte[] localIndexFamily,Configuration conf) throws IOException {
        super(scanner);
        this.indexMaintainers = indexMaintainers;
        this.startKey = region.getRegionInfo().getStartKey();
        this.endKey = region.getRegionInfo().getEndKey();
        this.localIndexFamily = localIndexFamily;
        this.region=region;
        maxBatchSize = conf.getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
        maxBatchSizeBytes = conf.getLong(MUTATE_BATCH_SIZE_BYTES_ATTRIB,
            QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE_BYTES);
        mutationList=new UngroupedAggregateRegionObserver.MutationList(maxBatchSize);   
    }

    @Override
    public boolean next(List<Cell> outResult, ScannerContext scannerContext) throws IOException {
        List<Cell> dataTableResults = new ArrayList<Cell>();
        boolean next = super.next(dataTableResults, scannerContext);
        addMutations(dataTableResults);
        if (ServerUtil.readyToCommit(mutationList.size(), mutationList.byteSize(), maxBatchSize, maxBatchSizeBytes)||!next) {
            region.batchMutate(mutationList.toArray(new Mutation[mutationList.size()]), HConstants.NO_NONCE,
                    HConstants.NO_NONCE);
            mutationList.clear();
        }
        return next;
    }

    private void addMutations(List<Cell> dataTableResults) throws IOException {
        if (!dataTableResults.isEmpty()) {
            result.setKeyValues(dataTableResults);
            for (IndexMaintainer maintainer : indexMaintainers) {
                result.getKey(ptr);
                ValueGetter valueGetter = maintainer
                        .createGetterFromKeyValues(ImmutableBytesPtr.copyBytesIfNecessary(ptr), dataTableResults);
                List<Cell> list = maintainer.buildUpdateMutation(kvBuilder, valueGetter, ptr,
                        dataTableResults.get(0).getTimestamp(), startKey, endKey).getFamilyCellMap()
                        .get(localIndexFamily);
                Put put = null;
                Delete del = null;
                for (Cell cell : list) {
                    if (KeyValue.Type.codeToType(cell.getTypeByte()) == KeyValue.Type.Put) {
                        if (put == null) {
                            put = new Put(CellUtil.cloneRow(cell));
                            mutationList.add(put);
                        }
                        put.add(cell);
                    } else {
                        if (del == null) {
                            del = new Delete(CellUtil.cloneRow(cell));
                            mutationList.add(del);
                        }
                        del.addDeleteMarker(cell);
                    }
                }
            }
        }
    }
    

}
