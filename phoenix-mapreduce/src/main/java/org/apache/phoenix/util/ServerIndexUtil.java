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
package org.apache.phoenix.util;

import static org.apache.phoenix.query.QueryConstants.VALUE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.VALUE_COLUMN_QUALIFIER;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.phoenix.compat.hbase.OffsetCell;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;

public class ServerIndexUtil {

    public static boolean isLocalIndexStore(Store store) {
        return store.getColumnFamilyDescriptor().getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX);
    }
    
    public static void writeLocalUpdates(Region region, final List<Mutation> mutations, boolean skipWAL) throws IOException {
        if(skipWAL) {
            for (Mutation m : mutations) {
                m.setDurability(Durability.SKIP_WAL);
            }
        }
        region.batchMutate(
            mutations.toArray(new Mutation[mutations.size()]));
    }
    
    
    public static void wrapResultUsingOffset(final RegionCoprocessorEnvironment environment,
            List<Cell> result, final int offset, ColumnReference[] dataColumns,
            TupleProjector tupleProjector, Region dataRegion, IndexMaintainer indexMaintainer,
            byte[][] viewConstants, ImmutableBytesWritable ptr) throws IOException {
        if (tupleProjector != null) {
            // Join back to data table here by issuing a local get projecting
            // all of the cq:cf from the KeyValueColumnExpression into the Get.
            Cell firstCell = result.get(0);
            byte[] indexRowKey = firstCell.getRowArray();
            ptr.set(indexRowKey, firstCell.getRowOffset() + offset, firstCell.getRowLength() - offset);
            byte[] dataRowKey = indexMaintainer.buildDataRowKey(ptr, viewConstants);
            Get get = new Get(dataRowKey);
            ImmutableStorageScheme storageScheme = indexMaintainer.getIndexStorageScheme();
            for (int i = 0; i < dataColumns.length; i++) {
                if (storageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
                    get.addFamily(dataColumns[i].getFamily());
                } else {
                    get.addColumn(dataColumns[i].getFamily(), dataColumns[i].getQualifier());
                }
            }
            Result joinResult = null;
            if (dataRegion != null) {
                joinResult = dataRegion.get(get);
            } else {
                TableName dataTable =
                    TableName.valueOf(MetaDataUtil.getLocalIndexUserTableName(environment.getRegion().
                        getTableDescriptor().getTableName().getNameAsString()));
                Table table = null;
                try {
                    table = environment.getConnection().getTable(dataTable);
                    joinResult = table.get(get);
                } finally {
                    if (table != null) table.close();
                }
            }
            // at this point join result has data from the data table. We now need to take this result and
            // add it to the cells that we are returning. 
            // TODO: handle null case (but shouldn't happen)
            Tuple joinTuple = new ResultTuple(joinResult);
            // This will create a byte[] that captures all of the values from the data table
            byte[] value =
                    tupleProjector.getSchema().toBytes(joinTuple, tupleProjector.getExpressions(),
                        tupleProjector.getValueBitSet(), ptr);
            Cell keyValue =
                    PhoenixKeyValueUtil.newKeyValue(firstCell.getRowArray(),
                        firstCell.getRowOffset(),firstCell.getRowLength(), VALUE_COLUMN_FAMILY,
                        VALUE_COLUMN_QUALIFIER, firstCell.getTimestamp(), value, 0, value.length);
            result.add(keyValue);
        }
        
        ListIterator<Cell> itr = result.listIterator();
        while (itr.hasNext()) {
            final Cell cell = itr.next();
            // TODO: Create DelegateCell class instead
            Cell newCell = new OffsetCell(cell, offset);
            itr.set(newCell);
        }
    }
}
