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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.collect.Lists;

/**
 * For local indexes, we optimize the initial index population by *not* sending
 * Puts over the wire for the index rows, as we don't need to do that. Instead,
 * we tap into our region observer to generate the index rows based on the data
 * rows as we scan
 */
public class PostLocalIndexDDLCompiler {
	private final PhoenixConnection connection;
    private final String tableName;
    
    public PostLocalIndexDDLCompiler(PhoenixConnection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

	public MutationPlan compile(PTable index) throws SQLException {
		try (final PhoenixStatement statement = new PhoenixStatement(connection)) {
            String query = "SELECT count(*) FROM " + tableName;
            final QueryPlan plan = statement.compileQuery(query);
            TableRef tableRef = plan.getTableRef();
            Scan scan = plan.getContext().getScan();
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            final PTable dataTable = tableRef.getTable();
            List<PTable> indexes = Lists.newArrayListWithExpectedSize(1);
            for (PTable indexTable : dataTable.getIndexes()) {
                if (indexTable.getKey().equals(index.getKey())) {
                    index = indexTable;
                    break;
                }
            }
            // Only build newly created index.
            indexes.add(index);
            IndexMaintainer.serialize(dataTable, ptr, indexes, plan.getContext().getConnection());
            // Set attribute on scan that UngroupedAggregateRegionObserver will switch on.
            // We'll detect that this attribute was set the server-side and write the index
            // rows per region as a result. The value of the attribute will be our persisted
            // index maintainers.
            // Define the LOCAL_INDEX_BUILD as a new static in BaseScannerRegionObserver
            scan.setAttribute(BaseScannerRegionObserver.LOCAL_INDEX_BUILD_PROTO, ByteUtil.copyKeyBytesIfNecessary(ptr));
            // By default, we'd use a FirstKeyOnly filter as nothing else needs to be projected for count(*).
            // However, in this case, we need to project all of the data columns that contribute to the index.
            IndexMaintainer indexMaintainer = index.getIndexMaintainer(dataTable, connection);
            for (ColumnReference columnRef : indexMaintainer.getAllColumns()) {
                if (index.getImmutableStorageScheme() == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
                    scan.addFamily(columnRef.getFamily());
                } else {
                    scan.addColumn(columnRef.getFamily(), columnRef.getQualifier());
                }
            }

            // Go through MutationPlan abstraction so that we can create local indexes
            // with a connectionless connection (which makes testing easier).
            return new BaseMutationPlan(plan.getContext(), Operation.UPSERT) {

                @Override
                public MutationState execute() throws SQLException {
                    connection.getMutationState().commitDDLFence(dataTable);
                    Tuple tuple = plan.iterator().next();
                    long rowCount = 0;
                    if (tuple != null) {
                        Cell kv = tuple.getValue(0);
                        ImmutableBytesWritable tmpPtr = new ImmutableBytesWritable(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
                        // A single Cell will be returned with the count(*) - we decode that here
                        rowCount = PLong.INSTANCE.getCodec().decodeLong(tmpPtr, SortOrder.getDefault());
                    }
                    // The contract is to return a MutationState that contains the number of rows modified. In this
                    // case, it's the number of rows in the data table which corresponds to the number of index
                    // rows that were added.
                    return new MutationState(0, connection, rowCount);
                }

            };
        }
	}
	
}
