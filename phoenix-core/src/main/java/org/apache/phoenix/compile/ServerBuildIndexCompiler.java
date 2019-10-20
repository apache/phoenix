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
import java.util.Collections;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ScanUtil;

import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;


/**
 * Class that compiles queryPlan to generate initial data values after a DDL command for
 * index table.
 */
public class ServerBuildIndexCompiler {
    private final PhoenixConnection connection;
    private final String indexTableFullName;
    private final String dataTableFullName;
    private PTable dataTable;
    private QueryPlan queryPlan;

    private class RowCountMutationPlan extends BaseMutationPlan {
        private RowCountMutationPlan(StatementContext context, PhoenixStatement.Operation operation) {
            super(context, operation);
        }
        @Override
        public MutationState execute() throws SQLException {
            connection.getMutationState().commitDDLFence(dataTable);
            Tuple tuple = queryPlan.iterator().next();
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
            return new MutationState(0, 0, connection, rowCount);
        }

        @Override
        public QueryPlan getQueryPlan() {
            return queryPlan;
        }
    };
    
    public ServerBuildIndexCompiler(PhoenixConnection connection, String dataTableFullName, String indexTableFullName) {
        this.connection = connection;
        this.dataTableFullName = dataTableFullName;
        this.indexTableFullName = indexTableFullName;
    }

    public MutationPlan compile() throws SQLException {
        PTable index = PhoenixRuntime.getTable(connection, indexTableFullName);
        dataTable = PhoenixRuntime.getTable(connection, dataTableFullName);
        if (index.getIndexType() == PTable.IndexType.GLOBAL &&  dataTable.isTransactional()) {
            throw new IllegalArgumentException(
                    "ServerBuildIndexCompiler does not support global indexes on transactional tables");
        }
        PostDDLCompiler compiler = new PostDDLCompiler(connection);
        TableRef dataTableRef = new TableRef(dataTable);
        compiler.compile(Collections.singletonList(dataTableRef),
                null, null, null, HConstants.LATEST_TIMESTAMP);
        queryPlan = compiler.getQueryPlan(dataTableRef);
        Scan dataTableScan = IndexManagementUtil.newLocalStateScan(queryPlan.getContext().getScan(),
                Collections.singletonList(index.getIndexMaintainer(dataTable, connection)));
        ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
        IndexMaintainer.serializeAdditional(dataTable, indexMetaDataPtr, Collections.singletonList(index),
                connection);
        byte[] attribValue = ByteUtil.copyKeyBytesIfNecessary(indexMetaDataPtr);

        // Set the scan attributes that UngroupedAggregateRegionObserver will switch on.
        // For local indexes, the BaseScannerRegionObserver.LOCAL_INDEX_BUILD_PROTO attribute, and
        // for global indexes PhoenixIndexCodec.INDEX_PROTO_MD attribute is set to the serialized form of index
        // metadata to build index rows from data table rows. For global indexes, we also need to set (1) the
        // BaseScannerRegionObserver.REBUILD_INDEXES attribute in order to signal UngroupedAggregateRegionObserver
        // that this scan is for building global indexes and (2) the MetaDataProtocol.PHOENIX_VERSION attribute
        // that will be passed as a mutation attribute for the scanned mutations that will be applied on
        // the index table possibly remotely
        ScanUtil.setClientVersion(dataTableScan, MetaDataProtocol.PHOENIX_VERSION);
        if (index.getIndexType() == PTable.IndexType.LOCAL) {
            dataTableScan.setAttribute(BaseScannerRegionObserver.LOCAL_INDEX_BUILD_PROTO, attribValue);
        } else {
            dataTableScan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, attribValue);
            dataTableScan.setAttribute(BaseScannerRegionObserver.REBUILD_INDEXES, TRUE_BYTES);
            ScanUtil.setClientVersion(dataTableScan, MetaDataProtocol.PHOENIX_VERSION);
        }
        if (dataTable.isTransactional()) {
            dataTableScan.setAttribute(BaseScannerRegionObserver.TX_STATE, connection.getMutationState().encodeTransaction());
        }

        // Go through MutationPlan abstraction so that we can create local indexes
        // with a connectionless connection (which makes testing easier).
        return new RowCountMutationPlan(queryPlan.getContext(), PhoenixStatement.Operation.UPSERT);
    }
}
