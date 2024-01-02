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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.execute.BaseQueryPlan;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.transform.TransformMaintainer;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.SQLException;

import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.apache.phoenix.util.ScanUtil.addEmptyColumnToScan;


/**
 * Class that compiles plan to generate initial data values after a DDL command for
 * transforming table (new table).
 */
public class ServerBuildTransformingTableCompiler extends ServerBuildIndexCompiler {

    public ServerBuildTransformingTableCompiler(PhoenixConnection connection, String tableName) {
        super(connection, tableName);
    }

    public MutationPlan compile(PTable newTable) throws SQLException {
        try (final PhoenixStatement statement = new PhoenixStatement(connection)) {
            String query = "SELECT /*+ NO_INDEX */ count(*) FROM " + tableName;
            this.plan = statement.compileQuery(query);
            TableRef tableRef = plan.getTableRef();
            Scan scan = plan.getContext().getScan();
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            dataTable = tableRef.getTable();

            // By default, we'd use a FirstKeyOnly filter as nothing else needs to be projected for count(*).
            // However, in this case, we need to project all of the data columns
            for (PColumnFamily family : dataTable.getColumnFamilies()) {
                scan.addFamily(family.getName().getBytes());
            }

            scan.setAttribute(BaseScannerRegionObserverConstants.DO_TRANSFORMING, TRUE_BYTES);
            TransformMaintainer.serialize(dataTable, ptr, newTable, plan.getContext().getConnection());

            ScanUtil.annotateScanWithMetadataAttributes(dataTable, scan);
            scan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, ByteUtil.copyKeyBytesIfNecessary(ptr));
            scan.setAttribute(BaseScannerRegionObserverConstants.REBUILD_INDEXES, TRUE_BYTES);
            ScanUtil.setClientVersion(scan, MetaDataProtocol.PHOENIX_VERSION);
            scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_PAGING, TRUE_BYTES);
            // Serialize page row size only if we're overriding, else use server side value
            String rebuildPageRowSize =
                    connection.getQueryServices().getProps()
                            .get(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS);
            if (rebuildPageRowSize != null) {
                scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_PAGE_ROWS,
                        Bytes.toBytes(Long.valueOf(rebuildPageRowSize)));
            }
            BaseQueryPlan.serializeViewConstantsIntoScan(scan, dataTable);
            PTable.QualifierEncodingScheme encodingScheme = newTable.getEncodingScheme();
            addEmptyColumnToScan(scan, SchemaUtil.getEmptyColumnFamily(newTable), EncodedColumnsUtil.getEmptyKeyValueInfo(encodingScheme).getFirst());

            if (dataTable.isTransactional()) {
                scan.setAttribute(BaseScannerRegionObserverConstants.TX_STATE, connection.getMutationState().encodeTransaction());
            }

            // Go through MutationPlan abstraction so that we can create local indexes
            // with a connectionless connection (which makes testing easier).
            return new RowCountMutationPlan(plan.getContext(), PhoenixStatement.Operation.UPSERT);
        }
    }
}