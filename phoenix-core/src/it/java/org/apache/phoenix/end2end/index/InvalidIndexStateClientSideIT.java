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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.coprocessor.MetaDataProtocol.PHOENIX_MAJOR_VERSION;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.PHOENIX_PATCH_NUMBER;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(ParallelStatsDisabledTest.class)
public class InvalidIndexStateClientSideIT extends ParallelStatsDisabledIT {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(InvalidIndexStateClientSideIT.class);

    @Test
    public void testCachedConnections() throws Throwable {
        final String schemaName = generateUniqueName();
        final String tableName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        final String indexName = generateUniqueName();
        final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        final Connection conn = DriverManager.getConnection(getUrl());

        // create table and indices
        String createTableSql =
                "CREATE TABLE " + fullTableName
                        + "(org_id VARCHAR NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER, v3 INTEGER)";
        conn.createStatement().execute(createTableSql);
        conn.createStatement()
                .execute("CREATE INDEX " + indexName + " ON " + fullTableName + "(v1)");
        conn.commit();
        PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
        ConnectionQueryServices queryServices = phoenixConn.getQueryServices();
        Table metaTable =
                phoenixConn.getQueryServices()
                        .getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        long ts = EnvironmentEdgeManager.currentTimeMillis();
        MutationCode code =
                IndexUtil
                        .updateIndexState(fullIndexName, ts, metaTable, PIndexState.PENDING_DISABLE)
                        .getMutationCode();
        assertEquals(MutationCode.TABLE_ALREADY_EXISTS, code);
        ts = EnvironmentEdgeManager.currentTimeMillis();

        final byte[] schemaBytes = PVarchar.INSTANCE.toBytes(schemaName);
        final byte[] tableBytes = PVarchar.INSTANCE.toBytes(tableName);
        PName tenantId = phoenixConn.getTenantId();
        final long tableTimestamp = HConstants.LATEST_TIMESTAMP;
        long tableResolvedTimestamp = HConstants.LATEST_TIMESTAMP;
        final long resolvedTimestamp = tableResolvedTimestamp;
        final byte[] tenantIdBytes =
                tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes();
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        Batch.Call<MetaDataService, MetaDataResponse> callable =
                new Batch.Call<MetaDataService, MetaDataResponse>() {
                    @Override
                    public MetaDataResponse call(MetaDataService instance) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<MetaDataResponse> rpcCallback =
                                new BlockingRpcCallback<MetaDataResponse>();
                        GetTableRequest.Builder builder = GetTableRequest.newBuilder();
                        builder.setTenantId(ByteStringer.wrap(tenantIdBytes));
                        builder.setSchemaName(ByteStringer.wrap(schemaBytes));
                        builder.setTableName(ByteStringer.wrap(tableBytes));
                        builder.setTableTimestamp(tableTimestamp);
                        builder.setClientTimestamp(resolvedTimestamp);
                        builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION,
                            13, PHOENIX_PATCH_NUMBER));
                        instance.getTable(controller, builder.build(), rpcCallback);
                        if (controller.getFailedOn() != null) {
                            throw controller.getFailedOn();
                        }
                        return rpcCallback.get();
                    }
                };
        int version = VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, 13, PHOENIX_PATCH_NUMBER);
        LOGGER.info("Client version: " + version);
        Table ht =
                queryServices.getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        try {
            final Map<byte[], MetaDataResponse> results =
                    ht.coprocessorService(MetaDataService.class, tableKey, tableKey, callable);

            assert (results.size() == 1);
            MetaDataResponse result = results.values().iterator().next();
            assert (result.getTable().getIndexesCount() == 1);
            assert (PIndexState.valueOf(result.getTable().getIndexes(0).getIndexState())
                    .equals(PIndexState.DISABLE));
        } catch (Exception e) {
            LOGGER.error("Exception Occurred: " + e);

        } finally {
            Closeables.closeQuietly(ht);
        }

    }

}
