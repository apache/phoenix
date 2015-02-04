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
package org.apache.phoenix.index;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.UpdateIndexStateRequest;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.write.KillServerOnFailurePolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Multimap;

/**
 * 
 * Handler called in the event that index updates cannot be written to their
 * region server. First attempts to disable the index and failing that falls
 * back to the default behavior of killing the region server.
 *
 * TODO: use delegate pattern instead
 * 
 * 
 * @since 2.1
 */
public class PhoenixIndexFailurePolicy extends  KillServerOnFailurePolicy {
    private static final Log LOG = LogFactory.getLog(PhoenixIndexFailurePolicy.class);
    private RegionCoprocessorEnvironment env;

    public PhoenixIndexFailurePolicy() {
    }

    @Override
    public void setup(Stoppable parent, RegionCoprocessorEnvironment env) {
      super.setup(parent, env);
      this.env = env;
    }

    /**
     * Attempt to disable the index table when we can't write to it, preventing future updates until the index is
     * brought up to date, but allowing historical reads to continue until then.
     * <p>
     * In the case that we cannot reach the metadata information, we will fall back to the default policy and kill
     * this server, so we can attempt to replay the edits on restart.
     * </p>
     * @param attempted the mutations that were attempted to be written and the tables to which they were written
     * @param cause root cause of the failure
     */
    @Override
    public void handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause) {

        try {
            handleFailureWithExceptions(attempted, cause);
        } catch (Throwable t) {
            LOG.warn("handleFailure failed", t);
            super.handleFailure(attempted, cause);
        }
    }

    private void handleFailureWithExceptions(Multimap<HTableInterfaceReference, Mutation> attempted,
            Exception cause) throws Throwable {
        Set<HTableInterfaceReference> refs = attempted.asMap().keySet();
        Map<String, Long> indexTableNames = new HashMap<String, Long>(refs.size());
        // start by looking at all the tables to which we attempted to write
        for (HTableInterfaceReference ref : refs) {
            long minTimeStamp = 0;

            // get the minimum timestamp across all the mutations we attempted on that table
            Collection<Mutation> mutations = attempted.get(ref);
            if (mutations != null) {
                for (Mutation m : mutations) {
                    for (List<Cell> kvs : m.getFamilyCellMap().values()) {
                        for (Cell kv : kvs) {
                            if (minTimeStamp == 0 || (kv.getTimestamp() >= 0 && minTimeStamp < kv.getTimestamp())) {
                                minTimeStamp = kv.getTimestamp();
                            }
                        }
                    }
                }
            }

            // its a local index table, so we need to convert it to the index table names we should disable
            if (ref.getTableName().startsWith(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX)) {
                for (String tableName : getLocalIndexNames(ref, mutations)) {
                    indexTableNames.put(tableName, minTimeStamp);
                }
            } else {
                indexTableNames.put(ref.getTableName(), minTimeStamp);
            }
        }

        // for all the index tables that we've found, try to disable them and if that fails, try to
        for (Map.Entry<String, Long> tableTimeElement :indexTableNames.entrySet()){
            String indexTableName = tableTimeElement.getKey();
            long minTimeStamp = tableTimeElement.getValue();
            // Disable the index by using the updateIndexState method of MetaDataProtocol end point coprocessor.
            byte[] indexTableKey = SchemaUtil.getTableKeyFromFullName(indexTableName);
            HTableInterface
                    systemTable =
                    env.getTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES));
            // Mimic the Put that gets generated by the client on an update of the index state
            Put put = new Put(indexTableKey);
            put.add(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_STATE_BYTES,
                    PIndexState.DISABLE.getSerializedBytes());
            put.add(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES,
                PLong.INSTANCE.toBytes(minTimeStamp));
            final List<Mutation> tableMetadata = Collections.<Mutation>singletonList(put);

            final Map<byte[], MetaDataResponse> results =
                    systemTable.coprocessorService(MetaDataService.class, indexTableKey, indexTableKey,
                            new Batch.Call<MetaDataService, MetaDataResponse>() {
                                @Override
                                public MetaDataResponse call(MetaDataService instance) throws IOException {
                                    ServerRpcController controller = new ServerRpcController();
                                    BlockingRpcCallback<MetaDataResponse> rpcCallback =
                                            new BlockingRpcCallback<MetaDataResponse>();
                                    UpdateIndexStateRequest.Builder builder = UpdateIndexStateRequest.newBuilder();
                                    for (Mutation m : tableMetadata) {
                                        MutationProto mp = ProtobufUtil.toProto(m);
                                        builder.addTableMetadataMutations(mp.toByteString());
                                    }
                                    instance.updateIndexState(controller, builder.build(), rpcCallback);
                                    if (controller.getFailedOn() != null) {
                                        throw controller.getFailedOn();
                                    }
                                    return rpcCallback.get();
                                }
                            });
            if (results.isEmpty()) {
                throw new IOException("Didn't get expected result size");
            }
            MetaDataResponse tmpResponse = results.values().iterator().next();
            MetaDataMutationResult result = MetaDataMutationResult.constructFromProto(tmpResponse);

            if (result.getMutationCode() == MutationCode.TABLE_NOT_FOUND) {
                LOG.info("Index " + indexTableName + " has been dropped. Ignore uncommitted mutations");
                continue;
            }
            if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                LOG.warn("Attempt to disable index " + indexTableName + " failed with code = "
                        + result.getMutationCode() + ". Will use default failure policy instead.");
                throw new DoNotRetryIOException("Attempt to disable " + indexTableName + " failed.");
            }
            LOG.info("Successfully disabled index " + indexTableName + " due to an exception while writing updates.",
                    cause);
        }
    }

    private Collection<? extends String> getLocalIndexNames(HTableInterfaceReference ref,
            Collection<Mutation> mutations) throws IOException {
        Set<String> indexTableNames = new HashSet<String>(1);
        PhoenixConnection conn = null;
        try {
            conn = QueryUtil.getConnection(this.env.getConfiguration()).unwrap(
                    PhoenixConnection.class);
            String userTableName = MetaDataUtil.getUserTableName(ref.getTableName());
            PTable dataTable = PhoenixRuntime.getTable(conn, userTableName);
            List<PTable> indexes = dataTable.getIndexes();
            // local index used to get view id from index mutation row key.
            PTable localIndex = null;
            Map<ImmutableBytesWritable, String> localIndexNames =
                    new HashMap<ImmutableBytesWritable, String>();
            for (PTable index : indexes) {
                if (index.getIndexType() == IndexType.LOCAL
                        && index.getIndexState() == PIndexState.ACTIVE) {
                    if (localIndex == null) localIndex = index;
                    localIndexNames.put(new ImmutableBytesWritable(MetaDataUtil.getViewIndexIdDataType().toBytes(
                            index.getViewIndexId())), index.getName().getString());
                }
            }
            if (localIndex == null) {
                return Collections.emptySet();
            }

            IndexMaintainer indexMaintainer = localIndex.getIndexMaintainer(dataTable, conn);
            HRegionInfo regionInfo = this.env.getRegion().getRegionInfo();
            int offset =
                    regionInfo.getStartKey().length == 0 ? regionInfo.getEndKey().length
                            : regionInfo.getStartKey().length;
            byte[] viewId = null;
            for (Mutation mutation : mutations) {
                viewId =
                        indexMaintainer.getViewIndexIdFromIndexRowKey(
                                new ImmutableBytesWritable(mutation.getRow(), offset,
                                        mutation.getRow().length - offset));
                String indexTableName = localIndexNames.get(new ImmutableBytesWritable(viewId));
                indexTableNames.add(indexTableName);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        }
        return indexTableNames;
    }
}
