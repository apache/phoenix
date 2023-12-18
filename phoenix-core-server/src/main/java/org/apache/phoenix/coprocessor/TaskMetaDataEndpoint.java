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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.TaskMetaDataProtos
    .TaskMetaDataService;
import org.apache.phoenix.coprocessor.generated.TaskMetaDataProtos
    .TaskMutateRequest;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.phoenix.coprocessor.MetaDataEndpointImpl
    .mutateRowsWithLocks;

/**
 * Phoenix metadata mutations for SYSTEM.TASK flows through this co-processor
 * Endpoint.
 */
public class TaskMetaDataEndpoint extends TaskMetaDataService
        implements RegionCoprocessor {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(TaskMetaDataEndpoint.class);

    private RegionCoprocessorEnvironment env;
    private PhoenixMetaDataCoprocessorHost phoenixAccessCoprocessorHost;
    private boolean accessCheckEnabled;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
        this.phoenixAccessCoprocessorHost =
            new PhoenixMetaDataCoprocessorHost(this.env);
        this.accessCheckEnabled = env.getConfiguration().getBoolean(
            QueryServices.PHOENIX_ACLS_ENABLED,
            QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
    }

    @Override
    public void upsertTaskDetails(RpcController controller,
            TaskMutateRequest request, RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        try {
            List<Mutation> taskMutations = ProtobufUtil.getMutations(request);
            if (taskMutations.isEmpty()) {
                done.run(builder.build());
                return;
            }
            byte[][] rowKeyMetaData = new byte[3][];
            MetaDataUtil.getTenantIdAndSchemaAndTableName(taskMutations,
                rowKeyMetaData);
            byte[] schemaName =
                rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] tableName =
                rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            String fullTableName = SchemaUtil.getTableName(schemaName,
                tableName);

            phoenixAccessCoprocessorHost.preUpsertTaskDetails(fullTableName);

            mutateRowsWithLocks(this.accessCheckEnabled, this.env.getRegion(),
                taskMutations, Collections.emptySet(), HConstants.NO_NONCE,
                HConstants.NO_NONCE);
        } catch (Throwable t) {
            LOGGER.error("Unable to write mutations to {}",
                PhoenixDatabaseMetaData.SYSTEM_TASK_NAME, t);
            builder.setReturnCode(
                MetaDataProtos.MutationCode.UNABLE_TO_UPSERT_TASK);
            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
            done.run(builder.build());
        }
    }
}
