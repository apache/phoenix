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

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.TaskMetaDataProtos;
import org.apache.phoenix.coprocessor.generated.TaskMetaDataProtos
    .TaskMetaDataService;
import org.apache.phoenix.protobuf.ProtobufUtil;

import java.io.IOException;
import java.util.List;

/**
 * Callable implementation for coprocessor endpoint associated with
 * SYSTEM.TASK
 */
public class TaskMetaDataServiceCallBack
        implements Batch.Call<TaskMetaDataService, MetaDataResponse> {

    private final List<Mutation> taskMutations;

    public TaskMetaDataServiceCallBack(List<Mutation> taskMutations) {
        this.taskMutations = taskMutations;
    }

    @Override
    public MetaDataResponse call(TaskMetaDataService instance)
            throws IOException {
        ServerRpcController controller = new ServerRpcController();
        CoprocessorRpcUtils.BlockingRpcCallback<MetaDataResponse> rpcCallback =
            new CoprocessorRpcUtils.BlockingRpcCallback<>();
        TaskMetaDataProtos.TaskMutateRequest.Builder builder =
            TaskMetaDataProtos.TaskMutateRequest.newBuilder();
        for (Mutation mutation : taskMutations) {
            ClientProtos.MutationProto mp = ProtobufUtil.toProto(mutation);
            builder.addTableMetadataMutations(mp.toByteString());
        }
        TaskMetaDataProtos.TaskMutateRequest build = builder.build();
        instance.upsertTaskDetails(controller, build, rpcCallback);
        if (controller.getFailedOn() != null) {
            throw controller.getFailedOn();
        }
        return rpcCallback.get();
    }
}
