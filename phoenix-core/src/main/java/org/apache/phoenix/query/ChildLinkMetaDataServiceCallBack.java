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

package org.apache.phoenix.query;

import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.phoenix.coprocessor.generated.ChildLinkMetaDataProtos
    .ChildLinkMetaDataService;
import org.apache.phoenix.coprocessor.generated.ChildLinkMetaDataProtos
    .CreateViewAddChildLinkRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.protobuf.ProtobufUtil;

import java.io.IOException;
import java.util.List;

/**
 * Callable implementation for coprocessor endpoint associated with
 * SYSTEM.CHILD_LINK
 */
class ChildLinkMetaDataServiceCallBack
    implements Batch.Call<ChildLinkMetaDataService, MetaDataResponse> {

    private final List<Mutation> childLinkMutations;
    private final RpcController controller;

    public ChildLinkMetaDataServiceCallBack(RpcController controller, List<Mutation> childLinkMutations) {
        this.controller = controller;
        this.childLinkMutations = childLinkMutations;
    }

    @Override
    public MetaDataResponse call(ChildLinkMetaDataService instance)
            throws IOException {
        BlockingRpcCallback<MetaDataResponse> rpcCallback =
            new BlockingRpcCallback<>();
        CreateViewAddChildLinkRequest.Builder builder =
            CreateViewAddChildLinkRequest.newBuilder();
        for (Mutation mutation : childLinkMutations) {
            MutationProto mp = ProtobufUtil.toProto(mutation);
            builder.addTableMetadataMutations(mp.toByteString());
        }
        CreateViewAddChildLinkRequest build = builder.build();
        instance.createViewAddChildLink(controller, build, rpcCallback);
        checkForRemoteExceptions(controller);
        return rpcCallback.get();
    }

    private void checkForRemoteExceptions(RpcController controller) throws IOException {
        if (controller != null) {
            if (controller instanceof ServerRpcController) {
                if (((ServerRpcController)controller).getFailedOn() != null) {
                    throw ((ServerRpcController)controller).getFailedOn();
                }
            } else {
                if (((HBaseRpcController)controller).getFailed() != null) {
                    throw ((HBaseRpcController)controller).getFailed();
                }
            }
        }
    }

}
