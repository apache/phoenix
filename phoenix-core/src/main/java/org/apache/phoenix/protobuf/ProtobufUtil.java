/*
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.phoenix.protobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.RpcController;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.PTableType;

public class ProtobufUtil {

    /**
     * Stores an exception encountered during RPC invocation so it can be passed back through to the
     * client.
     * @param controller the controller instance provided by the client when calling the service
     * @param ioe the exception encountered
     */
    public static void setControllerException(RpcController controller, IOException ioe) {
        if (controller != null) {
            if (controller instanceof ServerRpcController) {
                ((ServerRpcController) controller).setFailedOn(ioe);
            } else {
                controller.setFailed(StringUtils.stringifyException(ioe));
            }
        }
    }

    public static PTableProtos.PTableType toPTableTypeProto(PTableType type) {
        return PTableProtos.PTableType.values()[type.ordinal()];
    }

    public static PTableType toPTableType(PTableProtos.PTableType type) {
        return PTableType.values()[type.ordinal()];
    }

    public static List<Mutation> getMutations(MetaDataProtos.CreateTableRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    public static List<Mutation> getMutations(MetaDataProtos.DropTableRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    public static List<Mutation> getMutations(MetaDataProtos.AddColumnRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    public static List<Mutation> getMutations(MetaDataProtos.DropColumnRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    public static List<Mutation> getMutations(MetaDataProtos.UpdateIndexStateRequest request)
            throws IOException {
        return getMutations(request.getTableMetadataMutationsList());
    }

    /**
     * Each ByteString entry is a byte array serialized from MutationProto instance
     * @param mutations
     * @throws IOException
     */
    private static List<Mutation> getMutations(List<ByteString> mutations)
            throws IOException {
        List<Mutation> result = new ArrayList<Mutation>();
        for (ByteString mutation : mutations) {
            MutationProto mProto = MutationProto.parseFrom(mutation);
            result.add(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(mProto));
        }
        return result;
    }

    public static MutationProto toProto(Mutation mutation) throws IOException {
        MutationType type;
        if (mutation instanceof Put) {
            type = MutationType.PUT;
        } else if (mutation instanceof Delete) {
            type = MutationType.DELETE;
        } else {
            throw new IllegalArgumentException("Only Put and Delete are supported");
        }
        return org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(type, mutation);
    }
    
    public static ServerCachingProtos.ImmutableBytesWritable toProto(ImmutableBytesWritable w) {
        ServerCachingProtos.ImmutableBytesWritable.Builder builder = 
        		ServerCachingProtos.ImmutableBytesWritable.newBuilder();
        builder.setByteArray(HBaseZeroCopyByteString.wrap(w.get()));
        builder.setOffset(w.getOffset());
        builder.setLength(w.getLength());
        return builder.build();
    }
    
    public static ImmutableBytesWritable toImmutableBytesWritable(ServerCachingProtos.ImmutableBytesWritable proto) {
    	return new ImmutableBytesWritable(proto.getByteArray().toByteArray(), proto.getOffset(), proto.getLength());
    }
}
