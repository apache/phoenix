/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import com.google.protobuf.RpcController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.TaskMetaDataProtos;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for TaskMetaDataEndpoint
 */
public class TaskMetaDataEndpointTest {

    private TaskMetaDataEndpoint taskMetaDataEndpoint;
    private Configuration configuration;

    @Mock
    private Region region;

    @Mock
    private RegionInfo regionInfo;

    @Mock
    private Connection connection;

    @Mock
    private RpcController controller;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        configuration = new Configuration();
        RegionCoprocessorEnvironment environment =
                new RegionCoprocessorEnvironment() {

            @Override
            public Region getRegion() {
                return region;
            }

            @Override
            public RegionInfo getRegionInfo() {
                return regionInfo;
            }

            @Override
            public OnlineRegions getOnlineRegions() {
                return null;
            }

            @Override
            public ConcurrentMap<String, Object> getSharedData() {
                return null;
            }

            @Override
            public ServerName getServerName() {
                return null;
            }

            @Override
            public Connection getConnection() {
                return connection;
            }

            @Override
            public Connection createConnection(
                    Configuration conf) {
                return null;
            }

            @Override
            public MetricRegistry getMetricRegistryForRegionServer() {
                return null;
            }

            @Override
            public RawCellBuilder getCellBuilder() {
                return null;
            }

            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public String getHBaseVersion() {
                return null;
            }

            @Override
            public RegionCoprocessor getInstance() {
                return null;
            }

            @Override
            public int getPriority() {
                return 0;
            }

            @Override
            public int getLoadSequence() {
                return 0;
            }

            @Override
            public Configuration getConfiguration() {
                return configuration;
            }

            @Override
            public ClassLoader getClassLoader() {
                return null;
            }
        };
        taskMetaDataEndpoint = new TaskMetaDataEndpoint();
        taskMetaDataEndpoint.start(environment);
    }

    @Test
    public void testUpsertTaskDetails() throws Exception {
        Mutation mutation = new Put(Bytes.toBytes("row1"));
        TaskMetaDataProtos.TaskMutateRequest.Builder builder =
            TaskMetaDataProtos.TaskMutateRequest.newBuilder();
        ClientProtos.MutationProto mp = ProtobufUtil.toProto(mutation);
        builder.addTableMetadataMutations(mp.toByteString());
        TaskMetaDataProtos.TaskMutateRequest request = builder.build();
        CoprocessorRpcUtils.BlockingRpcCallback<MetaDataProtos.MetaDataResponse> rpcCallback =
            new CoprocessorRpcUtils.BlockingRpcCallback<>();
        Mockito.doNothing().when(region).mutateRowsWithLocks(
            Mockito.anyCollectionOf(Mutation.class), Mockito.any(), Mockito.anyLong(),
            Mockito.anyLong());
        taskMetaDataEndpoint.upsertTaskDetails(controller, request, rpcCallback);
        Mockito.verify(region, Mockito.times(1)).mutateRowsWithLocks(
            Mockito.anyCollectionOf(Mutation.class), Mockito.any(), Mockito.anyLong(),
            Mockito.anyLong());
    }

    @Test
    public void testUpsertTaskDetailsFailure() throws Exception {
        Mutation mutation = new Put(Bytes.toBytes("row2"));
        TaskMetaDataProtos.TaskMutateRequest.Builder builder =
          TaskMetaDataProtos.TaskMutateRequest.newBuilder();
        ClientProtos.MutationProto mp = ProtobufUtil.toProto(mutation);
        builder.addTableMetadataMutations(mp.toByteString());
        TaskMetaDataProtos.TaskMutateRequest request = builder.build();
        CoprocessorRpcUtils.BlockingRpcCallback<MetaDataProtos.MetaDataResponse> rpcCallback =
            new CoprocessorRpcUtils.BlockingRpcCallback<>();
        Mockito.doThrow(IOException.class).when(region).mutateRowsWithLocks(
            Mockito.anyCollectionOf(Mutation.class), Mockito.any(), Mockito.anyLong(),
            Mockito.anyLong());
        taskMetaDataEndpoint.upsertTaskDetails(controller, request, rpcCallback);
        Mockito.verify(region, Mockito.times(1)).mutateRowsWithLocks(
            Mockito.anyCollectionOf(Mutation.class), Mockito.any(), Mockito.anyLong(),
            Mockito.anyLong());
        assertEquals(MetaDataProtos.MutationCode.UNABLE_TO_UPSERT_TASK,
            rpcCallback.get().getReturnCode());
    }

}