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

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerMetadataCache;
import org.apache.phoenix.cache.ServerMetadataCacheImpl;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.coprocessorclient.metrics.MetricsMetadataCachingSource;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is first implementation of RegionServer coprocessor introduced by Phoenix.
 */
public class PhoenixRegionServerEndpoint
        extends RegionServerEndpointProtos.RegionServerEndpointService
        implements RegionServerCoprocessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixRegionServerEndpoint.class);
    private MetricsMetadataCachingSource metricsSource;
    protected Configuration conf;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        this.conf = env.getConfiguration();
        this.metricsSource = MetricsPhoenixCoprocessorSourceFactory
                                .getInstance().getMetadataCachingSource();
    }

    @Override
    public void validateLastDDLTimestamp(RpcController controller,
            RegionServerEndpointProtos.ValidateLastDDLTimestampRequest request,
            RpcCallback<RegionServerEndpointProtos.ValidateLastDDLTimestampResponse> done) {
        metricsSource.incrementValidateTimestampRequestCount();
        ServerMetadataCache cache = getServerMetadataCache();
        for (RegionServerEndpointProtos.LastDDLTimestampRequest lastDDLTimestampRequest
                : request.getLastDDLTimestampRequestsList()) {
            byte[] tenantID = lastDDLTimestampRequest.getTenantId().toByteArray();
            byte[] schemaName = lastDDLTimestampRequest.getSchemaName().toByteArray();
            byte[] tableName = lastDDLTimestampRequest.getTableName().toByteArray();
            long clientLastDDLTimestamp = lastDDLTimestampRequest.getLastDDLTimestamp();
            String tenantIDStr = Bytes.toString(tenantID);
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            try {
                VerifyLastDDLTimestamp.verifyLastDDLTimestamp(cache, tenantID, schemaName,
                        tableName, clientLastDDLTimestamp);
            } catch (Throwable t) {
                String errorMsg = String.format("Verifying last ddl timestamp FAILED for "
                        + "tenantID: %s,  fullTableName: %s", tenantIDStr, fullTableName);
                LOGGER.error(errorMsg,  t);
                IOException ioe = ClientUtil.createIOException(errorMsg, t);
                ProtobufUtil.setControllerException(controller, ioe);
                //If an index was dropped and a client tries to query it, we will validate table
                //first and encounter stale metadata, if we don't break the coproc will run into
                //table not found error since it will not be able to validate the dropped index.
                //this should be fine for views too since we will update the entire hierarchy.
                break;
            }
        }
    }

    @Override
    public void invalidateServerMetadataCache(RpcController controller,
            RegionServerEndpointProtos.InvalidateServerMetadataCacheRequest request,
            RpcCallback<RegionServerEndpointProtos.InvalidateServerMetadataCacheResponse> done) {
        for (RegionServerEndpointProtos.InvalidateServerMetadataCache invalidateCacheRequest
                : request.getInvalidateServerMetadataCacheRequestsList()) {
            byte[] tenantID = invalidateCacheRequest.getTenantId().toByteArray();
            byte[] schemaName = invalidateCacheRequest.getSchemaName().toByteArray();
            byte[] tableName = invalidateCacheRequest.getTableName().toByteArray();
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            String tenantIDStr = Bytes.toString(tenantID);
            LOGGER.info("PhoenixRegionServerEndpoint invalidating the cache for tenantID: {},"
                    + " tableName: {}", tenantIDStr, fullTableName);
            ServerMetadataCache cache = getServerMetadataCache();
            cache.invalidate(tenantID, schemaName, tableName);
        }
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singletonList(this);
    }

    public ServerMetadataCache getServerMetadataCache() {
        return ServerMetadataCacheImpl.getInstance(conf);
    }
}