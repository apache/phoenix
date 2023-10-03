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

import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerMetadataCache;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is first implementation of RegionServer coprocessor introduced by Phoenix.
 */
public class PhoenixRegionServerEndpoint
        extends RegionServerEndpointProtos.RegionServerEndpointService
        implements RegionServerCoprocessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixRegionServerEndpoint.class);
    protected Configuration conf;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        this.conf = env.getConfiguration();
    }

    @Override
    public void validateLastDDLTimestamp(RpcController controller,
            RegionServerEndpointProtos.ValidateLastDDLTimestampRequest request,
            RpcCallback<RegionServerEndpointProtos.ValidateLastDDLTimestampResponse> done) {
        for (RegionServerEndpointProtos.LastDDLTimestampRequest lastDDLTimestampRequest
                : request.getLastDDLTimestampRequestsList()) {
            byte[] tenantID = lastDDLTimestampRequest.getTenantId().toByteArray();
            byte[] schemaName = lastDDLTimestampRequest.getSchemaName().toByteArray();
            byte[] tableName = lastDDLTimestampRequest.getTableName().toByteArray();
            long clientLastDDLTimestamp = lastDDLTimestampRequest.getLastDDLTimestamp();
            String tenantIDStr = Bytes.toString(tenantID);
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            try {
                LOGGER.debug("Verifying last ddl timestamp for tenantID: {}, tableName: {}",
                        tenantIDStr, fullTableName);
                VerifyLastDDLTimestamp.verifyLastDDLTimestamp(this.conf, tenantID, schemaName,
                        tableName, clientLastDDLTimestamp);
            } catch (Throwable t) {
                String errorMsg = String.format("Verifying last ddl timestamp FAILED for "
                        + "tenantID: %s,  fullTableName: %s", tenantIDStr, fullTableName);
                LOGGER.error(errorMsg,  t);
                IOException ioe = ServerUtil.createIOException(errorMsg, t);
                ProtobufUtil.setControllerException(controller, ioe);
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
            ServerMetadataCache cache = ServerMetadataCache.getInstance(conf);
            cache.invalidate(tenantID, schemaName, tableName);
        }
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singletonList(this);
    }
}