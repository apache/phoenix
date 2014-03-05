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

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheRequest;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheResponse;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.RemoveServerCacheRequest;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.RemoveServerCacheResponse;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.ServerCachingService;
import org.apache.phoenix.protobuf.ProtobufUtil;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * 
 * Server-side implementation of {@link ServerCachingProtocol}
 *
 * 
 * @since 0.1
 */
public class ServerCachingEndpointImpl extends ServerCachingService implements CoprocessorService,
    Coprocessor {

  private RegionCoprocessorEnvironment env;

  @Override
  public void addServerCache(RpcController controller, AddServerCacheRequest request,
      RpcCallback<AddServerCacheResponse> done) {
    ImmutableBytesPtr tenantId = null;
    if (request.hasTenantId()) {
      tenantId = new ImmutableBytesPtr(request.getTenantId().toByteArray());
    }
    TenantCache tenantCache = GlobalCache.getTenantCache(this.env, tenantId);
    ImmutableBytesWritable cachePtr =
        org.apache.phoenix.protobuf.ProtobufUtil
            .toImmutableBytesWritable(request.getCachePtr());

    try {
      @SuppressWarnings("unchecked")
      Class<ServerCacheFactory> serverCacheFactoryClass =
          (Class<ServerCacheFactory>) Class.forName(request.getCacheFactory().getClassName());
      ServerCacheFactory cacheFactory = serverCacheFactoryClass.newInstance();
      tenantCache.addServerCache(new ImmutableBytesPtr(request.getCacheId().toByteArray()),
        cachePtr, cacheFactory);
    } catch (Throwable e) {
      ProtobufUtil.setControllerException(controller, new IOException(e));
    }
    AddServerCacheResponse.Builder responseBuilder = AddServerCacheResponse.newBuilder();
    responseBuilder.setReturn(true);
    AddServerCacheResponse result = responseBuilder.build();
    done.run(result);
  }

  @Override
  public void removeServerCache(RpcController controller, RemoveServerCacheRequest request,
      RpcCallback<RemoveServerCacheResponse> done) {
    ImmutableBytesPtr tenantId = null;
    if (request.hasTenantId()) {
      tenantId = new ImmutableBytesPtr(request.getTenantId().toByteArray());
    }
    TenantCache tenantCache = GlobalCache.getTenantCache(this.env, tenantId);
    try {
      tenantCache.removeServerCache(new ImmutableBytesPtr(request.getCacheId().toByteArray()));
    } catch (SQLException e) {
      ProtobufUtil.setControllerException(controller, new IOException(e));
    }
    RemoveServerCacheResponse.Builder responseBuilder = RemoveServerCacheResponse.newBuilder();
    responseBuilder.setReturn(true);
    RemoveServerCacheResponse result = responseBuilder.build();
    done.run(result);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment arg0) throws IOException {
    // nothing to do
  }

  @Override
  public Service getService() {
    return this;
  }
}
