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

import java.sql.SQLException;

import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;




/**
 * 
 * Server-side implementation of {@link ServerCachingProtocol}
 *
 * 
 * @since 0.1
 */
public class ServerCachingEndpointImpl extends BaseEndpointCoprocessor implements ServerCachingProtocol {

    @Override
    public boolean addServerCache(byte[] tenantId, byte[] cacheId, ImmutableBytesWritable cachePtr, ServerCacheFactory cacheFactory) throws SQLException {
        TenantCache tenantCache = GlobalCache.getTenantCache((RegionCoprocessorEnvironment)this.getEnvironment(), tenantId == null ? null : new ImmutableBytesPtr(tenantId));
        tenantCache.addServerCache(new ImmutableBytesPtr(cacheId), cachePtr, cacheFactory);
        return true;
    }

    @Override
    public boolean removeServerCache(byte[] tenantId, byte[] cacheId) throws SQLException {
        TenantCache tenantCache = GlobalCache.getTenantCache((RegionCoprocessorEnvironment)this.getEnvironment(), tenantId == null ? null : new ImmutableBytesPtr(tenantId));
        tenantCache.removeServerCache(new ImmutableBytesPtr(cacheId));
        return true;
    }
}
