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
package org.apache.phoenix.cache;

import java.io.Closeable;
import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.MemoryManager;


/**
 * 
 * Inteface to set and set cached values for a tenant
 *
 * 
 * @since 0.1
 */
public interface TenantCache {
    MemoryManager getMemoryManager();
    Closeable getServerCache(ImmutableBytesPtr cacheId);
    Closeable addServerCache(ImmutableBytesPtr cacheId, ImmutableBytesWritable cachePtr, ServerCacheFactory cacheFactory) throws SQLException;
    void removeServerCache(ImmutableBytesPtr cacheId) throws SQLException;
}
