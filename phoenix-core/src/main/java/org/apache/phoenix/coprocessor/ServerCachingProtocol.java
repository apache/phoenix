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

import java.io.Closeable;
import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;

import org.apache.phoenix.memory.MemoryManager.MemoryChunk;

/**
 * 
 * EndPoint coprocessor to send a cache to a region server.
 * Used for:
 * a) hash joins, to send the smaller side of the join to each region server
 * b) secondary indexes, to send the necessary meta data to each region server
 * 
 * @since 0.1
 */
public interface ServerCachingProtocol {
    public static interface ServerCacheFactory extends Writable {
        public Closeable newCache(ImmutableBytesWritable cachePtr, MemoryChunk chunk) throws SQLException;
    }
    /**
     * Add the cache to the region server cache.  
     * @param tenantId the tenantId or null if not applicable
     * @param cacheId unique identifier of the cache
     * @param cachePtr pointer to the byte array of the cache
     * @param cacheFactory factory that converts from byte array to object representation on the server side
     * @return true on success and otherwise throws
     * @throws SQLException 
     */
    public boolean addServerCache(byte[] tenantId, byte[] cacheId, ImmutableBytesWritable cachePtr, ServerCacheFactory cacheFactory) throws SQLException;
    /**
     * Remove the cache from the region server cache.  Called upon completion of
     * the operation when cache is no longer needed.
     * @param tenantId the tenantId or null if not applicable
     * @param cacheId unique identifier of the cache
     * @return true on success and otherwise throws
     * @throws SQLException 
     */
    public boolean removeServerCache(byte[] tenantId, byte[] cacheId) throws SQLException;
}