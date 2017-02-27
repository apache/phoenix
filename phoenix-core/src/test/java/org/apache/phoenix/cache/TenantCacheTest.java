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

import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.GlobalMemoryManager;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

import com.google.common.base.Ticker;

public class TenantCacheTest {

    @Test
    public void testInvalidateClosesMemoryChunk() throws SQLException {
        int maxServerCacheTimeToLive = 10000;
        long maxBytes = 1000;
        int maxWaitMs = 1000;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes, maxWaitMs);
        TenantCacheImpl newTenantCache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive);
        ImmutableBytesPtr cacheId = new ImmutableBytesPtr(Bytes.toBytes("a"));
        ImmutableBytesWritable cachePtr = new ImmutableBytesWritable(Bytes.toBytes("a"));
        newTenantCache.addServerCache(cacheId, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true);
        assertEquals(maxBytes-1, memoryManager.getAvailableMemory());
        newTenantCache.removeServerCache(cacheId);
        assertEquals(maxBytes, memoryManager.getAvailableMemory());
    }
    
    @Test
    public void testTimeoutClosesMemoryChunk() throws Exception {
        int maxServerCacheTimeToLive = 10;
        long maxBytes = 1000;
        int maxWaitMs = 10;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes, maxWaitMs);
        ManualTicker ticker = new ManualTicker();
        TenantCacheImpl cache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive, ticker);
        ImmutableBytesPtr cacheId1 = new ImmutableBytesPtr(Bytes.toBytes("a"));
        ImmutableBytesWritable cachePtr = new ImmutableBytesWritable(Bytes.toBytes("a"));
        cache.addServerCache(cacheId1, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true);
        assertEquals(maxBytes-1, memoryManager.getAvailableMemory());
        ticker.time += (maxServerCacheTimeToLive + 1) * 1000000;
        cache.cleanUp();
        assertEquals(maxBytes, memoryManager.getAvailableMemory());
    }
    
    public static class ManualTicker extends Ticker {
        public long time = 0;
        
        @Override
        public long read() {
            return time;
        }
        
    }
    
    public static ServerCacheFactory cacheFactory = new ServerCacheFactory() {

        @Override
        public void readFields(DataInput arg0) throws IOException {
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
        }

        @Override
        public Closeable newCache(ImmutableBytesWritable cachePtr, byte[] txState, MemoryChunk chunk, boolean useProtoForIndexMaintainer)
                throws SQLException {
            return chunk;
        }
        
    };
}
