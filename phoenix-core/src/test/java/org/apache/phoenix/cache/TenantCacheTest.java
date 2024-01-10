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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.coprocessorclient.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.GlobalMemoryManager;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.base.Ticker;

import static org.junit.Assert.*;

public class TenantCacheTest {

    @Test
    public void testInvalidateClosesMemoryChunk() throws SQLException {
        int maxServerCacheTimeToLive = 10000;
        int maxServerCachePersistenceTimeToLive = 10;
        long maxBytes = 1000;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes);
        TenantCacheImpl newTenantCache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive, maxServerCachePersistenceTimeToLive);
        ImmutableBytesPtr cacheId = new ImmutableBytesPtr(Bytes.toBytes(1L));
        ImmutableBytesWritable cachePtr = new ImmutableBytesWritable(Bytes.toBytes("a"));
        newTenantCache.addServerCache(cacheId, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, false, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(maxBytes-1, memoryManager.getAvailableMemory());
        newTenantCache.removeServerCache(cacheId);
        assertEquals(maxBytes, memoryManager.getAvailableMemory());
    }

    @Test
    public void testTimeoutClosesMemoryChunk() throws Exception {
        int maxServerCacheTimeToLive = 10;
        int maxServerCachePersistenceTimeToLive = 10;
        long maxBytes = 1000;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes);
        ManualTicker ticker = new ManualTicker();
        TenantCacheImpl cache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive, maxServerCachePersistenceTimeToLive, ticker);
        ImmutableBytesPtr cacheId1 = new ImmutableBytesPtr(Bytes.toBytes(1L));
        ImmutableBytesWritable cachePtr = new ImmutableBytesWritable(Bytes.toBytes("a"));
        cache.addServerCache(cacheId1, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, false, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(maxBytes-1, memoryManager.getAvailableMemory());
        ticker.time += (maxServerCacheTimeToLive + 1) * 1000000;
        cache.cleanUp();
        assertEquals(maxBytes, memoryManager.getAvailableMemory());
    }

    @Test
    public void testFreeMemoryOnAccess() throws Exception {
        int maxServerCacheTimeToLive = 10;
        int maxServerCachePersistenceTimeToLive = 10;
        long maxBytes = 1000;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes);
        ManualTicker ticker = new ManualTicker();
        TenantCacheImpl cache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive, maxServerCachePersistenceTimeToLive, ticker);
        ImmutableBytesPtr cacheId1 = new ImmutableBytesPtr(Bytes.toBytes(1L));
        ImmutableBytesWritable cachePtr = new ImmutableBytesWritable(Bytes.toBytes("a"));
        cache.addServerCache(cacheId1, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, false, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(maxBytes-1, memoryManager.getAvailableMemory());
        ticker.time += (maxServerCacheTimeToLive + 1) * 1000000;
        assertNull(cache.getServerCache(cacheId1));
        assertEquals(maxBytes, memoryManager.getAvailableMemory());
    }

    @Test
    public void testExpiredCacheOnAddingNew() throws Exception {
        int maxServerCacheTimeToLive = 10;
        int maxServerCachePersistenceTimeToLive = 10;
        long maxBytes = 10;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes);
        ManualTicker ticker = new ManualTicker();
        TenantCacheImpl cache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive, maxServerCachePersistenceTimeToLive, ticker);
        ImmutableBytesPtr cacheId1 = new ImmutableBytesPtr(Bytes.toBytes(1L));
        ImmutableBytesWritable cachePtr = new ImmutableBytesWritable(Bytes.toBytes("12345678"));
        cache.addServerCache(cacheId1, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, false, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(2, memoryManager.getAvailableMemory());
        ticker.time += (maxServerCacheTimeToLive + 1) * 1000000;
        cache.addServerCache(cacheId1, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, false, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(2, memoryManager.getAvailableMemory());
    }

    @Test
    public void testExpiresButStaysInPersistentAfterTimeout() throws Exception {
        int maxServerCacheTimeToLive = 100;
        int maxServerCachePersistenceTimeToLive = 1000;
        long maxBytes = 1000;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes);
        ManualTicker ticker = new ManualTicker();
        TenantCacheImpl cache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive, maxServerCachePersistenceTimeToLive, ticker);
        ImmutableBytesPtr cacheId1 = new ImmutableBytesPtr(Bytes.toBytes(1L));
        ImmutableBytesWritable cachePtr = new ImmutableBytesWritable(Bytes.toBytes("a"));
        cache.addServerCache(cacheId1, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, true, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(maxBytes-1, memoryManager.getAvailableMemory());
        assertNotNull(cache.getServerCache(cacheId1));

        // Expire it from live cache but not persistent cache
        ticker.time += (maxServerCacheTimeToLive + 1) * 1000000;
        cache.cleanUp();
        assertEquals(maxBytes-1, memoryManager.getAvailableMemory());
        assertNotNull(cache.getServerCache(cacheId1));

        // Expire it from persistent cache as well
        ticker.time += (maxServerCachePersistenceTimeToLive + 1) * 1000000;
        cache.cleanUp();
        assertEquals(maxBytes, memoryManager.getAvailableMemory());
        assertNull(cache.getServerCache(cacheId1));
    }

    @Test
    public void testExpiresButStaysInPersistentAfterRemove() throws Exception {
        int maxServerCacheTimeToLive = 100;
        int maxServerCachePersistenceTimeToLive = 1000;
        long maxBytes = 1000;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes);
        ManualTicker ticker = new ManualTicker();
        TenantCacheImpl cache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive, maxServerCachePersistenceTimeToLive, ticker);
        ImmutableBytesPtr cacheId1 = new ImmutableBytesPtr(Bytes.toBytes(1L));
        ImmutableBytesWritable cachePtr = new ImmutableBytesWritable(Bytes.toBytes("12"));
        cache.addServerCache(cacheId1, cachePtr, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, true, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(maxBytes-2, memoryManager.getAvailableMemory());
        assertNotNull(cache.getServerCache(cacheId1));

        // Remove should only remove from live cache
        cache.removeServerCache(cacheId1);
        assertEquals(maxBytes-2, memoryManager.getAvailableMemory());
        assertNotNull(cache.getServerCache(cacheId1));
    }

    @Test
    public void testEvictPersistentCacheIfSpaceIsNeeded() throws Exception {
        int maxServerCacheTimeToLive = 100;
        int maxServerCachePersistenceTimeToLive = 1000;
        long maxBytes = 10;
        GlobalMemoryManager memoryManager = new GlobalMemoryManager(maxBytes);
        ManualTicker ticker = new ManualTicker();
        TenantCacheImpl cache = new TenantCacheImpl(memoryManager, maxServerCacheTimeToLive, maxServerCachePersistenceTimeToLive, ticker);
        ImmutableBytesPtr cacheId1 = new ImmutableBytesPtr(Bytes.toBytes(1L));
        ImmutableBytesWritable cachePtr1 = new ImmutableBytesWritable(Bytes.toBytes("1234"));
        cache.addServerCache(cacheId1, cachePtr1, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, true, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(6, memoryManager.getAvailableMemory());

        // Remove it, but it should stay in persistent cache
        cache.removeServerCache(cacheId1);
        assertNotNull(cache.getServerCache(cacheId1));
        assertEquals(6, memoryManager.getAvailableMemory());

        // Let's do an entry that will require eviction
        ImmutableBytesPtr cacheId2 = new ImmutableBytesPtr(Bytes.toBytes(2L));
        ImmutableBytesWritable cachePtr2 = new ImmutableBytesWritable(Bytes.toBytes("12345678"));
        cache.addServerCache(cacheId2, cachePtr2, ByteUtil.EMPTY_BYTE_ARRAY, cacheFactory, true, true, MetaDataProtocol.PHOENIX_VERSION);
        assertEquals(2, memoryManager.getAvailableMemory());
        assertNull(cache.getServerCache(cacheId1));
        assertNotNull(cache.getServerCache(cacheId2));
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
        public Closeable newCache(ImmutableBytesWritable cachePtr, byte[] txState, MemoryChunk chunk, boolean useProtoForIndexMaintainer, int clientVersion)
                throws SQLException {
            return chunk;
        }
        
    };
}
