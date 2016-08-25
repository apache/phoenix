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

import static org.apache.phoenix.query.QueryServices.MAX_MEMORY_PERC_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_MEMORY_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_MEMORY_WAIT_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_TENANT_MEMORY_PERC_ATTRIB;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.ChildMemoryManager;
import org.apache.phoenix.memory.GlobalMemoryManager;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PMetaDataEntity;
import org.apache.phoenix.util.SizedUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;


/**
 * 
 * Global root cache for the server. Each tenant is managed as a child tenant cache of this one. Queries
 * not associated with a particular tenant use this as their tenant cache.
 *
 * 
 * @since 0.1
 */
public class GlobalCache extends TenantCacheImpl {
    private static final Logger logger = LoggerFactory.getLogger(GlobalCache.class);
    private static volatile GlobalCache INSTANCE; 
    
    private final Configuration config;
    // TODO: Use Guava cache with auto removal after lack of access 
    private final ConcurrentMap<ImmutableBytesWritable,TenantCache> perTenantCacheMap = new ConcurrentHashMap<ImmutableBytesWritable,TenantCache>();
    // Cache for lastest PTable for a given Phoenix table
    private volatile Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache;
    
    public long clearTenantCache() {
        long unfreedBytes = getMemoryManager().getMaxMemory() - getMemoryManager().getAvailableMemory();
        if (unfreedBytes != 0 && logger.isDebugEnabled()) {
            logger.debug("Found " + (getMemoryManager().getMaxMemory() - getMemoryManager().getAvailableMemory()) + " bytes not freed from global cache");
        }
        removeAllServerCache();
        for (Map.Entry<ImmutableBytesWritable, TenantCache> entry : perTenantCacheMap.entrySet()) {
            TenantCache cache = entry.getValue();
            long unfreedTenantBytes = cache.getMemoryManager().getMaxMemory() - cache.getMemoryManager().getAvailableMemory();
            if (unfreedTenantBytes != 0 && logger.isDebugEnabled()) {
                ImmutableBytesWritable cacheId = entry.getKey();
                logger.debug("Found " + unfreedTenantBytes + " bytes not freed for tenant " + Bytes.toStringBinary(cacheId.get(), cacheId.getOffset(), cacheId.getLength()));
            }
            unfreedBytes += unfreedTenantBytes;
            cache.removeAllServerCache();
        }
        perTenantCacheMap.clear();
        return unfreedBytes;
    }
    
    public Cache<ImmutableBytesPtr,PMetaDataEntity> getMetaDataCache() {
        // Lazy initialize QueryServices so that we only attempt to create an HBase Configuration
        // object upon the first attempt to connect to any cluster. Otherwise, an attempt will be
        // made at driver initialization time which is too early for some systems.
        Cache<ImmutableBytesPtr,PMetaDataEntity> result = metaDataCache;
        if (result == null) {
            synchronized(this) {
                result = metaDataCache;
                if(result == null) {
                    long maxTTL = Math.min(config.getLong(
                            QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB,
                            QueryServicesOptions.DEFAULT_MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS), config.getLong(
                            QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB,
                            QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS));
                    long maxSize = config.getLong(QueryServices.MAX_SERVER_METADATA_CACHE_SIZE_ATTRIB,
                            QueryServicesOptions.DEFAULT_MAX_SERVER_METADATA_CACHE_SIZE);
                    metaDataCache = result = CacheBuilder.newBuilder()
                            .maximumWeight(maxSize)
                            .expireAfterAccess(maxTTL, TimeUnit.MILLISECONDS)
                            .weigher(new Weigher<ImmutableBytesPtr, PMetaDataEntity>() {
                                @Override
                                public int weigh(ImmutableBytesPtr key, PMetaDataEntity table) {
                                    return SizedUtil.IMMUTABLE_BYTES_PTR_SIZE + key.getLength() + table.getEstimatedSize();
                                }
                            })
                            .build();
                }
            }
        }
        return result;
    }

    public static GlobalCache getInstance(RegionCoprocessorEnvironment env) {
        GlobalCache result = INSTANCE;
        if (result == null) {
            synchronized(GlobalCache.class) {
                result = INSTANCE;
                if(result == null) {
                    INSTANCE = result = new GlobalCache(env.getConfiguration());
                }
            }
        }
        return result;
    }
    
    /**
     * Get the tenant cache associated with the tenantId. If tenantId is not applicable, null may be
     * used in which case a global tenant cache is returned.
     * @param env the HBase configuration
     * @param tenantId the tenant ID or null if not applicable.
     * @return TenantCache
     */
    public static TenantCache getTenantCache(RegionCoprocessorEnvironment env, ImmutableBytesPtr tenantId) {
        GlobalCache globalCache = GlobalCache.getInstance(env);
        TenantCache tenantCache = tenantId == null ? globalCache : globalCache.getChildTenantCache(tenantId);      
        return tenantCache;
    }
    
    private static long getMaxMemorySize(Configuration config) {
        long maxSize = Runtime.getRuntime().maxMemory() * 
                config.getInt(MAX_MEMORY_PERC_ATTRIB, QueryServicesOptions.DEFAULT_MAX_MEMORY_PERC) / 100;
        maxSize = Math.min(maxSize, config.getLong(MAX_MEMORY_SIZE_ATTRIB, Long.MAX_VALUE));
        return maxSize;
    }
    
    private GlobalCache(Configuration config) {
        super(new GlobalMemoryManager(getMaxMemorySize(config),
                                      config.getInt(MAX_MEMORY_WAIT_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_MEMORY_WAIT_MS)),
              config.getInt(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS));
        this.config = config;
    }
    
    public Configuration getConfig() {
        return config;
    }
    
    /**
     * Retrieve the tenant cache given an tenantId.
     * @param tenantId the ID that identifies the tenant
     * @return the existing or newly created TenantCache
     */
    public TenantCache getChildTenantCache(ImmutableBytesPtr tenantId) {
        TenantCache tenantCache = perTenantCacheMap.get(tenantId);
        if (tenantCache == null) {
            int maxTenantMemoryPerc = config.getInt(MAX_TENANT_MEMORY_PERC_ATTRIB, QueryServicesOptions.DEFAULT_MAX_TENANT_MEMORY_PERC);
            int maxServerCacheTimeToLive = config.getInt(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS);
            TenantCacheImpl newTenantCache = new TenantCacheImpl(new ChildMemoryManager(getMemoryManager(), maxTenantMemoryPerc), maxServerCacheTimeToLive);
            tenantCache = perTenantCacheMap.putIfAbsent(tenantId, newTenantCache);
            if (tenantCache == null) {
                tenantCache = newTenantCache;
            }
        }
        return tenantCache;
    }

    public static class FunctionBytesPtr extends ImmutableBytesPtr {

        public FunctionBytesPtr(byte[] key) {
            super(key);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof FunctionBytesPtr) return super.equals(obj);
            return false;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }
}
