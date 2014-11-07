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
package org.apache.phoenix.query;

import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_SPOOL_DIRECTORY;
import static org.apache.phoenix.query.QueryServicesOptions.withDefaults;

import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec;
import org.apache.phoenix.util.ReadOnlyProps;


/**
 * QueryServices implementation to use for tests that do not execute queries
 *
 * 
 * @since 0.1
 */
public final class QueryServicesTestImpl extends BaseQueryServicesImpl {

    private static final int DEFAULT_THREAD_POOL_SIZE = 20;
    private static final int DEFAULT_QUEUE_SIZE = 0;
    // TODO: setting this down to 5mb causes insufficient memory exceptions. Need to investigate why
    private static final int DEFAULT_MAX_MEMORY_PERC = 30; // 30% of heap
    private static final int DEFAULT_THREAD_TIMEOUT_MS = 60000*5; //5min
    private static final int DEFAULT_SPOOL_THRESHOLD_BYTES = 1024 * 1024; // 1m
    private static final int DEFAULT_MAX_MEMORY_WAIT_MS = 0;
    private static final int DEFAULT_MAX_TENANT_MEMORY_PERC = 100;
    private static final int DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS = 60000 * 60; // 1HR (to prevent age-out of hash cache during debugging)
    private static final long DEFAULT_MAX_HASH_CACHE_SIZE = 1024*1024*10;  // 10 Mb
    private static final boolean DEFAULT_DROP_METADATA = false;
    
    private static final int DEFAULT_MASTER_INFO_PORT = -1;
    private static final int DEFAULT_REGIONSERVER_INFO_PORT = -1;
    private static final int DEFAULT_REGIONSERVER_LEASE_PERIOD_MS = 9000000;
    private static final int DEFAULT_RPC_TIMEOUT_MS = 9000000;
    private static final String DEFAULT_WAL_EDIT_CODEC = IndexedWALEditCodec.class.getName();
    public static final long DEFAULT_MAX_SERVER_METADATA_CACHE_SIZE =  1024L*1024L*4L; // 4 Mb
    public static final long DEFAULT_MAX_CLIENT_METADATA_CACHE_SIZE =  1024L*1024L*2L; // 2 Mb
    public static final int DEFAULT_MIN_STATS_UPDATE_FREQ_MS = 0;
    public static final boolean DEFAULT_EXPLAIN_CHUNK_COUNT = false; // TODO: update explain plans in test and set to true
    
    /**
     * Set number of salt buckets lower for sequence table during testing, as a high
     * value overwhelms our mini clusters.
     */
    public static final int DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS = 4;

    
    public QueryServicesTestImpl(ReadOnlyProps defaultProps) {
        this(defaultProps, ReadOnlyProps.EMPTY_PROPS);
    }
    
    private static QueryServicesOptions getDefaultServicesOptions() {
    	return withDefaults()
    	        .setExplainChunkCount(DEFAULT_EXPLAIN_CHUNK_COUNT)
    	        .setSequenceSaltBuckets(DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS)
                .setMinStatsUpdateFrequencyMs(DEFAULT_MIN_STATS_UPDATE_FREQ_MS)
                .setThreadPoolSize(DEFAULT_THREAD_POOL_SIZE)
                .setQueueSize(DEFAULT_QUEUE_SIZE)
                .setMaxMemoryPerc(DEFAULT_MAX_MEMORY_PERC)
                .setThreadTimeoutMs(DEFAULT_THREAD_TIMEOUT_MS)
                .setSpoolThresholdBytes(DEFAULT_SPOOL_THRESHOLD_BYTES)
                .setSpoolDirectory(DEFAULT_SPOOL_DIRECTORY)
                .setMaxMemoryWaitMs(DEFAULT_MAX_MEMORY_WAIT_MS)
                .setMaxTenantMemoryPerc(DEFAULT_MAX_TENANT_MEMORY_PERC)
                .setMaxServerCacheSize(DEFAULT_MAX_HASH_CACHE_SIZE)
                .setRowKeyOrderSaltedTable(true)
                .setMaxServerCacheTTLMs(DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS)
                .setMasterInfoPort(DEFAULT_MASTER_INFO_PORT)
                .setRegionServerInfoPort(DEFAULT_REGIONSERVER_INFO_PORT)
                .setRegionServerLeasePeriodMs(DEFAULT_REGIONSERVER_LEASE_PERIOD_MS)
                .setRpcTimeoutMs(DEFAULT_RPC_TIMEOUT_MS)
                .setWALEditCodec(DEFAULT_WAL_EDIT_CODEC)
                .setDropMetaData(DEFAULT_DROP_METADATA)
                .setMaxClientMetaDataCacheSize(DEFAULT_MAX_CLIENT_METADATA_CACHE_SIZE)
                .setMaxServerMetaDataCacheSize(DEFAULT_MAX_SERVER_METADATA_CACHE_SIZE);
    }
    
    public QueryServicesTestImpl(ReadOnlyProps defaultProps, ReadOnlyProps overrideProps) {
        super(defaultProps, getDefaultServicesOptions().setAll(overrideProps));
    }    
}
