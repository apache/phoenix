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

import static org.apache.phoenix.query.QueryServices.CALL_QUEUE_PRODUCER_ATTRIB_NAME;
import static org.apache.phoenix.query.QueryServices.CALL_QUEUE_ROUND_ROBIN_ATTRIB;
import static org.apache.phoenix.query.QueryServices.DATE_FORMAT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.query.QueryServices.GROUPBY_MAX_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.GROUPBY_SPILLABLE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.GROUPBY_SPILL_FILES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.IMMUTABLE_ROWS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB;
import static org.apache.phoenix.query.QueryServices.KEEP_ALIVE_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MASTER_INFO_PORT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_MEMORY_PERC_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_MEMORY_WAIT_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_MUTATION_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_SERVER_METADATA_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_SPOOL_TO_DISK_BYTES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_TENANT_MEMORY_PERC_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.QUEUE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.REGIONSERVER_INFO_PORT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.REGIONSERVER_LEASE_PERIOD_ATTRIB;
import static org.apache.phoenix.query.QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.RPC_TIMEOUT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.SCAN_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB;
import static org.apache.phoenix.query.QueryServices.THREAD_POOL_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.THREAD_TIMEOUT_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.USE_INDEXES_ATTRIB;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;

import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.ReadOnlyProps;


/**
 * Options for {@link QueryServices}.
 * 
 * 
 * @since 0.1
 */
public class QueryServicesOptions {
	public static final int DEFAULT_KEEP_ALIVE_MS = 60000;
	public static final int DEFAULT_THREAD_POOL_SIZE = 128;
	public static final int DEFAULT_QUEUE_SIZE = 500;
	public static final int DEFAULT_THREAD_TIMEOUT_MS = 600000; // 10min
	public static final int DEFAULT_SPOOL_THRESHOLD_BYTES = 1024 * 1024 * 20; // 20m
	public static final int DEFAULT_MAX_MEMORY_PERC = 15; // 15% of heap
	public static final int DEFAULT_MAX_MEMORY_WAIT_MS = 10000;
	public static final int DEFAULT_MAX_TENANT_MEMORY_PERC = 100;
	public static final long DEFAULT_MAX_SERVER_CACHE_SIZE = 1024*1024*100;  // 100 Mb
    public static final int DEFAULT_TARGET_QUERY_CONCURRENCY = 32;
    public static final int DEFAULT_MAX_QUERY_CONCURRENCY = 64;
    public static final String DEFAULT_DATE_FORMAT = DateUtil.DEFAULT_DATE_FORMAT;
    public static final int DEFAULT_STATS_UPDATE_FREQ_MS = 15 * 60000; // 15min
    public static final int DEFAULT_MAX_STATS_AGE_MS = 24 * 60 * 60000; // 1 day
    public static final boolean DEFAULT_CALL_QUEUE_ROUND_ROBIN = true; 
    public static final int DEFAULT_MAX_MUTATION_SIZE = 500000;
    public static final boolean DEFAULT_ROW_KEY_ORDER_SALTED_TABLE = true; // Merge sort on client to ensure salted tables are row key ordered
    public static final boolean DEFAULT_USE_INDEXES = true; // Use indexes
    public static final boolean DEFAULT_IMMUTABLE_ROWS = false; // Tables rows may be updated
    public static final boolean DEFAULT_DROP_METADATA = true; // Drop meta data also.
    
    public final static int DEFAULT_MUTATE_BATCH_SIZE = 1000; // Batch size for UPSERT SELECT and DELETE
	// The only downside of it being out-of-sync is that the parallelization of the scan won't be as balanced as it could be.
    public static final int DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS = 30000; // 30 sec (with no activity)
    public static final int DEFAULT_SCAN_CACHE_SIZE = 1000;
    public static final int DEFAULT_MAX_INTRA_REGION_PARALLELIZATION = DEFAULT_MAX_QUERY_CONCURRENCY;
    public static final int DEFAULT_DISTINCT_VALUE_COMPRESS_THRESHOLD = 1024 * 1024 * 1; // 1 Mb
    public static final int DEFAULT_INDEX_MUTATE_BATCH_SIZE_THRESHOLD = 5;
    public static final long DEFAULT_MAX_SPOOL_TO_DISK_BYTES = 1024000000;
    
    // 
    // Spillable GroupBy - SPGBY prefix
    //
    // Enable / disable spillable group by
    public static boolean DEFAULT_GROUPBY_SPILLABLE = true;
    // Number of spill files / partitions the keys are distributed to
    // Each spill file fits 2GB of data
    public static final int DEFAULT_GROUPBY_SPILL_FILES = 2;
    // Max size of 1st level main memory cache in bytes --> upper bound
    public static final long DEFAULT_GROUPBY_MAX_CACHE_MAX = 1024L*1024L*100L;  // 100 Mb
    
    public static final long DEFAULT_SEQUENCE_CACHE_SIZE = 100;  // reserve 100 sequences at a time
    public static final int DEFAULT_INDEX_MAX_FILESIZE_PERC = 50; // % of data table max file size for index table
    public static final long DEFAULT_MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS =  60000 * 30; // 30 mins   
    public static final long DEFAULT_MAX_SERVER_METADATA_CACHE_SIZE =  1024L*1024L*20L; // 20 Mb
    public static final long DEFAULT_MAX_CLIENT_METADATA_CACHE_SIZE =  1024L*1024L*10L; // 10 Mb
    public static final int DEFAULT_GROUPBY_ESTIMATED_DISTINCT_VALUES = 1000;
    
    private final Configuration config;
    
    private QueryServicesOptions(Configuration config) {
        this.config = config;
    }
    
    public ReadOnlyProps getProps() {
        // Ensure that HBase RPC time out value is at least as large as our thread time out for query. 
        int threadTimeOutMS = config.getInt(THREAD_TIMEOUT_MS_ATTRIB, DEFAULT_THREAD_TIMEOUT_MS);
        int hbaseRPCTimeOut = config.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
        if (threadTimeOutMS > hbaseRPCTimeOut) {
            config.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, threadTimeOutMS);
        }
        return new ReadOnlyProps(config.iterator());
    }
    
    public QueryServicesOptions setAll(ReadOnlyProps props) {
        for (Entry<String,String> entry : props) {
            config.set(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public static QueryServicesOptions withDefaults() {
        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        QueryServicesOptions options = new QueryServicesOptions(config)
            .setIfUnset(KEEP_ALIVE_MS_ATTRIB, DEFAULT_KEEP_ALIVE_MS)
            .setIfUnset(THREAD_POOL_SIZE_ATTRIB, DEFAULT_THREAD_POOL_SIZE)
            .setIfUnset(QUEUE_SIZE_ATTRIB, DEFAULT_QUEUE_SIZE)
            .setIfUnset(THREAD_TIMEOUT_MS_ATTRIB, DEFAULT_THREAD_TIMEOUT_MS)
            .setIfUnset(SPOOL_THRESHOLD_BYTES_ATTRIB, DEFAULT_SPOOL_THRESHOLD_BYTES)
            .setIfUnset(MAX_MEMORY_PERC_ATTRIB, DEFAULT_MAX_MEMORY_PERC)
            .setIfUnset(MAX_MEMORY_WAIT_MS_ATTRIB, DEFAULT_MAX_MEMORY_WAIT_MS)
            .setIfUnset(MAX_TENANT_MEMORY_PERC_ATTRIB, DEFAULT_MAX_TENANT_MEMORY_PERC)
            .setIfUnset(MAX_SERVER_CACHE_SIZE_ATTRIB, DEFAULT_MAX_SERVER_CACHE_SIZE)
            .setIfUnset(SCAN_CACHE_SIZE_ATTRIB, DEFAULT_SCAN_CACHE_SIZE)
            .setIfUnset(TARGET_QUERY_CONCURRENCY_ATTRIB, DEFAULT_TARGET_QUERY_CONCURRENCY)
            .setIfUnset(MAX_QUERY_CONCURRENCY_ATTRIB, DEFAULT_MAX_QUERY_CONCURRENCY)
            .setIfUnset(DATE_FORMAT_ATTRIB, DEFAULT_DATE_FORMAT)
            .setIfUnset(STATS_UPDATE_FREQ_MS_ATTRIB, DEFAULT_STATS_UPDATE_FREQ_MS)
            .setIfUnset(CALL_QUEUE_ROUND_ROBIN_ATTRIB, DEFAULT_CALL_QUEUE_ROUND_ROBIN)
            .setIfUnset(MAX_MUTATION_SIZE_ATTRIB, DEFAULT_MAX_MUTATION_SIZE)
            .setIfUnset(MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, DEFAULT_MAX_INTRA_REGION_PARALLELIZATION)
            .setIfUnset(ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, DEFAULT_ROW_KEY_ORDER_SALTED_TABLE)
            .setIfUnset(USE_INDEXES_ATTRIB, DEFAULT_USE_INDEXES)
            .setIfUnset(IMMUTABLE_ROWS_ATTRIB, DEFAULT_IMMUTABLE_ROWS)
            .setIfUnset(INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, DEFAULT_INDEX_MUTATE_BATCH_SIZE_THRESHOLD)
            .setIfUnset(MAX_SPOOL_TO_DISK_BYTES_ATTRIB, DEFAULT_MAX_SPOOL_TO_DISK_BYTES)
            .setIfUnset(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA)
            .setIfUnset(GROUPBY_SPILLABLE_ATTRIB, DEFAULT_GROUPBY_SPILLABLE)
            .setIfUnset(GROUPBY_MAX_CACHE_SIZE_ATTRIB, DEFAULT_GROUPBY_MAX_CACHE_MAX)
            .setIfUnset(GROUPBY_SPILL_FILES_ATTRIB, DEFAULT_GROUPBY_SPILL_FILES)
            .setIfUnset(SEQUENCE_CACHE_SIZE_ATTRIB, DEFAULT_SEQUENCE_CACHE_SIZE)
            ;
        // HBase sets this to 1, so we reset it to something more appropriate.
        // Hopefully HBase will change this, because we can't know if a user set
        // it to 1, so we'll change it.
        int scanCaching = config.getInt(SCAN_CACHE_SIZE_ATTRIB, 0);
        if (scanCaching == 1) {
            config.setInt(SCAN_CACHE_SIZE_ATTRIB, DEFAULT_SCAN_CACHE_SIZE);
        } else if (scanCaching <= 0) { // Provides the user with a way of setting it to 1
            config.setInt(SCAN_CACHE_SIZE_ATTRIB, 1);
        }
        return options;
    }
    
    public Configuration getConfiguration() {
        return config;
    }

    private QueryServicesOptions setIfUnset(String name, int value) {
        config.setIfUnset(name, Integer.toString(value));
        return this;
    }
    
    private QueryServicesOptions setIfUnset(String name, boolean value) {
        config.setIfUnset(name, Boolean.toString(value));
        return this;
    }
    
    private QueryServicesOptions setIfUnset(String name, long value) {
        config.setIfUnset(name, Long.toString(value));
        return this;
    }
    
    private QueryServicesOptions setIfUnset(String name, String value) {
        config.setIfUnset(name, value);
        return this;
    }
    
    public QueryServicesOptions setKeepAliveMs(int keepAliveMs) {
        return set(KEEP_ALIVE_MS_ATTRIB, keepAliveMs);
    }
    
    public QueryServicesOptions setThreadPoolSize(int threadPoolSize) {
        return set(THREAD_POOL_SIZE_ATTRIB, threadPoolSize);
    }
    
    public QueryServicesOptions setQueueSize(int queueSize) {
        config.setInt(QUEUE_SIZE_ATTRIB, queueSize);
        return this;
    }
    
    public QueryServicesOptions setThreadTimeoutMs(int threadTimeoutMs) {
        return set(THREAD_TIMEOUT_MS_ATTRIB, threadTimeoutMs);
    }
    
    public QueryServicesOptions setSpoolThresholdBytes(int spoolThresholdBytes) {
        return set(SPOOL_THRESHOLD_BYTES_ATTRIB, spoolThresholdBytes);
    }
    
    public QueryServicesOptions setMaxMemoryPerc(int maxMemoryPerc) {
        return set(MAX_MEMORY_PERC_ATTRIB, maxMemoryPerc);
    }
    
    public QueryServicesOptions setMaxMemoryWaitMs(int maxMemoryWaitMs) {
        return set(MAX_MEMORY_WAIT_MS_ATTRIB, maxMemoryWaitMs);
    }
    
    public QueryServicesOptions setMaxTenantMemoryPerc(int maxTenantMemoryPerc) {
        return set(MAX_TENANT_MEMORY_PERC_ATTRIB, maxTenantMemoryPerc);
    }
    
    public QueryServicesOptions setMaxServerCacheSize(long maxServerCacheSize) {
        return set(MAX_SERVER_CACHE_SIZE_ATTRIB, maxServerCacheSize);
    }

    public QueryServicesOptions setMaxServerMetaDataCacheSize(long maxMetaDataCacheSize) {
        return set(MAX_SERVER_METADATA_CACHE_SIZE_ATTRIB, maxMetaDataCacheSize);
    }

    public QueryServicesOptions setMaxClientMetaDataCacheSize(long maxMetaDataCacheSize) {
        return set(MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB, maxMetaDataCacheSize);
    }

    public QueryServicesOptions setScanFetchSize(int scanFetchSize) {
        return set(SCAN_CACHE_SIZE_ATTRIB, scanFetchSize);
    }
    
    public QueryServicesOptions setMaxQueryConcurrency(int maxQueryConcurrency) {
        return set(MAX_QUERY_CONCURRENCY_ATTRIB, maxQueryConcurrency);
    }

    public QueryServicesOptions setTargetQueryConcurrency(int targetQueryConcurrency) {
        return set(TARGET_QUERY_CONCURRENCY_ATTRIB, targetQueryConcurrency);
    }
    
    public QueryServicesOptions setDateFormat(String dateFormat) {
        return set(DATE_FORMAT_ATTRIB, dateFormat);
    }
    
    public QueryServicesOptions setStatsUpdateFrequencyMs(int frequencyMs) {
        return set(STATS_UPDATE_FREQ_MS_ATTRIB, frequencyMs);
    }
    
    public QueryServicesOptions setCallQueueRoundRobin(boolean isRoundRobin) {
        return set(CALL_QUEUE_PRODUCER_ATTRIB_NAME, isRoundRobin);
    }
    
    public QueryServicesOptions setMaxMutateSize(int maxMutateSize) {
        return set(MAX_MUTATION_SIZE_ATTRIB, maxMutateSize);
    }
    
    public QueryServicesOptions setMutateBatchSize(int mutateBatchSize) {
        return set(MUTATE_BATCH_SIZE_ATTRIB, mutateBatchSize);
    }
    
    public QueryServicesOptions setMaxIntraRegionParallelization(int maxIntraRegionParallelization) {
        return set(MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, maxIntraRegionParallelization);
    }
    
    public QueryServicesOptions setRowKeyOrderSaltedTable(boolean rowKeyOrderSaltedTable) {
        return set(ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, rowKeyOrderSaltedTable);
    }
    
    public QueryServicesOptions setDropMetaData(boolean dropMetadata) {
        return set(DROP_METADATA_ATTRIB, dropMetadata);
    }
    
    public QueryServicesOptions setGroupBySpill(boolean enabled) {
        return set(GROUPBY_SPILLABLE_ATTRIB, enabled);
    }

    public QueryServicesOptions setGroupBySpillMaxCacheSize(long size) {
        return set(GROUPBY_MAX_CACHE_SIZE_ATTRIB, size);
    }
    
    public QueryServicesOptions setGroupBySpillNumSpillFiles(long num) {
        return set(GROUPBY_SPILL_FILES_ATTRIB, num);
    }

    
    private QueryServicesOptions set(String name, boolean value) {
        config.set(name, Boolean.toString(value));
        return this;
    }
    
    private QueryServicesOptions set(String name, int value) {
        config.set(name, Integer.toString(value));
        return this;
    }
    
    private QueryServicesOptions set(String name, String value) {
        config.set(name, value);
        return this;
    }
    
    private QueryServicesOptions set(String name, long value) {
        config.set(name, Long.toString(value));
        return this;
    }

    public int getKeepAliveMs() {
        return config.getInt(KEEP_ALIVE_MS_ATTRIB, DEFAULT_KEEP_ALIVE_MS);
    }
    
    public int getThreadPoolSize() {
        return config.getInt(THREAD_POOL_SIZE_ATTRIB, DEFAULT_THREAD_POOL_SIZE);
    }
    
    public int getQueueSize() {
        return config.getInt(QUEUE_SIZE_ATTRIB, DEFAULT_QUEUE_SIZE);
    }
    
    public int getMaxMemoryPerc() {
        return config.getInt(MAX_MEMORY_PERC_ATTRIB, DEFAULT_MAX_MEMORY_PERC);
    }
    
    public int getMaxMemoryWaitMs() {
        return config.getInt(MAX_MEMORY_WAIT_MS_ATTRIB, DEFAULT_MAX_MEMORY_WAIT_MS);
    }

    public int getMaxMutateSize() {
        return config.getInt(MAX_MUTATION_SIZE_ATTRIB, DEFAULT_MAX_MUTATION_SIZE);
    }

    public int getMutateBatchSize() {
        return config.getInt(MUTATE_BATCH_SIZE_ATTRIB, DEFAULT_MUTATE_BATCH_SIZE);
    }
    
    public int getMaxIntraRegionParallelization() {
        return config.getInt(MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, DEFAULT_MAX_INTRA_REGION_PARALLELIZATION);
    }
    
    public boolean isUseIndexes() {
        return config.getBoolean(USE_INDEXES_ATTRIB, DEFAULT_USE_INDEXES);
    }

    public boolean isImmutableRows() {
        return config.getBoolean(IMMUTABLE_ROWS_ATTRIB, DEFAULT_IMMUTABLE_ROWS);
    }
    
    public boolean isDropMetaData() {
        return config.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
    }
    
    public boolean isSpillableGroupByEnabled() {
        return config.getBoolean(GROUPBY_SPILLABLE_ATTRIB, DEFAULT_GROUPBY_SPILLABLE);
    }
    
    public long getSpillableGroupByMaxCacheSize() {
        return config.getLong(GROUPBY_MAX_CACHE_SIZE_ATTRIB, DEFAULT_GROUPBY_MAX_CACHE_MAX);
    }
    
    public int getSpillableGroupByNumSpillFiles() {
        return config.getInt(GROUPBY_SPILL_FILES_ATTRIB, DEFAULT_GROUPBY_SPILL_FILES);
    }

    public QueryServicesOptions setMaxServerCacheTTLMs(int ttl) {
        return set(MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, ttl);
    }
    
    public QueryServicesOptions setMasterInfoPort(int port) {
        return set(MASTER_INFO_PORT_ATTRIB, port);
    }
    
    public QueryServicesOptions setRegionServerInfoPort(int port) {
        return set(REGIONSERVER_INFO_PORT_ATTRIB, port);
    }
    
    public QueryServicesOptions setRegionServerLeasePeriodMs(int period) {
        return set(REGIONSERVER_LEASE_PERIOD_ATTRIB, period);
    }
    
    public QueryServicesOptions setRpcTimeoutMs(int timeout) {
        return set(RPC_TIMEOUT_ATTRIB, timeout);
    }
    
    public QueryServicesOptions setUseIndexes(boolean useIndexes) {
        return set(USE_INDEXES_ATTRIB, useIndexes);
    }
    
    public QueryServicesOptions setImmutableRows(boolean isImmutableRows) {
        return set(IMMUTABLE_ROWS_ATTRIB, isImmutableRows);
    }

    public QueryServicesOptions setWALEditCodec(String walEditCodec) {
        return set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, walEditCodec);
    }

}
