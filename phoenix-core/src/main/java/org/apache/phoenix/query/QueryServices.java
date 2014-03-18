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

import java.util.concurrent.ExecutorService;

import org.apache.http.annotation.Immutable;
import org.apache.phoenix.iterate.SpoolTooBigToDiskException;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseable;



/**
 * 
 * Interface to group together services needed during querying.  The
 * parameters that may be set in {@link org.apache.hadoop.conf.Configuration}
 * are documented here: https://github.com/forcedotcom/phoenix/wiki/Tuning
 *     
 * 
 * @since 0.1
 */
@Immutable
public interface QueryServices extends SQLCloseable {
    public static final String KEEP_ALIVE_MS_ATTRIB = "phoenix.query.keepAliveMs";
    public static final String THREAD_POOL_SIZE_ATTRIB = "phoenix.query.threadPoolSize";
    public static final String QUEUE_SIZE_ATTRIB = "phoenix.query.queueSize";
    public static final String THREAD_TIMEOUT_MS_ATTRIB = "phoenix.query.timeoutMs";
    public static final String SPOOL_THRESHOLD_BYTES_ATTRIB = "phoenix.query.spoolThresholdBytes";
    
    /**
	 * max size to spool the the result into
	 * ${java.io.tmpdir}/ResultSpoolerXXX.bin if
	 * {@link QueryServices#SPOOL_THRESHOLD_BYTES_ATTRIB } is reached.
	 * <p>
	 * default is unlimited(-1)
	 * <p>
	 * if the threshold is reached, a {@link SpoolTooBigToDiskException } will be thrown 
	 */
	public static final String MAX_SPOOL_TO_DISK_BYTES_ATTRIB = "phoenix.query.maxSpoolToDiskBytes";
    
    public static final String MAX_MEMORY_PERC_ATTRIB = "phoenix.query.maxGlobalMemoryPercentage";
    public static final String MAX_MEMORY_WAIT_MS_ATTRIB = "phoenix.query.maxGlobalMemoryWaitMs";
    public static final String MAX_TENANT_MEMORY_PERC_ATTRIB = "phoenix.query.maxTenantMemoryPercentage";
    public static final String MAX_SERVER_CACHE_SIZE_ATTRIB = "phoenix.query.maxServerCacheBytes";
    public static final String TARGET_QUERY_CONCURRENCY_ATTRIB = "phoenix.query.targetConcurrency";
    public static final String MAX_QUERY_CONCURRENCY_ATTRIB = "phoenix.query.maxConcurrency";
    public static final String DATE_FORMAT_ATTRIB = "phoenix.query.dateFormat";
    public static final String NUMBER_FORMAT_ATTRIB = "phoenix.query.numberFormat";
    public static final String STATS_UPDATE_FREQ_MS_ATTRIB = "phoenix.query.statsUpdateFrequency";
    public static final String MAX_STATS_AGE_MS_ATTRIB = "phoenix.query.maxStatsAge";
    public static final String CALL_QUEUE_ROUND_ROBIN_ATTRIB = "ipc.server.callqueue.roundrobin";
    public static final String SCAN_CACHE_SIZE_ATTRIB = "hbase.client.scanner.caching";
    public static final String MAX_MUTATION_SIZE_ATTRIB = "phoenix.mutate.maxSize";
    public static final String MUTATE_BATCH_SIZE_ATTRIB = "phoenix.mutate.batchSize";
    public static final String MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB = "phoenix.coprocessor.maxServerCacheTimeToLiveMs";
    public static final String MAX_INTRA_REGION_PARALLELIZATION_ATTRIB  = "phoenix.query.maxIntraRegionParallelization";
    public static final String ROW_KEY_ORDER_SALTED_TABLE_ATTRIB  = "phoenix.query.rowKeyOrderSaltedTable";
    public static final String USE_INDEXES_ATTRIB  = "phoenix.query.useIndexes";
    public static final String IMMUTABLE_ROWS_ATTRIB  = "phoenix.mutate.immutableRows";
    public static final String INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB  = "phoenix.index.mutableBatchSizeThreshold";
    public static final String DROP_METADATA_ATTRIB  = "phoenix.schema.dropMetaData";
    public static final String GROUPBY_SPILLABLE_ATTRIB  = "phoenix.groupby.spillable";
    public static final String GROUPBY_SPILL_FILES_ATTRIB = "phoenix.groupby.spillFiles";
    public static final String GROUPBY_MAX_CACHE_SIZE_ATTRIB = "phoenix.groupby.maxCacheSize";
    public static final String GROUPBY_ESTIMATED_DISTINCT_VALUES_ATTRIB = "phoenix.groupby.estimatedDistinctValues";

    public static final String CALL_QUEUE_PRODUCER_ATTRIB_NAME = "CALL_QUEUE_PRODUCER";
    
    public static final String MASTER_INFO_PORT_ATTRIB = "hbase.master.info.port";
    public static final String REGIONSERVER_INFO_PORT_ATTRIB = "hbase.regionserver.info.port";
    public static final String REGIONSERVER_LEASE_PERIOD_ATTRIB = "hbase.regionserver.lease.period";
    public static final String RPC_TIMEOUT_ATTRIB = "hbase.rpc.timeout";
    public static final String ZOOKEEPER_QUARUM_ATTRIB = "hbase.zookeeper.quorum";
    public static final String ZOOKEEPER_PORT_ATTRIB = "hbase.zookeeper.property.clientPort";
    public static final String ZOOKEEPER_ROOT_NODE_ATTRIB = "zookeeper.znode.parent";
    public static final String DISTINCT_VALUE_COMPRESS_THRESHOLD_ATTRIB = "phoenix.distinct.value.compress.threshold";
    public static final String SEQUENCE_CACHE_SIZE_ATTRIB = "phoenix.sequence.cacheSize";
    public static final String INDEX_MAX_FILESIZE_PERC_ATTRIB = "phoenix.index.maxDataFileSizePerc";
    public static final String MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB = "phoenix.coprocessor.maxMetaDataCacheTimeToLiveMs";
    public static final String MAX_SERVER_METADATA_CACHE_SIZE_ATTRIB = "phoenix.coprocessor.maxMetaDataCacheSize";
    public static final String MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB = "phoenix.client.maxMetaDataCacheSize";

    public static final String AUTO_UPGRADE_WHITELIST_ATTRIB = "phoenix.client.autoUpgradeWhiteList";
    // Mainly for testing to force spilling
    public static final String MAX_MEMORY_SIZE_ATTRIB = "phoenix.query.maxGlobalMemorySize";
    
    /**
     * Get executor service used for parallel scans
     */
    public ExecutorService getExecutor();
    /**
     * Get the memory manager used to track memory usage
     */
    public MemoryManager getMemoryManager();
    
    /**
     * Get the properties from the HBase configuration in a
     * read-only structure that avoids any synchronization
     */
    public ReadOnlyProps getProps();
    
    /**
     * Get query optimizer used to choose the best query plan
     */
    public QueryOptimizer getOptimizer();
}
