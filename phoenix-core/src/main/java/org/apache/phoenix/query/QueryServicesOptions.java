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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.phoenix.query.QueryServices.ALLOW_ONLINE_TABLE_SCHEMA_UPDATE;
import static org.apache.phoenix.query.QueryServices.ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE;
import static org.apache.phoenix.query.QueryServices.AUTO_UPGRADE_ENABLED;
import static org.apache.phoenix.query.QueryServices.CALL_QUEUE_PRODUCER_ATTRIB_NAME;
import static org.apache.phoenix.query.QueryServices.CALL_QUEUE_ROUND_ROBIN_ATTRIB;
import static org.apache.phoenix.query.QueryServices.COLLECT_REQUEST_LEVEL_METRICS;
import static org.apache.phoenix.query.QueryServices.COMMIT_STATS_ASYNC;
import static org.apache.phoenix.query.QueryServices.DATE_FORMAT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.DELAY_FOR_SCHEMA_UPDATE_CHECK;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.query.QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.EXPLAIN_ROW_COUNT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB;
import static org.apache.phoenix.query.QueryServices.GLOBAL_METRICS_ENABLED;
import static org.apache.phoenix.query.QueryServices.GROUPBY_MAX_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.GROUPBY_SPILLABLE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.GROUPBY_SPILL_FILES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.HBASE_CLIENT_SCANNER_TIMEOUT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.IMMUTABLE_ROWS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB;
import static org.apache.phoenix.query.QueryServices.INDEX_POPULATION_SLEEP_TIME;
import static org.apache.phoenix.query.QueryServices.IS_NAMESPACE_MAPPING_ENABLED;
import static org.apache.phoenix.query.QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE;
import static org.apache.phoenix.query.QueryServices.KEEP_ALIVE_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.LOCAL_INDEX_CLIENT_UPGRADE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MASTER_INFO_PORT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_MEMORY_PERC_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_MEMORY_WAIT_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_MUTATION_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_SERVER_METADATA_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_SPOOL_TO_DISK_BYTES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MAX_TENANT_MEMORY_PERC_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MIN_STATS_UPDATE_FREQ_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.NUM_RETRIES_FOR_SCHEMA_UPDATE_CHECK;
import static org.apache.phoenix.query.QueryServices.QUEUE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.REGIONSERVER_INFO_PORT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.RENEW_LEASE_ENABLED;
import static org.apache.phoenix.query.QueryServices.RENEW_LEASE_THREAD_POOL_SIZE;
import static org.apache.phoenix.query.QueryServices.RENEW_LEASE_THRESHOLD_MILLISECONDS;
import static org.apache.phoenix.query.QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.RPC_TIMEOUT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS;
import static org.apache.phoenix.query.QueryServices.RUN_UPDATE_STATS_ASYNC;
import static org.apache.phoenix.query.QueryServices.SCAN_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.SCAN_RESULT_CHUNK_SIZE;
import static org.apache.phoenix.query.QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.SPOOL_DIRECTORY;
import static org.apache.phoenix.query.QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.STATS_USE_CURRENT_TIME_ATTRIB;
import static org.apache.phoenix.query.QueryServices.THREAD_POOL_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.THREAD_TIMEOUT_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.TRANSACTIONS_ENABLED;
import static org.apache.phoenix.query.QueryServices.UPLOAD_BINARY_DATA_TYPE_ENCODING;
import static org.apache.phoenix.query.QueryServices.USE_BYTE_BASED_REGEX_ATTRIB;
import static org.apache.phoenix.query.QueryServices.USE_INDEXES_ATTRIB;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.controller.ClientRpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableRefFactory;
import org.apache.phoenix.trace.util.Tracing;
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
	public static final int DEFAULT_QUEUE_SIZE = 5000;
	public static final int DEFAULT_THREAD_TIMEOUT_MS = 600000; // 10min
	public static final int DEFAULT_SPOOL_THRESHOLD_BYTES = 1024 * 1024 * 20; // 20m
    public static final String DEFAULT_SPOOL_DIRECTORY = System.getProperty("java.io.tmpdir");
	public static final int DEFAULT_MAX_MEMORY_PERC = 15; // 15% of heap
	public static final int DEFAULT_MAX_MEMORY_WAIT_MS = 10000;
	public static final int DEFAULT_MAX_TENANT_MEMORY_PERC = 100;
	public static final long DEFAULT_MAX_SERVER_CACHE_SIZE = 1024*1024*100;  // 100 Mb
    public static final int DEFAULT_TARGET_QUERY_CONCURRENCY = 32;
    public static final int DEFAULT_MAX_QUERY_CONCURRENCY = 64;
    public static final String DEFAULT_DATE_FORMAT = DateUtil.DEFAULT_DATE_FORMAT;
    public static final String DEFAULT_DATE_FORMAT_TIMEZONE = DateUtil.DEFAULT_TIME_ZONE_ID;
    public static final boolean DEFAULT_CALL_QUEUE_ROUND_ROBIN = true;
    public static final int DEFAULT_MAX_MUTATION_SIZE = 500000;
    public static final boolean DEFAULT_USE_INDEXES = true; // Use indexes
    public static final boolean DEFAULT_IMMUTABLE_ROWS = false; // Tables rows may be updated
    public static final boolean DEFAULT_DROP_METADATA = true; // Drop meta data also.
    public static final long DEFAULT_DRIVER_SHUTDOWN_TIMEOUT_MS = 5  * 1000; // Time to wait in ShutdownHook to exit gracefully.

    @Deprecated //use DEFAULT_MUTATE_BATCH_SIZE_BYTES
    public final static int DEFAULT_MUTATE_BATCH_SIZE = 100; // Batch size for UPSERT SELECT and DELETE
    //Batch size in bytes for UPSERT, SELECT and DELETE. By default, 10MB
    public final static long DEFAULT_MUTATE_BATCH_SIZE_BYTES = 134217728;
	// The only downside of it being out-of-sync is that the parallelization of the scan won't be as balanced as it could be.
    public static final int DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS = 30000; // 30 sec (with no activity)
    public static final int DEFAULT_SCAN_CACHE_SIZE = 1000;
    public static final int DEFAULT_MAX_INTRA_REGION_PARALLELIZATION = DEFAULT_MAX_QUERY_CONCURRENCY;
    public static final int DEFAULT_DISTINCT_VALUE_COMPRESS_THRESHOLD = 1024 * 1024 * 1; // 1 Mb
    public static final int DEFAULT_INDEX_MUTATE_BATCH_SIZE_THRESHOLD = 3;
    public static final long DEFAULT_MAX_SPOOL_TO_DISK_BYTES = 1024000000;
    // Only the first chunked batches are fetched in parallel, so this default
    // should be on the relatively bigger side of things. Bigger means more
    // latency and client-side spooling/buffering. Smaller means less initial
    // latency and less parallelization.
    public static final long DEFAULT_SCAN_RESULT_CHUNK_SIZE = 2999;
    public static final boolean DEFAULT_IS_NAMESPACE_MAPPING_ENABLED = false;
    public static final boolean DEFAULT_IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE = true;

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
    public static final long DEFAULT_MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS =  60000 * 30; // 30 mins
    public static final long DEFAULT_MAX_SERVER_METADATA_CACHE_SIZE =  1024L*1024L*20L; // 20 Mb
    public static final long DEFAULT_MAX_CLIENT_METADATA_CACHE_SIZE =  1024L*1024L*10L; // 10 Mb
    public static final int DEFAULT_GROUPBY_ESTIMATED_DISTINCT_VALUES = 1000;
    public static final int DEFAULT_CLOCK_SKEW_INTERVAL = 2000;
    public static final boolean DEFAULT_INDEX_FAILURE_HANDLING_REBUILD = true; // auto rebuild on
    public static final boolean DEFAULT_INDEX_FAILURE_BLOCK_WRITE = false; 
    public static final long DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_INTERVAL = 10000; // 10 secs
    public static final long DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME = 1; // 1 ms

    /**
     * HConstants#HIGH_QOS is the max we will see to a standard table. We go higher to differentiate
     * and give some room for things in the middle
     */
    public static final int DEFAULT_INDEX_PRIORITY = 1000;
    public static final int DEFAULT_METADATA_PRIORITY = 2000;
    public static final boolean DEFAULT_ALLOW_LOCAL_INDEX = true;
    public static final int DEFAULT_INDEX_HANDLER_COUNT = 30;
    public static final int DEFAULT_METADATA_HANDLER_COUNT = 30;

    public static final int DEFAULT_TRACING_PAGE_SIZE = 100;
    /**
     * Configuration key to overwrite the tablename that should be used as the target table
     */
    public static final String DEFAULT_TRACING_STATS_TABLE_NAME = "SYSTEM.TRACING_STATS";
    public static final String DEFAULT_TRACING_FREQ = Tracing.Frequency.NEVER.getKey();
    public static final double DEFAULT_TRACING_PROBABILITY_THRESHOLD = 0.05;

    public static final int DEFAULT_STATS_UPDATE_FREQ_MS = 15 * 60000; // 15min
    public static final int DEFAULT_STATS_GUIDEPOST_PER_REGION = 0; // Uses guidepost width by default
    // Since we're not taking into account the compression done by FAST_DIFF in our
    // counting of the bytes, default guidepost width to 100MB * 3 (where 3 is the
    // compression we're getting)
    public static final long DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES = 3* 100 * 1024 *1024;
    public static final boolean DEFAULT_STATS_USE_CURRENT_TIME = true;
    public static final boolean DEFAULT_RUN_UPDATE_STATS_ASYNC = true;
    public static final boolean DEFAULT_COMMIT_STATS_ASYNC = true;
    public static final int DEFAULT_STATS_POOL_SIZE = 4;
    // Maximum size (in bytes) that cached table stats should take upm
    public static final long DEFAULT_STATS_MAX_CACHE_SIZE = 256 * 1024 * 1024;

    public static final boolean DEFAULT_USE_REVERSE_SCAN = true;

    /**
     * Use only first time SYSTEM.SEQUENCE table is created.
     */
    public static final int DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS = 0;
    /**
     * Default value for coprocessor priority is between SYSTEM and USER priority.
     */
    public static final int DEFAULT_COPROCESSOR_PRIORITY = Coprocessor.PRIORITY_SYSTEM/2 + Coprocessor.PRIORITY_USER/2; // Divide individually to prevent any overflow
    public static final boolean DEFAULT_EXPLAIN_CHUNK_COUNT = true;
    public static final boolean DEFAULT_EXPLAIN_ROW_COUNT = true;
    public static final boolean DEFAULT_ALLOW_ONLINE_TABLE_SCHEMA_UPDATE = true;
    public static final int DEFAULT_RETRIES_FOR_SCHEMA_UPDATE_CHECK = 10;
    public static final long DEFAULT_DELAY_FOR_SCHEMA_UPDATE_CHECK = 5 * 1000; // 5 seconds.
    public static final boolean DEFAULT_KEEP_DELETED_CELLS = false;
    public static final boolean DEFAULT_STORE_NULLS = false;

    // TODO Change this to true as part of PHOENIX-1543
    // We'll also need this for transactions to work correctly
    public static final boolean DEFAULT_AUTO_COMMIT = false;
    public static final boolean DEFAULT_TABLE_ISTRANSACTIONAL = false;
    public static final boolean DEFAULT_TRANSACTIONS_ENABLED = false;
    public static final boolean DEFAULT_IS_GLOBAL_METRICS_ENABLED = true;
    
    public static final boolean DEFAULT_TRANSACTIONAL = false;
    public static final boolean DEFAULT_AUTO_FLUSH = false;

    private static final String DEFAULT_CLIENT_RPC_CONTROLLER_FACTORY = ClientRpcControllerFactory.class.getName();
    
    public static final String DEFAULT_CONSISTENCY_LEVEL = Consistency.STRONG.toString();

    public static final boolean DEFAULT_USE_BYTE_BASED_REGEX = false;
    public static final boolean DEFAULT_FORCE_ROW_KEY_ORDER = false;
    public static final boolean DEFAULT_ALLOW_USER_DEFINED_FUNCTIONS = false;
    public static final boolean DEFAULT_REQUEST_LEVEL_METRICS_ENABLED = false;
    public static final boolean DEFAULT_ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE = true;
    public static final int DEFAULT_MAX_VERSIONS_TRANSACTIONAL = Integer.MAX_VALUE;
    
    public static final boolean DEFAULT_RETURN_SEQUENCE_VALUES = false;
    public static final String DEFAULT_EXTRA_JDBC_ARGUMENTS = "";
    
    public static final long DEFAULT_INDEX_POPULATION_SLEEP_TIME = 5000;

    // QueryServer defaults -- ensure ThinClientUtil is also updated since phoenix-queryserver-client
    // doesn't depend on phoenix-core.
    public static final String DEFAULT_QUERY_SERVER_SERIALIZATION = "PROTOBUF";
    public static final int DEFAULT_QUERY_SERVER_HTTP_PORT = 8765;
    public static final long DEFAULT_QUERY_SERVER_UGI_CACHE_MAX_SIZE = 1000L;
    public static final int DEFAULT_QUERY_SERVER_UGI_CACHE_INITIAL_SIZE = 100;
    public static final int DEFAULT_QUERY_SERVER_UGI_CACHE_CONCURRENCY = 10;
    public static final boolean DEFAULT_QUERY_SERVER_SPNEGO_AUTH_DISABLED = false;

    public static final boolean DEFAULT_RENEW_LEASE_ENABLED = true;
    public static final int DEFAULT_RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS =
            DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD / 2;
    public static final int DEFAULT_RENEW_LEASE_THRESHOLD_MILLISECONDS =
            (3 * DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD) / 4;
    public static final int DEFAULT_RENEW_LEASE_THREAD_POOL_SIZE = 10;
    public static final boolean DEFAULT_LOCAL_INDEX_CLIENT_UPGRADE = true;
    public static final float DEFAULT_LIMITED_QUERY_SERIAL_THRESHOLD = 0.2f;
    
    public static final boolean DEFAULT_INDEX_ASYNC_BUILD_ENABLED = true;
    
    public static final String DEFAULT_CLIENT_CACHE_ENCODING = PTableRefFactory.Encoding.OBJECT.toString();
    public static final boolean DEFAULT_AUTO_UPGRADE_ENABLED = true;
    public static final int DEFAULT_CLIENT_CONNECTION_CACHE_MAX_DURATION = 86400000;
    public static final int DEFAULT_COLUMN_ENCODED_BYTES = QualifierEncodingScheme.TWO_BYTE_QUALIFIERS.getSerializedMetadataValue();
    public static final String DEFAULT_IMMUTABLE_STORAGE_SCHEME = ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS.toString();
    public static final String DEFAULT_MULTITENANT_IMMUTABLE_STORAGE_SCHEME = ImmutableStorageScheme.ONE_CELL_PER_COLUMN.toString();

    //by default, max connections from one client to one cluster is unlimited
    public static final int DEFAULT_CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS = 0;

    @SuppressWarnings("serial")
    public static final Set<String> DEFAULT_QUERY_SERVER_SKIP_WORDS = new HashSet<String>() {
      {
        add("secret");
        add("passwd");
        add("password");
        add("credential");
      }
    };
    public static final String DEFAULT_SCHEMA = null;
    public static final String DEFAULT_UPLOAD_BINARY_DATA_TYPE_ENCODING = "BASE64"; // for backward compatibility, till
                                                                                    // 4.10, psql and CSVBulkLoad
                                                                                    // expects binary data to be base 64
                                                                                    // encoded
    
    private final Configuration config;

    private QueryServicesOptions(Configuration config) {
        this.config = config;
    }

    public ReadOnlyProps getProps(ReadOnlyProps defaultProps) {
        return new ReadOnlyProps(defaultProps, config.iterator());
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
            .setIfUnset(STATS_USE_CURRENT_TIME_ATTRIB, DEFAULT_STATS_USE_CURRENT_TIME)
            .setIfUnset(RUN_UPDATE_STATS_ASYNC, DEFAULT_RUN_UPDATE_STATS_ASYNC)
            .setIfUnset(COMMIT_STATS_ASYNC, DEFAULT_COMMIT_STATS_ASYNC)
            .setIfUnset(KEEP_ALIVE_MS_ATTRIB, DEFAULT_KEEP_ALIVE_MS)
            .setIfUnset(THREAD_POOL_SIZE_ATTRIB, DEFAULT_THREAD_POOL_SIZE)
            .setIfUnset(QUEUE_SIZE_ATTRIB, DEFAULT_QUEUE_SIZE)
            .setIfUnset(THREAD_TIMEOUT_MS_ATTRIB, DEFAULT_THREAD_TIMEOUT_MS)
            .setIfUnset(SPOOL_THRESHOLD_BYTES_ATTRIB, DEFAULT_SPOOL_THRESHOLD_BYTES)
            .setIfUnset(SPOOL_DIRECTORY, DEFAULT_SPOOL_DIRECTORY)
            .setIfUnset(MAX_MEMORY_PERC_ATTRIB, DEFAULT_MAX_MEMORY_PERC)
            .setIfUnset(MAX_MEMORY_WAIT_MS_ATTRIB, DEFAULT_MAX_MEMORY_WAIT_MS)
            .setIfUnset(MAX_TENANT_MEMORY_PERC_ATTRIB, DEFAULT_MAX_TENANT_MEMORY_PERC)
            .setIfUnset(MAX_SERVER_CACHE_SIZE_ATTRIB, DEFAULT_MAX_SERVER_CACHE_SIZE)
            .setIfUnset(SCAN_CACHE_SIZE_ATTRIB, DEFAULT_SCAN_CACHE_SIZE)
            .setIfUnset(DATE_FORMAT_ATTRIB, DEFAULT_DATE_FORMAT)
            .setIfUnset(DATE_FORMAT_TIMEZONE_ATTRIB, DEFAULT_DATE_FORMAT_TIMEZONE)
            .setIfUnset(STATS_UPDATE_FREQ_MS_ATTRIB, DEFAULT_STATS_UPDATE_FREQ_MS)
            .setIfUnset(CALL_QUEUE_ROUND_ROBIN_ATTRIB, DEFAULT_CALL_QUEUE_ROUND_ROBIN)
            .setIfUnset(MAX_MUTATION_SIZE_ATTRIB, DEFAULT_MAX_MUTATION_SIZE)
            .setIfUnset(ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, DEFAULT_FORCE_ROW_KEY_ORDER)
            .setIfUnset(USE_INDEXES_ATTRIB, DEFAULT_USE_INDEXES)
            .setIfUnset(IMMUTABLE_ROWS_ATTRIB, DEFAULT_IMMUTABLE_ROWS)
            .setIfUnset(INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, DEFAULT_INDEX_MUTATE_BATCH_SIZE_THRESHOLD)
            .setIfUnset(MAX_SPOOL_TO_DISK_BYTES_ATTRIB, DEFAULT_MAX_SPOOL_TO_DISK_BYTES)
            .setIfUnset(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA)
            .setIfUnset(GROUPBY_SPILLABLE_ATTRIB, DEFAULT_GROUPBY_SPILLABLE)
            .setIfUnset(GROUPBY_MAX_CACHE_SIZE_ATTRIB, DEFAULT_GROUPBY_MAX_CACHE_MAX)
            .setIfUnset(GROUPBY_SPILL_FILES_ATTRIB, DEFAULT_GROUPBY_SPILL_FILES)
            .setIfUnset(SEQUENCE_CACHE_SIZE_ATTRIB, DEFAULT_SEQUENCE_CACHE_SIZE)
            .setIfUnset(SCAN_RESULT_CHUNK_SIZE, DEFAULT_SCAN_RESULT_CHUNK_SIZE)
            .setIfUnset(ALLOW_ONLINE_TABLE_SCHEMA_UPDATE, DEFAULT_ALLOW_ONLINE_TABLE_SCHEMA_UPDATE)
            .setIfUnset(NUM_RETRIES_FOR_SCHEMA_UPDATE_CHECK, DEFAULT_RETRIES_FOR_SCHEMA_UPDATE_CHECK)
            .setIfUnset(DELAY_FOR_SCHEMA_UPDATE_CHECK, DEFAULT_DELAY_FOR_SCHEMA_UPDATE_CHECK)
            .setIfUnset(GLOBAL_METRICS_ENABLED, DEFAULT_IS_GLOBAL_METRICS_ENABLED)
            .setIfUnset(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY, DEFAULT_CLIENT_RPC_CONTROLLER_FACTORY)
            .setIfUnset(USE_BYTE_BASED_REGEX_ATTRIB, DEFAULT_USE_BYTE_BASED_REGEX)
            .setIfUnset(FORCE_ROW_KEY_ORDER_ATTRIB, DEFAULT_FORCE_ROW_KEY_ORDER)
            .setIfUnset(COLLECT_REQUEST_LEVEL_METRICS, DEFAULT_REQUEST_LEVEL_METRICS_ENABLED)
            .setIfUnset(ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE, DEFAULT_ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE)
            .setIfUnset(ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE, DEFAULT_ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE)
            .setIfUnset(RENEW_LEASE_THRESHOLD_MILLISECONDS, DEFAULT_RENEW_LEASE_THRESHOLD_MILLISECONDS)
            .setIfUnset(RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS, DEFAULT_RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS)
            .setIfUnset(RENEW_LEASE_THREAD_POOL_SIZE, DEFAULT_RENEW_LEASE_THREAD_POOL_SIZE)
            .setIfUnset(IS_NAMESPACE_MAPPING_ENABLED, DEFAULT_IS_NAMESPACE_MAPPING_ENABLED)
            .setIfUnset(IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, DEFAULT_IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE)
            .setIfUnset(LOCAL_INDEX_CLIENT_UPGRADE_ATTRIB, DEFAULT_LOCAL_INDEX_CLIENT_UPGRADE)
            .setIfUnset(AUTO_UPGRADE_ENABLED, DEFAULT_AUTO_UPGRADE_ENABLED)
            .setIfUnset(UPLOAD_BINARY_DATA_TYPE_ENCODING, DEFAULT_UPLOAD_BINARY_DATA_TYPE_ENCODING);
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

    public QueryServicesOptions setSpoolDirectory(String spoolDirectory) {
        return set(SPOOL_DIRECTORY, spoolDirectory);
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

    public QueryServicesOptions setDateFormat(String dateFormat) {
        return set(DATE_FORMAT_ATTRIB, dateFormat);
    }

    public QueryServicesOptions setCallQueueRoundRobin(boolean isRoundRobin) {
        return set(CALL_QUEUE_PRODUCER_ATTRIB_NAME, isRoundRobin);
    }

    public QueryServicesOptions setMaxMutateSize(int maxMutateSize) {
        return set(MAX_MUTATION_SIZE_ATTRIB, maxMutateSize);
    }

    @Deprecated
    public QueryServicesOptions setMutateBatchSize(int mutateBatchSize) {
        return set(MUTATE_BATCH_SIZE_ATTRIB, mutateBatchSize);
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

    @Deprecated
    public int getMutateBatchSize() {
        return config.getInt(MUTATE_BATCH_SIZE_ATTRIB, DEFAULT_MUTATE_BATCH_SIZE);
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
    
    public boolean isGlobalMetricsEnabled() {
        return config.getBoolean(GLOBAL_METRICS_ENABLED, DEFAULT_IS_GLOBAL_METRICS_ENABLED);
    }

    public boolean isUseByteBasedRegex() {
        return config.getBoolean(USE_BYTE_BASED_REGEX_ATTRIB, DEFAULT_USE_BYTE_BASED_REGEX);
    }

    public int getScanCacheSize() {
        return config.getInt(SCAN_CACHE_SIZE_ATTRIB, DEFAULT_SCAN_CACHE_SIZE);
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
        return set(HBASE_CLIENT_SCANNER_TIMEOUT_ATTRIB, period);
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

    public QueryServicesOptions setStatsHistogramDepthBytes(long byteDepth) {
        return set(STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, byteDepth);
    }

    public QueryServicesOptions setStatsUpdateFrequencyMs(int frequencyMs) {
        return set(STATS_UPDATE_FREQ_MS_ATTRIB, frequencyMs);
    }

    public QueryServicesOptions setMinStatsUpdateFrequencyMs(int frequencyMs) {
        return set(MIN_STATS_UPDATE_FREQ_MS_ATTRIB, frequencyMs);
    }

    public QueryServicesOptions setSequenceSaltBuckets(int saltBuckets) {
        config.setInt(SEQUENCE_SALT_BUCKETS_ATTRIB, saltBuckets);
        return this;
    }

    public QueryServicesOptions setExplainChunkCount(boolean showChunkCount) {
        config.setBoolean(EXPLAIN_CHUNK_COUNT_ATTRIB, showChunkCount);
        return this;
    }

    public QueryServicesOptions setTransactionsEnabled(boolean transactionsEnabled) {
        config.setBoolean(TRANSACTIONS_ENABLED, transactionsEnabled);
        return this;
    }

    public QueryServicesOptions setExplainRowCount(boolean showRowCount) {
        config.setBoolean(EXPLAIN_ROW_COUNT_ATTRIB, showRowCount);
        return this;
    }

    public QueryServicesOptions setAllowOnlineSchemaUpdate(boolean allow) {
        config.setBoolean(ALLOW_ONLINE_TABLE_SCHEMA_UPDATE, allow);
        return this;
    }

    public QueryServicesOptions setNumRetriesForSchemaChangeCheck(int numRetries) {
        config.setInt(NUM_RETRIES_FOR_SCHEMA_UPDATE_CHECK, numRetries);
        return this;
    }

    public QueryServicesOptions setDelayInMillisForSchemaChangeCheck(long delayInMillis) {
        config.setLong(DELAY_FOR_SCHEMA_UPDATE_CHECK, delayInMillis);
        return this;
    
    }
    
    public QueryServicesOptions setUseByteBasedRegex(boolean flag) {
        config.setBoolean(USE_BYTE_BASED_REGEX_ATTRIB, flag);
        return this;
    }

    public QueryServicesOptions setForceRowKeyOrder(boolean forceRowKeyOrder) {
        config.setBoolean(FORCE_ROW_KEY_ORDER_ATTRIB, forceRowKeyOrder);
        return this;
    }
    
    public QueryServicesOptions setExtraJDBCArguments(String extraArgs) {
        config.set(EXTRA_JDBC_ARGUMENTS_ATTRIB, extraArgs);
        return this;
    }

    public QueryServicesOptions setRunUpdateStatsAsync(boolean flag) {
        config.setBoolean(RUN_UPDATE_STATS_ASYNC, flag);
        return this;
    }

    public QueryServicesOptions setCommitStatsAsync(boolean flag) {
        config.setBoolean(COMMIT_STATS_ASYNC, flag);
        return this;
    }
    
    public QueryServicesOptions setEnableRenewLease(boolean enable) {
        config.setBoolean(RENEW_LEASE_ENABLED, enable);
        return this;
    }
    
    public QueryServicesOptions setIndexHandlerCount(int count) {
        config.setInt(QueryServices.INDEX_HANDLER_COUNT_ATTRIB, count);
        return this;
    }
    
    public QueryServicesOptions setMetadataHandlerCount(int count) {
        config.setInt(QueryServices.METADATA_HANDLER_COUNT_ATTRIB, count);
        return this;
    }
    
    public QueryServicesOptions setHConnectionPoolCoreSize(int count) {
        config.setInt(QueryServices.HCONNECTION_POOL_CORE_SIZE, count);
        return this;
    }
    
    public QueryServicesOptions setHConnectionPoolMaxSize(int count) {
        config.setInt(QueryServices.HCONNECTION_POOL_MAX_SIZE, count);
        return this;
    }
    
    public QueryServicesOptions setMaxThreadsPerHTable(int count) {
        config.setInt(QueryServices.HTABLE_MAX_THREADS, count);
        return this;
    }
    
    public QueryServicesOptions setDefaultIndexPopulationWaitTime(long waitTime) {
        config.setLong(INDEX_POPULATION_SLEEP_TIME, waitTime);
        return this;
    }
    
}
