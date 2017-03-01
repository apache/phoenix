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

import java.util.concurrent.ThreadPoolExecutor;

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
    public static final String HBASE_CLIENT_KEYTAB = "hbase.myclient.keytab";
    public static final String HBASE_CLIENT_PRINCIPAL = "hbase.myclient.principal";
    public static final String SPOOL_DIRECTORY = "phoenix.spool.directory";
    public static final String AUTO_COMMIT_ATTRIB = "phoenix.connection.autoCommit";
    // consistency configuration setting
    public static final String CONSISTENCY_ATTRIB = "phoenix.connection.consistency";
    public static final String SCHEMA_ATTRIB = "phoenix.connection.schema";
    public static final String IS_NAMESPACE_MAPPING_ENABLED  = "phoenix.schema.isNamespaceMappingEnabled";
    public static final String IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE  = "phoenix.schema.mapSystemTablesToNamespace";
    // joni byte regex engine setting
    public static final String USE_BYTE_BASED_REGEX_ATTRIB = "phoenix.regex.byteBased";
    public static final String DRIVER_SHUTDOWN_TIMEOUT_MS = "phoenix.shutdown.timeoutMs";

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

    /**
     * Number of records to read per chunk when streaming records of a basic scan.
     */
    public static final String SCAN_RESULT_CHUNK_SIZE = "phoenix.query.scanResultChunkSize";

    public static final String MAX_MEMORY_PERC_ATTRIB = "phoenix.query.maxGlobalMemoryPercentage";
    public static final String MAX_MEMORY_WAIT_MS_ATTRIB = "phoenix.query.maxGlobalMemoryWaitMs";
    public static final String MAX_TENANT_MEMORY_PERC_ATTRIB = "phoenix.query.maxTenantMemoryPercentage";
    public static final String MAX_SERVER_CACHE_SIZE_ATTRIB = "phoenix.query.maxServerCacheBytes";
    public static final String DATE_FORMAT_TIMEZONE_ATTRIB = "phoenix.query.dateFormatTimeZone";
    public static final String DATE_FORMAT_ATTRIB = "phoenix.query.dateFormat";
    public static final String TIME_FORMAT_ATTRIB = "phoenix.query.timeFormat";
    public static final String TIMESTAMP_FORMAT_ATTRIB = "phoenix.query.timestampFormat";

    public static final String NUMBER_FORMAT_ATTRIB = "phoenix.query.numberFormat";
    public static final String CALL_QUEUE_ROUND_ROBIN_ATTRIB = "ipc.server.callqueue.roundrobin";
    public static final String SCAN_CACHE_SIZE_ATTRIB = "hbase.client.scanner.caching";
    public static final String MAX_MUTATION_SIZE_ATTRIB = "phoenix.mutate.maxSize";

    @Deprecated //USE MUTATE_BATCH_SIZE_BYTES_ATTRIB instead
    public static final String MUTATE_BATCH_SIZE_ATTRIB = "phoenix.mutate.batchSize";
    public static final String MUTATE_BATCH_SIZE_BYTES_ATTRIB = "phoenix.mutate.batchSizeBytes";
    public static final String MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB = "phoenix.coprocessor.maxServerCacheTimeToLiveMs";
    
    @Deprecated // Use FORCE_ROW_KEY_ORDER instead.
    public static final String ROW_KEY_ORDER_SALTED_TABLE_ATTRIB  = "phoenix.query.rowKeyOrderSaltedTable";
    
    public static final String USE_INDEXES_ATTRIB  = "phoenix.query.useIndexes";
    @Deprecated // use the IMMUTABLE keyword while creating the table
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
    public static final String HBASE_CLIENT_SCANNER_TIMEOUT_ATTRIB = "hbase.client.scanner.timeout.period";
    public static final String RPC_TIMEOUT_ATTRIB = "hbase.rpc.timeout";
    public static final String DYNAMIC_JARS_DIR_KEY = "hbase.dynamic.jars.dir";
    public static final String ZOOKEEPER_QUORUM_ATTRIB = "hbase.zookeeper.quorum";
    public static final String ZOOKEEPER_PORT_ATTRIB = "hbase.zookeeper.property.clientPort";
    public static final String ZOOKEEPER_ROOT_NODE_ATTRIB = "zookeeper.znode.parent";
    public static final String DISTINCT_VALUE_COMPRESS_THRESHOLD_ATTRIB = "phoenix.distinct.value.compress.threshold";
    public static final String SEQUENCE_CACHE_SIZE_ATTRIB = "phoenix.sequence.cacheSize";
    public static final String MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB = "phoenix.coprocessor.maxMetaDataCacheTimeToLiveMs";
    public static final String MAX_SERVER_METADATA_CACHE_SIZE_ATTRIB = "phoenix.coprocessor.maxMetaDataCacheSize";
    public static final String MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB = "phoenix.client.maxMetaDataCacheSize";

    public static final String AUTO_UPGRADE_WHITELIST_ATTRIB = "phoenix.client.autoUpgradeWhiteList";
    // Mainly for testing to force spilling
    public static final String MAX_MEMORY_SIZE_ATTRIB = "phoenix.query.maxGlobalMemorySize";

    // The following config settings is to deal with SYSTEM.CATALOG moves(PHOENIX-916) among region servers
    public static final String CLOCK_SKEW_INTERVAL_ATTRIB = "phoenix.clock.skew.interval";

    // A master switch if to enable auto rebuild an index which failed to be updated previously
    public static final String INDEX_FAILURE_HANDLING_REBUILD_ATTRIB = "phoenix.index.failure.handling.rebuild";
    public static final String INDEX_FAILURE_HANDLING_REBUILD_PERIOD = "phoenix.index.failure.handling.rebuild.period";

    // Time interval to check if there is an index needs to be rebuild
    public static final String INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB =
        "phoenix.index.failure.handling.rebuild.interval";
    
    public static final String INDEX_FAILURE_HANDLING_REBUILD_NUMBER_OF_BATCHES_PER_TABLE = "phoenix.index.rebuild.batch.perTable";

    // A master switch if to block writes when index build failed
    public static final String INDEX_FAILURE_BLOCK_WRITE = "phoenix.index.failure.block.write";

    // Index will be partially re-built from index disable time stamp - following overlap time
    public static final String INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB =
        "phoenix.index.failure.handling.rebuild.overlap.time";
    public static final String INDEX_PRIOIRTY_ATTRIB = "phoenix.index.rpc.priority";
    public static final String METADATA_PRIOIRTY_ATTRIB = "phoenix.metadata.rpc.priority";
    public static final String ALLOW_LOCAL_INDEX_ATTRIB = "phoenix.index.allowLocalIndex";

    // Config parameters for for configuring tracing
    public static final String TRACING_FREQ_ATTRIB = "phoenix.trace.frequency";
    public static final String TRACING_PAGE_SIZE_ATTRIB = "phoenix.trace.read.pagesize";
    public static final String TRACING_PROBABILITY_THRESHOLD_ATTRIB = "phoenix.trace.probability.threshold";
    public static final String TRACING_STATS_TABLE_NAME_ATTRIB = "phoenix.trace.statsTableName";
    public static final String TRACING_CUSTOM_ANNOTATION_ATTRIB_PREFIX = "phoenix.trace.custom.annotation.";

    public static final String USE_REVERSE_SCAN_ATTRIB = "phoenix.query.useReverseScan";

    // Config parameters for stats collection
    public static final String STATS_UPDATE_FREQ_MS_ATTRIB = "phoenix.stats.updateFrequency";
    public static final String MIN_STATS_UPDATE_FREQ_MS_ATTRIB = "phoenix.stats.minUpdateFrequency";
    public static final String STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB = "phoenix.stats.guidepost.width";
    public static final String STATS_GUIDEPOST_PER_REGION_ATTRIB = "phoenix.stats.guidepost.per.region";
    public static final String STATS_USE_CURRENT_TIME_ATTRIB = "phoenix.stats.useCurrentTime";
    public static final String STATS_ENABLED_ATTRIB = "phoenix.stats.enabled";
    public static final String RUN_UPDATE_STATS_ASYNC = "phoenix.update.stats.command.async";
    public static final String STATS_SERVER_POOL_SIZE = "phoenix.stats.pool.size";
    public static final String COMMIT_STATS_ASYNC = "phoenix.stats.commit.async";
    // Maximum size in bytes taken up by cached table stats in the client
    public static final String STATS_MAX_CACHE_SIZE = "phoenix.stats.cache.maxSize";

    public static final String SEQUENCE_SALT_BUCKETS_ATTRIB = "phoenix.sequence.saltBuckets";
    public static final String COPROCESSOR_PRIORITY_ATTRIB = "phoenix.coprocessor.priority";
    public static final String EXPLAIN_CHUNK_COUNT_ATTRIB = "phoenix.explain.displayChunkCount";
    public static final String EXPLAIN_ROW_COUNT_ATTRIB = "phoenix.explain.displayRowCount";
    public static final String ALLOW_ONLINE_TABLE_SCHEMA_UPDATE = "hbase.online.schema.update.enable";
    public static final String NUM_RETRIES_FOR_SCHEMA_UPDATE_CHECK = "phoenix.schema.change.retries";
    public static final String DELAY_FOR_SCHEMA_UPDATE_CHECK = "phoenix.schema.change.delay";
    public static final String DEFAULT_KEEP_DELETED_CELLS_ATTRIB = "phoenix.table.default.keep.deleted.cells";
    public static final String DEFAULT_STORE_NULLS_ATTRIB = "phoenix.table.default.store.nulls";
    public static final String DEFAULT_TABLE_ISTRANSACTIONAL_ATTRIB = "phoenix.table.istransactional.default";
    public static final String GLOBAL_METRICS_ENABLED = "phoenix.query.global.metrics.enabled";
    
    // Transaction related configs
    public static final String TRANSACTIONS_ENABLED = "phoenix.transactions.enabled";
    // Controls whether or not uncommitted data is automatically sent to HBase
    // at the end of a statement execution when transaction state is passed through.
    public static final String AUTO_FLUSH_ATTRIB = "phoenix.transactions.autoFlush";

    // rpc queue configs
    public static final String INDEX_HANDLER_COUNT_ATTRIB = "phoenix.rpc.index.handler.count";
    public static final String METADATA_HANDLER_COUNT_ATTRIB = "phoenix.rpc.metadata.handler.count";
    
    public static final String FORCE_ROW_KEY_ORDER_ATTRIB = "phoenix.query.force.rowkeyorder";
    public static final String ALLOW_USER_DEFINED_FUNCTIONS_ATTRIB = "phoenix.functions.allowUserDefinedFunctions";
    public static final String COLLECT_REQUEST_LEVEL_METRICS = "phoenix.query.request.metrics.enabled";
    public static final String ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE = "phoenix.view.allowNewColumnFamily";
    public static final String RETURN_SEQUENCE_VALUES_ATTRIB = "phoenix.sequence.returnValues";
    public static final String EXTRA_JDBC_ARGUMENTS_ATTRIB = "phoenix.jdbc.extra.arguments";
    
    public static final String MAX_VERSIONS_TRANSACTIONAL_ATTRIB = "phoenix.transactions.maxVersions";

    // queryserver configuration keys
    public static final String QUERY_SERVER_SERIALIZATION_ATTRIB = "phoenix.queryserver.serialization";
    public static final String QUERY_SERVER_META_FACTORY_ATTRIB = "phoenix.queryserver.metafactory.class";
    public static final String QUERY_SERVER_HTTP_PORT_ATTRIB = "phoenix.queryserver.http.port";
    public static final String QUERY_SERVER_ENV_LOGGING_ATTRIB = "phoenix.queryserver.envvars.logging.disabled";
    public static final String QUERY_SERVER_ENV_LOGGING_SKIPWORDS_ATTRIB = "phoenix.queryserver.envvars.logging.skipwords";
    public static final String QUERY_SERVER_KEYTAB_FILENAME_ATTRIB = "phoenix.queryserver.keytab.file";
    public static final String QUERY_SERVER_KERBEROS_PRINCIPAL_ATTRIB = "phoenix.queryserver.kerberos.principal";
    public static final String QUERY_SERVER_DNS_NAMESERVER_ATTRIB = "phoenix.queryserver.dns.nameserver";
    public static final String QUERY_SERVER_DNS_INTERFACE_ATTRIB = "phoenix.queryserver.dns.interface";
    public static final String QUERY_SERVER_HBASE_SECURITY_CONF_ATTRIB = "hbase.security.authentication";
    public static final String QUERY_SERVER_UGI_CACHE_MAX_SIZE = "phoenix.queryserver.ugi.cache.max.size";
    public static final String QUERY_SERVER_UGI_CACHE_INITIAL_SIZE = "phoenix.queryserver.ugi.cache.initial.size";
    public static final String QUERY_SERVER_UGI_CACHE_CONCURRENCY = "phoenix.queryserver.ugi.cache.concurrency";
    public static final String QUERY_SERVER_KERBEROS_ALLOWED_REALMS = "phoenix.queryserver.kerberos.allowed.realms";
    public static final String QUERY_SERVER_SPNEGO_AUTH_DISABLED_ATTRIB = "phoenix.queryserver.spnego.auth.disabled";

    public static final String RENEW_LEASE_ENABLED = "phoenix.scanner.lease.renew.enabled";
    public static final String RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS = "phoenix.scanner.lease.renew.interval";
    public static final String RENEW_LEASE_THRESHOLD_MILLISECONDS = "phoenix.scanner.lease.threshold";
    public static final String RENEW_LEASE_THREAD_POOL_SIZE = "phoenix.scanner.lease.pool.size";
    public static final String HCONNECTION_POOL_CORE_SIZE = "hbase.hconnection.threads.core";
    public static final String HCONNECTION_POOL_MAX_SIZE = "hbase.hconnection.threads.max";
    public static final String HTABLE_MAX_THREADS = "hbase.htable.threads.max";
    // time to wait before running second index population upsert select (so that any pending batches of rows on region server are also written to index)
    public static final String INDEX_POPULATION_SLEEP_TIME = "phoenix.index.population.wait.time";
    public static final String LOCAL_INDEX_CLIENT_UPGRADE_ATTRIB = "phoenix.client.localIndexUpgrade";
    public static final String LIMITED_QUERY_SERIAL_THRESHOLD = "phoenix.limited.query.serial.threshold";
    
    //currently BASE64 and ASCII is supported
    public static final String UPLOAD_BINARY_DATA_TYPE_ENCODING = "phoenix.upload.binaryDataType.encoding";

    public static final String INDEX_ASYNC_BUILD_ENABLED = "phoenix.index.async.build.enabled";
    
    public static final String CLIENT_CACHE_ENCODING = "phoenix.table.client.cache.encoding";
    public static final String AUTO_UPGRADE_ENABLED = "phoenix.autoupgrade.enabled";

    public static final String CLIENT_CONNECTION_CACHE_MAX_DURATION_MILLISECONDS =
        "phoenix.client.connection.max.duration";

    //max number of connections from a single client to a single cluster. 0 is unlimited.
    public static final String CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS =
        "phoenix.client.connection.max.allowed.connections";
    public static final String DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB  = "phoenix.default.column.encoded.bytes.attrib";
    public static final String DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB  = "phoenix.default.immutable.storage.scheme";
    public static final String DEFAULT_MULTITENANT_IMMUTABLE_STORAGE_SCHEME_ATTRIB  = "phoenix.default.multitenant.immutable.storage.scheme";

    /**
     * Get executor service used for parallel scans
     */
    public ThreadPoolExecutor getExecutor();
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
