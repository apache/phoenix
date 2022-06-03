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

import org.apache.phoenix.iterate.SpoolTooBigToDiskException;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseable;

import net.jcip.annotations.Immutable;



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
    public static final String SERVER_SPOOL_THRESHOLD_BYTES_ATTRIB =
            "phoenix.query.server.spoolThresholdBytes";
    public static final String CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB =
            "phoenix.query.client.spoolThresholdBytes";
    public static final String CLIENT_ORDERBY_SPOOLING_ENABLED_ATTRIB =
            "phoenix.query.client.orderBy.spooling.enabled";
    public static final String CLIENT_JOIN_SPOOLING_ENABLED_ATTRIB =
            "phoenix.query.client.join.spooling.enabled";
    public static final String SERVER_ORDERBY_SPOOLING_ENABLED_ATTRIB =
            "phoenix.query.server.orderBy.spooling.enabled";
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
    public static final String CLIENT_INDEX_ASYNC_THRESHOLD = "phoenix.index.async.threshold";

    /**
	 * max size to spool the the result into
	 * ${java.io.tmpdir}/ResultSpoolerXXX.bin if
	 * QueryServices#SPOOL_THRESHOLD_BYTES_ATTRIB is reached.
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
    public static final String MAX_MUTATION_SIZE_BYTES_ATTRIB = "phoenix.mutate.maxSizeBytes";
    public static final String HBASE_CLIENT_KEYVALUE_MAXSIZE = "hbase.client.keyvalue.maxsize";

    public static final String MUTATE_BATCH_SIZE_ATTRIB = "phoenix.mutate.batchSize";
    public static final String MUTATE_BATCH_SIZE_BYTES_ATTRIB = "phoenix.mutate.batchSizeBytes";
    public static final String MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB = "phoenix.coprocessor.maxServerCacheTimeToLiveMs";
    public static final String MAX_SERVER_CACHE_PERSISTENCE_TIME_TO_LIVE_MS_ATTRIB = "phoenix.coprocessor.maxServerCachePersistenceTimeToLiveMs";
    
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
    public static final String AGGREGATE_CHUNK_SIZE_INCREASE_ATTRIB = "phoenix.aggregate.chunk_size_increase";

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
    public static final String INDEX_REBUILD_QUERY_TIMEOUT_ATTRIB = "phoenix.index.rebuild.query.timeout";
    public static final String INDEX_REBUILD_RPC_TIMEOUT_ATTRIB = "phoenix.index.rebuild.rpc.timeout";
    public static final String INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT_ATTRIB = "phoenix.index.rebuild.client.scanner.timeout";
    public static final String INDEX_REBUILD_RPC_RETRIES_COUNTER = "phoenix.index.rebuild.rpc.retries.counter";
    public static final String INDEX_REBUILD_RPC_RETRY_PAUSE_TIME = "phoenix.index.rebuild.rpc.retry.pause";

    // Time interval to check if there is an index needs to be rebuild
    public static final String INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB =
        "phoenix.index.failure.handling.rebuild.interval";
    public static final String INDEX_REBUILD_TASK_INITIAL_DELAY = "phoenix.index.rebuild.task.initial.delay";
    
    public static final String INDEX_FAILURE_HANDLING_REBUILD_NUMBER_OF_BATCHES_PER_TABLE = "phoenix.index.rebuild.batch.perTable";
    // If index disable timestamp is older than this threshold, then index rebuild task won't attempt to rebuild it
    public static final String INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD = "phoenix.index.rebuild.disabletimestamp.threshold";
    // threshold number of ms an index has been in PENDING_DISABLE, beyond which we consider it disabled
    public static final String INDEX_PENDING_DISABLE_THRESHOLD = "phoenix.index.pending.disable.threshold";

    // Block writes to data table when index write fails
    public static final String INDEX_FAILURE_BLOCK_WRITE = "phoenix.index.failure.block.write";
    public static final String INDEX_FAILURE_DISABLE_INDEX = "phoenix.index.failure.disable.index";
    public static final String INDEX_FAILURE_THROW_EXCEPTION_ATTRIB = "phoenix.index.failure.throw.exception";
    public static final String INDEX_FAILURE_KILL_SERVER = "phoenix.index.failure.unhandled.killserver";

    // Index will be partially re-built from index disable time stamp - following overlap time
    @Deprecated
    public static final String INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB =
        "phoenix.index.failure.handling.rebuild.overlap.time";
    public static final String INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_BACKWARD_TIME_ATTRIB =
            "phoenix.index.failure.handling.rebuild.overlap.backward.time";
    public static final String INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB =
            "phoenix.index.failure.handling.rebuild.overlap.forward.time";
    public static final String INDEX_PRIOIRTY_ATTRIB = "phoenix.index.rpc.priority";
    public static final String METADATA_PRIOIRTY_ATTRIB = "phoenix.metadata.rpc.priority";
    public static final String ALLOW_LOCAL_INDEX_ATTRIB = "phoenix.index.allowLocalIndex";

    // Retries when doing server side writes to SYSTEM.CATALOG
    public static final String METADATA_WRITE_RETRIES_NUMBER = "phoenix.metadata.rpc.retries.number";
    public static final String METADATA_WRITE_RETRY_PAUSE = "phoenix.metadata.rpc.pause";

    // Config parameters for for configuring tracing
    public static final String TRACING_FREQ_ATTRIB = "phoenix.trace.frequency";
    public static final String TRACING_PAGE_SIZE_ATTRIB = "phoenix.trace.read.pagesize";
    public static final String TRACING_PROBABILITY_THRESHOLD_ATTRIB = "phoenix.trace.probability.threshold";
    public static final String TRACING_STATS_TABLE_NAME_ATTRIB = "phoenix.trace.statsTableName";
    public static final String TRACING_CUSTOM_ANNOTATION_ATTRIB_PREFIX = "phoenix.trace.custom.annotation.";
    public static final String TRACING_ENABLED = "phoenix.trace.enabled";
    public static final String TRACING_BATCH_SIZE = "phoenix.trace.batchSize";
    public static final String TRACING_THREAD_POOL_SIZE = "phoenix.trace.threadPoolSize";
    public static final String TRACING_TRACE_BUFFER_SIZE = "phoenix.trace.traceBufferSize";

    public static final String USE_REVERSE_SCAN_ATTRIB = "phoenix.query.useReverseScan";

    // Config parameters for stats collection
    public static final String STATS_UPDATE_FREQ_MS_ATTRIB = "phoenix.stats.updateFrequency";
    public static final String MIN_STATS_UPDATE_FREQ_MS_ATTRIB = "phoenix.stats.minUpdateFrequency";
    public static final String STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB = "phoenix.stats.guidepost.width";
    public static final String STATS_GUIDEPOST_PER_REGION_ATTRIB = "phoenix.stats.guidepost.per.region";
    public static final String STATS_USE_CURRENT_TIME_ATTRIB = "phoenix.stats.useCurrentTime";
    
    public static final String RUN_UPDATE_STATS_ASYNC = "phoenix.update.stats.command.async";
    public static final String STATS_SERVER_POOL_SIZE = "phoenix.stats.pool.size";
    public static final String COMMIT_STATS_ASYNC = "phoenix.stats.commit.async";
    // Maximum size in bytes taken up by cached table stats in the client
    public static final String STATS_MAX_CACHE_SIZE = "phoenix.stats.cache.maxSize";
    // The size of the thread pool used for refreshing cached table stats in stats client cache
    public static final String STATS_CACHE_THREAD_POOL_SIZE = "phoenix.stats.cache.threadPoolSize";

    public static final String LOG_SALT_BUCKETS_ATTRIB = "phoenix.log.saltBuckets";
    public static final String SEQUENCE_SALT_BUCKETS_ATTRIB = "phoenix.sequence.saltBuckets";
    public static final String COPROCESSOR_PRIORITY_ATTRIB = "phoenix.coprocessor.priority";
    public static final String EXPLAIN_CHUNK_COUNT_ATTRIB = "phoenix.explain.displayChunkCount";
    public static final String EXPLAIN_ROW_COUNT_ATTRIB = "phoenix.explain.displayRowCount";
    public static final String ALLOW_ONLINE_TABLE_SCHEMA_UPDATE = "hbase.online.schema.update.enable";
    public static final String NUM_RETRIES_FOR_SCHEMA_UPDATE_CHECK = "phoenix.schema.change.retries";
    public static final String DELAY_FOR_SCHEMA_UPDATE_CHECK = "phoenix.schema.change.delay";
    public static final String DEFAULT_STORE_NULLS_ATTRIB = "phoenix.table.default.store.nulls";
    public static final String DEFAULT_TABLE_ISTRANSACTIONAL_ATTRIB = "phoenix.table.istransactional.default";
    public static final String DEFAULT_TRANSACTION_PROVIDER_ATTRIB = "phoenix.table.transaction.provider.default";
    public static final String GLOBAL_METRICS_ENABLED = "phoenix.query.global.metrics.enabled";

    // Tag Name to determine the Phoenix Client Type
    public static final String CLIENT_METRICS_TAG = "phoenix.client.metrics.tag";
    
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

    // metadata configs
    public static final String DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB = "phoenix.system.default.keep.deleted.cells";
    public static final String DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB = "phoenix.system.default.max.versions";

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
    // Toggle for server-written updates to SYSTEM.CATALOG
    public static final String PHOENIX_ACLS_ENABLED = "phoenix.acls.enabled";

    public static final String INDEX_ASYNC_BUILD_ENABLED = "phoenix.index.async.build.enabled";

    public static final String MAX_INDEXES_PER_TABLE = "phoenix.index.maxIndexesPerTable";

    public static final String CLIENT_CACHE_ENCODING = "phoenix.table.client.cache.encoding";
    public static final String AUTO_UPGRADE_ENABLED = "phoenix.autoupgrade.enabled";

    public static final String CLIENT_CONNECTION_CACHE_MAX_DURATION_MILLISECONDS =
        "phoenix.client.connection.max.duration";

    //max number of connections from a single client to a single cluster. 0 is unlimited.
    public static final String CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS =
        "phoenix.client.connection.max.allowed.connections";
    //max number of connections from a single client to a single cluster. 0 is unlimited.
    public static final String INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS =
            "phoenix.internal.connection.max.allowed.connections";
    public static final String DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB  = "phoenix.default.column.encoded.bytes.attrib";
    public static final String DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB  = "phoenix.default.immutable.storage.scheme";
    public static final String DEFAULT_MULTITENANT_IMMUTABLE_STORAGE_SCHEME_ATTRIB  = "phoenix.default.multitenant.immutable.storage.scheme";


    public static final String STATS_COLLECTION_ENABLED = "phoenix.stats.collection.enabled";
    public static final String USE_STATS_FOR_PARALLELIZATION = "phoenix.use.stats.parallelization";

    // whether to enable server side RS -> RS calls for upsert select statements
    public static final String ENABLE_SERVER_UPSERT_SELECT ="phoenix.client.enable.server.upsert.select";

    public static final String PROPERTY_POLICY_PROVIDER_ENABLED = "phoenix.property.policy.provider.enabled";

    // whether to trigger mutations on the server at all (UPSERT/DELETE or DELETE FROM)
    public static final String ENABLE_SERVER_SIDE_DELETE_MUTATIONS ="phoenix.client.enable.server.delete.mutations";
    public static final String ENABLE_SERVER_SIDE_UPSERT_MUTATIONS ="phoenix.client.enable.server.upsert.mutations";

    //Update Cache Frequency default config attribute
    public static final String DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB  = "phoenix.default.update.cache.frequency";

    // Whether to enable cost-based-decision in the query optimizer
    public static final String COST_BASED_OPTIMIZER_ENABLED = "phoenix.costbased.optimizer.enabled";
    public static final String SMALL_SCAN_THRESHOLD_ATTRIB = "phoenix.query.smallScanThreshold";
    public static final String WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB =
            "phoenix.query.wildcard.dynamicColumns";
    public static final String LOG_LEVEL = "phoenix.log.level";
    public static final String AUDIT_LOG_LEVEL = "phoenix.audit.log.level";
    public static final String LOG_BUFFER_SIZE = "phoenix.log.buffer.size";
    public static final String LOG_BUFFER_WAIT_STRATEGY = "phoenix.log.wait.strategy";
    public static final String LOG_SAMPLE_RATE = "phoenix.log.sample.rate";
    public static final String LOG_HANDLER_COUNT = "phoenix.log.handler.count";

	public static final String SYSTEM_CATALOG_SPLITTABLE = "phoenix.system.catalog.splittable";

    // The parameters defined for handling task stored in table SYSTEM.TASK
	// The time interval between periodic scans of table SYSTEM.TASK
    public static final String TASK_HANDLING_INTERVAL_MS_ATTRIB = "phoenix.task.handling.interval.ms";
    // The maximum time for a task to stay in table SYSTEM.TASK
    public static final String TASK_HANDLING_MAX_INTERVAL_MS_ATTRIB = "phoenix.task.handling.maxInterval.ms";
    // The initial delay before the first task from table SYSTEM.TASK is handled
    public static final String TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB = "phoenix.task.handling.initial.delay.ms";
    // The minimum age of an unverified global index row to be eligible for deletion
    public static final String GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB = "phoenix.global.index.row.age.threshold.to.delete.ms";
    // Enable the IndexRegionObserver Coprocessor
    public static final String INDEX_REGION_OBSERVER_ENABLED_ATTRIB = "phoenix.index.region.observer.enabled";
    // Enable Phoenix server paging
    public static final String PHOENIX_SERVER_PAGING_ENABLED_ATTRIB = "phoenix.server.paging.enabled";
    // Enable support for long view index(default is false)
    public static final String LONG_VIEW_INDEX_ENABLED_ATTRIB = "phoenix.index.longViewIndex.enabled";
    // The number of index rows to be rebuild in one RPC call
    public static final String INDEX_REBUILD_PAGE_SIZE_IN_ROWS = "phoenix.index.rebuild_page_size_in_rows";
    // The number of index rows to be scanned in one RPC call
    String INDEX_PAGE_SIZE_IN_ROWS = "phoenix.index.page_size_in_rows";
    // Flag indicating that server side masking of ttl expired rows is enabled.
    public static final String PHOENIX_TTL_SERVER_SIDE_MASKING_ENABLED = "phoenix.ttl.server_side.masking.enabled";
    // The time limit on the amount of work to be done in one RPC call
    public static final String PHOENIX_SERVER_PAGE_SIZE_MS = "phoenix.server.page.size.ms";


    // Before 4.15 when we created a view we included the parent table column metadata in the view
    // metadata. After PHOENIX-3534 we allow SYSTEM.CATALOG to split and no longer store the parent
    // table column metadata along with the child view metadata. When we resolve a child view, we
    // resolve its ancestors and include their columns.
    // Also, before 4.15 when we added a column to a base table we would have to propagate the
    // column metadata to all its child views. After PHOENIX-3534 we no longer propagate metadata
    // changes from a parent to its children (we just resolve its ancestors and include their columns)
    // 
    // The following config is used to continue writing the parent table column metadata while
    // creating a view and also prevent metadata changes to a parent table/view that needs to be
    // propagated to its children. This is done to allow rollback of the splittable SYSTEM.CATALOG
    // feature
    //
    // By default this config is false meaning that rolling back the upgrade is not possible
    // If this config is true and you want to rollback the upgrade be sure to run the sql commands in
    // UpgradeUtil.addParentToChildLink which will recreate the PARENT->CHILD links in SYSTEM.CATALOG. This is needed
    // as from 4.15 onwards the PARENT->CHILD links are stored in a separate SYSTEM.CHILD_LINK table.
    public static final String ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK =
            "phoenix.allow.system.catalog.rollback";

    // Phoenix parameter used to indicate what implementation is used for providing the client
    // stats guide post cache.
    // QueryServicesOptions.DEFAULT_GUIDE_POSTS_CACHE_FACTORY_CLASS is used if this is not provided
    public static final String GUIDE_POSTS_CACHE_FACTORY_CLASS = "phoenix.guide.posts.cache.factory.class";

    public static final String PENDING_MUTATIONS_DDL_THROW_ATTRIB = "phoenix.pending.mutations.before.ddl.throw";

    /**
     * Parameter to indicate the source of operation attribute.
     * It can include metadata about the customer, service, etc.
     */
    String SOURCE_OPERATION_ATTRIB = "phoenix.source.operation";

    // The max point keys that can be generated for large in list clause
    public static final String MAX_IN_LIST_SKIP_SCAN_SIZE = "phoenix.max.inList.skipScan.size";

    /**
     * Parameter to skip the system tables existence check to avoid unnecessary calls to
     * Region server holding the SYSTEM.CATALOG table in batch oriented jobs.
     */
    String SKIP_SYSTEM_TABLES_EXISTENCE_CHECK = "phoenix.skip.system.tables.existence.check";
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
