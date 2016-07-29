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

import static org.apache.phoenix.query.QueryConstants.ASYNC_INDEX_INFO_QUERY;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpgradeUtil;

import com.google.common.collect.Lists;


/**
 * Coprocessor for metadata related operations. This coprocessor would only be registered
 * to SYSTEM.TABLE.
 */
public class MetaDataRegionObserver extends BaseRegionObserver {
    public static final Log LOG = LogFactory.getLog(MetaDataRegionObserver.class);
    protected ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);
    private boolean enableRebuildIndex = QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD;
    private long rebuildIndexTimeInterval = QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_INTERVAL;
    private boolean autoAsyncIndexBuild = QueryServicesOptions.DEFAULT_ASYNC_INDEX_AUTO_BUILD; 
    private boolean blockWriteRebuildIndex = false;

    @Override
    public void preClose(final ObserverContext<RegionCoprocessorEnvironment> c,
            boolean abortRequested) {
        executor.shutdownNow();
        GlobalCache.getInstance(c.getEnvironment()).getMetaDataCache().invalidateAll();
    }
    
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        // sleep a little bit to compensate time clock skew when SYSTEM.CATALOG moves 
        // among region servers because we relies on server time of RS which is hosting
        // SYSTEM.CATALOG
        long sleepTime = env.getConfiguration().getLong(QueryServices.CLOCK_SKEW_INTERVAL_ATTRIB, 
            QueryServicesOptions.DEFAULT_CLOCK_SKEW_INTERVAL);
        try {
            if(sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        enableRebuildIndex = env.getConfiguration().getBoolean(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB, 
            QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD);
        rebuildIndexTimeInterval = env.getConfiguration().getLong(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB, 
            QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_INTERVAL);
        autoAsyncIndexBuild = env.getConfiguration().getBoolean(QueryServices.ASYNC_INDEX_AUTO_BUILD_ATTRIB, 
                QueryServicesOptions.DEFAULT_ASYNC_INDEX_AUTO_BUILD);
        blockWriteRebuildIndex = env.getConfiguration().getBoolean(QueryServices.INDEX_FAILURE_BLOCK_WRITE,
        	QueryServicesOptions.DEFAULT_INDEX_FAILURE_BLOCK_WRITE);
    }
    
    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        final RegionCoprocessorEnvironment env = e.getEnvironment();

        Runnable r = new Runnable() {
            @Override
            public void run() {
                HTableInterface metaTable = null;
                HTableInterface statsTable = null;
                try {
                    ReadOnlyProps props=new ReadOnlyProps(env.getConfiguration().iterator());
                    Thread.sleep(1000);
                    metaTable = env.getTable(
                            SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, props));
                    statsTable = env.getTable(
                            SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES, props));
                    if (UpgradeUtil.truncateStats(metaTable, statsTable)) {
                        LOG.info("Stats are successfully truncated for upgrade 4.7!!");
                    }
                } catch (Exception exception) {
                    LOG.warn("Exception while truncate stats..,"
                            + " please check and delete stats manually inorder to get proper result with old client!!");
                    LOG.warn(exception.getStackTrace());
                } finally {
                    try {
                        if (metaTable != null) {
                            metaTable.close();
                        }
                        if (statsTable != null) {
                            statsTable.close();
                        }
                    } catch (IOException e) {}
                }
            }
        };
        (new Thread(r)).start();

        // turn off verbose deprecation logging
        Logger deprecationLogger = Logger.getLogger("org.apache.hadoop.conf.Configuration.deprecation");
        if (deprecationLogger != null) {
            deprecationLogger.setLevel(Level.WARN);
        }

        try {
            Class.forName(PhoenixDriver.class.getName());
        } catch (ClassNotFoundException ex) {
            LOG.error("Phoenix Driver class is not found. Fix the classpath.", ex);
        }
         
        // Enable async index rebuilder when autoAsyncIndexBuild is set to true 
        if (autoAsyncIndexBuild)
        {
            LOG.info("Enabling Async Index rebuilder");
            AsyncIndexRebuilderTask asyncIndexRebuilderTask = new AsyncIndexRebuilderTask(e.getEnvironment());
            // run async index rebuilder task every 10 secs to rebuild any newly created async indexes
            executor.scheduleAtFixedRate(asyncIndexRebuilderTask, 10000, rebuildIndexTimeInterval, TimeUnit.MILLISECONDS);
        }

        if (!enableRebuildIndex && !blockWriteRebuildIndex) {
            LOG.info("Failure Index Rebuild is skipped by configuration.");
            return;
        }

        // starts index rebuild schedule work
        BuildIndexScheduleTask task = new BuildIndexScheduleTask(e.getEnvironment());
        // run scheduled task every 10 secs
        executor.scheduleAtFixedRate(task, 10000, rebuildIndexTimeInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Task runs periodically to re-build async indexes when hbase is running in non-distributed mode or 
     * when mapreduce is running in local mode
     *
     */
    public static class AsyncIndexRebuilderTask extends TimerTask {
        RegionCoprocessorEnvironment env;

        public AsyncIndexRebuilderTask(RegionCoprocessorEnvironment env) {
            this.env = env;
        }

        @Override
        public void run() {
            PhoenixConnection conn = null;
            try {
                conn = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery(ASYNC_INDEX_INFO_QUERY);
                PhoenixConnection metaDataClientConn = conn;
                while (rs.next()) {
                    String tableName = rs.getString(PhoenixDatabaseMetaData.DATA_TABLE_NAME);
                    String tableSchema = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM);
                    String indexName = rs.getString(PhoenixDatabaseMetaData.TABLE_NAME);
                    
                    final PTable indexTable = PhoenixRuntime.getTable(conn, SchemaUtil.getTableName(tableSchema, indexName));
                    final PTable dataTable = PhoenixRuntime.getTable(conn, SchemaUtil.getTableName(tableSchema, tableName));
                    // this is set to ensure index tables remains consistent post population.
                    long maxTimeRange = indexTable.getTimeStamp()+1;

                    try {
                        final Properties props = new Properties();
                        Long txnScn = null;
                        if (!indexTable.isTransactional()) {
                            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(maxTimeRange));
                            metaDataClientConn = QueryUtil.getConnectionOnServer(props, env.getConfiguration()).unwrap(PhoenixConnection.class);
                            txnScn = maxTimeRange;
                        }
                        MetaDataClient client = new MetaDataClient(conn);
                        LOG.info("Building Index " + SchemaUtil.getTableName(tableSchema, indexName));
                        client.buildIndex(indexTable, new TableRef(dataTable), txnScn);
                    } catch (Throwable t) {
                        LOG.error("AsyncIndexRebuilderTask failed while building index!", t);
                    } finally {
                        if (metaDataClientConn != null) {
                            try {
                                metaDataClientConn.close();
                            } catch (SQLException ignored) {
                                LOG.debug("AsyncIndexRebuilderTask can't close metaDataClientConn", ignored);
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.error("AsyncIndexRebuilderTask failed!", t);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ignored) {
                        LOG.debug("AsyncIndexRebuilderTask can't close connection", ignored);
                    }
                }
            }
        }
    }
    
    /**
     * Task runs periodically to build indexes whose INDEX_NEED_PARTIALLY_REBUILD is set true
     *
     */
    public static class BuildIndexScheduleTask extends TimerTask {
        // inProgress is to prevent timer from invoking a new task while previous one is still
        // running
        private final static AtomicInteger inProgress = new AtomicInteger(0);
        RegionCoprocessorEnvironment env;

        public BuildIndexScheduleTask(RegionCoprocessorEnvironment env) {
            this.env = env;
        }

        @Override
        public void run() {
            // FIXME: we should replay the data table Put, as doing a partial index build would only add
            // the new rows and not delete the previous index value. Also, we should restrict the scan
            // to only data within this region (as otherwise *every* region will be running this code
            // separately, all updating the same data.
            RegionScanner scanner = null;
            PhoenixConnection conn = null;
            if (inProgress.get() > 0) {
                LOG.debug("New ScheduledBuildIndexTask skipped as there is already one running");
                return;
            }
            try {
                inProgress.incrementAndGet();
                Scan scan = new Scan();
                SingleColumnValueFilter filter = new SingleColumnValueFilter(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES,
                    CompareFilter.CompareOp.GREATER, PLong.INSTANCE.toBytes(0L));
                filter.setFilterIfMissing(true);
                scan.setFilter(filter);
                scan.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.TABLE_NAME_BYTES);
                scan.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES);
                scan.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.INDEX_STATE_BYTES);
                scan.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES);

                PTable dataPTable = null;
                MetaDataClient client = null;
                boolean hasMore = false;
                List<Cell> results = new ArrayList<Cell>();
                List<PTable> indexesToPartiallyRebuild = Collections.emptyList();
                scanner = this.env.getRegion().getScanner(scan);
                long earliestDisableTimestamp = Long.MAX_VALUE;

                do {
                    results.clear();
                    hasMore = scanner.next(results);
                    if (results.isEmpty()) break;

                    Result r = Result.create(results);
                    byte[] disabledTimeStamp = r.getValue(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES);

                    if (disabledTimeStamp == null || disabledTimeStamp.length == 0) {
                        continue;
                    }

                    // disableTimeStamp has to be a positive value
                    long disabledTimeStampVal = PLong.INSTANCE.getCodec().decodeLong(disabledTimeStamp, 0, SortOrder.getDefault());
                    if (disabledTimeStampVal <= 0) {
                        continue;
                    }
                    if (disabledTimeStampVal < earliestDisableTimestamp) {
                        earliestDisableTimestamp = disabledTimeStampVal;
                    }

                    byte[] dataTable = r.getValue(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES);
                    byte[] indexStat = r.getValue(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.INDEX_STATE_BYTES);
                    if ((dataTable == null || dataTable.length == 0)
                            || (indexStat == null || indexStat.length == 0)) {
                        // data table name can't be empty
                        continue;
                    }

                    byte[][] rowKeyMetaData = new byte[3][];
                    SchemaUtil.getVarChars(r.getRow(), 3, rowKeyMetaData);
                    byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
                    byte[] indexTable = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];

                    // validity check
                    if (indexTable == null || indexTable.length == 0) {
                        LOG.debug("Index rebuild has been skipped for row=" + r);
                        continue;
                    }

                    if (conn == null) {
                    	final Properties props = new Properties();
                    	// Set SCN so that we don't ping server and have the upper bound set back to
                    	// the timestamp when the failure occurred.
                    	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(Long.MAX_VALUE));
                    	// don't run a second index populations upsert select 
                        props.setProperty(QueryServices.INDEX_POPULATION_SLEEP_TIME, "0"); 
                        conn = QueryUtil.getConnectionOnServer(props, env.getConfiguration()).unwrap(PhoenixConnection.class);
                        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTable);
                        dataPTable = PhoenixRuntime.getTable(conn, dataTableFullName);
                        indexesToPartiallyRebuild = Lists.newArrayListWithExpectedSize(dataPTable.getIndexes().size());
                        client = new MetaDataClient(conn);
                    }

                    String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTable);
                    PTable indexPTable = PhoenixRuntime.getTable(conn, indexTableFullName);
                    if (!MetaDataUtil.tableRegionsOnline(this.env.getConfiguration(), indexPTable)) {
                        LOG.debug("Index rebuild has been skipped because not all regions of index table="
                                + indexPTable.getName() + " are online.");
                        continue;
                    }
                    // Allow index to begin incremental maintenance as index is back online and we
                    // cannot transition directly from DISABLED -> ACTIVE
                    if (Bytes.compareTo(PIndexState.DISABLE.getSerializedBytes(), indexStat) == 0) {
                        AlterIndexStatement statement = new AlterIndexStatement(
                                NamedTableNode.create(indexPTable.getSchemaName().getString(), indexPTable.getTableName().getString()),
                                dataPTable.getTableName().getString(),
                                false, PIndexState.INACTIVE);
                        client.alterIndex(statement);
                    }
                    indexesToPartiallyRebuild.add(indexPTable);
                } while (hasMore);

                if (!indexesToPartiallyRebuild.isEmpty()) {
                    long overlapTime = env.getConfiguration().getLong(
                        QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME);
                    long timeStamp = Math.max(0, earliestDisableTimestamp - overlapTime);
                    
                    LOG.info("Starting to build indexes=" + indexesToPartiallyRebuild + " from timestamp=" + timeStamp);
                    new Scan();
                    List<IndexMaintainer> maintainers = Lists.newArrayListWithExpectedSize(indexesToPartiallyRebuild.size());
                    for (PTable index : indexesToPartiallyRebuild) {
                        maintainers.add(index.getIndexMaintainer(dataPTable, conn));
                    }
                    Scan dataTableScan = IndexManagementUtil.newLocalStateScan(maintainers);
                    dataTableScan.setTimeRange(timeStamp, HConstants.LATEST_TIMESTAMP);
                    byte[] physicalTableName = dataPTable.getPhysicalName().getBytes();
                    try (HTableInterface dataHTable = conn.getQueryServices().getTable(physicalTableName)) {
                        Result result;
                        try (ResultScanner dataTableScanner = dataHTable.getScanner(dataTableScan)) {
                            int batchSize = conn.getMutateBatchSize();
                            List<Mutation> mutations = Lists.newArrayListWithExpectedSize(batchSize);
                            ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
                            IndexMaintainer.serializeAdditional(dataPTable, indexMetaDataPtr, indexesToPartiallyRebuild, conn);
                            byte[] attribValue = ByteUtil.copyKeyBytesIfNecessary(indexMetaDataPtr);
                            byte[] uuidValue = ServerCacheClient.generateId();
        
                            while ((result = dataTableScanner.next()) != null && !result.isEmpty()) {
                                Put put = null;
                                Delete del = null;
                                for (Cell cell : result.rawCells()) {
                                    if (KeyValue.Type.codeToType(cell.getTypeByte()) == KeyValue.Type.Put) {
                                        if (put == null) {
                                            put = new Put(CellUtil.cloneRow(cell));
                                            put.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                                            put.setAttribute(PhoenixIndexCodec.INDEX_MD, attribValue);
                                            put.setAttribute(BaseScannerRegionObserver.IGNORE_NEWER_MUTATIONS, PDataType.TRUE_BYTES);
                                            mutations.add(put);
                                        }
                                        put.add(cell);
                                    } else {
                                        if (del == null) {
                                            del = new Delete(CellUtil.cloneRow(cell));
                                            del.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                                            del.setAttribute(PhoenixIndexCodec.INDEX_MD, attribValue);
                                            del.setAttribute(BaseScannerRegionObserver.IGNORE_NEWER_MUTATIONS, PDataType.TRUE_BYTES);
                                            mutations.add(del);
                                        }
                                        del.addDeleteMarker(cell);
                                    }
                                }
                                if (mutations.size() == batchSize) {
                                    dataHTable.batch(mutations);
                                    uuidValue = ServerCacheClient.generateId();
                                }
                            }
                            if (!mutations.isEmpty()) {
                                dataHTable.batch(mutations);
                            }
                        }
                    }
                    for (PTable indexPTable : indexesToPartiallyRebuild) {
                        AlterIndexStatement statement = new AlterIndexStatement(
                                NamedTableNode.create(indexPTable.getSchemaName().getString(), indexPTable.getTableName().getString()),
                                dataPTable.getTableName().getString(),
                                false, PIndexState.ACTIVE);
                        client.alterIndex(statement);
                    }
                }
            } catch (Throwable t) {
                LOG.warn("ScheduledBuildIndexTask failed!", t);
            } finally {
                inProgress.decrementAndGet();
                if (scanner != null) {
                    try {
                        scanner.close();
                    } catch (IOException ignored) {
                        LOG.debug("ScheduledBuildIndexTask can't close scanner.", ignored);
                    }
                }
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ignored) {
                        LOG.debug("ScheduledBuildIndexTask can't close connection", ignored);
                    }
                }
            }
        }
    }
}
