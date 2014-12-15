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

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.phoenix.jdbc.PhoenixDriver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;


/**
 * Coprocessor for metadata related operations. This coprocessor would only be registered
 * to SYSTEM.TABLE.
 */
public class MetaDataRegionObserver extends BaseRegionObserver {
    public static final Log LOG = LogFactory.getLog(MetaDataRegionObserver.class);
    protected ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private boolean enableRebuildIndex = QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD;
    private long rebuildIndexTimeInterval = QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_INTERVAL;
  
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
    }
    

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        if (!enableRebuildIndex) {
            LOG.info("Failure Index Rebuild is skipped by configuration.");
            return;
        }
        // turn off verbose deprecation logging
        Logger deprecationLogger = Logger.getLogger("org.apache.hadoop.conf.Configuration.deprecation");
        if (deprecationLogger != null) {
            deprecationLogger.setLevel(Level.WARN);
        }
        try {
            Class.forName(PhoenixDriver.class.getName());
            // starts index rebuild schedule work
            BuildIndexScheduleTask task = new BuildIndexScheduleTask(e.getEnvironment());
            // run scheduled task every 10 secs
            executor.scheduleAtFixedRate(task, 10000, rebuildIndexTimeInterval, TimeUnit.MILLISECONDS);
        } catch (ClassNotFoundException ex) {
            LOG.error("BuildIndexScheduleTask cannot start!", ex);
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
      
        private String getJdbcUrl() {
            String zkQuorum = this.env.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);
            String zkClientPort = this.env.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT,
                Integer.toString(HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT));
            String zkParentNode = this.env.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT,
                HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
            return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum
                + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkClientPort
                + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkParentNode;
        }
      
        public void run() {
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
                    CompareFilter.CompareOp.NOT_EQUAL, PLong.INSTANCE.toBytes(0L));
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

                boolean hasMore = false;
                List<Cell> results = new ArrayList<Cell>();
                scanner = this.env.getRegion().getScanner(scan);

                do {
                    results.clear();
                    hasMore = scanner.next(results);
                    if (results.isEmpty()) break;

                    Result r = Result.create(results);
                    byte[] disabledTimeStamp = r.getValue(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES);

                    Long disabledTimeStampVal = 0L;
                    if (disabledTimeStamp == null || disabledTimeStamp.length == 0) {
                        continue;
                    }

                    // disableTimeStamp has to be a positive value
                    disabledTimeStampVal = (Long) PLong.INSTANCE.toObject(disabledTimeStamp);
                    if (disabledTimeStampVal <= 0) {
                        continue;
                    }

                    byte[] dataTable = r.getValue(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES);
                    byte[] indexStat = r.getValue(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.INDEX_STATE_BYTES);
                    if ((dataTable == null || dataTable.length == 0)
                            || (indexStat == null || indexStat.length == 0)
                            || ((Bytes.compareTo(PIndexState.DISABLE.getSerializedBytes(), indexStat) != 0) 
                                    && (Bytes.compareTo(PIndexState.INACTIVE.getSerializedBytes(), indexStat) != 0))) {
                        // index has to be either in disable or inactive state
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
                        conn = DriverManager.getConnection(getJdbcUrl()).unwrap(PhoenixConnection.class);
                    }

                    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTable);
                    String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTable);
                    PTable dataPTable = PhoenixRuntime.getTable(conn, dataTableFullName);
                    PTable indexPTable = PhoenixRuntime.getTable(conn, indexTableFullName);
                    if (!MetaDataUtil.tableRegionsOnline(this.env.getConfiguration(), indexPTable)) {
                        LOG.debug("Index rebuild has been skipped because not all regions of index table="
                                + indexPTable.getName() + " are online.");
                        continue;
                    }

                    MetaDataClient client = new MetaDataClient(conn);
                    long overlapTime = env.getConfiguration().getLong(
                        QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME);
                    long timeStamp = Math.max(0, disabledTimeStampVal - overlapTime);

                    LOG.info("Starting to build index=" + indexPTable.getName() + " from timestamp=" + timeStamp);
                    client.buildPartialIndexFromTimeStamp(indexPTable, new TableRef(dataPTable, Long.MAX_VALUE, timeStamp));

                } while (hasMore);
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
