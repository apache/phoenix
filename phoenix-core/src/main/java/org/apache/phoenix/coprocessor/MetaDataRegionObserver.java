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

import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.PostDDLCompiler;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.UpgradeUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Coprocessor for metadata related operations. This coprocessor would only be registered
 * to SYSTEM.TABLE.
 */
@SuppressWarnings("deprecation")
public class MetaDataRegionObserver extends BaseRegionObserver {
    public static final Log LOG = LogFactory.getLog(MetaDataRegionObserver.class);
    public static final String REBUILD_INDEX_APPEND_TO_URL_STRING = "REBUILDINDEX";
    private static final byte[] SYSTEM_CATALOG_KEY = SchemaUtil.getTableKey(
            ByteUtil.EMPTY_BYTE_ARRAY,
            QueryConstants.SYSTEM_SCHEMA_NAME_BYTES,
            PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE_BYTES);
    protected ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private boolean enableRebuildIndex = QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD;
    private long rebuildIndexTimeInterval = QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_INTERVAL;
    private static Map<PName, Long> batchExecutedPerTableMap = new HashMap<PName, Long>();
    @GuardedBy("MetaDataRegionObserver.class")
    private static Properties rebuildIndexConnectionProps;
    // Added for test purposes
    private long initialRebuildTaskDelay;

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
        Configuration config = env.getConfiguration();
        long sleepTime = config.getLong(QueryServices.CLOCK_SKEW_INTERVAL_ATTRIB,
            QueryServicesOptions.DEFAULT_CLOCK_SKEW_INTERVAL);
        try {
            if(sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        enableRebuildIndex =
                config.getBoolean(
                    QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB,
                    QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD);
        rebuildIndexTimeInterval =
                config.getLong(
                    QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB,
                    QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_INTERVAL);
        initialRebuildTaskDelay =
                config.getLong(
                    QueryServices.INDEX_REBUILD_TASK_INITIAL_DELAY,
                    QueryServicesOptions.DEFAULT_INDEX_REBUILD_TASK_INITIAL_DELAY);
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
                    final HTableInterface mTable=metaTable;
                    final HTableInterface sTable=statsTable;
                    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
                        @Override
                        public Void run() throws Exception {
                            if (UpgradeUtil.truncateStats(mTable, sTable)) {
                                LOG.info("Stats are successfully truncated for upgrade 4.7!!");
                            }
                            return null;
                        }
                    });

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
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.start();

        if (!enableRebuildIndex) {
            LOG.info("Failure Index Rebuild is skipped by configuration.");
            return;
        }
        // turn off verbose deprecation logging
        Logger deprecationLogger = Logger.getLogger("org.apache.hadoop.conf.Configuration.deprecation");
        if (deprecationLogger != null) {
            deprecationLogger.setLevel(Level.WARN);
        }
        // Ensure we only run one of the index rebuilder tasks
        if (ServerUtil.isKeyInRegion(SYSTEM_CATALOG_KEY, e.getEnvironment().getRegion())) {
            try {
                Class.forName(PhoenixDriver.class.getName());
                initRebuildIndexConnectionProps(e.getEnvironment().getConfiguration());
                // starts index rebuild schedule work
                BuildIndexScheduleTask task = new BuildIndexScheduleTask(e.getEnvironment());
                executor.scheduleWithFixedDelay(task, initialRebuildTaskDelay, rebuildIndexTimeInterval, TimeUnit.MILLISECONDS);
            } catch (ClassNotFoundException ex) {
                LOG.error("BuildIndexScheduleTask cannot start!", ex);
            }
        }
    }

    /**
     * Task runs periodically to build indexes whose INDEX_NEED_PARTIALLY_REBUILD is set true
     *
     */
    public static class BuildIndexScheduleTask extends TimerTask {
        RegionCoprocessorEnvironment env;
        private final long rebuildIndexBatchSize;
        private final long configuredBatches;
        private final long indexDisableTimestampThreshold;
        private final ReadOnlyProps props;
        private final List<String> onlyTheseTables;

        public BuildIndexScheduleTask(RegionCoprocessorEnvironment env) {
            this(env,null);
        }

        public BuildIndexScheduleTask(RegionCoprocessorEnvironment env, List<String> onlyTheseTables) {
            this.onlyTheseTables = onlyTheseTables == null ? null : ImmutableList.copyOf(onlyTheseTables);
            this.env = env;
            Configuration configuration = env.getConfiguration();
            this.rebuildIndexBatchSize = configuration.getLong(
                    QueryServices.INDEX_FAILURE_HANDLING_REBUILD_PERIOD, HConstants.LATEST_TIMESTAMP);
            this.configuredBatches = configuration.getLong(
                    QueryServices.INDEX_FAILURE_HANDLING_REBUILD_NUMBER_OF_BATCHES_PER_TABLE, 10);
            this.indexDisableTimestampThreshold =
                    configuration.getLong(QueryServices.INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD);
            this.props = new ReadOnlyProps(env.getConfiguration().iterator());
        }

        @Override
        public void run() {
            // FIXME: we should replay the data table Put, as doing a partial index build would only add
            // the new rows and not delete the previous index value. Also, we should restrict the scan
            // to only data within this region (as otherwise *every* region will be running this code
            // separately, all updating the same data.
            RegionScanner scanner = null;
            PhoenixConnection conn = null;
            try {
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

                Map<PTable, List<Pair<PTable,Long>>> dataTableToIndexesMap = null;
                boolean hasMore = false;
                List<Cell> results = new ArrayList<Cell>();
                scanner = this.env.getRegion().getScanner(scan);

                do {
                    results.clear();
                    hasMore = scanner.next(results);
                    if (results.isEmpty()) {
                        LOG.debug("Found no indexes with non zero INDEX_DISABLE_TIMESTAMP");
                        break;
                    }

                    Result r = Result.create(results);
                    byte[] disabledTimeStamp = r.getValue(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES);
                    Cell indexStateCell = r.getColumnLatestCell(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_STATE_BYTES);

                    if (disabledTimeStamp == null || disabledTimeStamp.length == 0) {
                        LOG.debug("Null or empty INDEX_DISABLE_TIMESTAMP");
                        continue;
                    }

                    long indexDisableTimestamp =
                            PLong.INSTANCE.getCodec().decodeLong(disabledTimeStamp, 0,
                                SortOrder.ASC);
                    byte[] dataTable = r.getValue(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES);
                    if ((dataTable == null || dataTable.length == 0) || indexStateCell == null) {
                        // data table name can't be empty
                        LOG.debug("Null or data table name or index state");
                        continue;
                    }

                    byte[] indexStateBytes = CellUtil.cloneValue(indexStateCell);
                    byte[][] rowKeyMetaData = new byte[3][];
                    SchemaUtil.getVarChars(r.getRow(), 3, rowKeyMetaData);
                    byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
                    byte[] indexTable = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];

                    // validity check
                    if (indexTable == null || indexTable.length == 0) {
                        LOG.debug("We find IndexTable empty during rebuild scan:" + scan
                                + "so, Index rebuild has been skipped for row=" + r);
                        continue;
                    }
                    
                    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTable);
                    if (onlyTheseTables != null && !onlyTheseTables.contains(dataTableFullName)) {
                        LOG.debug("Could not find " + dataTableFullName + " in " + onlyTheseTables);
                        continue;
                    }

                    if (conn == null) {
                        conn = getRebuildIndexConnection(env.getConfiguration());
                        dataTableToIndexesMap = Maps.newHashMap();
                    }
                    PTable dataPTable = PhoenixRuntime.getTableNoCache(conn, dataTableFullName);

                    String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTable);
                    PTable indexPTable = PhoenixRuntime.getTableNoCache(conn, indexTableFullName);
                    // Sanity check in case index was removed from table
                    if (!dataPTable.getIndexes().contains(indexPTable)) {
                        LOG.debug(dataTableFullName + " does not contain " + indexPTable.getName().getString());
                        continue;
                    }
                    
                    PIndexState indexState = PIndexState.fromSerializedValue(indexStateBytes[0]);
                    // Only perform relatively expensive check for all regions online when index
                    // is disabled or pending active since that's the state it's placed into when
                    // an index write fails.
                    if ((indexState == PIndexState.DISABLE || indexState == PIndexState.PENDING_ACTIVE)
                            && !MetaDataUtil.tableRegionsOnline(this.env.getConfiguration(), indexPTable)) {
                        LOG.debug("Index rebuild has been skipped because not all regions of index table="
                                + indexPTable.getName() + " are online.");
                        continue;
                    }
                    if (EnvironmentEdgeManager.currentTimeMillis() - Math.abs(indexDisableTimestamp) > indexDisableTimestampThreshold) {
                        /*
                         * It has been too long since the index has been disabled and any future
                         * attempts to reenable it likely will fail. So we are going to mark the
                         * index as disabled and set the index disable timestamp to 0 so that the
                         * rebuild task won't pick up this index again for rebuild.
                         */
                        try {
                            IndexUtil.updateIndexState(conn, indexTableFullName, PIndexState.DISABLE, 0l);
                            LOG.error("Unable to rebuild index " + indexTableFullName
                                    + ". Won't attempt again since index disable timestamp is older than current time by "
                                    + indexDisableTimestampThreshold
                                    + " milliseconds. Manual intervention needed to re-build the index");
                        } catch (Throwable ex) {
                            LOG.error(
                                "Unable to mark index " + indexTableFullName + " as disabled.", ex);
                        }
                        continue; // don't attempt another rebuild irrespective of whether
                                  // updateIndexState worked or not
                    }
                    // Allow index to begin incremental maintenance as index is back online and we
                    // cannot transition directly from DISABLED -> ACTIVE
                    if (indexState == PIndexState.DISABLE) {
                        IndexUtil.updateIndexState(conn, indexTableFullName, PIndexState.INACTIVE, null);
                        continue; // Must wait until clients start to do index maintenance again
                    } else if (indexState == PIndexState.PENDING_ACTIVE) {
                        IndexUtil.updateIndexState(conn, indexTableFullName, PIndexState.ACTIVE, null);
                        continue; // Must wait until clients start to do index maintenance again
                    } else if (indexState != PIndexState.INACTIVE && indexState != PIndexState.ACTIVE) {
                        LOG.warn("Unexpected index state of " + indexTableFullName + "=" + indexState + ". Skipping partial rebuild attempt.");
                        continue;
                    }
                    long currentTime = EnvironmentEdgeManager.currentTimeMillis();
                    long forwardOverlapDurationMs = env.getConfiguration().getLong(
                            QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB, 
                                    QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME);
                    // Wait until no failures have occurred in at least forwardOverlapDurationMs
                    if (indexStateCell.getTimestamp() + forwardOverlapDurationMs > currentTime) {
                        LOG.debug("Still must wait " + (indexStateCell.getTimestamp() + forwardOverlapDurationMs - currentTime) + " before starting rebuild for " + indexTableFullName);
                        continue; // Haven't waited long enough yet
                    }
                    Long upperBoundOfRebuild = indexStateCell.getTimestamp() + forwardOverlapDurationMs;
                    // Pass in upperBoundOfRebuild when setting index state or increasing disable ts
                    // and fail if index timestamp > upperBoundOfRebuild.
                    List<Pair<PTable,Long>> indexesToPartiallyRebuild = dataTableToIndexesMap.get(dataPTable);
                    if (indexesToPartiallyRebuild == null) {
                        indexesToPartiallyRebuild = Lists.newArrayListWithExpectedSize(dataPTable.getIndexes().size());
                        dataTableToIndexesMap.put(dataPTable, indexesToPartiallyRebuild);
                    }
                    LOG.debug("We have found " + indexPTable.getIndexState() + " Index:" + indexPTable.getName()
                            + " on data table:" + dataPTable.getName() + " which failed to be updated at "
                            + indexPTable.getIndexDisableTimestamp());
                    indexesToPartiallyRebuild.add(new Pair<PTable,Long>(indexPTable,upperBoundOfRebuild));
                } while (hasMore);

				if (dataTableToIndexesMap != null) {
                    long backwardOverlapDurationMs = env.getConfiguration().getLong(
							QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_BACKWARD_TIME_ATTRIB,
							env.getConfiguration().getLong(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB, 
							        QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_BACKWARD_TIME));
					for (Map.Entry<PTable, List<Pair<PTable,Long>>> entry : dataTableToIndexesMap.entrySet()) {
						PTable dataPTable = entry.getKey();
						List<Pair<PTable,Long>> pairs = entry.getValue();
                        List<PTable> indexesToPartiallyRebuild = Lists.newArrayListWithExpectedSize(pairs.size());
						try (
                        HTableInterface metaTable = env.getTable(
								SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, props))) {
							long earliestDisableTimestamp = Long.MAX_VALUE;
                            long latestUpperBoundTimestamp = Long.MIN_VALUE;
							List<IndexMaintainer> maintainers = Lists
									.newArrayListWithExpectedSize(pairs.size());
							int signOfDisableTimeStamp = 0;
							for (Pair<PTable,Long> pair : pairs) {
					            // We need a way of differentiating the block writes to data table case from
					            // the leave index active case. In either case, we need to know the time stamp
					            // at which writes started failing so we can rebuild from that point. If we
					            // keep the index active *and* have a positive INDEX_DISABLE_TIMESTAMP_BYTES,
					            // then writes to the data table will be blocked (this is client side logic
					            // and we can't change this in a minor release). So we use the sign of the
					            // time stamp to differentiate.
							    PTable index = pair.getFirst();
							    Long upperBoundTimestamp = pair.getSecond();
								long disabledTimeStampVal = index.getIndexDisableTimestamp();
								if (disabledTimeStampVal != 0) {
                                    if (signOfDisableTimeStamp != 0 && signOfDisableTimeStamp != Long.signum(disabledTimeStampVal)) {
                                        LOG.warn("Found unexpected mix of signs with INDEX_DISABLE_TIMESTAMP for " + dataPTable.getName().getString() + " with " + indexesToPartiallyRebuild); 
                                    }
								    signOfDisableTimeStamp = Long.signum(disabledTimeStampVal);
	                                disabledTimeStampVal = Math.abs(disabledTimeStampVal);
									if (disabledTimeStampVal < earliestDisableTimestamp) {
										earliestDisableTimestamp = disabledTimeStampVal;
									}

									indexesToPartiallyRebuild.add(index);
									maintainers.add(index.getIndexMaintainer(dataPTable, conn));
								}
								if (upperBoundTimestamp > latestUpperBoundTimestamp) {
								    latestUpperBoundTimestamp = upperBoundTimestamp;
								}
							}
							// No indexes are disabled, so skip this table
							if (earliestDisableTimestamp == Long.MAX_VALUE) {
		                        LOG.debug("No indexes are disabled so continuing");
								continue;
							}
							long scanBeginTime = Math.max(0, earliestDisableTimestamp - backwardOverlapDurationMs);
                            long scanEndTime = Math.min(latestUpperBoundTimestamp,
                                    getTimestampForBatch(scanBeginTime,batchExecutedPerTableMap.get(dataPTable.getName())));
							LOG.info("Starting to build " + dataPTable + " indexes " + indexesToPartiallyRebuild
									+ " from timestamp=" + scanBeginTime + " until " + scanEndTime);
							
							TableRef tableRef = new TableRef(null, dataPTable, HConstants.LATEST_TIMESTAMP, false);
							// TODO Need to set high timeout
							PostDDLCompiler compiler = new PostDDLCompiler(conn);
							MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), null, null, null, scanEndTime);
							Scan dataTableScan = IndexManagementUtil.newLocalStateScan(plan.getContext().getScan(), maintainers);

							dataTableScan.setTimeRange(scanBeginTime, scanEndTime);
							dataTableScan.setCacheBlocks(false);
							dataTableScan.setAttribute(BaseScannerRegionObserver.REBUILD_INDEXES, TRUE_BYTES);

							ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable(
									ByteUtil.EMPTY_BYTE_ARRAY);
							IndexMaintainer.serializeAdditional(dataPTable, indexMetaDataPtr, indexesToPartiallyRebuild,
									conn);
							byte[] attribValue = ByteUtil.copyKeyBytesIfNecessary(indexMetaDataPtr);
							dataTableScan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, attribValue);
                            LOG.info("Starting to partially build indexes:" + indexesToPartiallyRebuild
                                    + " on data table:" + dataPTable.getName() + " with the earliest disable timestamp:"
                                    + earliestDisableTimestamp + " till "
                                    + (scanEndTime == HConstants.LATEST_TIMESTAMP ? "LATEST_TIMESTAMP" : scanEndTime));
							MutationState mutationState = plan.execute();
							long rowCount = mutationState.getUpdateCount();
                            if (scanEndTime == latestUpperBoundTimestamp) {
                                LOG.info("Rebuild completed for all inactive/disabled indexes in data table:"
                                        + dataPTable.getName());
                            }
                            LOG.info(" no. of datatable rows read in rebuilding process is " + rowCount);
							for (PTable indexPTable : indexesToPartiallyRebuild) {
								String indexTableFullName = SchemaUtil.getTableName(
										indexPTable.getSchemaName().getString(),
										indexPTable.getTableName().getString());
								if (scanEndTime == latestUpperBoundTimestamp) {
									IndexUtil.updateIndexState(conn, indexTableFullName, PIndexState.ACTIVE, 0L, latestUpperBoundTimestamp);
									batchExecutedPerTableMap.remove(dataPTable.getName());
                                    LOG.info("Making Index:" + indexPTable.getTableName() + " active after rebuilding");
								} else {
								    // Increment timestamp so that client sees updated disable timestamp
                                    IndexUtil.updateIndexState(conn, indexTableFullName, indexPTable.getIndexState(), scanEndTime * signOfDisableTimeStamp, latestUpperBoundTimestamp);
									Long noOfBatches = batchExecutedPerTableMap.get(dataPTable.getName());
									if (noOfBatches == null) {
										noOfBatches = 0l;
									}
									batchExecutedPerTableMap.put(dataPTable.getName(), ++noOfBatches);
									LOG.info("During Round-robin build: Successfully updated index disabled timestamp  for "
													+ indexTableFullName + " to " + scanEndTime);
								}
							}
						} catch (Exception e) {
							LOG.error("Unable to rebuild " + dataPTable + " indexes " + indexesToPartiallyRebuild, e);
						}
					}
				}
			} catch (Throwable t) {
				LOG.warn("ScheduledBuildIndexTask failed!", t);
			} finally {
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

        private long getTimestampForBatch(long disabledTimeStamp, Long noOfBatches) {
            if (disabledTimeStamp < 0 || rebuildIndexBatchSize > (HConstants.LATEST_TIMESTAMP
                    - disabledTimeStamp)) { return HConstants.LATEST_TIMESTAMP; }
            long timestampForNextBatch = disabledTimeStamp + rebuildIndexBatchSize;
			if (timestampForNextBatch < 0 || timestampForNextBatch > EnvironmentEdgeManager.currentTimeMillis()
					|| (noOfBatches != null && noOfBatches > configuredBatches)) {
				// if timestampForNextBatch cross current time , then we should
				// build the complete index
				timestampForNextBatch = HConstants.LATEST_TIMESTAMP;
			}
            return timestampForNextBatch;
        }
    }
    
    @VisibleForTesting
    public static synchronized void initRebuildIndexConnectionProps(Configuration config) {
        if (rebuildIndexConnectionProps == null) {
            Properties props = new Properties();
            long indexRebuildQueryTimeoutMs =
                    config.getLong(QueryServices.INDEX_REBUILD_QUERY_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_QUERY_TIMEOUT);
            long indexRebuildRPCTimeoutMs =
                    config.getLong(QueryServices.INDEX_REBUILD_RPC_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_TIMEOUT);
            long indexRebuildClientScannerTimeOutMs =
                    config.getLong(QueryServices.INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT);
            int indexRebuildRpcRetriesCounter =
                    config.getInt(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_RETRIES_COUNTER);
            // Set various phoenix and hbase level timeouts and rpc retries
            props.setProperty(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,
                Long.toString(indexRebuildQueryTimeoutMs));
            props.setProperty(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
                Long.toString(indexRebuildClientScannerTimeOutMs));
            props.setProperty(HConstants.HBASE_RPC_TIMEOUT_KEY,
                Long.toString(indexRebuildRPCTimeoutMs));
            props.setProperty(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                Long.toString(indexRebuildRpcRetriesCounter));
            // don't run a second index populations upsert select
            props.setProperty(QueryServices.INDEX_POPULATION_SLEEP_TIME, "0");
            rebuildIndexConnectionProps = PropertiesUtil.combineProperties(props, config);
        }
    }

    public static PhoenixConnection getRebuildIndexConnection(Configuration config)
            throws SQLException, ClassNotFoundException {
        initRebuildIndexConnectionProps(config);
        //return QueryUtil.getConnectionOnServer(rebuildIndexConnectionProps, config).unwrap(PhoenixConnection.class);
        return QueryUtil.getConnectionOnServerWithCustomUrl(rebuildIndexConnectionProps,
            REBUILD_INDEX_APPEND_TO_URL_STRING).unwrap(PhoenixConnection.class);
    }
}