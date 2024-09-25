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

import static org.apache.phoenix.coprocessor.GlobalIndexRegionScanner.adjustScanFilter;
import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;
import static org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker.COMPACTION_UPDATE_STATS_ROW_COUNT;
import static org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker.CONCURRENT_UPDATE_STATS_ROW_COUNT;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.controller.InterRegionServerIndexRpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.index.PhoenixIndexFailurePolicyHelper;
import org.apache.phoenix.index.PhoenixIndexFailurePolicyHelper.MutateCommand;
import org.apache.phoenix.index.PhoenixIndexMetaData;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.stats.NoOpStatisticsCollector;
import org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker;
import org.apache.phoenix.schema.stats.StatisticsCollector;
import org.apache.phoenix.schema.stats.StatisticsCollectorFactory;

import org.apache.phoenix.schema.stats.StatsCollectionDisabledOnServerException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Region observer that aggregates ungrouped rows(i.e. SQL query with aggregation function and no GROUP BY).
 *
 *
 * @since 0.1
 */
public class UngroupedAggregateRegionObserver extends BaseScannerRegionObserver implements RegionCoprocessor {
    // TODO: move all constants into a single class
    public static final String UNGROUPED_AGG = "UngroupedAgg";
    public static final String DELETE_AGG = "DeleteAgg";
    public static final String DELETE_CQ = "DeleteCQ";
    public static final String DELETE_CF = "DeleteCF";
    public static final String EMPTY_CF = "EmptyCF";
    /**
     * This lock used for synchronizing the state of
     * {@link UngroupedAggregateRegionObserver#scansReferenceCount},
     * {@link UngroupedAggregateRegionObserver#isRegionClosingOrSplitting} variables used to avoid possible
     * dead lock situation in case below steps: 
     * 1. We get read lock when we start writing local indexes, deletes etc.. 
     * 2. when memstore reach threshold, flushes happen. Since they use read (shared) lock they 
     * happen without any problem until someone tries to obtain write lock. 
     * 3. at one moment we decide to split/bulkload/close and try to acquire write lock. 
     * 4. Since that moment all attempts to get read lock will be blocked. I.e. no more 
     * flushes will happen. But we continue to fill memstore with local index batches and 
     * finally we get RTBE.
     * 
     * The solution to this is to not allow or delay operations acquire the write lock.
     * 1) In case of split we just throw IOException so split won't happen but it will not cause any harm.
     * 2) In case of bulkload failing it by throwing the exception. 
     * 3) In case of region close by balancer/move wait before closing the reason and fail the query which 
     * does write after reading. 
     * 
     * See PHOENIX-3111 for more info.
     */

    private final Object lock = new Object();
    /**
     * To maintain the number of scans used for create index, delete and upsert select operations
     * which reads and writes to same region in coprocessors.
     */
    @GuardedBy("lock")
    private int scansReferenceCount = 0;
    @GuardedBy("lock")
    private boolean isRegionClosingOrSplitting = false;
    private static final Logger LOGGER = LoggerFactory.getLogger(UngroupedAggregateRegionObserver.class);
    private Configuration upsertSelectConfig;
    private Configuration compactionConfig;
    private Configuration indexWriteConfig;
    private ReadOnlyProps indexWriteProps;
    private boolean isPhoenixTableTTLEnabled;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        /*
         * We need to create a copy of region's configuration since we don't want any side effect of
         * setting the RpcControllerFactory.
         */
        upsertSelectConfig = PropertiesUtil.cloneConfig(e.getConfiguration());
        /*
         * Till PHOENIX-3995 is fixed, we need to use the
         * InterRegionServerIndexRpcControllerFactory. Although this would cause remote RPCs to use
         * index handlers on the destination region servers, it is better than using the regular
         * priority handlers which could result in a deadlock.
         */
        upsertSelectConfig.setClass(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY,
                InterRegionServerIndexRpcControllerFactory.class, RpcControllerFactory.class);

        compactionConfig = ServerUtil.getCompactionConfig(e.getConfiguration());

        // For retries of index write failures, use the same # of retries as the rebuilder
        indexWriteConfig = PropertiesUtil.cloneConfig(e.getConfiguration());
        indexWriteConfig.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                e.getConfiguration().getInt(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_RETRIES_COUNTER));
        indexWriteProps = new ReadOnlyProps(indexWriteConfig.iterator());
    }

    Configuration getUpsertSelectConfig() {
        return upsertSelectConfig;
    }

    void incrementScansReferenceCount() throws IOException {
        synchronized (lock) {
            if (isRegionClosingOrSplitting) {
                throw new IOException("Temporarily unable to write from scan because region is closing or splitting");
            }
            scansReferenceCount++;
            lock.notifyAll();
        }
    }

    void decrementScansReferenceCount() {
        synchronized (lock) {
            scansReferenceCount--;
            if (scansReferenceCount < 0) {
                LOGGER.warn(
                        "Scan reference count went below zero. Something isn't correct. Resetting it back to zero");
                scansReferenceCount = 0;
            }
            lock.notifyAll();
        }
    }

    void commitBatchWithRetries(final Region region, final List<Mutation> localRegionMutations, final long blockingMemstoreSize) throws IOException {
        try {
            commitBatch(region, localRegionMutations, blockingMemstoreSize);
        } catch (IOException e) {
            handleIndexWriteException(localRegionMutations, e, new MutateCommand() {
                @Override
                public void doMutation() throws IOException {
                    commitBatch(region, localRegionMutations, blockingMemstoreSize);
                }

                @Override
                public List<Mutation> getMutationList() {
                    return localRegionMutations;
                }
            });
        }
    }

    void commitBatch(Region region, List<Mutation> mutations, long blockingMemstoreSize) throws IOException {
        if (mutations.isEmpty()) {
            return;
        }

       Mutation[] mutationArray = new Mutation[mutations.size()];
      // When memstore size reaches blockingMemstoreSize we are waiting 3 seconds for the
      // flush happen which decrease the memstore size and then writes allowed on the region.
      for (int i = 0; blockingMemstoreSize > 0
              && region.getMemStoreHeapSize() + region.getMemStoreOffHeapSize() > blockingMemstoreSize
              && i < 30; i++) {
          try {
              checkForRegionClosingOrSplitting();
              Thread.sleep(100);
          } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException(e);
          }
      }
      // TODO: should we use the one that is all or none?
      LOGGER.debug("Committing batch of " + mutations.size() + " mutations for " + region.getRegionInfo().getTable().getNameAsString());
      region.batchMutate(mutations.toArray(mutationArray));
    }

    static void setIndexAndTransactionProperties(List<Mutation> mutations, byte[] indexUUID,
                                                 byte[] indexMaintainersPtr, byte[] txState,
                                                 byte[] clientVersionBytes, boolean useIndexProto) {
        for (Mutation m : mutations) {
           if (indexMaintainersPtr != null) {
               m.setAttribute(useIndexProto ? PhoenixIndexCodec.INDEX_PROTO_MD : PhoenixIndexCodec.INDEX_MD, indexMaintainersPtr);
           }
           if (indexUUID != null) {
             m.setAttribute(PhoenixIndexCodec.INDEX_UUID, indexUUID);
           }
           if (txState != null) {
               m.setAttribute(BaseScannerRegionObserverConstants.TX_STATE, txState);
           }
           if (clientVersionBytes != null) {
               m.setAttribute(BaseScannerRegionObserverConstants.CLIENT_VERSION, clientVersionBytes);
           }
        }
    }

    private void commitBatchWithTable(Table table, List<Mutation> mutations) throws IOException {
        if (mutations.isEmpty()) {
            return;
        }

        LOGGER.debug("Committing batch of " + mutations.size() + " mutations for " + table);
        try {
            Object[] results = new Object[mutations.size()];
            table.batch(mutations, results);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * There is a chance that region might be closing while running balancer/move/merge. In this
     * case if the memstore size reaches blockingMemstoreSize better to fail query because there is
     * a high chance that flush might not proceed and memstore won't be freed up.
     * @throws IOException
     */
    void checkForRegionClosingOrSplitting() throws IOException {
        synchronized (lock) {
            if (isRegionClosingOrSplitting) {
                lock.notifyAll();
                throw new IOException("Region is getting closed. Not allowing to write to avoid possible deadlock.");
            }
        }
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan)
            throws IOException {
        super.preScannerOpen(e, scan);
        if (ScanUtil.isAnalyzeTable(scan)) {
            scan.setAttribute(BaseScannerRegionObserverConstants.SCAN_ANALYZE_ACTUAL_START_ROW,
                    scan.getStartRow());
            scan.setAttribute(BaseScannerRegionObserverConstants.SCAN_ANALYZE_ACTUAL_STOP_ROW,
                    scan.getStopRow());
            scan.setAttribute(BaseScannerRegionObserverConstants.SCAN_ANALYZE_INCLUDE_START_ROW,
                    Bytes.toBytes(scan.includeStartRow()));
            scan.setAttribute(BaseScannerRegionObserverConstants.SCAN_ANALYZE_INCLUDE_STOP_ROW,
                    Bytes.toBytes(scan.includeStopRow()));
            // We are setting the start row and stop row such that it covers the entire region. As part
            // of Phonenix-1263 we are storing the guideposts against the physical table rather than
            // individual tenant specific tables.
            scan.withStartRow(HConstants.EMPTY_START_ROW);
            scan.withStopRow(HConstants.EMPTY_END_ROW);
            scan.setFilter(null);
        }
    }

    public static class MutationList extends ArrayList<Mutation> {
        private long byteSize = 0L;

        public MutationList() {
            super();
        }

        public MutationList(int size) {
            super(size);
        }

        @Override
        public boolean add(Mutation e) {
            boolean r = super.add(e);
            if (r) {
                this.byteSize += PhoenixKeyValueUtil.calculateMutationDiskSize(e);
            }
            return r;
        }

        public long byteSize() {
            return byteSize;
        }

        @Override
        public void clear() {
            byteSize = 0l;
            super.clear();
        }
    }

    static long getBlockingMemstoreSize(Region region, Configuration conf) {
        long flushSize = region.getTableDescriptor().getMemStoreFlushSize();

        if (flushSize <= 0) {
            flushSize = conf.getLongBytes(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                    TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
        }
        return flushSize * (conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
                HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER) - 1);
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan,
                                              final RegionScanner s) throws IOException, SQLException {
        final RegionCoprocessorEnvironment env = c.getEnvironment();
        final Region region = env.getRegion();
        long ts = scan.getTimeRange().getMax();
        boolean localIndexScan = ScanUtil.isLocalIndex(scan);
        boolean uncoveredGlobalIndexScan = ScanUtil.isUncoveredGlobalIndex(scan);
        if (ScanUtil.isAnalyzeTable(scan)) {
            byte[] gp_width_bytes =
                    scan.getAttribute(BaseScannerRegionObserverConstants.GUIDEPOST_WIDTH_BYTES);
            byte[] gp_per_region_bytes =
                    scan.getAttribute(BaseScannerRegionObserverConstants.GUIDEPOST_PER_REGION);
            // Let this throw, as this scan is being done for the sole purpose of collecting stats
            StatisticsCollector statsCollector = StatisticsCollectorFactory.createStatisticsCollector(
                    env, region.getRegionInfo().getTable().getNameAsString(), ts,
                    gp_width_bytes, gp_per_region_bytes);
            if (statsCollector instanceof NoOpStatisticsCollector) {
                throw new StatsCollectionDisabledOnServerException();
            } else {
                return collectStats(s, statsCollector, region, scan, env.getConfiguration());
            }
        } else if (ScanUtil.isIndexRebuild(scan)) {
            return User.runAsLoginUser(new PrivilegedExceptionAction<RegionScanner>() {
                @Override
                public RegionScanner run() throws Exception {
                    return rebuildIndices(s, region, scan, env);
                }
            });
        }

        boolean useNewValueColumnQualifier = EncodedColumnsUtil.useNewValueColumnQualifier(scan);
        int offsetToBe = 0;
        if (localIndexScan) {
            offsetToBe = region.getRegionInfo().getStartKey().length != 0 ? region.getRegionInfo().getStartKey().length :
                    region.getRegionInfo().getEndKey().length;
        }
        final int offset = offsetToBe;
        byte[] descRowKeyTableBytes = scan.getAttribute(BaseScannerRegionObserverConstants.UPGRADE_DESC_ROW_KEY);
        boolean isDescRowKeyOrderUpgrade = descRowKeyTableBytes != null;
        List<IndexMaintainer> indexMaintainers = IndexUtil.deSerializeIndexMaintainersFromScan(scan);
        RegionScanner theScanner = s;
        byte[] upsertSelectTable = scan.getAttribute(BaseScannerRegionObserverConstants.UPSERT_SELECT_TABLE);
        boolean isDelete = false;
        if (upsertSelectTable == null) {
            byte[] isDeleteAgg = scan.getAttribute(BaseScannerRegionObserverConstants.DELETE_AGG);
            isDelete = isDeleteAgg != null && Bytes.compareTo(PDataType.TRUE_BYTES, isDeleteAgg) == 0;
        }
        TupleProjector tupleProjector = null;
        byte[][] viewConstants = null;
        ColumnReference[] dataColumns = IndexUtil.deserializeDataTableColumnsToJoin(scan);
        final TupleProjector p = TupleProjector.deserializeProjectorFromScan(scan);
        final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
        boolean useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan));
        if (((localIndexScan || uncoveredGlobalIndexScan) && !isDelete && !isDescRowKeyOrderUpgrade) || (j == null && p != null)) {
            if (dataColumns != null) {
                tupleProjector = IndexUtil.getTupleProjector(scan, dataColumns);
            }
            viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
            ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
            theScanner =
                    getWrappedScanner(c, theScanner, offset, scan, dataColumns, tupleProjector,
                            region, indexMaintainers == null ? null : indexMaintainers.get(0), viewConstants, p, tempPtr, useQualifierAsIndex);
        }

        if (j != null) {
            theScanner = new HashJoinRegionScanner(theScanner, scan, p, j, ScanUtil.getTenantId(scan), env, useQualifierAsIndex, useNewValueColumnQualifier);
        }
        return new UngroupedAggregateRegionScanner(c, theScanner, region, scan, env, this);
    }

    public static void checkForLocalIndexColumnFamilies(Region region,
                                                        List<IndexMaintainer> indexMaintainers) throws IOException {
        TableDescriptor tableDesc = region.getTableDescriptor();
        String schemaName =
                tableDesc.getTableName().getNamespaceAsString()
                        .equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR) ? SchemaUtil
                        .getSchemaNameFromFullName(tableDesc.getTableName().getNameAsString())
                        : tableDesc.getTableName().getNamespaceAsString();
        String tableName = SchemaUtil.getTableNameFromFullName(tableDesc.getTableName().getNameAsString());
        for (IndexMaintainer indexMaintainer : indexMaintainers) {
            Set<ColumnReference> coveredColumns = indexMaintainer.getCoveredColumns();
            if (coveredColumns.isEmpty()) {
                byte[] localIndexCf = indexMaintainer.getEmptyKeyValueFamily().get();
                // When covered columns empty we store index data in default column family so check for it.
                if (tableDesc.getColumnFamily(localIndexCf) == null) {
                    ClientUtil.throwIOException("Column Family Not Found",
                            new ColumnFamilyNotFoundException(schemaName, tableName, Bytes
                                    .toString(localIndexCf)));
                }
            }
            for (ColumnReference reference : coveredColumns) {
                byte[] cf = IndexUtil.getLocalIndexColumnFamily(reference.getFamily());
                ColumnFamilyDescriptor family = region.getTableDescriptor().getColumnFamily(cf);
                if (family == null) {
                    ClientUtil.throwIOException("Column Family Not Found",
                            new ColumnFamilyNotFoundException(schemaName, tableName, Bytes.toString(cf)));
                }
            }
        }
    }

    void commit(final Region region, List<Mutation> mutations, byte[] indexUUID, final long blockingMemStoreSize,
                byte[] indexMaintainersPtr, byte[] txState, final Table targetHTable, boolean useIndexProto,
                boolean isPKChanging, byte[] clientVersionBytes)
            throws IOException {
        final List<Mutation> localRegionMutations = Lists.newArrayList();
        final List<Mutation> remoteRegionMutations = Lists.newArrayList();
        setIndexAndTransactionProperties(mutations, indexUUID, indexMaintainersPtr, txState, clientVersionBytes, useIndexProto);
        separateLocalAndRemoteMutations(targetHTable, region, mutations, localRegionMutations, remoteRegionMutations,
                isPKChanging);
        commitBatchWithRetries(region, localRegionMutations, blockingMemStoreSize);
        try {
            commitBatchWithTable(targetHTable, remoteRegionMutations);
        } catch (IOException e) {
            handleIndexWriteException(remoteRegionMutations, e, new MutateCommand() {
                @Override
                public void doMutation() throws IOException {
                    commitBatchWithTable(targetHTable, remoteRegionMutations);
                }

                @Override
                public List<Mutation> getMutationList() {
                    return remoteRegionMutations;
                }
            });
        }
        localRegionMutations.clear();
        remoteRegionMutations.clear();
    }

    private void handleIndexWriteException(final List<Mutation> localRegionMutations, IOException origIOE,
                                           MutateCommand mutateCommand) throws IOException {
        long serverTimestamp = ClientUtil.parseTimestampFromRemoteException(origIOE);
        SQLException inferredE = ClientUtil.parseLocalOrRemoteServerException(origIOE);
        if (inferredE != null && inferredE.getErrorCode() == SQLExceptionCode.INDEX_WRITE_FAILURE.getErrorCode()) {
            // For an index write failure, the data table write succeeded,
            // so when we retry we need to set REPLAY_WRITES
            for (Mutation mutation : localRegionMutations) {
                if (PhoenixIndexMetaData.isIndexRebuild(mutation.getAttributesMap())) {
                    mutation.setAttribute(BaseScannerRegionObserverConstants.REPLAY_WRITES,
                            BaseScannerRegionObserverConstants.REPLAY_INDEX_REBUILD_WRITES);
                } else {
                    mutation.setAttribute(BaseScannerRegionObserverConstants.REPLAY_WRITES,
                            BaseScannerRegionObserverConstants.REPLAY_ONLY_INDEX_WRITES);
                }
                // use the server timestamp for index write retrys
                PhoenixKeyValueUtil.setTimestamp(mutation, serverTimestamp);
            }
            IndexWriteException iwe = PhoenixIndexFailurePolicyHelper.getIndexWriteException(inferredE);
            try (PhoenixConnection conn =
                         QueryUtil.getConnectionOnServer(indexWriteConfig)
                                 .unwrap(PhoenixConnection.class)) {
                PhoenixIndexFailurePolicyHelper.doBatchWithRetries(mutateCommand, iwe, conn,
                        indexWriteProps);
            } catch (Exception e) {
                throw new DoNotRetryIOException(e);
            }
        } else {
            throw origIOE;
        }
    }

    private void separateLocalAndRemoteMutations(Table targetHTable, Region region, List<Mutation> mutations,
                                                 List<Mutation> localRegionMutations, List<Mutation> remoteRegionMutations,
                                                 boolean isPKChanging) {
        boolean areMutationsInSameTable = areMutationsInSameTable(targetHTable, region);
        //if we're writing to the same table, but the PK can change, that means that some
        //mutations might be in our current region, and others in a different one.
        if (areMutationsInSameTable && isPKChanging) {
            RegionInfo regionInfo = region.getRegionInfo();
            for (Mutation mutation : mutations) {
                if (regionInfo.containsRow(mutation.getRow())) {
                    localRegionMutations.add(mutation);
                } else {
                    remoteRegionMutations.add(mutation);
                }
            }
        } else if (areMutationsInSameTable && !isPKChanging) {
            localRegionMutations.addAll(mutations);
        } else {
            remoteRegionMutations.addAll(mutations);
        }
    }

    private boolean areMutationsInSameTable(Table targetHTable, Region region) {
        return (targetHTable == null || Bytes.compareTo(targetHTable.getName().getName(),
                region.getTableDescriptor().getTableName().getName()) == 0);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
                                      CompactionRequest request) throws IOException {
        final TableName tableName = c.getEnvironment().getRegion().getRegionInfo().getTable();
        // Compaction and split upcalls run with the effective user context of the requesting user.
        // This will lead to failure of cross cluster RPC if the effective user is not
        // the login user. Switch to the login user context to ensure we have the expected
        // security context.
        return User.runAsLoginUser(new PrivilegedExceptionAction<InternalScanner>() {
            @Override
            public InternalScanner run() throws Exception {
                InternalScanner internalScanner = scanner;
                boolean keepDeleted = false;
                boolean isMultiTenantIndexTable = false;
                if (tableName.getNameAsString().startsWith(MetaDataUtil.VIEW_INDEX_TABLE_PREFIX)) {
                    isMultiTenantIndexTable = true;
                }
                final String fullTableName = isMultiTenantIndexTable ?
                        SchemaUtil.getParentTableNameFromIndexTable(tableName.getNameAsString(),
                                MetaDataUtil.VIEW_INDEX_TABLE_PREFIX) :
                        tableName.getNameAsString();
                PTable table = null;
                try (PhoenixConnection conn = QueryUtil.getConnectionOnServer(
                        compactionConfig).unwrap(PhoenixConnection.class)) {
                    table = conn.getTableNoCache(fullTableName);
                } catch (Exception e) {
                    if (e instanceof TableNotFoundException) {
                        LOGGER.debug("Ignoring HBase table that is not a Phoenix table: "
                                + fullTableName);
                        // non-Phoenix HBase tables won't be found, do nothing
                    } else {
                        LOGGER.error(
                                "Unable to modify compaction scanner to retain deleted "
                                        + "cells for a table with disabled Index; "
                                        + fullTableName, e);
                    }
                }
                // The previous indexing design needs to retain delete markers and deleted
                // cells to rebuild disabled indexes. Thus, we skip major compaction for
                // them. GlobalIndexChecker is the coprocessor introduced by the current
                // indexing design.
                if (table != null &&
                        !PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME.equals(fullTableName) &&
                        !ServerUtil.hasCoprocessor(c.getEnvironment(),
                        GlobalIndexChecker.class.getName())) {
                    List<PTable>
                            indexes =
                            PTableType.INDEX.equals(table.getType()) ?
                                    Lists.newArrayList(table) :
                                    table.getIndexes();
                    // FIXME need to handle views and indexes on views as well
                    for (PTable index : indexes) {
                        if (index.getIndexDisableTimestamp() != 0) {
                            LOGGER.info("Modifying major compaction scanner to retain "
                                    + "deleted cells for a table with disabled index: "
                                    + fullTableName);
                            keepDeleted = true;
                            break;
                        }
                    }
                }
                if (table != null
                        && isPhoenixTableTTLEnabled(c.getEnvironment().getConfiguration())) {
                    internalScanner =
                            new CompactionScanner(c.getEnvironment(), store, scanner,
                                    BaseScannerRegionObserverConstants
                                            .getMaxLookbackInMillis(c.getEnvironment()
                                                    .getConfiguration()),
                                    SchemaUtil.getEmptyColumnFamily(table),
                                    table.getEncodingScheme()
                                            == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                                            QueryConstants.EMPTY_COLUMN_BYTES :
                                            table.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME),
                                    request.isMajor() || request.isAllFiles(), keepDeleted
                                    );
                }
                else if (isPhoenixTableTTLEnabled(c.getEnvironment().getConfiguration())) {
                    LOGGER.warn("Skipping compaction for table: {} " +
                            "as failed to retrieve PTable object", fullTableName);
                }
                if (scanType.equals(ScanType.COMPACT_DROP_DELETES)) {
                    try {
                        long clientTimeStamp = EnvironmentEdgeManager.currentTimeMillis();
                        DelegateRegionCoprocessorEnvironment
                                compactionConfEnv =
                                new DelegateRegionCoprocessorEnvironment(c.getEnvironment(),
                                        ConnectionType.COMPACTION_CONNECTION);
                        StatisticsCollector
                                statisticsCollector =
                                StatisticsCollectorFactory.createStatisticsCollector(
                                        compactionConfEnv, tableName.getNameAsString(),
                                        clientTimeStamp,
                                        store.getColumnFamilyDescriptor().getName());
                        statisticsCollector.init();
                        internalScanner =
                                statisticsCollector.createCompactionScanner(compactionConfEnv,
                                        store, internalScanner);
                    } catch (Exception e) {
                        // If we can't reach the stats table, don't interrupt the normal
                        // compaction operation, just log a warning.
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn("Unable to collect stats for " + tableName, e);
                        }
                    }
                }
                return internalScanner;
            }
        });
    }

    static PTable deserializeTable(byte[] b) {
        try {
            PTableProtos.PTable ptableProto = PTableProtos.PTable.parseFrom(b);
            return PTableImpl.createFromProto(ptableProto);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RegionScanner getRegionScanner(final RegionScanner innerScanner, final Region region, final Scan scan,
                                           final RegionCoprocessorEnvironment env, final boolean oldCoproc)
            throws IOException {
        if (oldCoproc) {
            return new IndexerRegionScanner(innerScanner, region, scan, env, this);
        } else {
            if (region.getTableDescriptor().hasCoprocessor(GlobalIndexChecker.class.getCanonicalName())) {
                return new IndexRepairRegionScanner(innerScanner, region, scan, env, this);
            } else {
                return new IndexRebuildRegionScanner(innerScanner, region, scan, env, this);
            }
        }
    }

    private RegionScanner rebuildIndices(RegionScanner innerScanner, final Region region, final Scan scan,
                                         final RegionCoprocessorEnvironment env) throws IOException {
        boolean oldCoproc = region.getTableDescriptor().hasCoprocessor(Indexer.class.getCanonicalName());
        byte[] valueBytes = scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_VERIFY_TYPE);
        IndexTool.IndexVerifyType verifyType = (valueBytes != null) ?
                IndexTool.IndexVerifyType.fromValue(valueBytes) : IndexTool.IndexVerifyType.NONE;
        if (oldCoproc && verifyType == IndexTool.IndexVerifyType.ONLY) {
            return new IndexerRegionScanner(innerScanner, region, scan, env, this);
        }
        RegionScanner scanner;
        if (!scan.isRaw()) {
            Scan rawScan = new Scan(scan);
            rawScan.setRaw(true);
            rawScan.readAllVersions();
            rawScan.getFamilyMap().clear();
            adjustScanFilter(rawScan);
            rawScan.setCacheBlocks(false);
            for (byte[] family : scan.getFamilyMap().keySet()) {
                rawScan.addFamily(family);
            }
            scanner = ((DelegateRegionScanner)innerScanner).getNewRegionScanner(rawScan);
            innerScanner.close();
        } else {
            if (adjustScanFilter(scan)) {
                scanner = ((DelegateRegionScanner) innerScanner).getNewRegionScanner(scan);
                innerScanner.close();
            } else {
                scanner = innerScanner;
            }
        }
        return getRegionScanner(scanner, region, scan, env, oldCoproc);
    }

    private RegionScanner collectStats(final RegionScanner innerScanner, StatisticsCollector stats,
                                       final Region region, final Scan scan, Configuration config) throws IOException {
        StatsCollectionCallable callable =
                new StatsCollectionCallable(stats, region, innerScanner, config, scan);
        byte[] asyncBytes = scan.getAttribute(BaseScannerRegionObserverConstants.RUN_UPDATE_STATS_ASYNC_ATTRIB);
        boolean async = false;
        if (asyncBytes != null) {
            async = Bytes.toBoolean(asyncBytes);
        }
        long rowCount = 0; // in case of async, we report 0 as number of rows updated
        StatisticsCollectionRunTracker statsRunTracker =
                StatisticsCollectionRunTracker.getInstance(config);
        final boolean runUpdateStats = statsRunTracker.addUpdateStatsCommandRegion(region.getRegionInfo(), scan.getFamilyMap().keySet());
        if (runUpdateStats) {
            if (!async) {
                rowCount = callable.call();
            } else {
                statsRunTracker.runTask(callable);
            }
        } else {
            rowCount = CONCURRENT_UPDATE_STATS_ROW_COUNT;
            LOGGER.info("UPDATE STATISTICS didn't run because another UPDATE STATISTICS command was already running on the region "
                    + region.getRegionInfo().getRegionNameAsString());
        }
        final boolean isIncompatibleClient =
                ScanUtil.isIncompatibleClientForServerReturnValidRowKey(scan);
        byte[] rowKey = getRowKeyForCollectStats(region, scan, isIncompatibleClient);
        byte[] rowCountBytes = PLong.INSTANCE.toBytes(rowCount);
        final Cell aggKeyValue =
                PhoenixKeyValueUtil.newKeyValue(rowKey, SINGLE_COLUMN_FAMILY,
                        SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
        RegionScanner scanner = new BaseRegionScanner(innerScanner) {
            @Override
            public RegionInfo getRegionInfo() {
                return region.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() {
                return true;
            }

            @Override
            public void close() throws IOException {
                // If we ran/scheduled StatsCollectionCallable the delegate
                // scanner is closed there. Otherwise close it here.
                if (!runUpdateStats) {
                    super.close();
                }
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                results.add(aggKeyValue);
                return false;
            }

            @Override
            public long getMaxResultSize() {
                return scan.getMaxResultSize();
            }
        };
        return scanner;
    }

    private static byte[] getRowKeyForCollectStats(Region region, Scan scan,
                                                   boolean isIncompatibleClient) {
        byte[] rowKey;
        if (!isIncompatibleClient) {
            byte[] startKey = scan.getAttribute(
                    BaseScannerRegionObserverConstants.SCAN_ANALYZE_ACTUAL_START_ROW) == null ?
                    region.getRegionInfo().getStartKey() : scan.getAttribute(
                    BaseScannerRegionObserverConstants.SCAN_ANALYZE_ACTUAL_START_ROW);
            byte[] endKey = scan.getAttribute(
                    BaseScannerRegionObserverConstants.SCAN_ANALYZE_ACTUAL_STOP_ROW) == null ?
                    region.getRegionInfo().getEndKey() : scan.getAttribute(
                    BaseScannerRegionObserverConstants.SCAN_ANALYZE_ACTUAL_STOP_ROW);
            rowKey = ByteUtil.getLargestPossibleRowKeyInRange(startKey, endKey);
            if (rowKey == null) {
                if (scan.getAttribute(
                        BaseScannerRegionObserverConstants.SCAN_ANALYZE_INCLUDE_START_ROW) !=
                        null && Bytes.toBoolean(scan.getAttribute(
                        BaseScannerRegionObserverConstants.SCAN_ANALYZE_INCLUDE_START_ROW))) {
                    rowKey = startKey;
                } else if (scan.getAttribute(
                        BaseScannerRegionObserverConstants.SCAN_ANALYZE_INCLUDE_STOP_ROW) !=
                        null && Bytes.toBoolean(scan.getAttribute(
                        BaseScannerRegionObserverConstants.SCAN_ANALYZE_INCLUDE_STOP_ROW))) {
                    rowKey = endKey;
                } else {
                    rowKey = HConstants.EMPTY_END_ROW;
                }
            }
        } else {
            rowKey = UNGROUPED_AGG_ROW_KEY;
        }
        return rowKey;
    }

    /**
     * 
     * Callable to encapsulate the collection of stats triggered by 
     * UPDATE STATISTICS command.
     *
     * Package private for tests.
     */
    static class StatsCollectionCallable implements Callable<Long> {
        private final StatisticsCollector statsCollector;
        private final Region region;
        private final RegionScanner innerScanner;
        private final Configuration config;
        private final Scan scan;

        StatsCollectionCallable(StatisticsCollector s, Region r, RegionScanner rs,
                                Configuration config, Scan scan) {
            this.statsCollector = s;
            this.region = r;
            this.innerScanner = rs;
            this.config = config;
            this.scan = scan;
        }

        @Override
        public Long call() throws IOException {
            return collectStatsInternal();
        }

        private boolean areStatsBeingCollectedViaCompaction() {
            return StatisticsCollectionRunTracker.getInstance(config)
                    .areStatsBeingCollectedOnCompaction(region.getRegionInfo());
        }

        private long collectStatsInternal() throws IOException {
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            region.startRegionOperation();
            boolean hasMore = false;
            boolean noErrors = false;
            boolean compactionRunning = areStatsBeingCollectedViaCompaction();
            long rowCount = 0;
            try {
                if (!compactionRunning) {
                    statsCollector.init();
                    synchronized (innerScanner) {
                        do {
                            List<Cell> results = new ArrayList<>();
                            hasMore = innerScanner.nextRaw(results);
                            statsCollector.collectStatistics(results);
                            rowCount++;
                            compactionRunning = areStatsBeingCollectedViaCompaction();
                        } while (hasMore && !compactionRunning);
                        noErrors = true;
                    }
                }
                return compactionRunning ? COMPACTION_UPDATE_STATS_ROW_COUNT : rowCount;
            } catch (IOException e) {
                LOGGER.error("IOException in update stats: " + Throwables.getStackTraceAsString(e));
                throw e;
            } finally {
                try {
                    if (noErrors && !compactionRunning) {
                        statsCollector.updateStatistics(region, scan);
                        LOGGER.info("UPDATE STATISTICS finished successfully for scanner: "
                                + innerScanner + ". Number of rows scanned: " + rowCount
                                + ". Time: " + (EnvironmentEdgeManager.currentTimeMillis() - startTime));
                    }
                    if (compactionRunning) {
                        LOGGER.info("UPDATE STATISTICS stopped in between because major compaction was running for region "
                                + region.getRegionInfo().getRegionNameAsString());
                    }
                } finally {
                    try {
                        StatisticsCollectionRunTracker.getInstance(config).removeUpdateStatsCommandRegion(region.getRegionInfo(), scan.getFamilyMap().keySet());
                        statsCollector.close();
                    } finally {
                        try {
                            innerScanner.close();
                        } finally {
                            region.closeRegionOperation();
                        }
                    }
                }
            }
        }
    }

    static List<Expression> deserializeExpressions(byte[] b) {
        ByteArrayInputStream stream = new ByteArrayInputStream(b);
        try {
            DataInputStream input = new DataInputStream(stream);
            int size = WritableUtils.readVInt(input);
            List<Expression> selectExpressions = Lists.newArrayListWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                ExpressionType type = ExpressionType.values()[WritableUtils.readVInt(input)];
                Expression selectExpression = type.newInstance();
                selectExpression.readFields(input);
                selectExpressions.add(selectExpression);
            }
            return selectExpressions;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // Don't allow splitting/closing if operations need read and write to same region are going on in the
    // the coprocessors to avoid dead lock scenario. See PHOENIX-3111.
    private void waitForScansToFinish(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
        int maxWaitTime = c.getEnvironment().getConfiguration().getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
                HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
        long start = EnvironmentEdgeManager.currentTimeMillis();
        synchronized (lock) {
            isRegionClosingOrSplitting = true;
            while (scansReferenceCount > 0) {
                try {
                    lock.wait(1000);
                    if (EnvironmentEdgeManager.currentTimeMillis() - start >= maxWaitTime) {
                        isRegionClosingOrSplitting = false; // must reset in case split is not retried
                        throw new IOException(String.format(
                                "Operations like local index building/delete/upsert select"
                                        + " might be going on so not allowing to split/close. scansReferenceCount=%s region=%s",
                                scansReferenceCount,
                                c.getEnvironment().getRegionInfo().getRegionNameAsString()));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> c,
                                 List<Pair<byte[], String>> familyPaths) throws IOException {
        // Don't allow bulkload if operations need read and write to same region are going on in the
        // the coprocessors to avoid dead lock scenario. See PHOENIX-3111.
        synchronized (lock) {
            if (scansReferenceCount > 0) {
                throw new DoNotRetryIOException("Operations like local index building/delete/upsert select"
                        + " might be going on so not allowing to bulkload.");
            }
        }
    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
            throws IOException {
        waitForScansToFinish(c);
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserverConstants.UNGROUPED_AGG) != null;
    }
}
