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

import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB;
import static org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker.COMPACTION_UPDATE_STATS_ROW_COUNT;
import static org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker.CONCURRENT_UPDATE_STATS_ROW_COUNT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
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
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker;
import org.apache.phoenix.schema.stats.StatisticsCollector;
import org.apache.phoenix.schema.stats.StatisticsCollectorFactory;
import org.apache.phoenix.schema.tuple.EncodedColumnQualiferCellsList;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.PositionBasedMultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;


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
    private static final Logger logger = LoggerFactory.getLogger(UngroupedAggregateRegionObserver.class);
    private KeyValueBuilder kvBuilder;
    private Configuration upsertSelectConfig;
    private Configuration compactionConfig;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        // Can't use ClientKeyValueBuilder on server-side because the memstore expects to
        // be able to get a single backing buffer for a KeyValue.
        this.kvBuilder = GenericKeyValueBuilder.INSTANCE;
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

        compactionConfig = PropertiesUtil.cloneConfig(e.getConfiguration());
        // lower the number of rpc retries, so we don't hang the compaction
        compactionConfig.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
            e.getConfiguration().getInt(QueryServices.METADATA_WRITE_RETRIES_NUMBER,
                QueryServicesOptions.DEFAULT_METADATA_WRITE_RETRIES_NUMBER));
        compactionConfig.setInt(HConstants.HBASE_CLIENT_PAUSE,
            e.getConfiguration().getInt(QueryServices.METADATA_WRITE_RETRY_PAUSE,
                QueryServicesOptions.DEFAULT_METADATA_WRITE_RETRY_PAUSE));
    }

    private void commitBatch(Region region, List<Mutation> mutations, long blockingMemstoreSize) throws IOException {
      if (mutations.isEmpty()) {
          return;
      }

        Mutation[] mutationArray = new Mutation[mutations.size()];
      // When memstore size reaches blockingMemstoreSize we are waiting 3 seconds for the
      // flush happen which decrease the memstore size and then writes allowed on the region.
      for (int i = 0; (region.getMemStoreHeapSize() + region.getMemStoreOffHeapSize()) > blockingMemstoreSize
                && i < 30; i++) {
          try {
              checkForRegionClosing();
              Thread.sleep(100);
          } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException(e);
          }
      }
      // TODO: should we use the one that is all or none?
      logger.debug("Committing bactch of " + mutations.size() + " mutations for " + region.getRegionInfo().getTable().getNameAsString());
      region.batchMutate(mutations.toArray(mutationArray));
    }

    private void setIndexAndTransactionProperties(List<Mutation> mutations, byte[] indexUUID,
            byte[] indexMaintainersPtr, byte[] txState, byte[] clientVersionBytes,
            boolean useIndexProto) {
        for (Mutation m : mutations) {
           if (indexMaintainersPtr != null) {
               m.setAttribute(useIndexProto ? PhoenixIndexCodec.INDEX_PROTO_MD : PhoenixIndexCodec.INDEX_MD, indexMaintainersPtr);
           }
           if (indexUUID != null) {
             m.setAttribute(PhoenixIndexCodec.INDEX_UUID, indexUUID);
           }
           if (txState != null) {
               m.setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
           }
           if (clientVersionBytes != null) {
               m.setAttribute(PhoenixIndexCodec.CLIENT_VERSION, clientVersionBytes);
           }
        }
    }

    private void commitBatchWithHTable(Table table, List<Mutation> mutations) throws IOException {
      if (mutations.isEmpty()) {
          return;
      }

        logger.debug("Committing batch of " + mutations.size() + " mutations for " + table);
        try {
            table.batch(mutations, null);
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
    private void checkForRegionClosing() throws IOException {
        synchronized (lock) {
            if(isRegionClosingOrSplitting) {
                lock.notifyAll();
                throw new IOException("Region is getting closed. Not allowing to write to avoid possible deadlock.");
            }
        }
    }

    public static void serializeIntoScan(Scan scan) {
        scan.setAttribute(BaseScannerRegionObserver.UNGROUPED_AGG, QueryConstants.TRUE);
    }

    @Override
    public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
            Scan scan) throws IOException {
        super.preScannerOpen(c, scan);
        if (ScanUtil.isAnalyzeTable(scan)) {
            // We are setting the start row and stop row such that it covers the entire region. As part
            // of Phonenix-1263 we are storing the guideposts against the physical table rather than
            // individual tenant specific tables.
            scan.withStartRow(HConstants.EMPTY_START_ROW);
            scan.withStopRow(HConstants.EMPTY_END_ROW);
            scan.setFilter(null);
        }
    }

   public static class MutationList extends ArrayList<Mutation> {
        private long byteSize = 0l;
        public MutationList() {
            super();
        }
        
        public MutationList(int size){
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
    
    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws IOException, SQLException {
        RegionCoprocessorEnvironment env = c.getEnvironment();
        Region region = env.getRegion();
        long ts = scan.getTimeRange().getMax();
        boolean localIndexScan = ScanUtil.isLocalIndex(scan);
        if (ScanUtil.isAnalyzeTable(scan)) {
            byte[] gp_width_bytes =
                    scan.getAttribute(BaseScannerRegionObserver.GUIDEPOST_WIDTH_BYTES);
            byte[] gp_per_region_bytes =
                    scan.getAttribute(BaseScannerRegionObserver.GUIDEPOST_PER_REGION);
            // Let this throw, as this scan is being done for the sole purpose of collecting stats
            StatisticsCollector statsCollector = StatisticsCollectorFactory.createStatisticsCollector(
                    env, region.getRegionInfo().getTable().getNameAsString(), ts,
                    gp_width_bytes, gp_per_region_bytes);
            return collectStats(s, statsCollector, region, scan, env.getConfiguration());
        } else if (ScanUtil.isIndexRebuild(scan)) { return rebuildIndices(s, region, scan, env.getConfiguration()); }
        int offsetToBe = 0;
        if (localIndexScan) {
            /*
             * For local indexes, we need to set an offset on row key expressions to skip
             * the region start key.
             */
            offsetToBe = region.getRegionInfo().getStartKey().length != 0 ? region.getRegionInfo().getStartKey().length :
                region.getRegionInfo().getEndKey().length;
            ScanUtil.setRowKeyOffset(scan, offsetToBe);
        }
        final int offset = offsetToBe;
        
        PTable projectedTable = null;
        PTable writeToTable = null;
        byte[][] values = null;
        byte[] descRowKeyTableBytes = scan.getAttribute(UPGRADE_DESC_ROW_KEY);
        boolean isDescRowKeyOrderUpgrade = descRowKeyTableBytes != null;
        if (isDescRowKeyOrderUpgrade) {
            logger.debug("Upgrading row key for " + region.getRegionInfo().getTable().getNameAsString());
            projectedTable = deserializeTable(descRowKeyTableBytes);
            try {
                writeToTable = PTableImpl.makePTable(projectedTable, true);
            } catch (SQLException e) {
                ServerUtil.throwIOException("Upgrade failed", e); // Impossible
            }
            values = new byte[projectedTable.getPKColumns().size()][];
        }
        boolean useProto = false;
        byte[] localIndexBytes = scan.getAttribute(LOCAL_INDEX_BUILD_PROTO);
        useProto = localIndexBytes != null;
        if (localIndexBytes == null) {
            localIndexBytes = scan.getAttribute(LOCAL_INDEX_BUILD);
        }
        List<IndexMaintainer> indexMaintainers = localIndexBytes == null ? null : IndexMaintainer.deserialize(localIndexBytes, useProto);
        MutationList indexMutations = localIndexBytes == null ? new MutationList() : new MutationList(1024);
        
        RegionScanner theScanner = s;
        
        byte[] replayMutations = scan.getAttribute(BaseScannerRegionObserver.REPLAY_WRITES);
        byte[] indexUUID = scan.getAttribute(PhoenixIndexCodec.INDEX_UUID);
        byte[] txState = scan.getAttribute(BaseScannerRegionObserver.TX_STATE);
        List<Expression> selectExpressions = null;
        byte[] upsertSelectTable = scan.getAttribute(BaseScannerRegionObserver.UPSERT_SELECT_TABLE);
        boolean isUpsert = false;
        boolean isDelete = false;
        byte[] deleteCQ = null;
        byte[] deleteCF = null;
        byte[] emptyCF = null;
        Table targetHTable = null;
        boolean isPKChanging = false;
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (upsertSelectTable != null) {
            isUpsert = true;
            projectedTable = deserializeTable(upsertSelectTable);
            targetHTable =
                    ConnectionFactory.createConnection(upsertSelectConfig).getTable(
                        TableName.valueOf(projectedTable.getPhysicalName().getBytes()));
            selectExpressions = deserializeExpressions(scan.getAttribute(BaseScannerRegionObserver.UPSERT_SELECT_EXPRS));
            values = new byte[projectedTable.getPKColumns().size()][];
            isPKChanging = ExpressionUtil.isPkPositionChanging(new TableRef(projectedTable), selectExpressions);
        } else {
            byte[] isDeleteAgg = scan.getAttribute(BaseScannerRegionObserver.DELETE_AGG);
            isDelete = isDeleteAgg != null && Bytes.compareTo(PDataType.TRUE_BYTES, isDeleteAgg) == 0;
            if (!isDelete) {
                deleteCF = scan.getAttribute(BaseScannerRegionObserver.DELETE_CF);
                deleteCQ = scan.getAttribute(BaseScannerRegionObserver.DELETE_CQ);
            }
            emptyCF = scan.getAttribute(BaseScannerRegionObserver.EMPTY_CF);
        }
        TupleProjector tupleProjector = null;
        byte[][] viewConstants = null;
        ColumnReference[] dataColumns = IndexUtil.deserializeDataTableColumnsToJoin(scan);
        final TupleProjector p = TupleProjector.deserializeProjectorFromScan(scan);
        final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
        boolean useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan));
        if ((localIndexScan && !isDelete && !isDescRowKeyOrderUpgrade) || (j == null && p != null)) {
            if (dataColumns != null) {
                tupleProjector = IndexUtil.getTupleProjector(scan, dataColumns);
                viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
            }
            ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
            theScanner =
                    getWrappedScanner(c, theScanner, offset, scan, dataColumns, tupleProjector, 
                        region, indexMaintainers == null ? null : indexMaintainers.get(0), viewConstants, p, tempPtr, useQualifierAsIndex);
        } 
        
        if (j != null)  {
            theScanner = new HashJoinRegionScanner(theScanner, p, j, ScanUtil.getTenantId(scan), env, useQualifierAsIndex, useNewValueColumnQualifier);
        }
        
        int maxBatchSize = 0;
        long maxBatchSizeBytes = 0L;
        MutationList mutations = new MutationList();
        boolean needToWrite = false;
        Configuration conf = env.getConfiguration();
        long flushSize = region.getTableDescriptor().getMemStoreFlushSize();

        if (flushSize <= 0) {
            flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                    TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
        }

        /**
         * Slow down the writes if the memstore size more than
         * (hbase.hregion.memstore.block.multiplier - 1) times hbase.hregion.memstore.flush.size
         * bytes. This avoids flush storm to hdfs for cases like index building where reads and
         * write happen to all the table regions in the server.
         */
        final long blockingMemStoreSize = flushSize * (
                conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
                        HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER)-1) ;

        boolean buildLocalIndex = indexMaintainers != null && dataColumns==null && !localIndexScan;
        if(buildLocalIndex) {
            checkForLocalIndexColumnFamilies(region, indexMaintainers);
        }
        if (isDescRowKeyOrderUpgrade || isDelete || isUpsert || (deleteCQ != null && deleteCF != null) || emptyCF != null || buildLocalIndex) {
            needToWrite = true;
            maxBatchSize = conf.getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
            mutations = new MutationList(Ints.saturatedCast(maxBatchSize + maxBatchSize / 10));
            maxBatchSizeBytes = conf.getLong(MUTATE_BATCH_SIZE_BYTES_ATTRIB,
                QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE_BYTES);
        }
        Aggregators aggregators = ServerAggregators.deserialize(
                scan.getAttribute(BaseScannerRegionObserver.AGGREGATORS), conf);
        Aggregator[] rowAggregators = aggregators.getAggregators();
        boolean hasMore;
        boolean hasAny = false;
        Pair<Integer, Integer> minMaxQualifiers = EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan);
        Tuple result = useQualifierAsIndex ? new PositionBasedMultiKeyValueTuple() : new MultiKeyValueTuple();
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("Starting ungrouped coprocessor scan " + scan + " "+region.getRegionInfo(), ScanUtil.getCustomAnnotations(scan)));
        }
        int rowCount = 0;
        final RegionScanner innerScanner = theScanner;
        boolean useIndexProto = true;
        byte[] indexMaintainersPtr = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
        // for backward compatiblity fall back to look by the old attribute
        if (indexMaintainersPtr == null) {
            indexMaintainersPtr = scan.getAttribute(PhoenixIndexCodec.INDEX_MD);
            useIndexProto = false;
        }

        byte[] clientVersionBytes = scan.getAttribute(PhoenixIndexCodec.CLIENT_VERSION);
        boolean acquiredLock = false;
        boolean incrScanRefCount = false;
        final TenantCache tenantCache = GlobalCache.getTenantCache(env, ScanUtil.getTenantId(scan));
        try (MemoryChunk em = tenantCache.getMemoryManager().allocate(0)) {
            if(needToWrite) {
                synchronized (lock) {
                    if (isRegionClosingOrSplitting) {
                        throw new IOException("Temporarily unable to write from scan because region is closing or splitting");
                    }
                    scansReferenceCount++;
                    incrScanRefCount = true;
                    lock.notifyAll();
                }
            }
            region.startRegionOperation();
            acquiredLock = true;
            long size = 0;
            synchronized (innerScanner) {
                do {
                    List<Cell> results = useQualifierAsIndex ? new EncodedColumnQualiferCellsList(minMaxQualifiers.getFirst(), minMaxQualifiers.getSecond(), encodingScheme) : new ArrayList<Cell>();
                    // Results are potentially returned even when the return value of s.next is false
                    // since this is an indication of whether or not there are more values after the
                    // ones returned
                    hasMore = innerScanner.nextRaw(results);
                    if (!results.isEmpty()) {
                        rowCount++;
                        result.setKeyValues(results);
                        if (isDescRowKeyOrderUpgrade) {
                            Arrays.fill(values, null);
                            Cell firstKV = results.get(0);
                            RowKeySchema schema = projectedTable.getRowKeySchema();
                            int maxOffset = schema.iterator(firstKV.getRowArray(), firstKV.getRowOffset() + offset, firstKV.getRowLength(), ptr);
                            for (int i = 0; i < schema.getFieldCount(); i++) {
                                Boolean hasValue = schema.next(ptr, i, maxOffset);
                                if (hasValue == null) {
                                    break;
                                }
                                Field field = schema.getField(i);
                                if (field.getSortOrder() == SortOrder.DESC) {
                                    // Special case for re-writing DESC ARRAY, as the actual byte value needs to change in this case
                                    if (field.getDataType().isArrayType()) {
                                        field.getDataType().coerceBytes(ptr, null, field.getDataType(),
                                            field.getMaxLength(), field.getScale(), field.getSortOrder(), 
                                            field.getMaxLength(), field.getScale(), field.getSortOrder(), true); // force to use correct separator byte
                                    }
                                    // Special case for re-writing DESC CHAR or DESC BINARY, to force the re-writing of trailing space characters
                                    else if (field.getDataType() == PChar.INSTANCE || field.getDataType() == PBinary.INSTANCE) {
                                        int len = ptr.getLength();
                                        while (len > 0 && ptr.get()[ptr.getOffset() + len - 1] == StringUtil.SPACE_UTF8) {
                                            len--;
                                        }
                                        ptr.set(ptr.get(), ptr.getOffset(), len);
                                        // Special case for re-writing DESC FLOAT and DOUBLE, as they're not inverted like they should be (PHOENIX-2171)
                                    } else if (field.getDataType() == PFloat.INSTANCE || field.getDataType() == PDouble.INSTANCE) {
                                        byte[] invertedBytes = SortOrder.invert(ptr.get(), ptr.getOffset(), ptr.getLength());
                                        ptr.set(invertedBytes);
                                    }
                                } else if (field.getDataType() == PBinary.INSTANCE) {
                                    // Remove trailing space characters so that the setValues call below will replace them
                                    // with the correct zero byte character. Note this is somewhat dangerous as these
                                    // could be legit, but I don't know what the alternative is.
                                    int len = ptr.getLength();
                                    while (len > 0 && ptr.get()[ptr.getOffset() + len - 1] == StringUtil.SPACE_UTF8) {
                                        len--;
                                    }
                                    ptr.set(ptr.get(), ptr.getOffset(), len);                                        
                                }
                                values[i] = ptr.copyBytes();
                            }
                            writeToTable.newKey(ptr, values);
                            if (Bytes.compareTo(
                                firstKV.getRowArray(), firstKV.getRowOffset() + offset, firstKV.getRowLength(), 
                                ptr.get(),ptr.getOffset() + offset,ptr.getLength()) == 0) {
                                continue;
                            }
                            byte[] newRow = ByteUtil.copyKeyBytesIfNecessary(ptr);
                            if (offset > 0) { // for local indexes (prepend region start key)
                                byte[] newRowWithOffset = new byte[offset + newRow.length];
                                System.arraycopy(firstKV.getRowArray(), firstKV.getRowOffset(), newRowWithOffset, 0, offset);;
                                System.arraycopy(newRow, 0, newRowWithOffset, offset, newRow.length);
                                newRow = newRowWithOffset;
                            }
                            byte[] oldRow = Bytes.copy(firstKV.getRowArray(), firstKV.getRowOffset(), firstKV.getRowLength());
                            for (Cell cell : results) {
                                // Copy existing cell but with new row key
                                Cell newCell = new KeyValue(newRow, 0, newRow.length,
                                    cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                                    cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                                    cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
                                    cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                switch (KeyValue.Type.codeToType(cell.getTypeByte())) {
                                case Put:
                                    // If Put, point delete old Put
                                    Delete del = new Delete(oldRow);
                                    del.addDeleteMarker(new KeyValue(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
                                        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                                        cell.getQualifierArray(), cell.getQualifierOffset(),
                                        cell.getQualifierLength(), cell.getTimestamp(), KeyValue.Type.Delete,
                                        ByteUtil.EMPTY_BYTE_ARRAY, 0, 0));
                                    mutations.add(del);

                                    Put put = new Put(newRow);
                                    put.add(newCell);
                                    mutations.add(put);
                                    break;
                                case Delete:
                                case DeleteColumn:
                                case DeleteFamily:
                                case DeleteFamilyVersion:
                                    Delete delete = new Delete(newRow);
                                    delete.addDeleteMarker(newCell);
                                    mutations.add(delete);
                                    break;
                                }
                            }
                        } else if (buildLocalIndex) {
                            for (IndexMaintainer maintainer : indexMaintainers) {
                                if (!results.isEmpty()) {
                                    result.getKey(ptr);
                                    ValueGetter valueGetter =
                                            maintainer.createGetterFromKeyValues(
                                                ImmutableBytesPtr.copyBytesIfNecessary(ptr),
                                                results);
                                    Put put = maintainer.buildUpdateMutation(kvBuilder,
                                        valueGetter, ptr, results.get(0).getTimestamp(),
                                        env.getRegion().getRegionInfo().getStartKey(),
                                        env.getRegion().getRegionInfo().getEndKey());
                                    indexMutations.add(put);
                                }
                            }
                            result.setKeyValues(results);
                        } else if (isDelete) {
                            // FIXME: the version of the Delete constructor without the lock
                            // args was introduced in 0.94.4, thus if we try to use it here
                            // we can no longer use the 0.94.2 version of the client.
                            Cell firstKV = results.get(0);
                            Delete delete = new Delete(firstKV.getRowArray(),
                                firstKV.getRowOffset(), firstKV.getRowLength(),ts);
                            if (replayMutations != null) {
                                delete.setAttribute(REPLAY_WRITES, replayMutations);
                            }
                            mutations.add(delete);
                            // force tephra to ignore this deletes
                            delete.setAttribute(PhoenixTransactionContext.TX_ROLLBACK_ATTRIBUTE_KEY, new byte[0]);
                        } else if (isUpsert) {
                            Arrays.fill(values, null);
                            int bucketNumOffset = 0;
                            if (projectedTable.getBucketNum() != null) {
                                values[0] = new byte[] { 0 };
                                bucketNumOffset = 1;
                            }
                            int i = bucketNumOffset;
                            List<PColumn> projectedColumns = projectedTable.getColumns();
                            for (; i < projectedTable.getPKColumns().size(); i++) {
                                Expression expression = selectExpressions.get(i - bucketNumOffset);
                                if (expression.evaluate(result, ptr)) {
                                    values[i] = ptr.copyBytes();
                                    // If SortOrder from expression in SELECT doesn't match the
                                    // column being projected into then invert the bits.
                                    if (expression.getSortOrder() !=
                                            projectedColumns.get(i).getSortOrder()) {
                                        SortOrder.invert(values[i], 0, values[i], 0,
                                            values[i].length);
                                    }
                                }else{
                                    values[i] = ByteUtil.EMPTY_BYTE_ARRAY;
                                }
                            }
                            projectedTable.newKey(ptr, values);
                            PRow row = projectedTable.newRow(kvBuilder, ts, ptr, false);
                            for (; i < projectedColumns.size(); i++) {
                                Expression expression = selectExpressions.get(i - bucketNumOffset);
                                if (expression.evaluate(result, ptr)) {
                                    PColumn column = projectedColumns.get(i);
                                    if (!column.getDataType().isSizeCompatible(ptr, null,
                                        expression.getDataType(), expression.getSortOrder(),
                                        expression.getMaxLength(), expression.getScale(),
                                        column.getMaxLength(), column.getScale())) {
                                        throw new DataExceedsCapacityException(
                                            column.getDataType(), column.getMaxLength(),
                                            column.getScale(), column.getName().getString(), ptr);
                                    }
                                    column.getDataType().coerceBytes(ptr, null,
                                        expression.getDataType(), expression.getMaxLength(),
                                        expression.getScale(), expression.getSortOrder(), 
                                        column.getMaxLength(), column.getScale(),
                                        column.getSortOrder(), projectedTable.rowKeyOrderOptimizable());
                                    byte[] bytes = ByteUtil.copyKeyBytesIfNecessary(ptr);
                                    row.setValue(column, bytes);
                                }
                            }
                            for (Mutation mutation : row.toRowMutations()) {
                                if (replayMutations != null) {
                                    mutation.setAttribute(REPLAY_WRITES, replayMutations);
                                }
                                mutations.add(mutation);
                            }
                            for (i = 0; i < selectExpressions.size(); i++) {
                                selectExpressions.get(i).reset();
                            }
                        } else if (deleteCF != null && deleteCQ != null) {
                            // No need to search for delete column, since we project only it
                            // if no empty key value is being set
                            if (emptyCF == null ||
                                    result.getValue(deleteCF, deleteCQ) != null) {
                                Delete delete = new Delete(results.get(0).getRowArray(),
                                    results.get(0).getRowOffset(),
                                    results.get(0).getRowLength());
                                delete.addColumns(deleteCF,  deleteCQ, ts);
                                // force tephra to ignore this deletes
                                delete.setAttribute(PhoenixTransactionContext.TX_ROLLBACK_ATTRIBUTE_KEY, new byte[0]);
                                mutations.add(delete);
                            }
                        }
                        if (emptyCF != null) {
                            /*
                             * If we've specified an emptyCF, then we need to insert an empty
                             * key value "retroactively" for any key value that is visible at
                             * the timestamp that the DDL was issued. Key values that are not
                             * visible at this timestamp will not ever be projected up to
                             * scans past this timestamp, so don't need to be considered.
                             * We insert one empty key value per row per timestamp.
                             */
                            Set<Long> timeStamps =
                                    Sets.newHashSetWithExpectedSize(results.size());
                            for (Cell kv : results) {
                                long kvts = kv.getTimestamp();
                                if (!timeStamps.contains(kvts)) {
                                    Put put = new Put(kv.getRowArray(), kv.getRowOffset(),
                                        kv.getRowLength());
                                    put.addColumn(emptyCF, QueryConstants.EMPTY_COLUMN_BYTES, kvts,
                                        ByteUtil.EMPTY_BYTE_ARRAY);
                                    mutations.add(put);
                                }
                            }
                        }
                        if (ServerUtil.readyToCommit(mutations.size(), mutations.byteSize(), maxBatchSize, maxBatchSizeBytes)) {
                            commit(region, mutations, indexUUID, blockingMemStoreSize, indexMaintainersPtr,
                                txState, targetHTable, useIndexProto, isPKChanging, clientVersionBytes);
                            mutations.clear();
                        }
                        // Commit in batches based on UPSERT_BATCH_SIZE_BYTES_ATTRIB in config

                        if (ServerUtil.readyToCommit(indexMutations.size(), indexMutations.byteSize(), maxBatchSize, maxBatchSizeBytes)) {
                            setIndexAndTransactionProperties(indexMutations, indexUUID, indexMaintainersPtr, txState, clientVersionBytes, useIndexProto);
                            commitBatch(region, indexMutations, blockingMemStoreSize);
                            indexMutations.clear();
                        }
                        size += aggregators.aggregate(rowAggregators, result);
                        while(size > em.getSize()) {
                            logger.info("Request: {}, resizing {} by 1024*1024", size, em.getSize());
                            em.resize(em.getSize() + 1024*1024);
                        }
                        hasAny = true;
                    }
                } while (hasMore);
                if (!mutations.isEmpty()) {
                    commit(region, mutations, indexUUID, blockingMemStoreSize, indexMaintainersPtr, txState,
                        targetHTable, useIndexProto, isPKChanging, clientVersionBytes);
                    mutations.clear();
                }

                if (!indexMutations.isEmpty()) {
                    commitBatch(region, indexMutations, blockingMemStoreSize);
                    indexMutations.clear();
                }
            }
        } finally {
            if (needToWrite && incrScanRefCount) {
                synchronized (lock) {
                    scansReferenceCount--;
                    if (scansReferenceCount < 0) {
                        logger.warn(
                            "Scan reference count went below zero. Something isn't correct. Resetting it back to zero");
                        scansReferenceCount = 0;
                    }
                    lock.notifyAll();
                }
            }
            try {
                if (targetHTable != null) {
                    targetHTable.close();
                }
            } finally {
                try {
                    innerScanner.close();
                } finally {
                    if (acquiredLock) region.closeRegionOperation();
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("Finished scanning " + rowCount + " rows for ungrouped coprocessor scan " + scan, ScanUtil.getCustomAnnotations(scan)));
        }

        final boolean hadAny = hasAny;
        Cell keyValue = null;
        if (hadAny) {
            byte[] value = aggregators.toBytes(rowAggregators);
            keyValue = PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length);
        }
        final Cell aggKeyValue = keyValue;

        RegionScanner scanner = new BaseRegionScanner(innerScanner) {
            private boolean done = !hadAny;

            @Override
            public boolean isFilterDone() {
                return done;
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                if (done) return false;
                done = true;
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

    private void checkForLocalIndexColumnFamilies(Region region,
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
            if(coveredColumns.isEmpty()) {
                byte[] localIndexCf = indexMaintainer.getEmptyKeyValueFamily().get();
                // When covered columns empty we store index data in default column family so check for it.
                if (tableDesc.getColumnFamily(localIndexCf) == null) {
                    ServerUtil.throwIOException("Column Family Not Found",
                        new ColumnFamilyNotFoundException(schemaName, tableName, Bytes
                                .toString(localIndexCf)));
                }
            }
            for (ColumnReference reference : coveredColumns) {
                byte[] cf = IndexUtil.getLocalIndexColumnFamily(reference.getFamily());
                ColumnFamilyDescriptor family = region.getTableDescriptor().getColumnFamily(cf);
                if (family == null) {
                    ServerUtil.throwIOException("Column Family Not Found",
                        new ColumnFamilyNotFoundException(schemaName, tableName, Bytes.toString(cf)));
                }
            }
        }
    }

    private void commit(Region region, List<Mutation> mutations, byte[] indexUUID, long blockingMemStoreSize,
            byte[] indexMaintainersPtr, byte[] txState, Table targetHTable, boolean useIndexProto,
                        boolean isPKChanging, byte[] clientVersionBytes)
            throws IOException {
        List<Mutation> localRegionMutations = Lists.newArrayList();
        List<Mutation> remoteRegionMutations = Lists.newArrayList();
        setIndexAndTransactionProperties(mutations, indexUUID, indexMaintainersPtr, txState,
            clientVersionBytes, useIndexProto);
        separateLocalAndRemoteMutations(targetHTable, region, mutations, localRegionMutations, remoteRegionMutations,
            isPKChanging);
        commitBatch(region, localRegionMutations, blockingMemStoreSize);
        commitBatchWithHTable(targetHTable, remoteRegionMutations);
        localRegionMutations.clear();
        remoteRegionMutations.clear();
    }

    private void separateLocalAndRemoteMutations(Table targetHTable, Region region, List<Mutation> mutations,
                                                 List<Mutation> localRegionMutations, List<Mutation> remoteRegionMutations,
                                                 boolean isPKChanging){
        boolean areMutationsInSameTable = areMutationsInSameTable(targetHTable, region);
        //if we're writing to the same table, but the PK can change, that means that some
        //mutations might be in our current region, and others in a different one.
        if (areMutationsInSameTable && isPKChanging) {
            RegionInfo regionInfo = region.getRegionInfo();
            for (Mutation mutation : mutations){
                if (regionInfo.containsRow(mutation.getRow())){
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
    public InternalScanner preCompact(
        org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        InternalScanner scanner, ScanType scanType,
        org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker tracker,
        CompactionRequest request) throws IOException {
        if (scanType.equals(ScanType.COMPACT_DROP_DELETES)) {
            final TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
            // Compaction and split upcalls run with the effective user context of the requesting user.
            // This will lead to failure of cross cluster RPC if the effective user is not
            // the login user. Switch to the login user context to ensure we have the expected
            // security context.
            return User.runAsLoginUser(new PrivilegedExceptionAction<InternalScanner>() {
                @Override public InternalScanner run() throws Exception {
                    InternalScanner internalScanner = scanner;
                    try {
                        long clientTimeStamp = EnvironmentEdgeManager.currentTimeMillis();
                        DelegateRegionCoprocessorEnvironment compactionConfEnv = new DelegateRegionCoprocessorEnvironment(compactionConfig, c.getEnvironment());
                        StatisticsCollector stats = StatisticsCollectorFactory.createStatisticsCollector(
                            compactionConfEnv, table.getNameAsString(), clientTimeStamp,
                            store.getColumnFamilyDescriptor().getName());
                        internalScanner =
                                stats.createCompactionScanner(compactionConfEnv,
                                    store, scanner);
                    } catch (Exception e) {
                        // If we can't reach the stats table, don't interrupt the normal
                        // compaction operation, just log a warning.
                        if (logger.isWarnEnabled()) {
                            logger.warn("Unable to collect stats for " + table, e);
                        }
                    }
                    return internalScanner;
                }
            });
        }
        return scanner;
    }

    private static PTable deserializeTable(byte[] b) {
        try {
            PTableProtos.PTable ptableProto = PTableProtos.PTable.parseFrom(b);
            return PTableImpl.createFromProto(ptableProto);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private RegionScanner rebuildIndices(final RegionScanner innerScanner, final Region region, final Scan scan,
            Configuration config) throws IOException {
        byte[] indexMetaData = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
        boolean useProto = true;
        // for backward compatibility fall back to look up by the old attribute
        if (indexMetaData == null) {
            useProto = false;
            indexMetaData = scan.getAttribute(PhoenixIndexCodec.INDEX_MD);
        }
        byte[] clientVersionBytes = scan.getAttribute(PhoenixIndexCodec.CLIENT_VERSION);
        boolean hasMore;
        int rowCount = 0;
        try {
            int maxBatchSize = config.getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
            long maxBatchSizeBytes = config.getLong(MUTATE_BATCH_SIZE_BYTES_ATTRIB,
                QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE_BYTES);
            MutationList mutations = new MutationList(maxBatchSize);
            region.startRegionOperation();
            byte[] uuidValue = ServerCacheClient.generateId();
            synchronized (innerScanner) {
                do {
                    List<Cell> results = new ArrayList<Cell>();
                    hasMore = innerScanner.nextRaw(results);
                    if (!results.isEmpty()) {
                        Put put = null;
                        Delete del = null;
                        for (Cell cell : results) {

                            if (KeyValue.Type.codeToType(cell.getTypeByte()) == KeyValue.Type.Put) {
                                if (put == null) {
                                    put = new Put(CellUtil.cloneRow(cell));
                                    put.setAttribute(useProto ? PhoenixIndexCodec.INDEX_PROTO_MD : PhoenixIndexCodec.INDEX_MD, indexMetaData);
                                    put.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                                    put.setAttribute(REPLAY_WRITES, REPLAY_ONLY_INDEX_WRITES);
                                    put.setAttribute(PhoenixIndexCodec.CLIENT_VERSION, clientVersionBytes);
                                    mutations.add(put);
                                    // Since we're replaying existing mutations, it makes no sense to write them to the wal
                                    put.setDurability(Durability.SKIP_WAL);
                                }
                                put.add(cell);
                            } else {
                                if (del == null) {
                                    del = new Delete(CellUtil.cloneRow(cell));
                                    del.setAttribute(useProto ? PhoenixIndexCodec.INDEX_PROTO_MD : PhoenixIndexCodec.INDEX_MD, indexMetaData);
                                    del.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                                    del.setAttribute(REPLAY_WRITES, REPLAY_ONLY_INDEX_WRITES);
                                    del.setAttribute(PhoenixIndexCodec.CLIENT_VERSION, clientVersionBytes);
                                    mutations.add(del);
                                    // Since we're replaying existing mutations, it makes no sense to write them to the wal
                                    del.setDurability(Durability.SKIP_WAL);
                                }
                                del.addDeleteMarker(cell);
                            }
                        }
                        if (ServerUtil.readyToCommit(mutations.size(), mutations.byteSize(), maxBatchSize, maxBatchSizeBytes)) {
                            region.batchMutate(mutations.toArray(new Mutation[mutations.size()]));
                            uuidValue = ServerCacheClient.generateId();
                            mutations.clear();
                        }
                        rowCount++;
                    }
                    
                } while (hasMore);
                if (!mutations.isEmpty()) {
                    region.batchMutate(mutations.toArray(new Mutation[mutations.size()]));
                }
            }
        } catch (IOException e) {
            logger.error("IOException during rebuilding: " + Throwables.getStackTraceAsString(e));
            throw e;
        } finally {
            region.closeRegionOperation();
        }
        byte[] rowCountBytes = PLong.INSTANCE.toBytes(Long.valueOf(rowCount));
        final Cell aggKeyValue = PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY,
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
                innerScanner.close();
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
    
    private RegionScanner collectStats(final RegionScanner innerScanner, StatisticsCollector stats,
            final Region region, final Scan scan, Configuration config) throws IOException {
        StatsCollectionCallable callable =
                new StatsCollectionCallable(stats, region, innerScanner, config, scan);
        byte[] asyncBytes = scan.getAttribute(BaseScannerRegionObserver.RUN_UPDATE_STATS_ASYNC_ATTRIB);
        boolean async = false;
        if (asyncBytes != null) {
            async = Bytes.toBoolean(asyncBytes);
        }
        long rowCount = 0; // in case of async, we report 0 as number of rows updated
        StatisticsCollectionRunTracker statsRunTracker =
                StatisticsCollectionRunTracker.getInstance(config);
        boolean runUpdateStats = statsRunTracker.addUpdateStatsCommandRegion(region.getRegionInfo());
        if (runUpdateStats) {
            if (!async) {
                rowCount = callable.call();
            } else {
                statsRunTracker.runTask(callable);
            }
        } else {
            rowCount = CONCURRENT_UPDATE_STATS_ROW_COUNT;
            logger.info("UPDATE STATISTICS didn't run because another UPDATE STATISTICS command was already running on the region "
                    + region.getRegionInfo().getRegionNameAsString());
        }
        byte[] rowCountBytes = PLong.INSTANCE.toBytes(Long.valueOf(rowCount));
        final Cell aggKeyValue =
                PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY,
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
                // No-op because we want to manage closing of the inner scanner ourselves.
                // This happens inside StatsCollectionCallable.
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
                            List<Cell> results = new ArrayList<Cell>();
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
                logger.error("IOException in update stats: " + Throwables.getStackTraceAsString(e));
                throw e;
            } finally {
                try {
                    if (noErrors && !compactionRunning) {
                        statsCollector.updateStatistic(region, scan);
                        logger.info("UPDATE STATISTICS finished successfully for scanner: "
                                + innerScanner + ". Number of rows scanned: " + rowCount
                                + ". Time: " + (System.currentTimeMillis() - startTime));
                    }
                    if (compactionRunning) {
                        logger.info("UPDATE STATISTICS stopped in between because major compaction was running for region "
                                + region.getRegionInfo().getRegionNameAsString());
                    }
                } finally {
                    try {
                        StatisticsCollectionRunTracker.getInstance(config).removeUpdateStatsCommandRegion(region.getRegionInfo());
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

    private static List<Expression> deserializeExpressions(byte[] b) {
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

    public static byte[] serialize(PTable projectedTable) {
        PTableProtos.PTable ptableProto = PTableImpl.toProto(projectedTable);
        return ptableProto.toByteArray();
    }

    public static byte[] serialize(List<Expression> selectExpressions) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, selectExpressions.size());
            for (int i = 0; i < selectExpressions.size(); i++) {
                Expression expression = selectExpressions.get(i);
                WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
                expression.write(output);
            }
            return stream.toByteArray();
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
        synchronized (lock) {
            isRegionClosingOrSplitting = true;
            while (scansReferenceCount > 0) {
                try {
                    lock.wait(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserver.UNGROUPED_AGG) != null;
    }

    @Override
    public void preCompactScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Store store,
            ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
            final CompactionRequest request) throws IOException {
        // Compaction and split upcalls run with the effective user context of the requesting user.
        // This will lead to failure of cross cluster RPC if the effective user is not
        // the login user. Switch to the login user context to ensure we have the expected
        // security context.
        final String fullTableName = c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        // since we will make a call to syscat, do nothing if we are compacting syscat itself
        if (request.isMajor() && !PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME.equals(fullTableName)) {
            User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    // If the index is disabled, keep the deleted cells so the rebuild doesn't corrupt the index
                    try (PhoenixConnection conn =
                            QueryUtil.getConnectionOnServer(compactionConfig).unwrap(PhoenixConnection.class)) {
                        PTable table = PhoenixRuntime.getTableNoCache(conn, fullTableName);
                        List<PTable> indexes = PTableType.INDEX.equals(table.getType()) ? Lists.newArrayList(table) : table.getIndexes();
                        // FIXME need to handle views and indexes on views as well
                        for (PTable index : indexes) {
                            if (index.getIndexDisableTimestamp() != 0) {
                                logger.info(
                                    "Modifying major compaction scanner to retain deleted cells for a table with disabled index: "
                                            + fullTableName);
                                options.setKeepDeletedCells(KeepDeletedCells.TRUE);
                                options.readAllVersions();
                                options.setTTL(Long.MAX_VALUE);
                            }
                        }
                    } catch (Exception e) {
                        if (e instanceof TableNotFoundException) {
                            logger.debug("Ignoring HBase table that is not a Phoenix table: " + fullTableName);
                            // non-Phoenix HBase tables won't be found, do nothing
                        } else {
                            logger.error("Unable to modify compaction scanner to retain deleted cells for a table with disabled Index; "
                                    + fullTableName,
                                    e);
                        }
                    }
                    return null;
                }
            });
        }
    }
}
