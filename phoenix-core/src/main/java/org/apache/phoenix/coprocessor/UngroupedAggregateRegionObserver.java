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
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.ServerCacheClient;
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
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
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
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TimeKeeper;
import org.apache.tephra.TxConstants;
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
public class UngroupedAggregateRegionObserver extends BaseScannerRegionObserver {
    // TODO: move all constants into a single class
    public static final String UNGROUPED_AGG = "UngroupedAgg";
    public static final String DELETE_AGG = "DeleteAgg";
    public static final String DELETE_CQ = "DeleteCQ";
    public static final String DELETE_CF = "DeleteCF";
    public static final String EMPTY_CF = "EmptyCF";
    /**
     * This lock used for synchronizing the state of
     * {@link UngroupedAggregateRegionObserver#scansReferenceCount},
     * {@link UngroupedAggregateRegionObserver#isRegionClosing} variables used to avoid possible
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
    private boolean isRegionClosing = false;
    private static final Logger logger = LoggerFactory.getLogger(UngroupedAggregateRegionObserver.class);
    private KeyValueBuilder kvBuilder;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        // Can't use ClientKeyValueBuilder on server-side because the memstore expects to
        // be able to get a single backing buffer for a KeyValue.
        this.kvBuilder = GenericKeyValueBuilder.INSTANCE;
    }

    private void commitBatch(Region region, List<Mutation> mutations, byte[] indexUUID, long blockingMemstoreSize,
            byte[] indexMaintainersPtr, byte[] txState, boolean useIndexProto) throws IOException {
        if (indexMaintainersPtr != null) {
            mutations.get(0).setAttribute(useIndexProto ? PhoenixIndexCodec.INDEX_PROTO_MD : PhoenixIndexCodec.INDEX_MD, indexMaintainersPtr);
        }

        if (txState != null) {
            mutations.get(0).setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
        }
      if (indexUUID != null) {
          for (Mutation m : mutations) {
              m.setAttribute(PhoenixIndexCodec.INDEX_UUID, indexUUID);
          }
      }
      
      Mutation[] mutationArray = new Mutation[mutations.size()];
      // When memstore size reaches blockingMemstoreSize we are waiting 3 seconds for the
      // flush happen which decrease the memstore size and then writes allowed on the region.
      for (int i = 0; region.getMemstoreSize() > blockingMemstoreSize && i < 30; i++) {
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
      region.batchMutate(mutations.toArray(mutationArray), HConstants.NO_NONCE, HConstants.NO_NONCE);
    }
    
    private void commitBatchWithHTable(HTable table, Region region, List<Mutation> mutations, byte[] indexUUID,
            long blockingMemstoreSize, byte[] indexMaintainersPtr, byte[] txState, boolean useIndexProto) throws IOException {

        if (indexUUID != null) {
            // Need to add indexMaintainers for each mutation as table.batch can be distributed across servers
            for (Mutation m : mutations) {
                if (indexMaintainersPtr != null) {
                    m.setAttribute(useIndexProto ? PhoenixIndexCodec.INDEX_PROTO_MD : PhoenixIndexCodec.INDEX_MD, indexMaintainersPtr);
                }
                if (txState != null) {
                    m.setAttribute(BaseScannerRegionObserver.TX_STATE, txState);
                }
                m.setAttribute(PhoenixIndexCodec.INDEX_UUID, indexUUID);
            }
        }
        // When memstore size reaches blockingMemstoreSize we are waiting 3 seconds for the
        // flush happen which decrease the memstore size and then writes allowed on the region.
        for (int i = 0; region.getMemstoreSize() > blockingMemstoreSize && i < 30; i++) {
            try {
                checkForRegionClosing();
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
        logger.debug("Committing batch of " + mutations.size() + " mutations for " + table);
        try {
            table.batch(mutations);
        } catch (InterruptedException e) {
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
            if(isRegionClosing) {
                lock.notifyAll();
                throw new IOException("Region is getting closed. Not allowing to write to avoid possible deadlock.");
            }
        }
    }

    public static void serializeIntoScan(Scan scan) {
        scan.setAttribute(BaseScannerRegionObserver.UNGROUPED_AGG, QueryConstants.TRUE);
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
            throws IOException {
        s = super.preScannerOpen(e, scan, s);
        if (ScanUtil.isAnalyzeTable(scan)) {
//            if (!ScanUtil.isLocalIndex(scan)) {
//                scan.getFamilyMap().clear();
//            }
//            scan.getFamilyMap().clear();
            // We are setting the start row and stop row such that it covers the entire region. As part
            // of Phonenix-1263 we are storing the guideposts against the physical table rather than
            // individual tenant specific tables.
            scan.setStartRow(HConstants.EMPTY_START_ROW);
            scan.setStopRow(HConstants.EMPTY_END_ROW);
            scan.setFilter(null);
        }
        return s;
    }

    class MutationList extends ArrayList<Mutation> implements HeapSize {
        private long heapSize = 0l;
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
                incrementHeapSize(e.heapSize());
            }
            return r;
        }

        @Override
        public long heapSize() {
            return heapSize;
        }

        private void incrementHeapSize(long heapSize) {
            this.heapSize += heapSize;
        }

        @Override
        public void clear() {
            heapSize = 0l;
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
        
        byte[] indexUUID = scan.getAttribute(PhoenixIndexCodec.INDEX_UUID);
        byte[] txState = scan.getAttribute(BaseScannerRegionObserver.TX_STATE);
        List<Expression> selectExpressions = null;
        byte[] upsertSelectTable = scan.getAttribute(BaseScannerRegionObserver.UPSERT_SELECT_TABLE);
        boolean isUpsert = false;
        boolean isDelete = false;
        byte[] deleteCQ = null;
        byte[] deleteCF = null;
        byte[] emptyCF = null;
        HTable targetHTable = null;
        boolean areMutationInSameRegion = true;
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (upsertSelectTable != null) {
            isUpsert = true;
            projectedTable = deserializeTable(upsertSelectTable);
            targetHTable = new HTable(env.getConfiguration(), projectedTable.getPhysicalName().getBytes());
            selectExpressions = deserializeExpressions(scan.getAttribute(BaseScannerRegionObserver.UPSERT_SELECT_EXPRS));
            values = new byte[projectedTable.getPKColumns().size()][];
            areMutationInSameRegion = Bytes.compareTo(targetHTable.getTableName(),
                    region.getTableDesc().getTableName().getName()) == 0
                    && !isPkPositionChanging(new TableRef(projectedTable), selectExpressions);
            
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
        Configuration conf = c.getEnvironment().getConfiguration();
        long flushSize = region.getTableDesc().getMemStoreFlushSize();

        if (flushSize <= 0) {
            flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                    HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE);
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
        if (isDescRowKeyOrderUpgrade || isDelete || isUpsert || (deleteCQ != null && deleteCF != null) || emptyCF != null || buildLocalIndex) {
            needToWrite = true;
            maxBatchSize = env.getConfiguration().getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
            mutations = new MutationList(Ints.saturatedCast(maxBatchSize + maxBatchSize / 10));
            maxBatchSizeBytes = env.getConfiguration().getLong(MUTATE_BATCH_SIZE_BYTES_ATTRIB,
                QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE_BYTES);
        }
        Aggregators aggregators = ServerAggregators.deserialize(
                scan.getAttribute(BaseScannerRegionObserver.AGGREGATORS), env.getConfiguration());
        Aggregator[] rowAggregators = aggregators.getAggregators();
        boolean hasMore;
        boolean hasAny = false;
        Pair<Integer, Integer> minMaxQualifiers = EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan);
        Tuple result = useQualifierAsIndex ? new PositionBasedMultiKeyValueTuple() : new MultiKeyValueTuple();
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("Starting ungrouped coprocessor scan " + scan + " "+region.getRegionInfo(), ScanUtil.getCustomAnnotations(scan)));
        }
        long rowCount = 0;
        final RegionScanner innerScanner = theScanner;
        boolean useIndexProto = true;
        byte[] indexMaintainersPtr = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
        // for backward compatiblity fall back to look by the old attribute
        if (indexMaintainersPtr == null) {
            indexMaintainersPtr = scan.getAttribute(PhoenixIndexCodec.INDEX_MD);
            useIndexProto = false;
        }
        boolean acquiredLock = false;
        try {
            if(needToWrite) {
                synchronized (lock) {
                    scansReferenceCount++;
                }
            }
            region.startRegionOperation();
            acquiredLock = true;
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
                            mutations.add(delete);
                            // force tephra to ignore this deletes
                            delete.setAttribute(TxConstants.TX_ROLLBACK_ATTRIBUTE_KEY, new byte[0]);
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
                                delete.deleteColumns(deleteCF,  deleteCQ, ts);
                                // force tephra to ignore this deletes
                                delete.setAttribute(TxConstants.TX_ROLLBACK_ATTRIBUTE_KEY, new byte[0]);
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
                                    put.add(emptyCF, QueryConstants.EMPTY_COLUMN_BYTES, kvts,
                                        ByteUtil.EMPTY_BYTE_ARRAY);
                                    mutations.add(put);
                                }
                            }
                        }
                        if (readyToCommit(mutations, maxBatchSize, maxBatchSizeBytes)) {
                            commit(region, mutations, indexUUID, blockingMemStoreSize, indexMaintainersPtr, txState,
                                    areMutationInSameRegion, targetHTable, useIndexProto);
                            mutations.clear();
                        }
                        // Commit in batches based on UPSERT_BATCH_SIZE_BYTES_ATTRIB in config

                        if (readyToCommit(indexMutations, maxBatchSize, maxBatchSizeBytes)) {
                            commitBatch(region, indexMutations, null, blockingMemStoreSize, null, txState,
                                    useIndexProto);
                            indexMutations.clear();
                        }
                        aggregators.aggregate(rowAggregators, result);
                        hasAny = true;
                    }
                } while (hasMore);
                if (!mutations.isEmpty()) {
                    commit(region, mutations, indexUUID, blockingMemStoreSize, indexMaintainersPtr, txState,
                            areMutationInSameRegion, targetHTable, useIndexProto);
                    mutations.clear();
                }

                if (!indexMutations.isEmpty()) {
                    commitBatch(region, indexMutations, null, blockingMemStoreSize, indexMaintainersPtr, txState, useIndexProto);
                    indexMutations.clear();
                }
            }
        } finally {
            if(needToWrite) {
                synchronized (lock) {
                    scansReferenceCount--;
                }
            }
            if (targetHTable != null) {
                targetHTable.close();
            }
            try {
                innerScanner.close();
            } finally {
                if (acquiredLock) region.closeRegionOperation();
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("Finished scanning " + rowCount + " rows for ungrouped coprocessor scan " + scan, ScanUtil.getCustomAnnotations(scan)));
        }

        final boolean hadAny = hasAny;
        KeyValue keyValue = null;
        if (hadAny) {
            byte[] value = aggregators.toBytes(rowAggregators);
            keyValue = KeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length);
        }
        final KeyValue aggKeyValue = keyValue;

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

    private void commit(Region region, List<Mutation> mutations, byte[] indexUUID, long blockingMemstoreSize,
            byte[] indexMaintainersPtr, byte[] txState, boolean areMutationsForSameRegion, HTable hTable, boolean useIndexProto)
            throws IOException {
        if (!areMutationsForSameRegion) {
            assert hTable != null;// table cannot be null
            commitBatchWithHTable(hTable, region, mutations, indexUUID, blockingMemstoreSize, indexMaintainersPtr,
                    txState, useIndexProto);
        } else {
            commitBatch(region, mutations, indexUUID, blockingMemstoreSize, indexMaintainersPtr, txState, useIndexProto);
        }
    }

    private boolean isPkPositionChanging(TableRef tableRef, List<Expression> projectedExpressions) throws SQLException {
        // If the row ends up living in a different region, we'll get an error otherwise.
        for (int i = 0; i < tableRef.getTable().getPKColumns().size(); i++) {
            PColumn column = tableRef.getTable().getPKColumns().get(i);
            Expression source = projectedExpressions.get(i);
            if (source == null || !source
                    .equals(new ColumnRef(tableRef, column.getPosition()).newColumnExpression())) { return true; }
        }
        return false;
    }

    private boolean readyToCommit(MutationList mutations, int maxBatchSize, long maxBatchSizeBytes) {
        return !mutations.isEmpty() && (maxBatchSize > 0 && mutations.size() > maxBatchSize)
                || (maxBatchSizeBytes > 0 && mutations.heapSize() > maxBatchSizeBytes);
    }

    @Override
    public InternalScanner preCompact(final ObserverContext<RegionCoprocessorEnvironment> c, final Store store,
            final InternalScanner scanner, final ScanType scanType) throws IOException {
        // Compaction and split upcalls run with the effective user context of the requesting user.
        // This will lead to failure of cross cluster RPC if the effective user is not
        // the login user. Switch to the login user context to ensure we have the expected
        // security context.
        return User.runAsLoginUser(new PrivilegedExceptionAction<InternalScanner>() {
            @Override
            public InternalScanner run() throws Exception {
                TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
                InternalScanner internalScanner = scanner;
                if (scanType.equals(ScanType.COMPACT_DROP_DELETES)) {
                    try {
                        long clientTimeStamp = TimeKeeper.SYSTEM.getCurrentTime();
                        StatisticsCollector stats = StatisticsCollectorFactory.createStatisticsCollector(
                            c.getEnvironment(), table.getNameAsString(), clientTimeStamp,
                            store.getFamily().getName());
                        internalScanner = stats.createCompactionScanner(c.getEnvironment(), store, scanner);
                    } catch (IOException e) {
                        // If we can't reach the stats table, don't interrupt the normal
                      // compaction operation, just log a warning.
                      if (logger.isWarnEnabled()) {
                          logger.warn("Unable to collect stats for " + table, e);
                      }
                    }
                }
                return internalScanner;
            }
        });
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
        boolean hasMore;
        long rowCount = 0;
        try {
            int batchSize = config.getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
            List<Mutation> mutations = Lists.newArrayListWithExpectedSize(batchSize);
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
                                    put.setAttribute(BaseScannerRegionObserver.IGNORE_NEWER_MUTATIONS,
                                            PDataType.TRUE_BYTES);
                                    mutations.add(put);
                                }
                                put.add(cell);
                            } else {
                                if (del == null) {
                                    del = new Delete(CellUtil.cloneRow(cell));
                                    del.setAttribute(useProto ? PhoenixIndexCodec.INDEX_PROTO_MD : PhoenixIndexCodec.INDEX_MD, indexMetaData);
                                    del.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                                    del.setAttribute(BaseScannerRegionObserver.IGNORE_NEWER_MUTATIONS,
                                            PDataType.TRUE_BYTES);
                                    mutations.add(del);
                                }
                                del.addDeleteMarker(cell);
                            }
                        }
                        if (mutations.size() >= batchSize) {
                            region.batchMutate(mutations.toArray(new Mutation[mutations.size()]), HConstants.NO_NONCE,
                                    HConstants.NO_NONCE);
                            uuidValue = ServerCacheClient.generateId();
                            mutations.clear();
                        }
                        rowCount++;
                    }
                    
                } while (hasMore);
                if (!mutations.isEmpty()) {
                    region.batchMutate(mutations.toArray(new Mutation[mutations.size()]), HConstants.NO_NONCE,
                            HConstants.NO_NONCE);
                }
            }
        } catch (IOException e) {
            logger.error("IOException during rebuilding: " + Throwables.getStackTraceAsString(e));
            throw e;
        } finally {
            region.closeRegionOperation();
        }
        byte[] rowCountBytes = PLong.INSTANCE.toBytes(Long.valueOf(rowCount));
        final KeyValue aggKeyValue = KeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY,
                SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);

        RegionScanner scanner = new BaseRegionScanner(innerScanner) {
            @Override
            public HRegionInfo getRegionInfo() {
                return region.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() {
                return true;
            }

            @Override
            public void close() throws IOException {
                // no-op because we want to manage closing of the inner scanner ourselves.
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
        final KeyValue aggKeyValue =
                KeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
        RegionScanner scanner = new BaseRegionScanner(innerScanner) {
            @Override
            public HRegionInfo getRegionInfo() {
                return region.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() {
                return true;
            }

            @Override
            public void close() throws IOException {
                // no-op because we want to manage closing of the inner scanner ourselves.
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
        private final StatisticsCollector stats;
        private final Region region;
        private final RegionScanner innerScanner;
        private final Configuration config;
        private final Scan scan;

        StatsCollectionCallable(StatisticsCollector s, Region r, RegionScanner rs,
                Configuration config, Scan scan) {
            this.stats = s;
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
            long startTime = System.currentTimeMillis();
            region.startRegionOperation();
            boolean hasMore = false;
            boolean noErrors = false;
            boolean compactionRunning = areStatsBeingCollectedViaCompaction();
            long rowCount = 0;
            try {
                if (!compactionRunning) {
                    stats.init();
                    synchronized (innerScanner) {
                        do {
                            List<Cell> results = new ArrayList<Cell>();
                            hasMore = innerScanner.nextRaw(results);
                            stats.collectStatistics(results);
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
                        stats.updateStatistic(region, scan);
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
                        stats.close();
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
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow)
            throws IOException {
        // Don't allow splitting if operations need read and write to same region are going on in the
        // the coprocessors to avoid dead lock scenario. See PHOENIX-3111.
        synchronized (lock) {
            if (scansReferenceCount != 0) {
                throw new IOException("Operations like local index building/delete/upsert select"
                        + " might be going on so not allowing to split.");
            }
        }
    }

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> c,
            List<Pair<byte[], String>> familyPaths) throws IOException {
        // Don't allow bulkload if operations need read and write to same region are going on in the
        // the coprocessors to avoid dead lock scenario. See PHOENIX-3111.
        synchronized (lock) {
            if (scansReferenceCount != 0) {
                throw new DoNotRetryIOException("Operations like local index building/delete/upsert select"
                        + " might be going on so not allowing to bulkload.");
            }
        }
    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
            throws IOException {
        synchronized (lock) {
            while (scansReferenceCount != 0) {
                isRegionClosing = true;
                try {
                    lock.wait(1000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserver.UNGROUPED_AGG) != null;
    }
}
