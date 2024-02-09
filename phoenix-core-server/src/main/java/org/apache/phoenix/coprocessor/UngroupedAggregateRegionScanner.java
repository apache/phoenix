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

import static org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver.checkForLocalIndexColumnFamilies;
import static org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver.deserializeExpressions;
import static org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver.deserializeTable;
import static org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver.getBlockingMemstoreSize;
import static org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver.setIndexAndTransactionProperties;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.LOCAL_INDEX_BUILD;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.LOCAL_INDEX_BUILD_PROTO;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.REPLAY_WRITES;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.UPGRADE_DESC_ROW_KEY;
import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.SOURCE_OPERATION_ATTRIB;
import static org.apache.phoenix.schema.PTableImpl.getColumnsToClone;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;
import static org.apache.phoenix.util.WALAnnotationUtil.annotateMutation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.memory.InsufficientMemoryException;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema;
import org.apache.phoenix.schema.tuple.EncodedColumnQualiferCellsList;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.PositionBasedMultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.thirdparty.com.google.common.primitives.Ints;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UngroupedAggregateRegionScanner extends BaseRegionScanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(UngroupedAggregateRegionScanner.class);

    private long pageSizeMs;
    private  int maxBatchSize = 0;
    private  Scan scan;
    private  RegionScanner innerScanner;
    private  Region region;
    private final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver;
    private final RegionCoprocessorEnvironment env;
    private final boolean useQualifierAsIndex;
    private boolean needToWrite = false;
    private final Pair<Integer, Integer> minMaxQualifiers;
    private byte[][] values = null;
    private final PTable.QualifierEncodingScheme encodingScheme;
    private PTable writeToTable = null;
    private PTable projectedTable = null;
    private final boolean isDescRowKeyOrderUpgrade;
    private final int offset;
    private final boolean buildLocalIndex;
    private final List<IndexMaintainer> indexMaintainers;
    private boolean isPKChanging = false;
    private final long ts;
    private PhoenixTransactionProvider txnProvider = null;
    private final UngroupedAggregateRegionObserver.MutationList indexMutations;
    private boolean isDelete = false;
    private final byte[] replayMutations;
    private boolean isUpsert = false;
    private List<Expression> selectExpressions = null;
    private byte[] deleteCQ = null;
    private byte[] deleteCF = null;
    private byte[] emptyCF = null;
    private byte[] emptyCQ = null;
    private final byte[] indexUUID;
    private final byte[] txState;
    private final byte[] clientVersionBytes;
    private final long blockingMemStoreSize;
    private long maxBatchSizeBytes = 0L;
    private Table targetHTable = null;
    private boolean incrScanRefCount = false;
    private byte[] indexMaintainersPtr;
    private boolean useIndexProto;

    public UngroupedAggregateRegionScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
                                           final RegionScanner innerScanner, final Region region, final Scan scan,
                                           final RegionCoprocessorEnvironment env,
                                           final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver)
            throws IOException, SQLException{
        super(innerScanner);
        this.env = env;
        this.region = region;
        this.scan = scan;
        this.ungroupedAggregateRegionObserver = ungroupedAggregateRegionObserver;
        this.innerScanner = innerScanner;
        Configuration conf = env.getConfiguration();
        pageSizeMs = getPageSizeMsForRegionScanner(scan);
        ts = scan.getTimeRange().getMax();
        boolean localIndexScan = ScanUtil.isLocalIndex(scan);
        encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
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
        offset = offsetToBe;

        byte[] descRowKeyTableBytes = scan.getAttribute(UPGRADE_DESC_ROW_KEY);
        isDescRowKeyOrderUpgrade = descRowKeyTableBytes != null;
        if (isDescRowKeyOrderUpgrade) {
            LOGGER.debug("Upgrading row key for " + region.getRegionInfo().getTable().getNameAsString());
            projectedTable = deserializeTable(descRowKeyTableBytes);
            try {
                writeToTable = PTableImpl.builderWithColumns(projectedTable,
                        getColumnsToClone(projectedTable))
                        .setRowKeyOrderOptimizable(true)
                        .build();
            } catch (SQLException e) {
                ClientUtil.throwIOException("Upgrade failed", e); // Impossible
            }
            values = new byte[projectedTable.getPKColumns().size()][];
        }
        boolean useProto = false;
        byte[] localIndexBytes = scan.getAttribute(LOCAL_INDEX_BUILD_PROTO);
        useProto = localIndexBytes != null;
        if (localIndexBytes == null) {
            localIndexBytes = scan.getAttribute(LOCAL_INDEX_BUILD);
        }
        indexMaintainers = localIndexBytes == null ? null : IndexMaintainer.deserialize(localIndexBytes, useProto);
        indexMutations = localIndexBytes == null ? new UngroupedAggregateRegionObserver.MutationList() : new UngroupedAggregateRegionObserver.MutationList(1024);
        byte[] transforming = scan.getAttribute(BaseScannerRegionObserverConstants.DO_TRANSFORMING);

        replayMutations = scan.getAttribute(REPLAY_WRITES);
        indexUUID = scan.getAttribute(PhoenixIndexCodec.INDEX_UUID);
        txState = scan.getAttribute(BaseScannerRegionObserverConstants.TX_STATE);
        clientVersionBytes = scan.getAttribute(BaseScannerRegionObserverConstants.CLIENT_VERSION);
        if (txState != null) {
            int clientVersion = clientVersionBytes == null ? ScanUtil.UNKNOWN_CLIENT_VERSION : Bytes.toInt(clientVersionBytes);
            txnProvider = TransactionFactory.getTransactionProvider(txState, clientVersion);
        }
        byte[] upsertSelectTable = scan.getAttribute(BaseScannerRegionObserverConstants.UPSERT_SELECT_TABLE);
        if (upsertSelectTable != null) {
            isUpsert = true;
            projectedTable = deserializeTable(upsertSelectTable);
            //The Connection is a singleton. It MUST NOT be closed.
            targetHTable = ServerUtil.ConnectionFactory.getConnection(
                ServerUtil.ConnectionType.DEFAULT_SERVER_CONNECTION,
                env).getTable(TableName.valueOf(projectedTable.getPhysicalName().getBytes()));
            selectExpressions = deserializeExpressions(scan.getAttribute(BaseScannerRegionObserverConstants.UPSERT_SELECT_EXPRS));
            values = new byte[projectedTable.getPKColumns().size()][];
            isPKChanging = ExpressionUtil.isPkPositionChanging(new TableRef(projectedTable), selectExpressions);
        } else {
            byte[] isDeleteAgg = scan.getAttribute(BaseScannerRegionObserverConstants.DELETE_AGG);
            isDelete = isDeleteAgg != null && Bytes.compareTo(PDataType.TRUE_BYTES, isDeleteAgg) == 0;
            if (!isDelete) {
                deleteCF = scan.getAttribute(BaseScannerRegionObserverConstants.DELETE_CF);
                deleteCQ = scan.getAttribute(BaseScannerRegionObserverConstants.DELETE_CQ);
            }
            emptyCF = scan.getAttribute(BaseScannerRegionObserverConstants.EMPTY_CF);
            emptyCQ = scan.getAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER);
            if (emptyCF != null && emptyCQ == null) {
                // In case some old version sets EMPTY_CF but not EMPTY_COLUMN_QUALIFIER
                // Not sure if it's really needed, but better safe than sorry
                emptyCQ = QueryConstants.EMPTY_COLUMN_BYTES;
            }

        }
        ColumnReference[] dataColumns = IndexUtil.deserializeDataTableColumnsToJoin(scan);
        useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan));

        /**
         * Slow down the writes if the memstore size more than
         * (hbase.hregion.memstore.block.multiplier - 1) times hbase.hregion.memstore.flush.size
         * bytes. This avoids flush storm to hdfs for cases like index building where reads and
         * write happen to all the table regions in the server.
         */
        blockingMemStoreSize = getBlockingMemstoreSize(region, conf) ;

        buildLocalIndex = indexMaintainers != null && dataColumns==null && !localIndexScan;
        if(buildLocalIndex) {
            checkForLocalIndexColumnFamilies(region, indexMaintainers);
        }
        if (isDescRowKeyOrderUpgrade || isDelete || isUpsert
                || (deleteCQ != null && deleteCF != null) || emptyCF != null || buildLocalIndex) {
            needToWrite = true;
            if((isUpsert && (targetHTable == null ||
                    !targetHTable.getName().equals(region.getTableDescriptor().getTableName())))) {
                needToWrite = false;
            }
            maxBatchSize = conf.getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
            maxBatchSizeBytes = conf.getLongBytes(MUTATE_BATCH_SIZE_BYTES_ATTRIB,
                    QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE_BYTES);
        }
        minMaxQualifiers = EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(LogUtil.addCustomAnnotations("Starting ungrouped coprocessor scan " + scan + " " + region.getRegionInfo(), ScanUtil.getCustomAnnotations(scan)));
        }
        useIndexProto = true;
        indexMaintainersPtr = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
        // for backward compatiblity fall back to look by the old attribute
        if (indexMaintainersPtr == null) {
            indexMaintainersPtr = scan.getAttribute(PhoenixIndexCodec.INDEX_MD);
            useIndexProto = false;
        }

        if (needToWrite) {
            ungroupedAggregateRegionObserver.incrementScansReferenceCount();
            incrScanRefCount = true;
        }
    }

    @Override
    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (needToWrite && incrScanRefCount) {
            ungroupedAggregateRegionObserver.decrementScansReferenceCount();
        }
        try {
            if (targetHTable != null) {
                try {
                    targetHTable.close();
                } catch (IOException e) {
                    LOGGER.error("Closing table: " + targetHTable + " failed: ", e);
                }
            }
        } finally {
            innerScanner.close();
        }
    }

    boolean descRowKeyOrderUpgrade(List<Cell> results, ImmutableBytesWritable ptr,
                                UngroupedAggregateRegionObserver.MutationList mutations) throws IOException {
        Arrays.fill(values, null);
        Cell firstKV = results.get(0);
        RowKeySchema schema = projectedTable.getRowKeySchema();
        int maxOffset = schema.iterator(firstKV.getRowArray(), firstKV.getRowOffset() + offset, firstKV.getRowLength(), ptr);
        for (int i = 0; i < schema.getFieldCount(); i++) {
            Boolean hasValue = schema.next(ptr, i, maxOffset);
            if (hasValue == null) {
                break;
            }
            ValueSchema.Field field = schema.getField(i);
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
            return false;
        }
        byte[] newRow = ByteUtil.copyKeyBytesIfNecessary(ptr);
        if (offset > 0) { // for local indexes (prepend region start key)
            byte[] newRowWithOffset = new byte[offset + newRow.length];
            System.arraycopy(firstKV.getRowArray(), firstKV.getRowOffset(), newRowWithOffset, 0, offset);
            System.arraycopy(newRow, 0, newRowWithOffset, offset, newRow.length);
            newRow = newRowWithOffset;
        }
        byte[] oldRow = Bytes.copy(firstKV.getRowArray(), firstKV.getRowOffset(), firstKV.getRowLength());
        for (Cell cell : results) {
            // Copy existing cell but with new row key
            Cell newCell =
                    CellBuilderFactory.create(CellBuilderType.DEEP_COPY).
                            setRow(newRow).
                            setFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()).
                            setQualifier(cell.getQualifierArray(),
                                    cell.getQualifierOffset(), cell.getQualifierLength()).
                            setTimestamp(cell.getTimestamp()).
                            setType(cell.getType()).setValue(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength()).build();
            switch (cell.getType()) {
                case Put:
                    // If Put, point delete old Put
                    Delete del = new Delete(oldRow);
                    Cell newDelCell =
                            CellBuilderFactory.create(CellBuilderType.DEEP_COPY).
                                    setRow(newRow).
                                    setFamily(cell.getFamilyArray(), cell.getFamilyOffset(),
                                            cell.getFamilyLength()).
                                    setQualifier(cell.getQualifierArray(),
                                            cell.getQualifierOffset(), cell.getQualifierLength()).
                                    setTimestamp(cell.getTimestamp()).
                                    setType(Cell.Type.Delete).
                                    setValue(ByteUtil.EMPTY_BYTE_ARRAY,
                                            0, 0).build();
                    del.add(newDelCell);
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
                    delete.add(newCell);
                    mutations.add(delete);
                    break;
            }
        }
        return true;
    }

    void buildLocalIndex(Tuple result, List<Cell> results, ImmutableBytesWritable ptr) throws IOException {
        for (IndexMaintainer maintainer : indexMaintainers) {
            if (!results.isEmpty()) {
                result.getKey(ptr);
                ValueGetter valueGetter =
                        maintainer.createGetterFromKeyValues(
                                ImmutableBytesPtr.copyBytesIfNecessary(ptr),
                                results);
                Put put = maintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                        valueGetter, ptr, results.get(0).getTimestamp(),
                        env.getRegion().getRegionInfo().getStartKey(),
                        env.getRegion().getRegionInfo().getEndKey(),
                        false);

                if (txnProvider != null) {
                    put = txnProvider.markPutAsCommitted(put, ts, ts);
                }
                indexMutations.add(put);
            }
        }
        result.setKeyValues(results);
    }
    void deleteRow(List<Cell> results, UngroupedAggregateRegionObserver.MutationList mutations) {
        Cell firstKV = results.get(0);
        Delete delete = new Delete(firstKV.getRowArray(),
                firstKV.getRowOffset(), firstKV.getRowLength(),ts);
        if (replayMutations != null) {
            delete.setAttribute(REPLAY_WRITES, replayMutations);
        }
        byte[] sourceOperationBytes =
                scan.getAttribute(SOURCE_OPERATION_ATTRIB);
        if (sourceOperationBytes != null) {
            delete.setAttribute(SOURCE_OPERATION_ATTRIB, sourceOperationBytes);
        }

        mutations.add(delete);
    }

    void deleteCForQ(Tuple result, List<Cell> results, UngroupedAggregateRegionObserver.MutationList mutations) {
        // No need to search for delete column, since we project only it
        // if no empty key value is being set
        if (emptyCF == null ||
                result.getValue(deleteCF, deleteCQ) != null) {
            Delete delete = new Delete(results.get(0).getRowArray(),
                    results.get(0).getRowOffset(),
                    results.get(0).getRowLength());
            delete.addColumns(deleteCF,  deleteCQ, ts);
            // TODO: We need to set SOURCE_OPERATION_ATTRIB here also. The control will come here if
            // TODO: we drop a column. We also delete metadata from SYSCAT table for the dropped column
            // TODO: and delete the column. In short, we need to set this attribute for the DM for SYSCAT metadata
            // TODO: and for data table rows.
            mutations.add(delete);
        }
    }
    void upsert(Tuple result, ImmutableBytesWritable ptr, UngroupedAggregateRegionObserver.MutationList mutations) {
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
            } else {
                values[i] = ByteUtil.EMPTY_BYTE_ARRAY;
            }
        }
        projectedTable.newKey(ptr, values);
        PRow row = projectedTable.newRow(GenericKeyValueBuilder.INSTANCE, ts, ptr, false);
        for (; i < projectedColumns.size(); i++) {
            Expression expression = selectExpressions.get(i - bucketNumOffset);
            if (expression.evaluate(result, ptr)) {
                PColumn column = projectedColumns.get(i);
                if (!column.getDataType().isSizeCompatible(ptr, null,
                        expression.getDataType(), expression.getSortOrder(),
                        expression.getMaxLength(), expression.getScale(),
                        column.getMaxLength(), column.getScale())) {
                    throw new DataExceedsCapacityException(
                            column.getDataType(),
                            column.getMaxLength(),
                            column.getScale(),
                            column.getName().getString());
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
            } else if (txnProvider != null && projectedTable.getType() == PTableType.INDEX) {
                mutation = txnProvider.markPutAsCommitted((Put)mutation, ts, ts);
            }
            mutations.add(mutation);
        }
        for (i = 0; i < selectExpressions.size(); i++) {
            selectExpressions.get(i).reset();
        }
    }

    void insertEmptyKeyValue(List<Cell> results, UngroupedAggregateRegionObserver.MutationList mutations) {
        Set<Long> timeStamps =
                Sets.newHashSetWithExpectedSize(results.size());
        for (Cell kv : results) {
            long kvts = kv.getTimestamp();
            if (!timeStamps.contains(kvts)) {
                Put put = new Put(kv.getRowArray(), kv.getRowOffset(),
                        kv.getRowLength());
                // The value is not dependent on encoding ("x")
                put.addColumn(emptyCF, emptyCQ, kvts,
                    QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
                mutations.add(put);
                timeStamps.add(kvts);
            }
        }
    }

    @Override
    public boolean next(List<Cell> resultsToReturn) throws IOException {
        boolean hasMore;
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        Configuration conf = env.getConfiguration();
        final TenantCache tenantCache = GlobalCache.getTenantCache(env, ScanUtil.getTenantId(scan));
        try (MemoryManager.MemoryChunk em = tenantCache.getMemoryManager().allocate(0)) {
            Aggregators aggregators = ServerAggregators.deserialize(
                    scan.getAttribute(BaseScannerRegionObserverConstants.AGGREGATORS), conf, em);
            Aggregator[] rowAggregators = aggregators.getAggregators();
            aggregators.reset(rowAggregators);
            Cell lastCell = null;
            boolean hasAny = false;
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            Tuple result = useQualifierAsIndex ? new PositionBasedMultiKeyValueTuple() : new MultiKeyValueTuple();
            UngroupedAggregateRegionObserver.MutationList mutations = new UngroupedAggregateRegionObserver.MutationList();
            if (isDescRowKeyOrderUpgrade || isDelete || isUpsert
                    || (deleteCQ != null && deleteCF != null) || emptyCF != null || buildLocalIndex) {
                mutations = new UngroupedAggregateRegionObserver.MutationList(Ints.saturatedCast(maxBatchSize + maxBatchSize / 10));
            }
            region.startRegionOperation();
            try {
                synchronized (innerScanner) {
                    do {
                        ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                        List<Cell> results = useQualifierAsIndex ? new EncodedColumnQualiferCellsList(minMaxQualifiers.getFirst(), minMaxQualifiers.getSecond(), encodingScheme) : new ArrayList<Cell>();
                        // Results are potentially returned even when the return value of s.next is false
                        // since this is an indication of whether or not there are more values after the
                        // ones returned
                        hasMore = innerScanner.nextRaw(results);
                        if (isDummy(results)) {
                            if (!hasAny) {
                                resultsToReturn.addAll(results);
                                return true;
                            }
                            break;
                        }
                        if (!results.isEmpty()) {
                            lastCell = results.get(0);
                            result.setKeyValues(results);
                            if (isDescRowKeyOrderUpgrade) {
                                if (!descRowKeyOrderUpgrade(results, ptr, mutations)) {
                                    continue;
                                }
                            } else if (buildLocalIndex) {
                                buildLocalIndex(result, results, ptr);
                            } else if (isDelete) {
                                deleteRow(results, mutations);
                            } else if (isUpsert) {
                                upsert(result, ptr, mutations);
                            } else if (deleteCF != null && deleteCQ != null) {
                                deleteCForQ(result, results, mutations);
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
                                insertEmptyKeyValue(results, mutations);
                            }
                            if (ServerUtil.readyToCommit(mutations.size(), mutations.byteSize(), maxBatchSize, maxBatchSizeBytes)) {
                                annotateAndCommit(mutations);
                            }
                            // Commit in batches based on UPSERT_BATCH_SIZE_BYTES_ATTRIB in config

                            if (ServerUtil.readyToCommit(indexMutations.size(), indexMutations.byteSize(), maxBatchSize, maxBatchSizeBytes)) {
                                setIndexAndTransactionProperties(indexMutations, indexUUID, indexMaintainersPtr, txState, clientVersionBytes, useIndexProto);
                                ungroupedAggregateRegionObserver.commitBatch(region, indexMutations, blockingMemStoreSize);
                                indexMutations.clear();
                            }
                            aggregators.aggregate(rowAggregators, result);
                            hasAny = true;
                        }
                    } while (hasMore && (EnvironmentEdgeManager.currentTimeMillis() - startTime) < pageSizeMs);
                    if (!mutations.isEmpty()) {
                        annotateAndCommit(mutations);
                    }
                    if (!indexMutations.isEmpty()) {
                        ungroupedAggregateRegionObserver.commitBatch(region, indexMutations, blockingMemStoreSize);
                        indexMutations.clear();
                    }
                }
            } catch (InsufficientMemoryException e) {
                throw new DoNotRetryIOException(e);
            } catch (DataExceedsCapacityException e) {
                throw new DoNotRetryIOException(e.getMessage(), e);
            } catch (Throwable e) {
                LOGGER.error("Exception in UngroupedAggregateRegionScanner for region "
                        + region.getRegionInfo().getRegionNameAsString(), e);
                throw e;
            } finally {
                region.closeRegionOperation();
            }
            Cell keyValue;
            if (hasAny) {
                byte[] value = aggregators.toBytes(rowAggregators);
                if (pageSizeMs == Long.MAX_VALUE) {
                    byte[] rowKey;
                    final boolean isIncompatibleClient =
                            ScanUtil.isIncompatibleClientForServerReturnValidRowKey(scan);
                    if (!isIncompatibleClient) {
                        rowKey = CellUtil.cloneRow(lastCell);
                    } else {
                        // Paging is not set. To be compatible with older clients, do not set the row key
                        rowKey = UNGROUPED_AGG_ROW_KEY;
                    }
                    keyValue = PhoenixKeyValueUtil.newKeyValue(rowKey, SINGLE_COLUMN_FAMILY,
                            SINGLE_COLUMN,
                            AGG_TIMESTAMP, value, 0, value.length);
                } else {
                    keyValue = PhoenixKeyValueUtil.newKeyValue(CellUtil.cloneRow(lastCell), SINGLE_COLUMN_FAMILY, SINGLE_COLUMN,
                            AGG_TIMESTAMP, value, 0, value.length);
                }
                resultsToReturn.add(keyValue);
            }
            return hasMore;
        }
    }

    private void annotateAndCommit(UngroupedAggregateRegionObserver.MutationList mutations) throws IOException {
        annotateDataMutations(mutations, scan);
        if (isDelete || isUpsert) {
            annotateDataMutationsWithExternalSchemaId(mutations, scan);
        }
        ungroupedAggregateRegionObserver.commit(region, mutations, indexUUID, blockingMemStoreSize, indexMaintainersPtr, txState,
            targetHTable, useIndexProto, isPKChanging, clientVersionBytes);
        mutations.clear();
    }

    @Override
    public long getMaxResultSize() {
        return scan.getMaxResultSize();
    }

    private void annotateDataMutations(UngroupedAggregateRegionObserver.MutationList mutationsList,
                                       Scan scan) {

        byte[] tenantId =
                scan.getAttribute(MutationState.MutationMetadataType.TENANT_ID.toString());
        byte[] schemaName =
                scan.getAttribute(MutationState.MutationMetadataType.SCHEMA_NAME.toString());
        byte[] logicalTableName =
                scan.getAttribute(MutationState.MutationMetadataType.LOGICAL_TABLE_NAME.toString());
        byte[] tableType =
                scan.getAttribute(MutationState.MutationMetadataType.TABLE_TYPE.toString());
        byte[] ddlTimestamp =
                scan.getAttribute(MutationState.MutationMetadataType.TIMESTAMP.toString());

        for (Mutation m : mutationsList) {
            annotateMutation(m, tenantId, schemaName, logicalTableName, tableType, ddlTimestamp);
        }
    }

    private void annotateDataMutationsWithExternalSchemaId(
            UngroupedAggregateRegionObserver.MutationList mutationsList,
            Scan scan) {
        byte[] externalSchemaRegistryId = scan.getAttribute(
                MutationState.MutationMetadataType.EXTERNAL_SCHEMA_ID.toString());
        for (Mutation m : mutationsList) {
            annotateMutation(m, externalSchemaRegistryId);
        }
    }

}
