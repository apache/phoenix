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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
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
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.stats.StatisticsCollector;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TimeKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/**
 * Region observer that aggregates ungrouped rows(i.e. SQL query with aggregation function and no GROUP BY).
 * 
 * 
 * @since 0.1
 */
public class UngroupedAggregateRegionObserver extends BaseScannerRegionObserver{
    // TODO: move all constants into a single class
    public static final String UNGROUPED_AGG = "UngroupedAgg";
    public static final String DELETE_AGG = "DeleteAgg";
    public static final String UPSERT_SELECT_TABLE = "UpsertSelectTable";
    public static final String UPSERT_SELECT_EXPRS = "UpsertSelectExprs";
    public static final String DELETE_CQ = "DeleteCQ";
    public static final String DELETE_CF = "DeleteCF";
    public static final String EMPTY_CF = "EmptyCF";
    private static final Logger logger = LoggerFactory.getLogger(UngroupedAggregateRegionObserver.class);
    private KeyValueBuilder kvBuilder;
    
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        // Can't use ClientKeyValueBuilder on server-side because the memstore expects to
        // be able to get a single backing buffer for a KeyValue.
        this.kvBuilder = GenericKeyValueBuilder.INSTANCE;
    }

    private static void commitBatch(HRegion region, List<Mutation> mutations, byte[] indexUUID) throws IOException {
      if (indexUUID != null) {
          for (Mutation m : mutations) {
              m.setAttribute(PhoenixIndexCodec.INDEX_UUID, indexUUID);
          }
      }
      Mutation[] mutationArray = new Mutation[mutations.size()];
      // TODO: should we use the one that is all or none?
      region.batchMutate(mutations.toArray(mutationArray));
    }

    public static void serializeIntoScan(Scan scan) {
        scan.setAttribute(BaseScannerRegionObserver.UNGROUPED_AGG, QueryConstants.TRUE);
    }
    
    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
            throws IOException {
        s = super.preScannerOpen(e, scan, s);
        if (ScanUtil.isAnalyzeTable(scan)) {
            // We are setting the start row and stop row such that it covers the entire region. As part
            // of Phonenix-1263 we are storing the guideposts against the physical table rather than 
            // individual tenant specific tables.
            scan.setStartRow(HConstants.EMPTY_START_ROW);
            scan.setStopRow(HConstants.EMPTY_END_ROW);
            scan.setFilter(null);
        }
        return s;
    }
    
    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws IOException {
        int offset = 0;
        HRegion region = c.getEnvironment().getRegion();
        long ts = scan.getTimeRange().getMax();
        StatisticsCollector stats = null;
        if(ScanUtil.isAnalyzeTable(scan)) {
            byte[] gp_width_bytes = scan.getAttribute(BaseScannerRegionObserver.GUIDEPOST_WIDTH_BYTES);
            byte[] gp_per_region_bytes = scan.getAttribute(BaseScannerRegionObserver.GUIDEPOST_PER_REGION);
            // Let this throw, as this scan is being done for the sole purpose of collecting stats
            stats = new StatisticsCollector(c.getEnvironment(), region.getRegionInfo().getTable().getNameAsString(), ts, gp_width_bytes, gp_per_region_bytes);
        }
        if (ScanUtil.isLocalIndex(scan)) {
            /*
             * For local indexes, we need to set an offset on row key expressions to skip
             * the region start key.
             */
            offset = region.getStartKey().length != 0 ? region.getStartKey().length:region.getEndKey().length;
            ScanUtil.setRowKeyOffset(scan, offset);
        }

        byte[] localIndexBytes = scan.getAttribute(LOCAL_INDEX_BUILD);
        List<IndexMaintainer> indexMaintainers = localIndexBytes == null ? null : IndexMaintainer.deserialize(localIndexBytes);
        List<Mutation> indexMutations = localIndexBytes == null ? Collections.<Mutation>emptyList() : Lists.<Mutation>newArrayListWithExpectedSize(1024);
        
        RegionScanner theScanner = s;
        
        byte[] indexUUID = scan.getAttribute(PhoenixIndexCodec.INDEX_UUID);
        PTable projectedTable = null;
        List<Expression> selectExpressions = null;
        byte[] upsertSelectTable = scan.getAttribute(BaseScannerRegionObserver.UPSERT_SELECT_TABLE);
        boolean isUpsert = false;
        boolean isDelete = false;
        byte[] deleteCQ = null;
        byte[] deleteCF = null;
        byte[][] values = null;
        byte[] emptyCF = null;
        ImmutableBytesWritable ptr = null;
        if (upsertSelectTable != null) {
            isUpsert = true;
            projectedTable = deserializeTable(upsertSelectTable);
            selectExpressions = deserializeExpressions(scan.getAttribute(BaseScannerRegionObserver.UPSERT_SELECT_EXPRS));
            values = new byte[projectedTable.getPKColumns().size()][];
            ptr = new ImmutableBytesWritable();
        } else {
            byte[] isDeleteAgg = scan.getAttribute(BaseScannerRegionObserver.DELETE_AGG);
            isDelete = isDeleteAgg != null && Bytes.compareTo(PDataType.TRUE_BYTES, isDeleteAgg) == 0;
            if (!isDelete) {
                deleteCF = scan.getAttribute(BaseScannerRegionObserver.DELETE_CF);
                deleteCQ = scan.getAttribute(BaseScannerRegionObserver.DELETE_CQ);
            }
            emptyCF = scan.getAttribute(BaseScannerRegionObserver.EMPTY_CF);
        }
        if(localIndexBytes != null) {
            ptr = new ImmutableBytesWritable();
        }
        TupleProjector tupleProjector = null;
        HRegion dataRegion = null;
        byte[][] viewConstants = null;
        ColumnReference[] dataColumns = IndexUtil.deserializeDataTableColumnsToJoin(scan);
        boolean localIndexScan = ScanUtil.isLocalIndex(scan);
        final TupleProjector p = TupleProjector.deserializeProjectorFromScan(scan);
        final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
        if ((localIndexScan && !isDelete) || (j == null && p != null)) {
            if (dataColumns != null) {
                tupleProjector = IndexUtil.getTupleProjector(scan, dataColumns);
                dataRegion = IndexUtil.getDataRegion(c.getEnvironment());
                viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
            }
            ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
            theScanner =
                    getWrappedScanner(c, theScanner, offset, scan, dataColumns, tupleProjector, 
                            dataRegion, indexMaintainers == null ? null : indexMaintainers.get(0), viewConstants, p, tempPtr);
        } 
        
        if (j != null)  {
            theScanner = new HashJoinRegionScanner(theScanner, p, j, ScanUtil.getTenantId(scan), c.getEnvironment());
        }
        
        int batchSize = 0;
        List<Mutation> mutations = Collections.emptyList();
        boolean buildLocalIndex = indexMaintainers != null && dataColumns==null && !localIndexScan;
        if (isDelete || isUpsert || (deleteCQ != null && deleteCF != null) || emptyCF != null || buildLocalIndex) {
            // TODO: size better
            mutations = Lists.newArrayListWithExpectedSize(1024);
            batchSize = c.getEnvironment().getConfiguration().getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
        }
        Aggregators aggregators = ServerAggregators.deserialize(
                scan.getAttribute(BaseScannerRegionObserver.AGGREGATORS), c.getEnvironment().getConfiguration());
        Aggregator[] rowAggregators = aggregators.getAggregators();
        boolean hasMore;
        boolean hasAny = false;
        MultiKeyValueTuple result = new MultiKeyValueTuple();
        if (logger.isDebugEnabled()) {
        	logger.debug(LogUtil.addCustomAnnotations("Starting ungrouped coprocessor scan " + scan + " "+region.getRegionInfo(), ScanUtil.getCustomAnnotations(scan)));
        }
        long rowCount = 0;
        final RegionScanner innerScanner = theScanner;
        region.startRegionOperation();
        try {
            synchronized (innerScanner) {
                do {
                    List<Cell> results = new ArrayList<Cell>();
                    // Results are potentially returned even when the return value of s.next is false
                    // since this is an indication of whether or not there are more values after the
                    // ones returned
                    hasMore = innerScanner.nextRaw(results);
                    if (stats != null) {
                        stats.collectStatistics(results);
                    }
                    if (!results.isEmpty()) {
                        rowCount++;
                        result.setKeyValues(results);
                        try {
                            if (buildLocalIndex) {
                                for (IndexMaintainer maintainer : indexMaintainers) {
                                    if (!results.isEmpty()) {
                                        result.getKey(ptr);
                                        ValueGetter valueGetter =
                                            maintainer.createGetterFromKeyValues(
                                                ImmutableBytesPtr.copyBytesIfNecessary(ptr),
                                                results);
                                        Put put = maintainer.buildUpdateMutation(kvBuilder,
                                            valueGetter, ptr, ts,
                                            c.getEnvironment().getRegion().getStartKey(),
                                            c.getEnvironment().getRegion().getEndKey());
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
                            } else if (isUpsert) {
                                Arrays.fill(values, null);
                                int i = 0;
                                List<PColumn> projectedColumns = projectedTable.getColumns();
                                for (; i < projectedTable.getPKColumns().size(); i++) {
                                    Expression expression = selectExpressions.get(i);
                                    if (expression.evaluate(result, ptr)) {
                                        values[i] = ptr.copyBytes();
                                        // If SortOrder from expression in SELECT doesn't match the
                                        // column being projected into then invert the bits.
                                        if (expression.getSortOrder() !=
                                            projectedColumns.get(i).getSortOrder()) {
                                            SortOrder.invert(values[i], 0, values[i], 0,
                                                values[i].length);
                                        }
                                    }
                                }
                                projectedTable.newKey(ptr, values);
                                PRow row = projectedTable.newRow(kvBuilder, ts, ptr);
                                for (; i < projectedColumns.size(); i++) {
                                    Expression expression = selectExpressions.get(i);
                                    if (expression.evaluate(result, ptr)) {
                                        PColumn column = projectedColumns.get(i);
                                        Object value = expression.getDataType()
                                            .toObject(ptr, column.getSortOrder());
                                        // We are guaranteed that the two column will have the
                                        // same type.
                                        if (!column.getDataType().isSizeCompatible(ptr, value,
                                                column.getDataType(), expression.getMaxLength(),
                                                expression.getScale(), column.getMaxLength(),
                                                column.getScale())) {
                                            throw new DataExceedsCapacityException(
                                                column.getDataType(), column.getMaxLength(),
                                                column.getScale());
                                        }
                                        column.getDataType().coerceBytes(ptr, value,
                                            expression.getDataType(), expression.getMaxLength(),
                                            expression.getScale(), expression.getSortOrder(), 
                                            column.getMaxLength(), column.getScale(),
                                            column.getSortOrder());
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
                            // Commit in batches based on UPSERT_BATCH_SIZE_ATTRIB in config
                            if (!mutations.isEmpty() && batchSize > 0 &&
                                    mutations.size() % batchSize == 0) {
                                commitBatch(region, mutations, indexUUID);
                                mutations.clear();
                            }
                            // Commit in batches based on UPSERT_BATCH_SIZE_ATTRIB in config
                            if (!indexMutations.isEmpty() && batchSize > 0 &&
                                    indexMutations.size() % batchSize == 0) {
                                commitIndexMutations(c, region, indexMutations);
                            }
                        } catch (ConstraintViolationException e) {
                            // Log and ignore in count
                            logger.error(LogUtil.addCustomAnnotations("Failed to create row in " +
                                region.getRegionNameAsString() + " with values " +
                                SchemaUtil.toString(values),
                                ScanUtil.getCustomAnnotations(scan)), e);
                            continue;
                        }
                        aggregators.aggregate(rowAggregators, result);
                        hasAny = true;
                    }
                } while (hasMore);
            }
        } finally {
            try {
                if (stats != null) {
                    try {
                        stats.updateStatistic(region);
                    } finally {
                        stats.close();
                    }
                }
            } finally {
                try {
                    innerScanner.close();
                } finally {
                    region.closeRegionOperation();
                }
            }
        }
        
        if (logger.isDebugEnabled()) {
        	logger.debug(LogUtil.addCustomAnnotations("Finished scanning " + rowCount + " rows for ungrouped coprocessor scan " + scan, ScanUtil.getCustomAnnotations(scan)));
        }

        if (!mutations.isEmpty()) {
            commitBatch(region,mutations, indexUUID);
        }

        if (!indexMutations.isEmpty()) {
            commitIndexMutations(c, region, indexMutations);
        }

        final boolean hadAny = hasAny;
        KeyValue keyValue = null;
        if (hadAny) {
            byte[] value = aggregators.toBytes(rowAggregators);
            keyValue = KeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length);
        }
        final KeyValue aggKeyValue = keyValue;
        
        RegionScanner scanner = new BaseRegionScanner() {
            private boolean done = !hadAny;

            @Override
            public HRegionInfo getRegionInfo() {
                return innerScanner.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() {
                return done;
            }

            @Override
            public void close() throws IOException {
                innerScanner.close();
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

    private void commitIndexMutations(final ObserverContext<RegionCoprocessorEnvironment> c,
            HRegion region, List<Mutation> indexMutations) throws IOException {
        // Get indexRegion corresponding to data region
        HRegion indexRegion = IndexUtil.getIndexRegion(c.getEnvironment());
        if (indexRegion != null) {
            commitBatch(indexRegion, indexMutations, null);
        } else {
            TableName indexTable =
                    TableName.valueOf(MetaDataUtil.getLocalIndexPhysicalName(region.getTableDesc()
                            .getName()));
            HTableInterface table = null;
            try {
                table = c.getEnvironment().getTable(indexTable);
                table.batch(indexMutations);
            } catch (InterruptedException ie) {
                ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(),
                    ie);
            } finally {
                if (table != null) table.close();
             }
        }
        indexMutations.clear();
    }
    
    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c,
        final Store store, InternalScanner scanner, final ScanType scanType)
        throws IOException {
        TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
        InternalScanner internalScanner = scanner;
        if (scanType.equals(ScanType.COMPACT_DROP_DELETES)) {
            try {
                boolean useCurrentTime = 
                        c.getEnvironment().getConfiguration().getBoolean(QueryServices.STATS_USE_CURRENT_TIME_ATTRIB, 
                                QueryServicesOptions.DEFAULT_STATS_USE_CURRENT_TIME);
                // Provides a means of clients controlling their timestamps to not use current time
                // when background tasks are updating stats. Instead we track the max timestamp of
                // the cells and use that.
                long clientTimeStamp = useCurrentTime ? TimeKeeper.SYSTEM.getCurrentTime() : StatisticsCollector.NO_TIMESTAMP;
                StatisticsCollector stats = new StatisticsCollector(
                        c.getEnvironment(), table.getNameAsString(),
                        clientTimeStamp, store.getFamily().getName());
                internalScanner = stats.createCompactionScanner(c.getEnvironment().getRegion(), store, scanner);
            } catch (IOException e) {
                // If we can't reach the stats table, don't interrupt the normal
                // compaction operation, just log a warning.
                if(logger.isWarnEnabled()) {
                    logger.warn("Unable to collect stats for " + table, e);
                }
            }
        }
        return internalScanner;
    }
    
    
    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r)
            throws IOException {
        HRegion region = e.getEnvironment().getRegion();
        TableName table = region.getRegionInfo().getTable();
        StatisticsCollector stats = null;
        try {
            boolean useCurrentTime = 
                    e.getEnvironment().getConfiguration().getBoolean(QueryServices.STATS_USE_CURRENT_TIME_ATTRIB, 
                            QueryServicesOptions.DEFAULT_STATS_USE_CURRENT_TIME);
            // Provides a means of clients controlling their timestamps to not use current time
            // when background tasks are updating stats. Instead we track the max timestamp of
            // the cells and use that.
            long clientTimeStamp = useCurrentTime ? TimeKeeper.SYSTEM.getCurrentTime() : StatisticsCollector.NO_TIMESTAMP;
            stats = new StatisticsCollector(e.getEnvironment(), table.getNameAsString(), clientTimeStamp);
            stats.splitStats(region, l, r);
        } catch (IOException ioe) { 
            if(logger.isWarnEnabled()) {
                logger.warn("Error while collecting stats during split for " + table,ioe);
            }
        } finally {
            if (stats != null) stats.close();
        }
    }

    private static PTable deserializeTable(byte[] b) {
        try {
            PTableProtos.PTable ptableProto = PTableProtos.PTable.parseFrom(b);
            return PTableImpl.createFromProto(ptableProto);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserver.UNGROUPED_AGG) != null;
    }
}
