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

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.phoenix.client.KeyValueBuilder;
import org.apache.phoenix.exception.ValueTypeIncompatibleException;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.join.ScanProjector;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;


/**
 * Region observer that aggregates ungrouped rows(i.e. SQL query with aggregation function and no GROUP BY).
 * 
 * 
 * @since 0.1
 */
public class UngroupedAggregateRegionObserver extends BaseScannerRegionObserver {
    private static final Logger logger = LoggerFactory.getLogger(UngroupedAggregateRegionObserver.class);
    // TODO: move all constants into a single class
    public static final String UNGROUPED_AGG = "UngroupedAgg";
    public static final String DELETE_AGG = "DeleteAgg";
    public static final String UPSERT_SELECT_TABLE = "UpsertSelectTable";
    public static final String UPSERT_SELECT_EXPRS = "UpsertSelectExprs";
    public static final String DELETE_CQ = "DeleteCQ";
    public static final String DELETE_CF = "DeleteCF";
    public static final String EMPTY_CF = "EmptyCF";
    private KeyValueBuilder kvBuilder;
    
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        this.kvBuilder = KeyValueBuilder.get(e.getHBaseVersion());
    }

    private static void commitBatch(HRegion region, List<Pair<Mutation,Integer>> mutations, byte[] indexUUID) throws IOException {
        if (indexUUID != null) {
            for (Pair<Mutation,Integer> pair : mutations) {
                pair.getFirst().setAttribute(PhoenixIndexCodec.INDEX_UUID, indexUUID);
            }
        }
        @SuppressWarnings("unchecked")
        Pair<Mutation,Integer>[] mutationArray = new Pair[mutations.size()];
        // TODO: should we use the one that is all or none?
        region.batchMutate(mutations.toArray(mutationArray));
    }
    
    public static void serializeIntoScan(Scan scan) {
        scan.setAttribute(UNGROUPED_AGG, QueryConstants.TRUE);
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws IOException {
        byte[] isUngroupedAgg = scan.getAttribute(UNGROUPED_AGG);
        if (isUngroupedAgg == null) {
            return s;
        }
        
        final ScanProjector p = ScanProjector.deserializeProjectorFromScan(scan);
        final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
        RegionScanner theScanner = s;
        if (p != null || j != null)  {
            theScanner = new HashJoinRegionScanner(s, p, j, ScanUtil.getTenantId(scan), c.getEnvironment());
        }
        final RegionScanner innerScanner = theScanner;
        
        byte[] indexUUID = scan.getAttribute(PhoenixIndexCodec.INDEX_UUID);
        PTable projectedTable = null;
        List<Expression> selectExpressions = null;
        byte[] upsertSelectTable = scan.getAttribute(UPSERT_SELECT_TABLE);
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
            selectExpressions = deserializeExpressions(scan.getAttribute(UPSERT_SELECT_EXPRS));
            values = new byte[projectedTable.getPKColumns().size()][];
            ptr = new ImmutableBytesWritable();
        } else {
            byte[] isDeleteAgg = scan.getAttribute(DELETE_AGG);
            isDelete = isDeleteAgg != null && Bytes.compareTo(PDataType.TRUE_BYTES, isDeleteAgg) == 0;
            if (!isDelete) {
                deleteCF = scan.getAttribute(DELETE_CF);
                deleteCQ = scan.getAttribute(DELETE_CQ);
            }
            emptyCF = scan.getAttribute(EMPTY_CF);
        }
        
        int batchSize = 0;
        long ts = scan.getTimeRange().getMax();
        HRegion region = c.getEnvironment().getRegion();
        List<Pair<Mutation,Integer>> mutations = Collections.emptyList();
        if (isDelete || isUpsert || (deleteCQ != null && deleteCF != null) || emptyCF != null) {
            // TODO: size better
            mutations = Lists.newArrayListWithExpectedSize(1024);
            batchSize = c.getEnvironment().getConfiguration().getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
        }
        Aggregators aggregators = ServerAggregators.deserialize(
                scan.getAttribute(GroupedAggregateRegionObserver.AGGREGATORS), c.getEnvironment().getConfiguration());
        Aggregator[] rowAggregators = aggregators.getAggregators();
        boolean hasMore;
        boolean hasAny = false;
        MultiKeyValueTuple result = new MultiKeyValueTuple();
        if (logger.isInfoEnabled()) {
        	logger.info("Starting ungrouped coprocessor scan " + scan);
        }
        long rowCount = 0;
        MultiVersionConsistencyControl.setThreadReadPoint(innerScanner.getMvccReadPoint());
        region.startRegionOperation();
        try {
            do {
                List<KeyValue> results = new ArrayList<KeyValue>();
                // Results are potentially returned even when the return value of s.next is false
                // since this is an indication of whether or not there are more values after the
                // ones returned
                hasMore = innerScanner.nextRaw(results, null);
                if (!results.isEmpty()) {
                	rowCount++;
                    result.setKeyValues(results);
                    try {
                        if (isDelete) {
                            @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
                            // FIXME: the version of the Delete constructor without the lock args was introduced
                            // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
                            // of the client.
                            Delete delete = new Delete(results.get(0).getRow(),ts,null);
                            mutations.add(new Pair<Mutation,Integer>(delete,null));
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
                                    if (expression.getSortOrder() != projectedColumns.get(i).getSortOrder()) {
                                        SortOrder.invert(values[i], 0, values[i], 0, values[i].length);
                                    }
                                }
                            }
                            projectedTable.newKey(ptr, values);
                            PRow row = projectedTable.newRow(kvBuilder, ts, ptr);
                            for (; i < projectedColumns.size(); i++) {
                                Expression expression = selectExpressions.get(i);
                                if (expression.evaluate(result, ptr)) {
                                    PColumn column = projectedColumns.get(i);
                                    byte[] bytes = ptr.copyBytes();
                                    Object value = expression.getDataType().toObject(bytes, column.getSortOrder());
                                    // If SortOrder from expression in SELECT doesn't match the
                                    // column being projected into then invert the bits.
                                    if (expression.getSortOrder() != column.getSortOrder()) {
                                        SortOrder.invert(bytes, 0, bytes, 0, bytes.length);
                                    }
                                    // We are guaranteed that the two column will have the same type.
                                    if (!column.getDataType().isSizeCompatible(column.getDataType(),
                                            value, bytes,
                                            expression.getMaxLength(), column.getMaxLength(), 
                                            expression.getScale(), column.getScale())) {
                                        throw new ValueTypeIncompatibleException(column.getDataType(),
                                                column.getMaxLength(), column.getScale());
                                    }
                                    bytes = column.getDataType().coerceBytes(bytes, value, expression.getDataType(),
                                            expression.getMaxLength(), expression.getScale(), column.getMaxLength(), column.getScale());
                                    row.setValue(column, bytes);
                                }
                            }
                            for (Mutation mutation : row.toRowMutations()) {
                                mutations.add(new Pair<Mutation,Integer>(mutation,null));
                            }
                        } else if (deleteCF != null && deleteCQ != null) {
                            // No need to search for delete column, since we project only it
                            // if no empty key value is being set
                            if (emptyCF == null || result.getValue(deleteCF, deleteCQ) != null) {
                                Delete delete = new Delete(results.get(0).getRow());
                                delete.deleteColumns(deleteCF,  deleteCQ, ts);
                                mutations.add(new Pair<Mutation,Integer>(delete,null));
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
                            Set<Long> timeStamps = Sets.newHashSetWithExpectedSize(results.size());
                            for (KeyValue kv : results) {
                                long kvts = kv.getTimestamp();
                                if (!timeStamps.contains(kvts)) {
                                    Put put = new Put(kv.getRow());
                                    put.add(emptyCF, QueryConstants.EMPTY_COLUMN_BYTES, kvts, ByteUtil.EMPTY_BYTE_ARRAY);
                                    mutations.add(new Pair<Mutation,Integer>(put,null));
                                }
                            }
                        }
                        // Commit in batches based on UPSERT_BATCH_SIZE_ATTRIB in config
                        if (!mutations.isEmpty() && batchSize > 0 && mutations.size() % batchSize == 0) {
                            commitBatch(region,mutations, indexUUID);
                            mutations.clear();
                        }
                    } catch (ConstraintViolationException e) {
                        // Log and ignore in count
                        logger.error("Failed to create row in " + region.getRegionNameAsString() + " with values " + SchemaUtil.toString(values), e);
                        continue;
                    }
                    aggregators.aggregate(rowAggregators, result);
                    hasAny = true;
                }
            } while (hasMore);
        } finally {
            innerScanner.close();
            region.closeRegionOperation();
        }
        
        if (logger.isInfoEnabled()) {
        	logger.info("Finished scanning " + rowCount + " rows for ungrouped coprocessor scan " + scan);
        }

        if (!mutations.isEmpty()) {
            commitBatch(region,mutations, indexUUID);
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
            public boolean next(List<KeyValue> results) throws IOException {
                if (done) return false;
                done = true;
                results.add(aggKeyValue);
                return false;
            }
        };
        return scanner;
    }
    
    private static PTable deserializeTable(byte[] b) {
        ByteArrayInputStream stream = new ByteArrayInputStream(b);
        try {
            DataInputStream input = new DataInputStream(stream);
            PTable table = new PTableImpl();
            table.readFields(input);
            return table;
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
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            projectedTable.write(output);
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
}
