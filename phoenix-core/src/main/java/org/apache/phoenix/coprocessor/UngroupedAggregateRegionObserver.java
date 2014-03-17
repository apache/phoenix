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
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.exception.ValueTypeIncompatibleException;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.join.ScanProjector;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
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
public class UngroupedAggregateRegionObserver extends BaseScannerRegionObserver {

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
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws IOException {
        byte[] isUngroupedAgg = scan.getAttribute(BaseScannerRegionObserver.UNGROUPED_AGG);
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
        
        int batchSize = 0;
        long ts = scan.getTimeRange().getMax();
        HRegion region = c.getEnvironment().getRegion();
        List<Mutation> mutations = Collections.emptyList();
        if (isDelete || isUpsert || (deleteCQ != null && deleteCF != null) || emptyCF != null) {
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
        if (logger.isInfoEnabled()) {
        	logger.info("Starting ungrouped coprocessor scan " + scan);
        }
        long rowCount = 0;
        region.startRegionOperation();
        try {
            do {
                List<Cell> results = new ArrayList<Cell>();
                // Results are potentially returned even when the return value of s.next is false
                // since this is an indication of whether or not there are more values after the
                // ones returned
                hasMore = innerScanner.nextRaw(results);
                if (!results.isEmpty()) {
                    rowCount++;
                    result.setKeyValues(results);
                    try {
                        if (isDelete) {
                            // FIXME: the version of the Delete constructor without the lock args was introduced
                            // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
                            // of the client.
                            Cell firstKV = results.get(0);
                            Delete delete = new Delete(firstKV.getRowArray(), firstKV.getRowOffset(), 
                                firstKV.getRowLength(),ts);
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
                                    Object value = expression.getDataType().toObject(ptr, column.getSortOrder());
                                    // We are guaranteed that the two column will have the same type.
                                    if (!column.getDataType().isSizeCompatible(ptr, value, column.getDataType(),
                                            expression.getMaxLength(),  expression.getScale(), 
                                            column.getMaxLength(), column.getScale())) {
                                        throw new ValueTypeIncompatibleException(column.getDataType(),
                                                column.getMaxLength(), column.getScale());
                                    }
                                    column.getDataType().coerceBytes(ptr, value, expression.getDataType(),
                                            expression.getMaxLength(), expression.getScale(), expression.getSortOrder(), 
                                            column.getMaxLength(), column.getScale(), column.getSortOrder());
                                    byte[] bytes = ByteUtil.copyKeyBytesIfNecessary(ptr);
                                    row.setValue(column, bytes);
                                }
                            }
                            for (Mutation mutation : row.toRowMutations()) {
                                mutations.add(mutation);
                            }
                        } else if (deleteCF != null && deleteCQ != null) {
                            // No need to search for delete column, since we project only it
                            // if no empty key value is being set
                            if (emptyCF == null || result.getValue(deleteCF, deleteCQ) != null) {
                                Delete delete = new Delete(results.get(0).getRowArray(), results.get(0).getRowOffset(), 
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
                            Set<Long> timeStamps = Sets.newHashSetWithExpectedSize(results.size());
                            for (Cell kv : results) {
                                long kvts = kv.getTimestamp();
                                if (!timeStamps.contains(kvts)) {
                                    Put put = new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
                                    put.add(emptyCF, QueryConstants.EMPTY_COLUMN_BYTES, kvts, ByteUtil.EMPTY_BYTE_ARRAY);
                                    mutations.add(put);
                                }
                            }
                        }
                        // Commit in batches based on UPSERT_BATCH_SIZE_ATTRIB in config
                        if (!mutations.isEmpty() && batchSize > 0 && mutations.size() % batchSize == 0) {
                            commitBatch(region, mutations, indexUUID);
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
}
