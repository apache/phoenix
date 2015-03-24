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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import com.google.common.collect.ImmutableList;


abstract public class BaseScannerRegionObserver extends BaseRegionObserver {
    
    public static final String AGGREGATORS = "_Aggs";
    public static final String UNORDERED_GROUP_BY_EXPRESSIONS = "_UnorderedGroupByExpressions";
    public static final String KEY_ORDERED_GROUP_BY_EXPRESSIONS = "_OrderedGroupByExpressions";
    public static final String ESTIMATED_DISTINCT_VALUES = "_EstDistinctValues";
    public static final String NON_AGGREGATE_QUERY = "_NonAggregateQuery";
    public static final String TOPN = "_TopN";
    public static final String UNGROUPED_AGG = "_UngroupedAgg";
    public static final String DELETE_AGG = "_DeleteAgg";
    public static final String UPSERT_SELECT_TABLE = "_UpsertSelectTable";
    public static final String UPSERT_SELECT_EXPRS = "_UpsertSelectExprs";
    public static final String DELETE_CQ = "_DeleteCQ";
    public static final String DELETE_CF = "_DeleteCF";
    public static final String EMPTY_CF = "_EmptyCF";
    public static final String SPECIFIC_ARRAY_INDEX = "_SpecificArrayIndex";
    public static final String GROUP_BY_LIMIT = "_GroupByLimit";
    public static final String LOCAL_INDEX = "_LocalIndex";
    public static final String LOCAL_INDEX_BUILD = "_LocalIndexBuild";
    public static final String LOCAL_INDEX_JOIN_SCHEMA = "_LocalIndexJoinSchema";
    public static final String DATA_TABLE_COLUMNS_TO_JOIN = "_DataTableColumnsToJoin";
    public static final String VIEW_CONSTANTS = "_ViewConstants";
    public static final String STARTKEY_OFFSET = "_StartKeyOffset";
    public static final String EXPECTED_UPPER_REGION_KEY = "_ExpectedUpperRegionKey";
    public static final String REVERSE_SCAN = "_ReverseScan";
    public static final String ANALYZE_TABLE = "_ANALYZETABLE";
    public static final String GUIDEPOST_WIDTH_BYTES = "_GUIDEPOST_WIDTH_BYTES";
    public static final String GUIDEPOST_PER_REGION = "_GUIDEPOST_PER_REGION";
    /**
     * Attribute name used to pass custom annotations in Scans and Mutations (later). Custom annotations
     * are used to augment log lines emitted by Phoenix. See https://issues.apache.org/jira/browse/PHOENIX-1198.
     */
    public static final String CUSTOM_ANNOTATIONS = "_Annot"; 

    /** Exposed for testing */
    public static final String SCANNER_OPENED_TRACE_INFO = "Scanner opened on server";
    protected Configuration rawConf;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        this.rawConf =
                ((RegionCoprocessorEnvironment) e).getRegionServerServices().getConfiguration();
    }

    /**
     * Used by logger to identify coprocessor
     */
    @Override
    public String toString() {
        return this.getClass().getName();
    }
    
    
    private static void throwIfScanOutOfRegion(Scan scan, HRegion region) throws DoNotRetryIOException {
        boolean isLocalIndex = ScanUtil.isLocalIndex(scan);
        byte[] lowerInclusiveScanKey = scan.getStartRow();
        byte[] upperExclusiveScanKey = scan.getStopRow();
        byte[] lowerInclusiveRegionKey = region.getStartKey();
        byte[] upperExclusiveRegionKey = region.getEndKey();
        boolean isStaleRegionBoundaries;
        if (isLocalIndex) {
            byte[] expectedUpperRegionKey = scan.getAttribute(EXPECTED_UPPER_REGION_KEY);
            isStaleRegionBoundaries = expectedUpperRegionKey != null &&
                    Bytes.compareTo(upperExclusiveRegionKey, expectedUpperRegionKey) != 0;
        } else {
            isStaleRegionBoundaries = Bytes.compareTo(lowerInclusiveScanKey, lowerInclusiveRegionKey) < 0 ||
                    ( Bytes.compareTo(upperExclusiveScanKey, upperExclusiveRegionKey) > 0 && upperExclusiveRegionKey.length != 0);
        }
        if (isStaleRegionBoundaries) {
            Exception cause = new StaleRegionBoundaryCacheException(region.getRegionInfo().getTable().getNameAsString());
            throw new DoNotRetryIOException(cause.getMessage(), cause);
        }
    }

    abstract protected boolean isRegionObserverFor(Scan scan);
    abstract protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable;
    
    @Override
    public RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Scan scan, final RegionScanner s) throws IOException {
        if (isRegionObserverFor(scan)) {
            throwIfScanOutOfRegion(scan, c.getEnvironment().getRegion());
            // Muck with the start/stop row of the scan and set as reversed at the
            // last possible moment. You need to swap the start/stop and make the
            // start exclusive and the stop inclusive.
            ScanUtil.setupReverseScan(scan);
        }
        return s;
    }

    /**
     * Wrapper for {@link #postScannerOpen(ObserverContext, Scan, RegionScanner)} that ensures no non IOException is thrown,
     * to prevent the coprocessor from becoming blacklisted.
     * 
     */
    @Override
    public final RegionScanner postScannerOpen(
            final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan,
            final RegionScanner s) throws IOException {
       try {
            if (!isRegionObserverFor(scan)) {
                return s;
            }
            boolean success =false;
            // Save the current span. When done with the child span, reset the span back to
            // what it was. Otherwise, this causes the thread local storing the current span 
            // to not be reset back to null causing catastrophic infinite loops
            // and region servers to crash. See https://issues.apache.org/jira/browse/PHOENIX-1596
            // TraceScope can't be used here because closing the scope will end up calling 
            // currentSpan.stop() and that should happen only when we are closing the scanner.
            final Span savedSpan = Trace.currentSpan();
            final Span child = Trace.startSpan(SCANNER_OPENED_TRACE_INFO, savedSpan).getSpan();
            try {
                RegionScanner scanner = doPostScannerOpen(c, scan, s);
                scanner = new DelegateRegionScanner(scanner) {
                    // This isn't very obvious but close() could be called in a thread
                    // that is different from the thread that created the scanner.
                    @Override
                    public void close() throws IOException {
                        try {
                            delegate.close();
                        } finally {
                            if (child != null) {
                                child.stop();
                            }
                        }
                    }
                };
                success = true;
                return scanner;
            } finally {
                try {
                    if (!success && child != null) {
                        child.stop();
                    }
                } finally {
                    Trace.continueSpan(savedSpan);
                }
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
            return null; // impossible
        }
    }

    /**
     * Return wrapped scanner that catches unexpected exceptions (i.e. Phoenix bugs) and
     * re-throws as DoNotRetryIOException to prevent needless retrying hanging the query
     * for 30 seconds. Unfortunately, until HBASE-7481 gets fixed, there's no way to do
     * the same from a custom filter.
     * @param offset starting position in the rowkey.
     * @param scan
     * @param tupleProjector
     * @param dataRegion
     * @param indexMaintainer
     * @param viewConstants
     */
    protected RegionScanner getWrappedScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
            final RegionScanner s, final int offset, final Scan scan,
            final ColumnReference[] dataColumns, final TupleProjector tupleProjector,
            final HRegion dataRegion, final IndexMaintainer indexMaintainer,
            final byte[][] viewConstants, final TupleProjector projector,
            final ImmutableBytesWritable ptr) {
        return getWrappedScanner(c, s, null, null, offset, scan, dataColumns, tupleProjector,
                dataRegion, indexMaintainer, viewConstants, null, null, projector, ptr);
    }
    
    /**
     * Return wrapped scanner that catches unexpected exceptions (i.e. Phoenix bugs) and
     * re-throws as DoNotRetryIOException to prevent needless retrying hanging the query
     * for 30 seconds. Unfortunately, until HBASE-7481 gets fixed, there's no way to do
     * the same from a custom filter.
     * @param arrayFuncRefs
     * @param arrayKVRefs
     * @param offset starting position in the rowkey.
     * @param scan
     * @param tupleProjector
     * @param dataRegion
     * @param indexMaintainer
     * @param viewConstants
     */
    protected RegionScanner getWrappedScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
            final RegionScanner s, final Set<KeyValueColumnExpression> arrayKVRefs,
            final Expression[] arrayFuncRefs, final int offset, final Scan scan,
            final ColumnReference[] dataColumns, final TupleProjector tupleProjector,
            final HRegion dataRegion, final IndexMaintainer indexMaintainer,
            final byte[][] viewConstants, final KeyValueSchema kvSchema, 
            final ValueBitSet kvSchemaBitSet, final TupleProjector projector,
            final ImmutableBytesWritable ptr) {
        return new RegionScanner() {

            @Override
            public boolean next(List<Cell> results) throws IOException {
                try {
                    return s.next(results);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean next(List<Cell> result, int limit) throws IOException {
                try {
                    return s.next(result, limit);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public void close() throws IOException {
                s.close();
            }

            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() throws IOException {
                return s.isFilterDone();
            }

            @Override
            public boolean reseek(byte[] row) throws IOException {
                return s.reseek(row);
            }

            @Override
            public long getMvccReadPoint() {
                return s.getMvccReadPoint();
            }

            @Override
            public boolean nextRaw(List<Cell> result) throws IOException {
                try {
                    boolean next = s.nextRaw(result);
                    if (result.size() == 0) {
                        return next;
                    }
                    if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
                        replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
                    }
                    if (ScanUtil.isLocalIndex(scan) && !ScanUtil.isAnalyzeTable(scan)) {
                        IndexUtil.wrapResultUsingOffset(c, result, offset, dataColumns,
                            tupleProjector, dataRegion, indexMaintainer, viewConstants, ptr);
                    }
                    if (projector != null) {
                        Tuple tuple = projector.projectResults(new ResultTuple(Result.create(result)));
                        result.clear();
                        result.add(tuple.getValue(0));
                    }
                    // There is a scanattribute set to retrieve the specific array element
                    return next;
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean nextRaw(List<Cell> result, int limit) throws IOException {
                try {
                    boolean next = s.nextRaw(result, limit);
                    if (result.size() == 0) {
                        return next;
                    }
                    if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
                        replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
                    }
                    if ((offset > 0 || ScanUtil.isLocalIndex(scan))  && !ScanUtil.isAnalyzeTable(scan)) {
                        IndexUtil.wrapResultUsingOffset(c, result, offset, dataColumns,
                            tupleProjector, dataRegion, indexMaintainer, viewConstants, ptr);
                    }
                    if (projector != null) {
                        Tuple tuple = projector.projectResults(new ResultTuple(Result.create(result)));
                        result.clear();
                        result.add(tuple.getValue(0));
                    }
                    // There is a scanattribute set to retrieve the specific array element
                    return next;
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            private void replaceArrayIndexElement(final Set<KeyValueColumnExpression> arrayKVRefs,
                    final Expression[] arrayFuncRefs, List<Cell> result) {
                // make a copy of the results array here, as we're modifying it below
                MultiKeyValueTuple tuple = new MultiKeyValueTuple(ImmutableList.copyOf(result));
                // The size of both the arrays would be same?
                // Using KeyValueSchema to set and retrieve the value
                // collect the first kv to get the row
                Cell rowKv = result.get(0);
                for (KeyValueColumnExpression kvExp : arrayKVRefs) {
                    if (kvExp.evaluate(tuple, ptr)) {
                        for (int idx = tuple.size() - 1; idx >= 0; idx--) {
                            Cell kv = tuple.getValue(idx);
                            if (Bytes.equals(kvExp.getColumnFamily(), 0, kvExp.getColumnFamily().length,
                                    kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength())
                                && Bytes.equals(kvExp.getColumnName(), 0, kvExp.getColumnName().length,
                                        kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength())) {
                                // remove the kv that has the full array values.
                                result.remove(idx);
                                break;
                            }
                        }
                    }
                }
                byte[] value = kvSchema.toBytes(tuple, arrayFuncRefs,
                        kvSchemaBitSet, ptr);
                // Add a dummy kv with the exact value of the array index
                result.add(new KeyValue(rowKv.getRowArray(), rowKv.getRowOffset(), rowKv.getRowLength(),
                        QueryConstants.ARRAY_VALUE_COLUMN_FAMILY, 0, QueryConstants.ARRAY_VALUE_COLUMN_FAMILY.length,
                        QueryConstants.ARRAY_VALUE_COLUMN_QUALIFIER, 0,
                        QueryConstants.ARRAY_VALUE_COLUMN_QUALIFIER.length, HConstants.LATEST_TIMESTAMP,
                        Type.codeToType(rowKv.getTypeByte()), value, 0, value.length));
            }

            @Override
            public long getMaxResultSize() {
                return s.getMaxResultSize();
            }
        };
    }
}