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
import java.util.ListIterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
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
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.PositionBasedMultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.PositionBasedResultTuple;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.tephra.Transaction;

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
    public static final String EMPTY_COLUMN_QUALIFIER = "_EmptyColumnQualifier";
    public static final String SPECIFIC_ARRAY_INDEX = "_SpecificArrayIndex";
    public static final String GROUP_BY_LIMIT = "_GroupByLimit";
    public static final String LOCAL_INDEX = "_LocalIndex";
    public static final String LOCAL_INDEX_BUILD = "_LocalIndexBuild";
    /* 
    * Attribute to denote that the index maintainer has been serialized using its proto-buf presentation.
    * Needed for backward compatibility purposes. TODO: get rid of this in next major release.
    */
    public static final String LOCAL_INDEX_BUILD_PROTO = "_LocalIndexBuild"; 
    public static final String LOCAL_INDEX_JOIN_SCHEMA = "_LocalIndexJoinSchema";
    public static final String DATA_TABLE_COLUMNS_TO_JOIN = "_DataTableColumnsToJoin";
    public static final String COLUMNS_STORED_IN_SINGLE_CELL = "_ColumnsStoredInSingleCell";
    public static final String VIEW_CONSTANTS = "_ViewConstants";
    public static final String EXPECTED_UPPER_REGION_KEY = "_ExpectedUpperRegionKey";
    public static final String REVERSE_SCAN = "_ReverseScan";
    public static final String ANALYZE_TABLE = "_ANALYZETABLE";
    public static final String REBUILD_INDEXES = "_RebuildIndexes";
    public static final String TX_STATE = "_TxState";
    public static final String GUIDEPOST_WIDTH_BYTES = "_GUIDEPOST_WIDTH_BYTES";
    public static final String GUIDEPOST_PER_REGION = "_GUIDEPOST_PER_REGION";
    public static final String UPGRADE_DESC_ROW_KEY = "_UPGRADE_DESC_ROW_KEY";
    public static final String SCAN_REGION_SERVER = "_SCAN_REGION_SERVER";
    public static final String RUN_UPDATE_STATS_ASYNC_ATTRIB = "_RunUpdateStatsAsync";
    public static final String SKIP_REGION_BOUNDARY_CHECK = "_SKIP_REGION_BOUNDARY_CHECK";
    public static final String TX_SCN = "_TxScn";
    public static final String SCAN_ACTUAL_START_ROW = "_ScanActualStartRow";
    public static final String IGNORE_NEWER_MUTATIONS = "_IGNORE_NEWER_MUTATIONS";
    public final static String SCAN_OFFSET = "_RowOffset";
    public static final String SCAN_START_ROW_SUFFIX = "_ScanStartRowSuffix";
    public static final String SCAN_STOP_ROW_SUFFIX = "_ScanStopRowSuffix";
    public final static String MIN_QUALIFIER = "_MinQualifier";
    public final static String MAX_QUALIFIER = "_MaxQualifier";
    public final static String USE_NEW_VALUE_COLUMN_QUALIFIER = "_UseNewValueColumnQualifier";
    public final static String QUALIFIER_ENCODING_SCHEME = "_QualifierEncodingScheme";
    public final static String IMMUTABLE_STORAGE_ENCODING_SCHEME = "_ImmutableStorageEncodingScheme";
    public final static String USE_ENCODED_COLUMN_QUALIFIER_LIST = "_UseEncodedColumnQualifierList";
    
    /**
     * Attribute name used to pass custom annotations in Scans and Mutations (later). Custom annotations
     * are used to augment log lines emitted by Phoenix. See https://issues.apache.org/jira/browse/PHOENIX-1198.
     */
    public static final String CUSTOM_ANNOTATIONS = "_Annot";

    /** Exposed for testing */
    public static final String SCANNER_OPENED_TRACE_INFO = "Scanner opened on server";
    protected Configuration rawConf;
    protected QualifierEncodingScheme encodingScheme;
    protected boolean useNewValueColumnQualifier;

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


    private static void throwIfScanOutOfRegion(Scan scan, Region region) throws DoNotRetryIOException {
        boolean isLocalIndex = ScanUtil.isLocalIndex(scan);
        byte[] lowerInclusiveScanKey = scan.getStartRow();
        byte[] upperExclusiveScanKey = scan.getStopRow();
        byte[] lowerInclusiveRegionKey = region.getRegionInfo().getStartKey();
        byte[] upperExclusiveRegionKey = region.getRegionInfo().getEndKey();
        boolean isStaleRegionBoundaries;
        if (isLocalIndex) {
            byte[] expectedUpperRegionKey =
                    scan.getAttribute(EXPECTED_UPPER_REGION_KEY) == null ? scan.getStopRow() : scan
                            .getAttribute(EXPECTED_UPPER_REGION_KEY);
            isStaleRegionBoundaries = expectedUpperRegionKey != null &&
                    Bytes.compareTo(upperExclusiveRegionKey, expectedUpperRegionKey) != 0;
        } else {
            isStaleRegionBoundaries = Bytes.compareTo(lowerInclusiveScanKey, lowerInclusiveRegionKey) < 0 ||
                    ( Bytes.compareTo(upperExclusiveScanKey, upperExclusiveRegionKey) > 0 && upperExclusiveRegionKey.length != 0) ||
                    (upperExclusiveRegionKey.length != 0 && upperExclusiveScanKey.length == 0);
        }
        if (isStaleRegionBoundaries) {
            Exception cause = new StaleRegionBoundaryCacheException(region.getRegionInfo().getTable().getNameAsString());
            throw new DoNotRetryIOException(cause.getMessage(), cause);
        }
        if(isLocalIndex) {
            ScanUtil.setupLocalIndexScan(scan, lowerInclusiveRegionKey, upperExclusiveRegionKey);
        }
    }

    abstract protected boolean isRegionObserverFor(Scan scan);
    abstract protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable;

    protected boolean skipRegionBoundaryCheck(Scan scan) {
        byte[] skipCheckBytes = scan.getAttribute(SKIP_REGION_BOUNDARY_CHECK);
        return skipCheckBytes != null && Bytes.toBoolean(skipCheckBytes);
    }

    @Override
    public RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Scan scan, final RegionScanner s) throws IOException {
        byte[] txnScn = scan.getAttribute(TX_SCN);
        if (txnScn!=null) {
            TimeRange timeRange = scan.getTimeRange();
            scan.setTimeRange(timeRange.getMin(), Bytes.toLong(txnScn));
        }
        if (isRegionObserverFor(scan)) {
            // For local indexes, we need to throw if out of region as we'll get inconsistent
            // results otherwise while in other cases, it may just mean out client-side data
            // on region boundaries is out of date and can safely be ignored.
            if (!skipRegionBoundaryCheck(scan) || ScanUtil.isLocalIndex(scan)) {
                throwIfScanOutOfRegion(scan, c.getEnvironment().getRegion());
            }
            // Muck with the start/stop row of the scan and set as reversed at the
            // last possible moment. You need to swap the start/stop and make the
            // start exclusive and the stop inclusive.
            ScanUtil.setupReverseScan(scan);
        }
        this.encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
        this.useNewValueColumnQualifier = EncodedColumnsUtil.useNewValueColumnQualifier(scan);
        return s;
    }

    private class RegionScannerHolder extends DelegateRegionScanner {
            private final Scan scan;
            private final ObserverContext<RegionCoprocessorEnvironment> c;
            private boolean wasOverriden;
            
            public RegionScannerHolder(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, final RegionScanner scanner) {
                super(scanner);
                this.c = c;
                this.scan = scan;
            }
    
            private void overrideDelegate() throws IOException {
                if (wasOverriden) {
                    return;
                }
                boolean success = false;
                // Save the current span. When done with the child span, reset the span back to
                // what it was. Otherwise, this causes the thread local storing the current span
                // to not be reset back to null causing catastrophic infinite loops
                // and region servers to crash. See https://issues.apache.org/jira/browse/PHOENIX-1596
                // TraceScope can't be used here because closing the scope will end up calling
                // currentSpan.stop() and that should happen only when we are closing the scanner.
                final Span savedSpan = Trace.currentSpan();
                final Span child = Trace.startSpan(SCANNER_OPENED_TRACE_INFO, savedSpan).getSpan();
                try {
                    RegionScanner scanner = doPostScannerOpen(c, scan, delegate);
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
                    this.delegate = scanner;
                    wasOverriden = true;
                    success = true;
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegionInfo().getRegionNameAsString(), t);
                } finally {
                    try {
                        if (!success && child != null) {
                            child.stop();
                        }
                    } finally {
                        Trace.continueSpan(savedSpan);
                    }
                }
            }
            
            @Override
            public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
                overrideDelegate();
                return super.next(result, scannerContext);
            }

            @Override
            public boolean next(List<Cell> result) throws IOException {
                overrideDelegate();
                return super.next(result);
            }

            @Override
            public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
                overrideDelegate();
                return super.nextRaw(result, scannerContext);
            }
            
            @Override
            public boolean nextRaw(List<Cell> result) throws IOException {
                overrideDelegate();
                return super.nextRaw(result);
            }
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
            return new RegionScannerHolder(c, scan, s);
        } catch (Throwable t) {
            // If the exception is NotServingRegionException then throw it as
            // StaleRegionBoundaryCacheException to handle it by phoenix client other wise hbase
            // client may recreate scans with wrong region boundaries.
            if(t instanceof NotServingRegionException) {
                Exception cause = new StaleRegionBoundaryCacheException(c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString());
                throw new DoNotRetryIOException(cause.getMessage(), cause);
            }
            ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString(), t);
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
    RegionScanner getWrappedScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
            final RegionScanner s, final int offset, final Scan scan,
            final ColumnReference[] dataColumns, final TupleProjector tupleProjector,
            final Region dataRegion, final IndexMaintainer indexMaintainer,
            final byte[][] viewConstants, final TupleProjector projector,
            final ImmutableBytesWritable ptr, final boolean useQualiferAsListIndex) {
        return getWrappedScanner(c, s, null, null, offset, scan, dataColumns, tupleProjector,
                dataRegion, indexMaintainer, null, viewConstants, null, null, projector, ptr, useQualiferAsListIndex);
    }

    /**
     * Return wrapped scanner that catches unexpected exceptions (i.e. Phoenix bugs) and
     * re-throws as DoNotRetryIOException to prevent needless retrying hanging the query
     * for 30 seconds. Unfortunately, until HBASE-7481 gets fixed, there's no way to do
     * the same from a custom filter.
     * @param arrayKVRefs
     * @param arrayFuncRefs
     * @param offset starting position in the rowkey.
     * @param scan
     * @param tupleProjector
     * @param dataRegion
     * @param indexMaintainer
     * @param tx current transaction
     * @param viewConstants
     */
    RegionScanner getWrappedScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
            final RegionScanner s, final Set<KeyValueColumnExpression> arrayKVRefs,
            final Expression[] arrayFuncRefs, final int offset, final Scan scan,
            final ColumnReference[] dataColumns, final TupleProjector tupleProjector,
            final Region dataRegion, final IndexMaintainer indexMaintainer,
            Transaction tx, 
            final byte[][] viewConstants, final KeyValueSchema kvSchema,
            final ValueBitSet kvSchemaBitSet, final TupleProjector projector,
            final ImmutableBytesWritable ptr, final boolean useQualifierAsListIndex) {
        return new RegionScanner() {

            private boolean hasReferences = checkForReferenceFiles();
            private HRegionInfo regionInfo = c.getEnvironment().getRegionInfo();
            private byte[] actualStartKey = getActualStartKey();

            // If there are any reference files after local index region merge some cases we might
            // get the records less than scan start row key. This will happen when we replace the
            // actual region start key with merge region start key. This method gives whether are
            // there any reference files in the region or not.
            private boolean checkForReferenceFiles() {
                if(!ScanUtil.isLocalIndex(scan)) return false;
                for (byte[] family : scan.getFamilies()) {
                    if (c.getEnvironment().getRegion().getStore(family).hasReferences()) {
                        return true;
                    }
                }
                return false;
            }

            // Get the actual scan start row of local index. This will be used to compare the row
            // key of the results less than scan start row when there are references.
            public byte[] getActualStartKey() {
                return ScanUtil.isLocalIndex(scan) ? ScanUtil.getActualStartRow(scan, regionInfo)
                        : null;
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                try {
                    return s.next(results);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
                try {
                    return s.next(result, scannerContext);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString(), t);
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
                    Cell arrayElementCell = null;
                    if (result.size() == 0) {
                        return next;
                    }
                    if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
                        int arrayElementCellPosition = replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
                        arrayElementCell = result.get(arrayElementCellPosition);
                    }
                    if (ScanUtil.isLocalIndex(scan) && !ScanUtil.isAnalyzeTable(scan)) {
                        if(hasReferences && actualStartKey!=null) {
                            next = scanTillScanStartRow(s, arrayKVRefs, arrayFuncRefs, result,
                                null, arrayElementCell);
                            if (result.isEmpty()) {
                                return next;
                            }
                        }
                        IndexUtil.wrapResultUsingOffset(c, result, offset, dataColumns,
                            tupleProjector, dataRegion, indexMaintainer, viewConstants, ptr);
                    }
                    if (projector != null) {
                        Tuple toProject = useQualifierAsListIndex ? new PositionBasedResultTuple(result) : new ResultTuple(Result.create(result));
                        Tuple tuple = projector.projectResults(toProject, useNewValueColumnQualifier);
                        result.clear();
                        result.add(tuple.getValue(0));
                        if (arrayElementCell != null) {
                            result.add(arrayElementCell);
                        }
                    }
                    // There is a scanattribute set to retrieve the specific array element
                    return next;
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
                throws IOException {
              try {
                boolean next = s.nextRaw(result, scannerContext);
                Cell arrayElementCell = null;
                if (result.size() == 0) {
                    return next;
                }
                if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
                    int arrayElementCellPosition = replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
                    arrayElementCell = result.get(arrayElementCellPosition);
                }
                if ((offset > 0 || ScanUtil.isLocalIndex(scan))  && !ScanUtil.isAnalyzeTable(scan)) {
                    if(hasReferences && actualStartKey!=null) {
                        next = scanTillScanStartRow(s, arrayKVRefs, arrayFuncRefs, result,
                            scannerContext, arrayElementCell);
                        if (result.isEmpty()) {
                            return next;
                        }
                    }
                    IndexUtil.wrapResultUsingOffset(c, result, offset, dataColumns,
                        tupleProjector, dataRegion, indexMaintainer, viewConstants, ptr);
                }
                if (projector != null) {
                    Tuple toProject = useQualifierAsListIndex ? new PositionBasedMultiKeyValueTuple(result) : new ResultTuple(Result.create(result));
                    Tuple tuple = projector.projectResults(toProject, useNewValueColumnQualifier);
                    result.clear();
                    result.add(tuple.getValue(0));
                    if(arrayElementCell != null)
                        result.add(arrayElementCell);
                }
                // There is a scanattribute set to retrieve the specific array element
                return next;
              } catch (Throwable t) {
                ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString(), t);
                return false; // impossible
              }
            }

            /**
             * When there is a merge in progress while scanning local indexes we might get the key values less than scan start row.
             * In that case we need to scan until get the row key more or  equal to scan start key.
             * TODO try to fix this case in LocalIndexStoreFileScanner when there is a merge.
             */
            private boolean scanTillScanStartRow(final RegionScanner s,
                    final Set<KeyValueColumnExpression> arrayKVRefs,
                    final Expression[] arrayFuncRefs, List<Cell> result,
                    ScannerContext scannerContext, Cell arrayElementCell) throws IOException {
                boolean next = true;
                Cell firstCell = result.get(0);
                while (Bytes.compareTo(firstCell.getRowArray(), firstCell.getRowOffset(),
                    firstCell.getRowLength(), actualStartKey, 0, actualStartKey.length) < 0) {
                    result.clear();
                    if(scannerContext == null) {
                        next = s.nextRaw(result);
                    } else {
                        next = s.nextRaw(result, scannerContext);
                    }
                    if (result.isEmpty()) {
                        return next;
                    }
                    if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
                        int arrayElementCellPosition = replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
                        arrayElementCell = result.get(arrayElementCellPosition);
                    }
                    firstCell = result.get(0);
                }
                return next;
            }

            private int replaceArrayIndexElement(final Set<KeyValueColumnExpression> arrayKVRefs,
                    final Expression[] arrayFuncRefs, List<Cell> result) {
             // make a copy of the results array here, as we're modifying it below
                MultiKeyValueTuple tuple = new MultiKeyValueTuple(ImmutableList.copyOf(result));
                // The size of both the arrays would be same?
                // Using KeyValueSchema to set and retrieve the value
                // collect the first kv to get the row
                Cell rowKv = result.get(0);
                for (KeyValueColumnExpression kvExp : arrayKVRefs) {
                    if (kvExp.evaluate(tuple, ptr)) {
                        ListIterator<Cell> itr = result.listIterator();
                        while (itr.hasNext()) {
                            Cell kv = itr.next();
                            if (Bytes.equals(kvExp.getColumnFamily(), 0, kvExp.getColumnFamily().length,
                                    kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength())
                                && Bytes.equals(kvExp.getColumnQualifier(), 0, kvExp.getColumnQualifier().length,
                                        kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength())) {
                                // remove the kv that has the full array values.
                                itr.remove();
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
                return result.size() - 1;
            }

            @Override
            public long getMaxResultSize() {
                return s.getMaxResultSize();
            }

            @Override
            public int getBatch() {
                return s.getBatch();
            }
        };
    }
}
