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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ScannerContextUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.filter.PagedFilter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.iterate.NonAggregateRegionScannerFactory;
import org.apache.phoenix.iterate.RegionScannerFactory;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForFilter;

abstract public class BaseScannerRegionObserver extends CompatBaseScannerRegionObserver {

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
    public static final String UNCOVERED_GLOBAL_INDEX = "_UncoveredGlobalIndex";
    public static final String INDEX_REBUILD_PAGING = "_IndexRebuildPaging";
    // The number of index rows to be rebuild in one RPC call
    public static final String INDEX_REBUILD_PAGE_ROWS = "_IndexRebuildPageRows";
    public static final String INDEX_PAGE_ROWS = "_IndexPageRows";
    public static final String SERVER_PAGE_SIZE_MS = "_ServerPageSizeMs";
    // Index verification type done by the index tool
    public static final String INDEX_REBUILD_VERIFY_TYPE = "_IndexRebuildVerifyType";
    public static final String INDEX_RETRY_VERIFY = "_IndexRetryVerify";
    public static final String INDEX_REBUILD_DISABLE_LOGGING_VERIFY_TYPE =
        "_IndexRebuildDisableLoggingVerifyType";
    public static final String INDEX_REBUILD_DISABLE_LOGGING_BEYOND_MAXLOOKBACK_AGE =
        "_IndexRebuildDisableLoggingBeyondMaxLookbackAge";
    @Deprecated
    public static final String LOCAL_INDEX_FILTER = "_LocalIndexFilter";
    @Deprecated
    public static final String LOCAL_INDEX_LIMIT = "_LocalIndexLimit";
    @Deprecated
    public static final String LOCAL_INDEX_FILTER_STR = "_LocalIndexFilterStr";
    public static final String INDEX_FILTER = "_IndexFilter";
    public static final String INDEX_LIMIT = "_IndexLimit";
    public static final String INDEX_FILTER_STR = "_IndexFilterStr";

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
    public static final String PHOENIX_TTL = "_PhoenixTTL";
    public static final String MASK_PHOENIX_TTL_EXPIRED = "_MASK_TTL_EXPIRED";
    public static final String DELETE_PHOENIX_TTL_EXPIRED = "_DELETE_TTL_EXPIRED";
    public static final String PHOENIX_TTL_SCAN_TABLE_NAME = "_PhoenixTTLScanTableName";
    public static final String SCAN_ACTUAL_START_ROW = "_ScanActualStartRow";
    public static final String REPLAY_WRITES = "_IGNORE_NEWER_MUTATIONS";
    public final static String SCAN_OFFSET = "_RowOffset";
    public static final String SCAN_START_ROW_SUFFIX = "_ScanStartRowSuffix";
    public static final String SCAN_STOP_ROW_SUFFIX = "_ScanStopRowSuffix";
    public final static String MIN_QUALIFIER = "_MinQualifier";
    public final static String MAX_QUALIFIER = "_MaxQualifier";
    public final static String USE_NEW_VALUE_COLUMN_QUALIFIER = "_UseNewValueColumnQualifier";
    public final static String QUALIFIER_ENCODING_SCHEME = "_QualifierEncodingScheme";
    public final static String IMMUTABLE_STORAGE_ENCODING_SCHEME = "_ImmutableStorageEncodingScheme";
    public final static String USE_ENCODED_COLUMN_QUALIFIER_LIST = "_UseEncodedColumnQualifierList";
    public static final String CLIENT_VERSION = "_ClientVersion";
    public static final String CHECK_VERIFY_COLUMN = "_CheckVerifyColumn";
    public static final String PHYSICAL_DATA_TABLE_NAME = "_PhysicalDataTableName";
    public static final String EMPTY_COLUMN_FAMILY_NAME = "_EmptyCFName";
    public static final String EMPTY_COLUMN_QUALIFIER_NAME = "_EmptyCQName";
    public static final String INDEX_ROW_KEY = "_IndexRowKey";
    
    public final static byte[] REPLAY_TABLE_AND_INDEX_WRITES = PUnsignedTinyint.INSTANCE.toBytes(1);
    public final static byte[] REPLAY_ONLY_INDEX_WRITES = PUnsignedTinyint.INSTANCE.toBytes(2);
    // In case of Index Write failure, we need to determine that Index mutation
    // is part of normal client write or Index Rebuilder. # PHOENIX-5080
    public final static byte[] REPLAY_INDEX_REBUILD_WRITES = PUnsignedTinyint.INSTANCE.toBytes(3);
    
    public enum ReplayWrite {
        TABLE_AND_INDEX,
        INDEX_ONLY,
        REBUILD_INDEX_ONLY;
        
        public static ReplayWrite fromBytes(byte[] replayWriteBytes) {
            if (replayWriteBytes == null) {
                return null;
            }
            if (Bytes.compareTo(REPLAY_TABLE_AND_INDEX_WRITES, replayWriteBytes) == 0) {
                return TABLE_AND_INDEX;
            }
            if (Bytes.compareTo(REPLAY_ONLY_INDEX_WRITES, replayWriteBytes) == 0) {
                return INDEX_ONLY;
            }
            if (Bytes.compareTo(REPLAY_INDEX_REBUILD_WRITES, replayWriteBytes) == 0) {
                return REBUILD_INDEX_ONLY;
            }
            throw new IllegalArgumentException("Unknown ReplayWrite code of " + Bytes.toStringBinary(replayWriteBytes));
        }
    };
    
    /**
     * Attribute name used to pass custom annotations in Scans and Mutations (later). Custom annotations
     * are used to augment log lines emitted by Phoenix. See https://issues.apache.org/jira/browse/PHOENIX-1198.
     */
    public static final String CUSTOM_ANNOTATIONS = "_Annot";

    /** Exposed for testing */
    public static final String SCANNER_OPENED_TRACE_INFO = "Scanner opened on server";

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
            // For local indexes we have to abort any scan that was open during a split.
            // We detect that condition as follows:
            // 1. The scanner's stop row has to always match the region's end key.
            // 2. Phoenix sets the SCAN_ACTUAL_START_ROW attribute to the scan's original start row
            //    We cannot directly compare that with the region's start key, but can enforce that
            //    the original start row still falls within the new region.
            byte[] expectedUpperRegionKey =
                    scan.getAttribute(EXPECTED_UPPER_REGION_KEY) == null ? scan.getStopRow() : scan
                            .getAttribute(EXPECTED_UPPER_REGION_KEY);

            byte[] actualStartRow = scan.getAttribute(SCAN_ACTUAL_START_ROW);
            isStaleRegionBoundaries = (expectedUpperRegionKey != null &&
                    Bytes.compareTo(upperExclusiveRegionKey, expectedUpperRegionKey) != 0) || 
                    (actualStartRow != null && Bytes.compareTo(actualStartRow, lowerInclusiveRegionKey) < 0);
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
            ScanUtil.setupLocalIndexScan(scan);
        }
    }

    abstract protected boolean isRegionObserverFor(Scan scan);
    abstract protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable;

    protected boolean skipRegionBoundaryCheck(Scan scan) {
        byte[] skipCheckBytes = scan.getAttribute(SKIP_REGION_BOUNDARY_CHECK);
        return skipCheckBytes != null && Bytes.toBoolean(skipCheckBytes);
    }

    @Override
    public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
            Scan scan) throws IOException {
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
            if (scan.getFilter() != null && !(scan.getFilter() instanceof PagedFilter)) {
                byte[] pageSizeMsBytes = scan.getAttribute(BaseScannerRegionObserver.SERVER_PAGE_SIZE_MS);
                if (pageSizeMsBytes != null) {
                    scan.setFilter(new PagedFilter(scan.getFilter(), getPageSizeMsForFilter(scan)));
                }
            }
        }
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
                boolean res = super.next(result);
                ScannerContextUtil.incrementSizeProgress(scannerContext, result);
                ScannerContextUtil.updateTimeProgress(scannerContext);
                return res;
            }

            @Override
            public boolean next(List<Cell> result) throws IOException {
                overrideDelegate();
                return super.next(result);
            }

            @Override
            public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
                overrideDelegate();
                boolean res = super.nextRaw(result);
                ScannerContextUtil.incrementSizeProgress(scannerContext, result);
                ScannerContextUtil.updateTimeProgress(scannerContext);
                return res;
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
            // Make sure PageRegionScanner wraps only the lowest region scanner, i.e., HBase region scanner. We assume
            // here every Phoenix region scanner extends BaseRegionScanner
            return new RegionScannerHolder(c, scan, s instanceof BaseRegionScanner ? s :
                    new PagedRegionScanner(c.getEnvironment().getRegion(), s, scan));
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
            final ImmutableBytesWritable ptr, final boolean useQualiferAsListIndex)
            throws IOException {

        RegionScannerFactory regionScannerFactory = new NonAggregateRegionScannerFactory(c.getEnvironment());

        return regionScannerFactory.getWrappedScanner(c.getEnvironment(), s, null, null, offset, scan, dataColumns, tupleProjector,
                dataRegion, indexMaintainer, null, viewConstants, null, null, projector, ptr, useQualiferAsListIndex);
    }


   /* We want to override the store scanner so that we can read "past" a delete
    marker on an SCN / lookback query to see the underlying edit. This was possible
    in HBase 1.x, but not possible after the interface changes in HBase 2.0. HBASE-24321 in
     HBase 2.3 gave us this ability back, but we need to use it through a compatibility shim
     so we can compile against 2.1 and 2.2. When 2.3 is the minimum supported HBase
     version, the shim can be retired and the logic moved into the real coproc.

    We also need to override the flush compaction coproc hooks in order to implement max lookback
     age to keep versions from being purged.

    Because the required APIs aren't present in HBase 2.1 and 2.2, we override in the 2.3
     version of CompatBaseScannerRegionObserver and no-op in the other versions. */

}
