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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;


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
    public static final String EXPECTED_UPPER_REGION_KEY = "_ExpectedUpperRegionKey";
    public static final String REVERSE_SCAN = "_ReverseScan";
    public static final String ANALYZE_TABLE = "_ANALYZETABLE";
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
}