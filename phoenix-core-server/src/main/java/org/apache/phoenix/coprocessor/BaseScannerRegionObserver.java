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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ScannerContextUtil;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.filter.PagingFilter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.iterate.NonAggregateRegionScannerFactory;
import org.apache.phoenix.iterate.RegionScannerFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForFilter;

abstract public class BaseScannerRegionObserver implements RegionObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseScannerRegionObserver.class);

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
                    scan.getAttribute(BaseScannerRegionObserverConstants.EXPECTED_UPPER_REGION_KEY) == null ? scan.getStopRow() : scan
                            .getAttribute(BaseScannerRegionObserverConstants.EXPECTED_UPPER_REGION_KEY);

            byte[] actualStartRow = scan.getAttribute(BaseScannerRegionObserverConstants.SCAN_ACTUAL_START_ROW);
            isStaleRegionBoundaries = (expectedUpperRegionKey != null &&
                    Bytes.compareTo(upperExclusiveRegionKey, expectedUpperRegionKey) != 0) || 
                    (actualStartRow != null && Bytes.compareTo(actualStartRow, lowerInclusiveRegionKey) < 0);
        } else {
            if (scan.isReversed()) {
                isStaleRegionBoundaries =
                        Bytes.compareTo(upperExclusiveScanKey, lowerInclusiveRegionKey) < 0 ||
                                (Bytes.compareTo(lowerInclusiveScanKey, upperExclusiveRegionKey) >
                                        0 && upperExclusiveRegionKey.length != 0) ||
                                (upperExclusiveRegionKey.length != 0 &&
                                        lowerInclusiveScanKey.length == 0);
            } else {
                isStaleRegionBoundaries =
                        Bytes.compareTo(lowerInclusiveScanKey, lowerInclusiveRegionKey) < 0 ||
                                (Bytes.compareTo(upperExclusiveScanKey, upperExclusiveRegionKey) >
                                        0 && upperExclusiveRegionKey.length != 0) ||
                                (upperExclusiveRegionKey.length != 0 &&
                                        upperExclusiveScanKey.length == 0);
            }
        }
        if (isStaleRegionBoundaries) {
            LOGGER.error("Throwing StaleRegionBoundaryCacheException due to mismatched scan "
                            + "boundaries. Region: {} , lowerInclusiveScanKey: {} , "
                            + "upperExclusiveScanKey: {} , lowerInclusiveRegionKey: {} , "
                            + "upperExclusiveRegionKey: {} , scan reversed: {}",
                    region.getRegionInfo().getRegionNameAsString(),
                    Bytes.toStringBinary(lowerInclusiveScanKey),
                    Bytes.toStringBinary(upperExclusiveScanKey),
                    Bytes.toStringBinary(lowerInclusiveRegionKey),
                    Bytes.toStringBinary(upperExclusiveRegionKey),
                    scan.isReversed());
            Exception cause = new StaleRegionBoundaryCacheException(
                    region.getRegionInfo().getTable().getNameAsString());
            throw new DoNotRetryIOException(cause.getMessage(), cause);
        }
        if(isLocalIndex) {
            ScanUtil.setupLocalIndexScan(scan);
        }
    }

    abstract protected boolean isRegionObserverFor(Scan scan);
    abstract protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable;

    protected boolean skipRegionBoundaryCheck(Scan scan) {
        byte[] skipCheckBytes = scan.getAttribute(BaseScannerRegionObserverConstants.SKIP_REGION_BOUNDARY_CHECK);
        return skipCheckBytes != null && Bytes.toBoolean(skipCheckBytes);
    }

    @Override
    public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
            Scan scan) throws IOException {
        byte[] txnScn = scan.getAttribute(BaseScannerRegionObserverConstants.TX_SCN);
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
            // Set the paging filter. Make sure that the paging filter is the top level
            // filter if paging is enabled, that is pageSizeMsBytes != null.
            if (!(scan.getFilter() instanceof PagingFilter)) {
                byte[] pageSizeMsBytes =
                        scan.getAttribute(BaseScannerRegionObserverConstants.SERVER_PAGE_SIZE_MS);
                if (pageSizeMsBytes != null) {
                    scan.setFilter(new PagingFilter(scan.getFilter(),
                            getPageSizeMsForFilter(scan)));
                }
            }
        }
    }

    private class RegionScannerHolder extends DelegateRegionScanner {
            private final Scan scan;
            private final ObserverContext<RegionCoprocessorEnvironment> c;
            private boolean wasOverriden;
            
            public RegionScannerHolder(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
                    final RegionScanner scanner) {
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
                final Span child = Trace.startSpan(BaseScannerRegionObserverConstants.SCANNER_OPENED_TRACE_INFO, savedSpan).getSpan();
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
                    ClientUtil.throwIOException(c.getEnvironment().getRegionInfo().getRegionNameAsString(), t);
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
                return res;
            }
            
            @Override
            public boolean nextRaw(List<Cell> result) throws IOException {
                overrideDelegate();
                return super.nextRaw(result);
            }
            @Override
            public RegionScanner getNewRegionScanner(Scan scan) throws IOException {
                try {
                    return new RegionScannerHolder(c, scan,
                            ((DelegateRegionScanner) delegate).getNewRegionScanner(scan));
                } catch (ClassCastException e) {
                    throw new DoNotRetryIOException(e);
                }
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
            byte[] emptyCF = scan.getAttribute(
                    BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME);
            byte[] emptyCQ = scan.getAttribute(
                    BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME);
            // Make sure PageRegionScanner wraps only the lowest region scanner, i.e., HBase region
            // scanner. We assume here every Phoenix region scanner extends DelegateRegionScanner.
            if (s instanceof DelegateRegionScanner) {
                return new RegionScannerHolder(c, scan, s);
            } else {
                // An old client may not set these attributes which are required by TTLRegionScanner
                if (emptyCF != null && emptyCQ != null) {
                    return new RegionScannerHolder(c, scan,
                            new TTLRegionScanner(c.getEnvironment(), scan,
                                    new PagingRegionScanner(c.getEnvironment().getRegion(), s,
                                            scan)));
                }
                return new RegionScannerHolder(c, scan,
                        new PagingRegionScanner(c.getEnvironment().getRegion(), s, scan));

            }
        } catch (Throwable t) {
            // If the exception is NotServingRegionException then throw it as
            // StaleRegionBoundaryCacheException to handle it by phoenix client other wise hbase
            // client may recreate scans with wrong region boundaries.
            if(t instanceof NotServingRegionException) {
                LOGGER.error("postScannerOpen error for region {} . "
                                + "Thorwing it as StaleRegionBoundaryCacheException",
                        s.getRegionInfo().getRegionNameAsString(), t);
                Exception cause = new StaleRegionBoundaryCacheException(c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString());
                throw new DoNotRetryIOException(cause.getMessage(), cause);
            } else {
                LOGGER.error("postScannerOpen error for region {}",
                        s.getRegionInfo().getRegionNameAsString(), t);
            }
            ClientUtil.throwIOException(c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString(), t);
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

    public void setScanOptionsForFlushesAndCompactions(ScanOptions options) {
        // We want the store to give us all the deleted cells to StoreCompactionScanner
        options.setKeepDeletedCells(KeepDeletedCells.TTL);
        options.setTTL(HConstants.FOREVER);
        options.setMaxVersions(Integer.MAX_VALUE);
        options.setMinVersions(Integer.MAX_VALUE);
    }

    @Override
    public void preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
            CompactionRequest request) throws IOException {
        Configuration conf = c.getEnvironment().getConfiguration();
        if (isPhoenixTableTTLEnabled(conf)) {
            setScanOptionsForFlushesAndCompactions(options);
            return;
        }
        if (isMaxLookbackTimeEnabled(conf)) {
            setScanOptionsForFlushesAndCompactionsWhenPhoenixTTLIsDisabled(conf, options, store,
                    scanType);

        }
    }

    @Override
    public void preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            ScanOptions options, FlushLifeCycleTracker tracker) throws IOException {
        Configuration conf = c.getEnvironment().getConfiguration();

        if (isPhoenixTableTTLEnabled(conf)) {
            setScanOptionsForFlushesAndCompactions(options);
            return;
        }
        if (isMaxLookbackTimeEnabled(conf)) {
            setScanOptionsForFlushesAndCompactionsWhenPhoenixTTLIsDisabled(conf, options, store,
                    ScanType.COMPACT_RETAIN_DELETES);
        }
    }

    @Override
    public void preMemStoreCompactionCompactScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> c, Store store, ScanOptions options)
            throws IOException {
        Configuration conf = c.getEnvironment().getConfiguration();
        if (isPhoenixTableTTLEnabled(conf)) {
            setScanOptionsForFlushesAndCompactions(options);
            return;
        }
        if (isMaxLookbackTimeEnabled(conf)) {
            MemoryCompactionPolicy inMemPolicy =
                    store.getColumnFamilyDescriptor().getInMemoryCompaction();
            ScanType scanType;
            //the eager and adaptive in-memory compaction policies can purge versions; the others
            // can't. (Eager always does; adaptive sometimes does)
            if (inMemPolicy.equals(MemoryCompactionPolicy.EAGER) ||
                    inMemPolicy.equals(MemoryCompactionPolicy.ADAPTIVE)) {
                scanType = ScanType.COMPACT_DROP_DELETES;
            } else {
                scanType = ScanType.COMPACT_RETAIN_DELETES;
            }
            setScanOptionsForFlushesAndCompactionsWhenPhoenixTTLIsDisabled(conf, options, store,
                    scanType);
        }
    }

    @Override
    public void preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            ScanOptions options) throws IOException {

        Configuration conf = c.getEnvironment().getConfiguration();
        if (isPhoenixTableTTLEnabled(conf)) {
            setScanOptionsForFlushesAndCompactions(options);
            return;
        }
        if (!storeFileScanDoesntNeedAlteration(options)) {
            //PHOENIX-4277 -- When doing a point-in-time (SCN) Scan, HBase by default will hide
            // mutations that happen before a delete marker. This overrides that behavior.
            options.setMinVersions(options.getMinVersions());
            KeepDeletedCells keepDeletedCells = KeepDeletedCells.TRUE;
            if (store.getColumnFamilyDescriptor().getTimeToLive() != HConstants.FOREVER) {
                keepDeletedCells = KeepDeletedCells.TTL;
            }
            options.setKeepDeletedCells(keepDeletedCells);
        }
    }

    private boolean storeFileScanDoesntNeedAlteration(ScanOptions options) {
        Scan scan = options.getScan();
        boolean isRaw = scan.isRaw();
        //true if keep deleted cells is either TRUE or TTL
        boolean keepDeletedCells = options.getKeepDeletedCells().equals(KeepDeletedCells.TRUE) ||
                options.getKeepDeletedCells().equals(KeepDeletedCells.TTL);
        boolean timeRangeIsLatest = scan.getTimeRange().getMax() == HConstants.LATEST_TIMESTAMP;
        boolean timestampIsTransactional =
                isTransactionalTimestamp(scan.getTimeRange().getMax());
        return isRaw
                || keepDeletedCells
                || timeRangeIsLatest
                || timestampIsTransactional;
    }

    private boolean isTransactionalTimestamp(long ts) {
        //have to use the HBase edge manager because the Phoenix one is in phoenix-core
        return ts > (long) (EnvironmentEdgeManager.currentTime() * 1.1);
    }

    /*
     * If KeepDeletedCells.FALSE, KeepDeletedCells.TTL ,
     * let delete markers age once lookback age is done.
     */
    public KeepDeletedCells getKeepDeletedCells(ScanOptions options, ScanType scanType) {
        //if we're doing a minor compaction or flush, always set keep deleted cells
        //to true. Otherwise, if keep deleted cells is false or TTL, use KeepDeletedCells TTL,
        //where the value of the ttl might be overriden to the max lookback age elsewhere
        return (options.getKeepDeletedCells() == KeepDeletedCells.TRUE
                || scanType.equals(ScanType.COMPACT_RETAIN_DELETES)) ?
                KeepDeletedCells.TRUE : KeepDeletedCells.TTL;
    }

    /*
     * if the user set a TTL we should leave MIN_VERSIONS at the default (0 in most of the cases).
     * Otherwise the data (1st version) will not be removed after the TTL. If no TTL, we want
     * Math.max(maxVersions, minVersions, 1)
     */
    public int getMinVersions(ScanOptions options, ColumnFamilyDescriptor cfDescriptor) {
        return cfDescriptor.getTimeToLive() != HConstants.FOREVER ? options.getMinVersions()
                : Math.max(Math.max(options.getMinVersions(),
                cfDescriptor.getMaxVersions()),1);
    }

    /**
     *
     * @param conf HBase Configuration
     * @param columnDescriptor ColumnFamilyDescriptor for the store being compacted
     * @param options ScanOptions of overrides to the compaction scan
     * @return Time to live in milliseconds, based on both HBase TTL and Phoenix max lookback age
     */
    public long getTimeToLiveForCompactions(Configuration conf,
            ColumnFamilyDescriptor columnDescriptor,
            ScanOptions options) {
        long ttlConfigured = columnDescriptor.getTimeToLive();
        long ttlInMillis = ttlConfigured * 1000;
        long maxLookbackTtl = BaseScannerRegionObserverConstants.getMaxLookbackInMillis(conf);
        if (isMaxLookbackTimeEnabled(maxLookbackTtl)) {
            if (ttlConfigured == HConstants.FOREVER
                    && columnDescriptor.getKeepDeletedCells() != KeepDeletedCells.TRUE) {
                // If user configured default TTL(FOREVER) and keep deleted cells to false or
                // TTL then to remove unwanted delete markers we should change ttl to max lookback age
                ttlInMillis = maxLookbackTtl;
            } else {
                //if there is a TTL, use TTL instead of max lookback age.
                // Max lookback age should be more recent or equal to TTL
                ttlInMillis = Math.max(ttlInMillis, maxLookbackTtl);
            }
        }

        return ttlInMillis;
    }

    public void setScanOptionsForFlushesAndCompactionsWhenPhoenixTTLIsDisabled(Configuration conf,
            ScanOptions options,
            final Store store,
            ScanType type) {
        ColumnFamilyDescriptor cfDescriptor = store.getColumnFamilyDescriptor();
        options.setTTL(getTimeToLiveForCompactions(conf, cfDescriptor,
                options));
        options.setKeepDeletedCells(getKeepDeletedCells(options, type));
        options.setMaxVersions(Integer.MAX_VALUE);
        options.setMinVersions(getMinVersions(options, cfDescriptor));
    }

    public static boolean isMaxLookbackTimeEnabled(Configuration conf){
        return isMaxLookbackTimeEnabled(conf.getLong(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                BaseScannerRegionObserverConstants.DEFAULT_PHOENIX_MAX_LOOKBACK_AGE));
    }

    public static boolean isMaxLookbackTimeEnabled(long maxLookbackTime){
        return maxLookbackTime > 0L;
    }

    public static boolean isPhoenixTableTTLEnabled(Configuration conf) {
        return conf.getBoolean(QueryServices.PHOENIX_TABLE_TTL_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_TABLE_TTL_ENABLED);
    }
}
