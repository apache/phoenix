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
package org.apache.phoenix.iterate;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EXPECTED_UPPER_REGION_KEY;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.filter.ColumnProjectionFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.PTableStats;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 *
 * Class that parallelizes the scan over a table using the ExecutorService provided.  Each region of the table will be scanned in parallel with
 * the results accessible through {@link #getIterators()}
 *
 * 
 * @since 0.1
 */
public class ParallelIterators extends ExplainTable implements ResultIterators {
	private static final Logger logger = LoggerFactory.getLogger(ParallelIterators.class);
    private final List<List<Scan>> scans;
    private final List<KeyRange> splits;
    private final PTableStats tableStats;
    private final byte[] physicalTableName;
    private final QueryPlan plan;
    private final ParallelIteratorFactory iteratorFactory;
    
    public static interface ParallelIteratorFactory {
        PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner, Scan scan) throws SQLException;
    }

    private static final int DEFAULT_THREAD_TIMEOUT_MS = 60000; // 1min
    private static final int ESTIMATED_GUIDEPOSTS_PER_REGION = 20;

    static final Function<HRegionLocation, KeyRange> TO_KEY_RANGE = new Function<HRegionLocation, KeyRange>() {
        @Override
        public KeyRange apply(HRegionLocation region) {
            return KeyRange.getKeyRange(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
        }
    };

    private PTable getTable() {
        return plan.getTableRef().getTable();
    }
    
    private boolean useStats() {
        Scan scan = context.getScan();
        boolean isPointLookup = context.getScanRanges().isPointLookup();
        /*
         *  Don't use guide posts if:
         *  1) We're doing a point lookup, as HBase is fast enough at those
         *     to not need them to be further parallelized. TODO: pref test to verify
         *  2) We're collecting stats, as in this case we need to scan entire
         *     regions worth of data to track where to put the guide posts.
         */
        if (isPointLookup || ScanUtil.isAnalyzeTable(scan)) {
            return false;
        }
        return true;
    }
    
    public ParallelIterators(QueryPlan plan, Integer perScanLimit, ParallelIteratorFactory iteratorFactory)
            throws SQLException {
        super(plan.getContext(), plan.getTableRef(), plan.getGroupBy());
        this.plan = plan;
        StatementContext context = plan.getContext();
        TableRef tableRef = plan.getTableRef();
        PTable table = tableRef.getTable();
        FilterableStatement statement = plan.getStatement();
        RowProjector projector = plan.getProjector();
        physicalTableName = table.getPhysicalName().getBytes();
        tableStats = useStats() ? new MetaDataClient(context.getConnection()).getTableStats(table) : PTableStats.EMPTY_STATS;
        Scan scan = context.getScan();
        if (projector.isProjectEmptyKeyValue()) {
            Map<byte [], NavigableSet<byte []>> familyMap = scan.getFamilyMap();
            // If nothing projected into scan and we only have one column family, just allow everything
            // to be projected and use a FirstKeyOnlyFilter to skip from row to row. This turns out to
            // be quite a bit faster.
            // Where condition columns also will get added into familyMap
            // When where conditions are present, we can not add FirstKeyOnlyFilter at beginning.
            if (familyMap.isEmpty() && context.getWhereCoditionColumns().isEmpty()
                    && table.getColumnFamilies().size() == 1) {
                // Project the one column family. We must project a column family since it's possible
                // that there are other non declared column families that we need to ignore.
                scan.addFamily(table.getColumnFamilies().get(0).getName().getBytes());
                ScanUtil.andFilterAtBeginning(scan, new FirstKeyOnlyFilter());
            } else {
                byte[] ecf = SchemaUtil.getEmptyColumnFamily(table);
                // Project empty key value unless the column family containing it has
                // been projected in its entirety.
                if (!familyMap.containsKey(ecf) || familyMap.get(ecf) != null) {
                    scan.addColumn(ecf, QueryConstants.EMPTY_COLUMN_BYTES);
                }
            }
        } else if (table.getViewType() == ViewType.MAPPED) {
            // Since we don't have the empty key value in MAPPED tables, we must select all CFs in HRS. But only the
            // selected column values are returned back to client
            for (PColumnFamily family : table.getColumnFamilies()) {
                scan.addFamily(family.getName().getBytes());
            }
        }
        
        // TODO adding all CFs here is not correct. It should be done only after ColumnProjectionOptimization.
        if (perScanLimit != null) {
            ScanUtil.andFilterAtEnd(scan, new PageFilter(perScanLimit));
        }

        doColumnProjectionOptimization(context, scan, table, statement);
        
        this.iteratorFactory = iteratorFactory;
        this.scans = getParallelScans();
        List<KeyRange> splitRanges = Lists.newArrayListWithExpectedSize(scans.size() * ESTIMATED_GUIDEPOSTS_PER_REGION);
        for (List<Scan> scanList : scans) {
            for (Scan aScan : scanList) {
                splitRanges.add(KeyRange.getKeyRange(aScan.getStartRow(), aScan.getStopRow()));
            }
        }
        this.splits = ImmutableList.copyOf(splitRanges);
    }

    private void doColumnProjectionOptimization(StatementContext context, Scan scan, PTable table, FilterableStatement statement) {
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        if (familyMap != null && !familyMap.isEmpty()) {
            // columnsTracker contain cf -> qualifiers which should get returned.
            Map<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>> columnsTracker = 
                    new TreeMap<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>>();
            Set<byte[]> conditionOnlyCfs = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
            int referencedCfCount = familyMap.size();
            for (Pair<byte[], byte[]> whereCol : context.getWhereCoditionColumns()) {
                if (!(familyMap.containsKey(whereCol.getFirst()))) {
                    referencedCfCount++;
                }
            }
            boolean useOptimization;
            if (statement.getHint().hasHint(Hint.SEEK_TO_COLUMN)) {
                // Do not use the optimization
                useOptimization = false;
            } else if (statement.getHint().hasHint(Hint.NO_SEEK_TO_COLUMN)) {
                // Strictly use the optimization
                useOptimization = true;
            } else {
                // when referencedCfCount is >1 and no Hints, we are not using the optimization
                useOptimization = referencedCfCount == 1;
            }
            if (useOptimization) {
                for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
                    ImmutableBytesPtr cf = new ImmutableBytesPtr(entry.getKey());
                    NavigableSet<byte[]> qs = entry.getValue();
                    NavigableSet<ImmutableBytesPtr> cols = null;
                    if (qs != null) {
                        cols = new TreeSet<ImmutableBytesPtr>();
                        for (byte[] q : qs) {
                            cols.add(new ImmutableBytesPtr(q));
                        }
                    }
                    columnsTracker.put(cf, cols);
                }
            }
            // Making sure that where condition CFs are getting scanned at HRS.
            for (Pair<byte[], byte[]> whereCol : context.getWhereCoditionColumns()) {
                if (useOptimization) {
                    if (!(familyMap.containsKey(whereCol.getFirst()))) {
                        scan.addFamily(whereCol.getFirst());
                        conditionOnlyCfs.add(whereCol.getFirst());
                    }
                } else {
                    if (familyMap.containsKey(whereCol.getFirst())) {
                        // where column's CF is present. If there are some specific columns added against this CF, we
                        // need to ensure this where column also getting added in it.
                        // If the select was like select cf1.*, then that itself will select the whole CF. So no need to
                        // specifically add the where column. Adding that will remove the cf1.* stuff and only this
                        // where condition column will get returned!
                        NavigableSet<byte[]> cols = familyMap.get(whereCol.getFirst());
                        // cols is null means the whole CF will get scanned.
                        if (cols != null) {
                            scan.addColumn(whereCol.getFirst(), whereCol.getSecond());
                        }
                    } else {
                        // where column's CF itself is not present in family map. We need to add the column
                        scan.addColumn(whereCol.getFirst(), whereCol.getSecond());
                    }
                }
            }
            if (useOptimization && !columnsTracker.isEmpty()) {
                for (ImmutableBytesPtr f : columnsTracker.keySet()) {
                    // This addFamily will remove explicit cols in scan familyMap and make it as entire row.
                    // We don't want the ExplicitColumnTracker to be used. Instead we have the ColumnProjectionFilter
                    scan.addFamily(f.get());
                }
                // We don't need this filter for aggregates, as we're not returning back what's
                // in the scan in this case. We still want the other optimization that causes
                // the ExplicitColumnTracker not to be used, though.
                if (!(statement.isAggregate())) {
                    ScanUtil.andFilterAtEnd(scan, new ColumnProjectionFilter(SchemaUtil.getEmptyColumnFamily(table),
                            columnsTracker, conditionOnlyCfs));
                }
            }
        }
    }

    public List<KeyRange> getSplits() {
        return splits;
    }

    public List<List<Scan>> getScans() {
        return scans;
    }

    private static List<byte[]> toBoundaries(List<HRegionLocation> regionLocations) {
        int nBoundaries = regionLocations.size() - 1;
        List<byte[]> ranges = Lists.newArrayListWithExpectedSize(nBoundaries);
        for (int i = 0; i < nBoundaries; i++) {
            HRegionInfo regionInfo = regionLocations.get(i).getRegionInfo();
            ranges.add(regionInfo.getEndKey());
        }
        return ranges;
    }
    
    private static int getIndexContainingInclusive(List<byte[]> boundaries, byte[] inclusiveKey) {
        int guideIndex = Collections.binarySearch(boundaries, inclusiveKey, Bytes.BYTES_COMPARATOR);
        // If we found an exact match, return the index+1, as the inclusiveKey will be contained
        // in the next region (since we're matching on the end boundary).
        guideIndex = (guideIndex < 0 ? -(guideIndex + 1) : (guideIndex + 1));
        return guideIndex;
    }
    
    private static int getIndexContainingExclusive(List<byte[]> boundaries, byte[] exclusiveKey) {
        int guideIndex = Collections.binarySearch(boundaries, exclusiveKey, Bytes.BYTES_COMPARATOR);
        // If we found an exact match, return the index we found as the exclusiveKey won't be
        // contained in the next region as with getIndexContainingInclusive.
        guideIndex = (guideIndex < 0 ? -(guideIndex + 1) : guideIndex);
        return guideIndex;
    }
    
    private List<byte[]> getGuidePosts() {
        /*
         *  Don't use guide posts if:
         *  1) We're doing a point lookup, as HBase is fast enough at those
         *     to not need them to be further parallelized. TODO: pref test to verify
         *  2) We're collecting stats, as in this case we need to scan entire
         *     regions worth of data to track where to put the guide posts.
         */
        if (!useStats()) {
            return Collections.emptyList();
        }
        
        List<byte[]> gps = null;
        PTable table = getTable();
        Map<byte[],GuidePostsInfo> guidePostMap = tableStats.getGuidePosts();
        byte[] defaultCF = SchemaUtil.getEmptyColumnFamily(getTable());
        if (table.getColumnFamilies().isEmpty()) {
            // For sure we can get the defaultCF from the table
            if (guidePostMap.get(defaultCF) != null) {
                gps = guidePostMap.get(defaultCF).getGuidePosts();
            }
        } else {
            Scan scan = context.getScan();
            if (scan.getFamilyMap().size() > 0 && !scan.getFamilyMap().containsKey(defaultCF)) {
                // If default CF is not used in scan, use first CF referenced in scan
                GuidePostsInfo guidePostsInfo = guidePostMap.get(scan.getFamilyMap().keySet().iterator().next());
                if (guidePostsInfo != null) {
                    gps = guidePostsInfo.getGuidePosts();
                }
            } else {
                // Otherwise, favor use of default CF.
                if (guidePostMap.get(defaultCF) != null) {
                    gps = guidePostMap.get(defaultCF).getGuidePosts();
                }
            }
        }
        if (gps == null) {
            return Collections.emptyList();
        }
        return gps;
    }
    
    private static String toString(List<byte[]> gps) {
        StringBuilder buf = new StringBuilder(gps.size() * 100);
        buf.append("[");
        for (int i = 0; i < gps.size(); i++) {
            buf.append(Bytes.toStringBinary(gps.get(i)));
            buf.append(",");
            if (i < gps.size()-1 && (i % 10) == 0) {
                buf.append("\n");
            }
        }
        buf.setCharAt(buf.length()-1, ']');
        return buf.toString();
    }
    
    private List<Scan> addNewScan(List<List<Scan>> parallelScans, List<Scan> scans, Scan scan, byte[] startKey, boolean crossedRegionBoundary) {
        PTable table = getTable();
        boolean startNewScanList = false;
        if (!plan.isRowKeyOrdered()) {
            startNewScanList = true;
        } else if (crossedRegionBoundary) {
            if (table.getIndexType() == IndexType.LOCAL) {
                startNewScanList = true;
            } else if (table.getBucketNum() != null) {
                startNewScanList = scans.isEmpty() ||
                        ScanUtil.crossesPrefixBoundary(startKey,
                                ScanUtil.getPrefix(scans.get(scans.size()-1).getStartRow(), SaltingUtil.NUM_SALTING_BYTES), 
                                SaltingUtil.NUM_SALTING_BYTES);
            }
        }
        if (scan != null) {
            scans.add(scan);
        }
        if (startNewScanList && !scans.isEmpty()) {
            parallelScans.add(scans);
            scans = Lists.newArrayListWithExpectedSize(1);
        }
        return scans;
    }

    private List<List<Scan>> getParallelScans() throws SQLException {
        return getParallelScans(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
    }

    /**
     * Compute the list of parallel scans to run for a given query. The inner scans
     * may be concatenated together directly, while the other ones may need to be
     * merge sorted, depending on the query.
     * @return list of parallel scans to run for a given query.
     * @throws SQLException
     */
    private List<List<Scan>> getParallelScans(byte[] startKey, byte[] stopKey) throws SQLException {
        Scan scan = context.getScan();
        List<HRegionLocation> regionLocations = context.getConnection().getQueryServices()
                .getAllTableRegions(physicalTableName);
        
        List<byte[]> regionBoundaries = toBoundaries(regionLocations);
        ScanRanges scanRanges = context.getScanRanges();
        PTable table = getTable();
        boolean isSalted = table.getBucketNum() != null;
        boolean isLocalIndex = table.getIndexType() == IndexType.LOCAL;
        List<byte[]> gps = getGuidePosts();
        if (logger.isDebugEnabled()) {
            logger.debug("Guideposts: " + toString(gps));
        }
        boolean traverseAllRegions = isSalted || isLocalIndex;
        if (!traverseAllRegions) {
            byte[] scanStartRow = scan.getStartRow();
            if (scanStartRow.length != 0 && Bytes.compareTo(scanStartRow, startKey) > 0) {
                startKey = scanStartRow;
            }
            byte[] scanStopRow = scan.getStopRow();
            if (stopKey.length == 0 || Bytes.compareTo(scanStopRow, stopKey) < 0) {
                stopKey = scanStopRow;
            }
        }
        
        int regionIndex = 0;
        int stopIndex = regionBoundaries.size();
        if (startKey.length > 0) {
            regionIndex = getIndexContainingInclusive(regionBoundaries, startKey);
        }
        if (stopKey.length > 0) {
            stopIndex = Math.min(stopIndex, regionIndex + getIndexContainingExclusive(regionBoundaries.subList(regionIndex, stopIndex), stopKey));
            if (isLocalIndex) {
                stopKey = regionLocations.get(stopIndex).getRegionInfo().getEndKey();
            }
        }
        List<List<Scan>> parallelScans = Lists.newArrayListWithExpectedSize(stopIndex - regionIndex + 1);
        
        byte[] currentKey = startKey;
        int guideIndex = currentKey.length == 0 ? 0 : getIndexContainingInclusive(gps, currentKey);
        int gpsSize = gps.size();
        int estGuidepostsPerRegion = gpsSize == 0 ? 1 : gpsSize / regionLocations.size() + 1;
        int keyOffset = 0;
        List<Scan> scans = Lists.newArrayListWithExpectedSize(estGuidepostsPerRegion);
        // Merge bisect with guideposts for all but the last region
        while (regionIndex <= stopIndex) {
            byte[] currentGuidePost, endKey, endRegionKey = EMPTY_BYTE_ARRAY;
            if (regionIndex == stopIndex) {
                endKey = stopKey;
            } else {
                endKey = regionBoundaries.get(regionIndex);
            }
            if (isLocalIndex) {
                HRegionInfo regionInfo = regionLocations.get(regionIndex).getRegionInfo();
                endRegionKey = regionInfo.getEndKey();
                keyOffset = ScanUtil.getRowKeyOffset(regionInfo.getStartKey(), endRegionKey);
            }
            while (guideIndex < gpsSize
                    && (Bytes.compareTo(currentGuidePost = gps.get(guideIndex), endKey) <= 0 || endKey.length == 0)) {
                Scan newScan = scanRanges.intersectScan(scan, currentKey, currentGuidePost, keyOffset, false);
                scans = addNewScan(parallelScans, scans, newScan, currentGuidePost, false);
                currentKey = currentGuidePost;
                guideIndex++;
            }
            Scan newScan = scanRanges.intersectScan(scan, currentKey, endKey, keyOffset, true);
            if (isLocalIndex) {
                if (newScan != null) {
                    newScan.setAttribute(EXPECTED_UPPER_REGION_KEY, endRegionKey);
                } else if (!scans.isEmpty()) {
                    scans.get(scans.size()-1).setAttribute(EXPECTED_UPPER_REGION_KEY, endRegionKey);
                }
            }
            scans = addNewScan(parallelScans, scans, newScan, endKey, true);
            currentKey = endKey;
            regionIndex++;
        }
        if (!scans.isEmpty()) { // Add any remaining scans
            parallelScans.add(scans);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("The parallelScans: " + parallelScans,
                    ScanUtil.getCustomAnnotations(scan)));
        }
        return parallelScans;
    }

    public static <T> List<T> reverseIfNecessary(List<T> list, boolean reverse) {
        if (!reverse) {
            return list;
        }
        return Lists.reverse(list);
    }
    
    private static void addConcatResultIterator(List<PeekingResultIterator> iterators, final List<PeekingResultIterator> concatIterators) {
        if (!concatIterators.isEmpty()) {
            iterators.add(ConcatResultIterator.newConcatResultIterator(concatIterators));
        }
    }
    /**
     * Executes the scan in parallel across all regions, blocking until all scans are complete.
     * @return the result iterators for the scan of each region
     */
    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        boolean success = false;
        boolean isReverse = ScanUtil.isReversed(context.getScan());
        boolean isLocalIndex = getTable().getIndexType() == IndexType.LOCAL;
        final ConnectionQueryServices services = context.getConnection().getQueryServices();
        ReadOnlyProps props = services.getProps();
        int numSplits = size();
        List<PeekingResultIterator> iterators = new ArrayList<PeekingResultIterator>(numSplits);
        List<List<Pair<Scan,Future<PeekingResultIterator>>>> futures = Lists.newArrayListWithExpectedSize(numSplits);
        // TODO: what purpose does this scanID serve?
        final UUID scanId = UUID.randomUUID();
        try {
            submitWork(scanId, scans, futures, splits.size());
            int timeoutMs = props.getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, DEFAULT_THREAD_TIMEOUT_MS);
            boolean clearedCache = false;
            for (List<Pair<Scan,Future<PeekingResultIterator>>> future : reverseIfNecessary(futures,isReverse)) {
                List<PeekingResultIterator> concatIterators = Lists.newArrayListWithExpectedSize(future.size());
                for (Pair<Scan,Future<PeekingResultIterator>> scanPair : reverseIfNecessary(future,isReverse)) {
                    try {
                        PeekingResultIterator iterator = scanPair.getSecond().get(timeoutMs, TimeUnit.MILLISECONDS);
                        concatIterators.add(iterator);
                    } catch (ExecutionException e) {
                        try { // Rethrow as SQLException
                            throw ServerUtil.parseServerException(e);
                        } catch (StaleRegionBoundaryCacheException e2) { // Catch only to try to recover from region boundary cache being out of date
                            List<List<Pair<Scan,Future<PeekingResultIterator>>>> newFutures = Lists.newArrayListWithExpectedSize(2);
                            if (!clearedCache) { // Clear cache once so that we rejigger job based on new boundaries
                                services.clearTableRegionCache(physicalTableName);
                                clearedCache = true;
                            }
                            // Resubmit just this portion of work again
                            Scan oldScan = scanPair.getFirst();
                            byte[] startKey = oldScan.getStartRow();
                            byte[] endKey = oldScan.getStopRow();
                            if (isLocalIndex) {
                                endKey = oldScan.getAttribute(EXPECTED_UPPER_REGION_KEY);
                            }
                            List<List<Scan>> newNestedScans = this.getParallelScans(startKey, endKey);
                            // Add any concatIterators that were successful so far
                            // as we need these to be in order
                            addConcatResultIterator(iterators, concatIterators);
                            concatIterators = Collections.emptyList();
                            submitWork(scanId, newNestedScans, newFutures, newNestedScans.size());
                            for (List<Pair<Scan,Future<PeekingResultIterator>>> newFuture : reverseIfNecessary(newFutures, isReverse)) {
                                for (Pair<Scan,Future<PeekingResultIterator>> newScanPair : reverseIfNecessary(newFuture, isReverse)) {
                                    // Immediate do a get (not catching exception again) and then add the iterators we
                                    // get back immediately. They'll be sorted as expected, since they're replacing the
                                    // original one.
                                    PeekingResultIterator iterator = newScanPair.getSecond().get(timeoutMs, TimeUnit.MILLISECONDS);
                                    iterators.add(iterator);
                                }
                            }
                        }
                    }
                }
                addConcatResultIterator(iterators, concatIterators);
            }

            success = true;
            return iterators;
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            if (!success) {
                SQLCloseables.closeAllQuietly(iterators);
                // Don't call cancel, as it causes the HConnection to get into a funk
//                for (Pair<KeyRange,Future<PeekingResultIterator>> future : futures) {
//                    future.getSecond().cancel(true);
//                }
            }
        }
    }
    
    private static final class ScanLocation {
    	private final int outerListIndex;
    	private final int innerListIndex;
    	private final Scan scan;
    	public ScanLocation(Scan scan, int outerListIndex, int innerListIndex) {
    		this.outerListIndex = outerListIndex;
    		this.innerListIndex = innerListIndex;
    		this.scan = scan;
    	}
    	public int getOuterListIndex() {
    		return outerListIndex;
    	}
    	public int getInnerListIndex() {
    		return innerListIndex;
    	}
    	public Scan getScan() {
    		return scan;
    	}
    }
    private void submitWork(final UUID scanId, List<List<Scan>> nestedScans,
            List<List<Pair<Scan,Future<PeekingResultIterator>>>> nestedFutures, int estFlattenedSize) {
        final ConnectionQueryServices services = context.getConnection().getQueryServices();
        ExecutorService executor = services.getExecutor();
        // Pre-populate nestedFutures lists so that we can shuffle the scans
        // and add the future to the right nested list. By shuffling the scans
        // we get better utilization of the cluster since our thread executor
        // will spray the scans across machines as opposed to targeting a
        // single one since the scans are in row key order.
        List<ScanLocation> scanLocations = Lists.newArrayListWithExpectedSize(estFlattenedSize);
        for (int i = 0; i < nestedScans.size(); i++) {
            List<Scan> scans = nestedScans.get(i);
            List<Pair<Scan,Future<PeekingResultIterator>>> futures = Lists.newArrayListWithExpectedSize(scans.size());
            nestedFutures.add(futures);
            for (int j = 0; j < scans.size(); j++) {
            	Scan scan = nestedScans.get(i).get(j);
                scanLocations.add(new ScanLocation(scan, i, j));
                futures.add(null); // placeholder
            }
        }
        Collections.shuffle(scanLocations);
        for (ScanLocation scanLocation : scanLocations) {
            final Scan scan = scanLocation.getScan();
            Future<PeekingResultIterator> future =
                executor.submit(Tracing.wrap(new JobCallable<PeekingResultIterator>() {

                @Override
                public PeekingResultIterator call() throws Exception {
                    long startTime = System.currentTimeMillis();
                    ResultIterator scanner = new TableResultIterator(context, tableRef, scan);
                    if (logger.isDebugEnabled()) {
                        logger.debug(LogUtil.addCustomAnnotations("Id: " + scanId + ", Time: " + (System.currentTimeMillis() - startTime) + "ms, Scan: " + scan, ScanUtil.getCustomAnnotations(scan)));
                    }
                    return iteratorFactory.newIterator(context, scanner, scan);
                }

                /**
                 * Defines the grouping for round robin behavior.  All threads spawned to process
                 * this scan will be grouped together and time sliced with other simultaneously
                 * executing parallel scans.
                 */
                @Override
                public Object getJobId() {
                    return ParallelIterators.this;
                }
            }, "Parallel scanner for table: " + tableRef.getTable().getName().getString()));
            // Add our future in the right place so that we can concatenate the
            // results of the inner futures versus merge sorting across all of them.
            nestedFutures.get(scanLocation.getOuterListIndex()).set(scanLocation.getInnerListIndex(), new Pair<Scan,Future<PeekingResultIterator>>(scan,future));
        }
    }

    @Override
    public int size() {
        return this.scans.size();
    }

    @Override
    public void explain(List<String> planSteps) {
        StringBuilder buf = new StringBuilder();
        buf.append("CLIENT PARALLEL " + size() + "-WAY ");
        explain(buf.toString(),planSteps);
    }

	@Override
	public String toString() {
		return "ParallelIterators [scans=" + scans + "]";
	}
}