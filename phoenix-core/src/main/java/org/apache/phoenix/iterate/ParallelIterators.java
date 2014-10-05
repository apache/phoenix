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
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.filter.ColumnProjectionFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ByteUtil;
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
    private final PTable physicalTable;
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

    public ParallelIterators(QueryPlan plan, Integer perScanLimit, ParallelIteratorFactory iteratorFactory)
            throws SQLException {
        super(plan.getContext(), plan.getTableRef(), plan.getGroupBy());
        this.plan = plan;
        StatementContext context = plan.getContext();
        TableRef tableRef = plan.getTableRef();
        FilterableStatement statement = plan.getStatement();
        RowProjector projector = plan.getProjector();
        MetaDataClient client = new MetaDataClient(context.getConnection());
        PTable physicalTable = tableRef.getTable();
        String physicalName = tableRef.getTable().getPhysicalName().getString();
        if ((physicalTable.getViewIndexId() == null) && (!physicalName.equals(physicalTable.getName().getString()))) { // tableRef is not for the physical table
            String physicalSchemaName = SchemaUtil.getSchemaNameFromFullName(physicalName);
            String physicalTableName = SchemaUtil.getTableNameFromFullName(physicalName);
            // TODO: this will be an extra RPC to ensure we have the latest guideposts, but is almost always
            // unnecessary. We should instead track when the last time an update cache was done for this
            // for physical table and not do it again until some interval has passed (it's ok to use stale stats).
            MetaDataMutationResult result = client.updateCache(null, /* use global tenant id to get physical table */
                    physicalSchemaName, physicalTableName);
            physicalTable = result.getTable();
            if(physicalTable == null) {
                client = new MetaDataClient(context.getConnection());
                physicalTable = client.getConnection().getMetaDataCache()
                        .getTable(new PTableKey(null, physicalTableName));
            }
        }
        this.physicalTable = physicalTable;
        Scan scan = context.getScan();
        PTable table = tableRef.getTable();
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
        this.scans = getParallelScans(context.getScan());
        List<List<Scan>> scans = getParallelScans(context.getScan());
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
    
    private List<byte[]> getGuidePosts(PTable table) {
        Scan scan = context.getScan();
        boolean isPointLookup = context.getScanRanges().isPointLookup();
        byte[] defaultCF = SchemaUtil.getEmptyColumnFamily(table);
        List<byte[]> gps = Collections.emptyList();
        /*
         *  Don't use guide posts if:
         *  1) We're doing a point lookup, as HBase is fast enough at those
         *     to not need them to be further parallelized. TODO: pref test to verify
         *  2) We're collecting stats, as in this case we need to scan entire
         *     regions worth of data to track where to put the guide posts.
         */
        if (!isPointLookup && !ScanUtil.isAnalyzeTable(scan)) {
            if (table.getColumnFamilies().isEmpty()) {
                // For sure we can get the defaultCF from the table
                return table.getGuidePosts();
            }
            try {
                if (scan.getFamilyMap().size() > 0 && !scan.getFamilyMap().containsKey(defaultCF)) {
                    // If default CF is not used in scan, use first CF referenced in scan
                    return table.getColumnFamily(scan.getFamilyMap().keySet().iterator().next()).getGuidePosts();
                }
                // Otherwise, favor use of default CF.
                return table.getColumnFamily(defaultCF).getGuidePosts();
            } catch (ColumnFamilyNotFoundException cfne) {
                // Alter table does this
            }
        }
        return gps;
        
    }
    
    private static String toString(List<byte[]> gps) {
        StringBuilder buf = new StringBuilder(gps.size() * 100);
        buf.append("[");
        for (int i = 0; i < gps.size(); i++) {
            buf.append(Bytes.toStringBinary(gps.get(i)));
            buf.append(",");
            if (i+1 < gps.size() && ((i+1) % 10) == 0) {
                buf.append("\n");
            }
        }
        buf.setCharAt(buf.length()-1, ']');
        return buf.toString();
    }
    
    private List<Scan> addNewScan(List<List<Scan>> parallelScans, List<Scan> scans, Scan scan, boolean crossedRegionBoundary) {
        if (scan == null) {
            return scans;
        }
        if (!scans.isEmpty()) {
            boolean startNewScanList = false;
            if (!plan.isRowKeyOrdered()) {
                startNewScanList = true;
            } else if (crossedRegionBoundary) {
                if (physicalTable.getBucketNum() != null) {
                    byte[] previousStartKey = scans.get(scans.size()-1).getStartRow();
                    byte[] currentStartKey = scan.getStartRow();
                    byte[] prefix = ScanUtil.getPrefix(previousStartKey, SaltingUtil.NUM_SALTING_BYTES);
                    startNewScanList = ScanUtil.crossesPrefixBoundary(currentStartKey, prefix, SaltingUtil.NUM_SALTING_BYTES);
                }
            }
            if (startNewScanList) {
                parallelScans.add(scans);
                scans = Lists.newArrayListWithExpectedSize(1);
            }
        }
        scans.add(scan);
        return scans;
    }
    /**
     * Compute the list of parallel scans to run for a given query. The inner scans
     * may be concatenated together directly, while the other ones may need to be
     * merge sorted, depending on the query.
     * @return list of parallel scans to run for a given query.
     * @throws SQLException
     */
    private List<List<Scan>> getParallelScans(final Scan scan) throws SQLException {
        List<HRegionLocation> regionLocations = context.getConnection().getQueryServices()
                .getAllTableRegions(physicalTable.getPhysicalName().getBytes());
        List<byte[]> regionBoundaries = toBoundaries(regionLocations);
        ScanRanges scanRanges = context.getScanRanges();
        boolean isSalted = physicalTable.getBucketNum() != null;
        List<byte[]> gps = getGuidePosts(physicalTable);
        if (logger.isDebugEnabled()) {
            logger.debug("Guideposts: " + toString(gps));
        }
        boolean traverseAllRegions = isSalted;
        
        byte[] startKey = ByteUtil.EMPTY_BYTE_ARRAY;
        byte[] currentKey = ByteUtil.EMPTY_BYTE_ARRAY;
        byte[] stopKey = ByteUtil.EMPTY_BYTE_ARRAY;
        int regionIndex = 0;
        int stopIndex = regionBoundaries.size();
        if (!traverseAllRegions) {
            startKey = scan.getStartRow();
            if (startKey.length > 0) {
                currentKey = startKey;
                regionIndex = getIndexContainingInclusive(regionBoundaries, startKey);
            }
            stopKey = scan.getStopRow();
            if (stopKey.length > 0) {
                stopIndex = Math.min(stopIndex, regionIndex + getIndexContainingExclusive(regionBoundaries.subList(regionIndex, stopIndex), stopKey));
            }
        }
        List<List<Scan>> parallelScans = Lists.newArrayListWithExpectedSize(stopIndex - regionIndex + 1);
        
        int guideIndex = currentKey.length == 0 ? 0 : getIndexContainingInclusive(gps, currentKey);
        int gpsSize = gps.size();
        int estGuidepostsPerRegion = gpsSize == 0 ? 1 : gpsSize / regionLocations.size() + 1;
        int keyOffset = 0;
        List<Scan> scans = Lists.newArrayListWithExpectedSize(estGuidepostsPerRegion);
        // Merge bisect with guideposts for all but the last region
        while (regionIndex <= stopIndex) {
            byte[] currentGuidePost;
            byte[] endKey = regionIndex == stopIndex ? stopKey : regionBoundaries.get(regionIndex);
            while (guideIndex < gpsSize
                    && (Bytes.compareTo(currentGuidePost = gps.get(guideIndex), endKey) <= 0 || endKey.length == 0)) {
                Scan newScan = scanRanges.intersectScan(scan, currentKey, currentGuidePost, keyOffset);
                scans = addNewScan(parallelScans, scans, newScan, false);
                currentKey = currentGuidePost;
                guideIndex++;
            }
            Scan newScan = scanRanges.intersectScan(scan, currentKey, endKey, keyOffset);
            scans = addNewScan(parallelScans, scans, newScan, true);
            currentKey = endKey;
            regionIndex++;
        }
        if (!scans.isEmpty()) { // Add any remaining scans
            parallelScans.add(scans);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("The parallelScans: " + parallelScans);
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
                                services.clearTableRegionCache(physicalTable.getName().getBytes());
                                clearedCache = true;
                            }
                            // Resubmit just this portion of work again
                            Scan oldScan = scanPair.getFirst();
                            List<List<Scan>> newNestedScans = this.getParallelScans(oldScan);
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
                executor.submit(new JobCallable<PeekingResultIterator>() {

                @Override
                public PeekingResultIterator call() throws Exception {
                    long startTime = System.currentTimeMillis();
                    ResultIterator scanner = new TableResultIterator(context, tableRef, scan);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Id: " + scanId + ", Time: " + (System.currentTimeMillis() - startTime) + "ms, Scan: " + scan);
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
            });
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