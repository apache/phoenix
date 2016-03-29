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
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SCAN_ACTUAL_START_ROW;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_FAILED_QUERY_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.filter.ColumnProjectionFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.PTableStats;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PrefixByteCodec;
import org.apache.phoenix.util.PrefixByteDecoder;
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
public abstract class BaseResultIterators extends ExplainTable implements ResultIterators {
	private static final Logger logger = LoggerFactory.getLogger(BaseResultIterators.class);
    private static final int ESTIMATED_GUIDEPOSTS_PER_REGION = 20;
    private static final int MIN_SEEK_TO_COLUMN_VERSION = VersionUtil.encodeVersion("0", "98", "12");

    private final List<List<Scan>> scans;
    private final List<KeyRange> splits;
    private final PTableStats tableStats;
    private final byte[] physicalTableName;
    private final QueryPlan plan;
    protected final String scanId;
    protected final MutationState mutationState;
    private final ParallelScanGrouper scanGrouper;
    // TODO: too much nesting here - breakup into new classes.
    private final List<List<List<Pair<Scan,Future<PeekingResultIterator>>>>> allFutures;
    private Long estimatedRows;
    private Long estimatedSize;
    private boolean hasGuidePosts;
    
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
         *     to not need them to be further parallelized. TODO: perf test to verify
         *  2) We're collecting stats, as in this case we need to scan entire
         *     regions worth of data to track where to put the guide posts.
         */
        if (isPointLookup || ScanUtil.isAnalyzeTable(scan)) {
            return false;
        }
        return true;
    }
    
    private static void initializeScan(QueryPlan plan, Integer perScanLimit) {
        StatementContext context = plan.getContext();
        TableRef tableRef = plan.getTableRef();
        PTable table = tableRef.getTable();
        Scan scan = context.getScan();

        Map<byte [], NavigableSet<byte []>> familyMap = scan.getFamilyMap();
        // Hack for PHOENIX-2067 to force raw scan over all KeyValues to fix their row keys
        if (context.getConnection().isDescVarLengthRowKeyUpgrade()) {
            // We project *all* KeyValues across all column families as we make a pass over
            // a physical table and we want to make sure we catch all KeyValues that may be
            // dynamic or part of an updatable view.
            familyMap.clear();
            scan.setMaxVersions();
            scan.setFilter(null); // Remove any filter
            scan.setRaw(true); // Traverse (and subsequently clone) all KeyValues
            // Pass over PTable so we can re-write rows according to the row key schema
            scan.setAttribute(BaseScannerRegionObserver.UPGRADE_DESC_ROW_KEY, UngroupedAggregateRegionObserver.serialize(table));
        } else {
            FilterableStatement statement = plan.getStatement();
            RowProjector projector = plan.getProjector();
            boolean optimizeProjection = false;
            boolean keyOnlyFilter = familyMap.isEmpty() && context.getWhereConditionColumns().isEmpty();
            if (!projector.projectEverything()) {
                // If nothing projected into scan and we only have one column family, just allow everything
                // to be projected and use a FirstKeyOnlyFilter to skip from row to row. This turns out to
                // be quite a bit faster.
                // Where condition columns also will get added into familyMap
                // When where conditions are present, we cannot add FirstKeyOnlyFilter at beginning.
                // FIXME: we only enter this if the number of column families is 1 because otherwise
                // local indexes break because it appears that the column families in the PTable do
                // not match the actual column families of the table (which is bad).
                if (keyOnlyFilter && table.getColumnFamilies().size() == 1) {
                    // Project the one column family. We must project a column family since it's possible
                    // that there are other non declared column families that we need to ignore.
                    scan.addFamily(table.getColumnFamilies().get(0).getName().getBytes());
                } else {
                    optimizeProjection = true;
                    if (projector.projectEveryRow()) {
                        if (table.getViewType() == ViewType.MAPPED) {
                            // Since we don't have the empty key value in MAPPED tables, 
                            // we must project all CFs in HRS. However, only the
                            // selected column values are returned back to client.
                            context.getWhereConditionColumns().clear();
                            for (PColumnFamily family : table.getColumnFamilies()) {
                                context.addWhereCoditionColumn(family.getName().getBytes(), null);
                            }
                        } else {
                            byte[] ecf = SchemaUtil.getEmptyColumnFamily(table);
                            // Project empty key value unless the column family containing it has
                            // been projected in its entirety.
                            if (!familyMap.containsKey(ecf) || familyMap.get(ecf) != null) {
                                scan.addColumn(ecf, QueryConstants.EMPTY_COLUMN_BYTES);
                            }
                        }
                    }
                }
            }
            // Add FirstKeyOnlyFilter if there are no references to key value columns
            if (keyOnlyFilter) {
                ScanUtil.andFilterAtBeginning(scan, new FirstKeyOnlyFilter());
            }

            if (perScanLimit != null) {
                ScanUtil.andFilterAtEnd(scan, new PageFilter(perScanLimit));
            }

            if (optimizeProjection) {
                optimizeProjection(context, scan, table, statement);
            }
        }
    }

    private static void optimizeProjection(StatementContext context, Scan scan, PTable table, FilterableStatement statement) {
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        // columnsTracker contain cf -> qualifiers which should get returned.
        Map<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>> columnsTracker = 
                new TreeMap<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>>();
        Set<byte[]> conditionOnlyCfs = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        int referencedCfCount = familyMap.size();
        boolean filteredColumnNotInProjection = false;
        for (Pair<byte[], byte[]> whereCol : context.getWhereConditionColumns()) {
            byte[] filteredFamily = whereCol.getFirst();
            if (!(familyMap.containsKey(filteredFamily))) {
                referencedCfCount++;
                filteredColumnNotInProjection = true;
            } else if (!filteredColumnNotInProjection) {
                NavigableSet<byte[]> projectedColumns = familyMap.get(filteredFamily);
                if (projectedColumns != null) {
                    byte[] filteredColumn = whereCol.getSecond();
                    if (filteredColumn == null) {
                        filteredColumnNotInProjection = true;
                    } else {
                        filteredColumnNotInProjection = !projectedColumns.contains(filteredColumn);
                    }
                }
            }
        }
        boolean preventSeekToColumn;
        if (statement.getHint().hasHint(Hint.SEEK_TO_COLUMN)) {
            // Allow seeking to column during filtering
            preventSeekToColumn = false;
        } else if (statement.getHint().hasHint(Hint.NO_SEEK_TO_COLUMN)) {
            // Prevent seeking to column during filtering
            preventSeekToColumn = true;
        } else {
            int hbaseServerVersion = context.getConnection().getQueryServices().getLowestClusterHBaseVersion();
            // When only a single column family is referenced, there are no hints, and HBase server version
            // is less than when the fix for HBASE-13109 went in (0.98.12), then we prevent seeking to a
            // column.
            preventSeekToColumn = referencedCfCount == 1 && hbaseServerVersion < MIN_SEEK_TO_COLUMN_VERSION;
        }
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
        // Making sure that where condition CFs are getting scanned at HRS.
        for (Pair<byte[], byte[]> whereCol : context.getWhereConditionColumns()) {
            byte[] family = whereCol.getFirst();
            if (preventSeekToColumn) {
                if (!(familyMap.containsKey(family))) {
                    conditionOnlyCfs.add(family);
                }
                scan.addFamily(family);
            } else {
                if (familyMap.containsKey(family)) {
                    // where column's CF is present. If there are some specific columns added against this CF, we
                    // need to ensure this where column also getting added in it.
                    // If the select was like select cf1.*, then that itself will select the whole CF. So no need to
                    // specifically add the where column. Adding that will remove the cf1.* stuff and only this
                    // where condition column will get returned!
                    NavigableSet<byte[]> cols = familyMap.get(family);
                    // cols is null means the whole CF will get scanned.
                    if (cols != null) {
                        if (whereCol.getSecond() == null) {
                            scan.addFamily(family);                            
                        } else {
                            scan.addColumn(family, whereCol.getSecond());
                        }
                    }
                } else if (whereCol.getSecond() == null) {
                    scan.addFamily(family);
                } else {
                    // where column's CF itself is not present in family map. We need to add the column
                    scan.addColumn(family, whereCol.getSecond());
                }
            }
        }
        if (!columnsTracker.isEmpty()) {
            if (preventSeekToColumn) {
                for (ImmutableBytesPtr f : columnsTracker.keySet()) {
                    // This addFamily will remove explicit cols in scan familyMap and make it as entire row.
                    // We don't want the ExplicitColumnTracker to be used. Instead we have the ColumnProjectionFilter
                    scan.addFamily(f.get());
                }
            }
            // We don't need this filter for aggregates, as we're not returning back what's
            // in the scan in this case. We still want the other optimization that causes
            // the ExplicitColumnTracker not to be used, though.
            if (!statement.isAggregate() && filteredColumnNotInProjection) {
                ScanUtil.andFilterAtEnd(scan, new ColumnProjectionFilter(SchemaUtil.getEmptyColumnFamily(table),
                        columnsTracker, conditionOnlyCfs));
            }
        }
    }
    
    public BaseResultIterators(QueryPlan plan, Integer perScanLimit, ParallelScanGrouper scanGrouper) throws SQLException {
        super(plan.getContext(), plan.getTableRef(), plan.getGroupBy(), plan.getOrderBy(), plan.getStatement().getHint(), plan.getLimit());
        this.plan = plan;
        this.scanGrouper = scanGrouper;
        StatementContext context = plan.getContext();
        // Clone MutationState as the one on the connection will change if auto commit is on
        // yet we need the original one with the original transaction from TableResultIterator.
        this.mutationState = new MutationState(context.getConnection().getMutationState());
        TableRef tableRef = plan.getTableRef();
        PTable table = tableRef.getTable();
        physicalTableName = table.getPhysicalName().getBytes();
        tableStats = useStats() ? new MetaDataClient(context.getConnection()).getTableStats(table) : PTableStats.EMPTY_STATS;
        // Used to tie all the scans together during logging
        scanId = UUID.randomUUID().toString();
        
        initializeScan(plan, perScanLimit);
        
        this.scans = getParallelScans();
        List<KeyRange> splitRanges = Lists.newArrayListWithExpectedSize(scans.size() * ESTIMATED_GUIDEPOSTS_PER_REGION);
        for (List<Scan> scanList : scans) {
            for (Scan aScan : scanList) {
                splitRanges.add(KeyRange.getKeyRange(aScan.getStartRow(), aScan.getStopRow()));
            }
        }
        this.splits = ImmutableList.copyOf(splitRanges);
        // If split detected, this will be more than one, but that's unlikely
        this.allFutures = Lists.newArrayListWithExpectedSize(1);
    }

    @Override
    public List<KeyRange> getSplits() {
        if (splits == null)
            return Collections.emptyList();
        else
            return splits;
    }

    @Override
    public List<List<Scan>> getScans() {
        if (scans == null)
            return Collections.emptyList();
        else
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

    private GuidePostsInfo getGuidePosts(Set<byte[]> whereConditions) {
        /*
         * Don't use guide posts if: 1) We're doing a point lookup, as HBase is fast enough at those to not need them to
         * be further parallelized. TODO: pref test to verify 2) We're collecting stats, as in this case we need to scan
         * entire regions worth of data to track where to put the guide posts.
         */
        if (!useStats()) { return GuidePostsInfo.NO_GUIDEPOST; }

        GuidePostsInfo gps = null;
        PTable table = getTable();
        Map<byte[], GuidePostsInfo> guidePostMap = tableStats.getGuidePosts();
        byte[] defaultCF = SchemaUtil.getEmptyColumnFamily(getTable());
        if (table.getColumnFamilies().isEmpty()) {
            // For sure we can get the defaultCF from the table
            gps = getDefaultFamilyGuidePosts(guidePostMap, defaultCF);
        } else {
            if (whereConditions.isEmpty() || whereConditions.contains(defaultCF)) {
                gps = getDefaultFamilyGuidePosts(guidePostMap, defaultCF);
            } else {
                byte[] familyInWhere = whereConditions.iterator().next();
                GuidePostsInfo guidePostsInfo = guidePostMap.get(familyInWhere);
                if (guidePostsInfo != null) {
                    gps = guidePostsInfo;
                } else {
                    // As there are no guideposts collected for the where family we go with the default CF
                    gps = getDefaultFamilyGuidePosts(guidePostMap, defaultCF);
                }
            }
        }
        if (gps == null) { return GuidePostsInfo.NO_GUIDEPOST; }
        return gps;
    }

    private GuidePostsInfo getDefaultFamilyGuidePosts(Map<byte[], GuidePostsInfo> guidePostMap, byte[] defaultCF) {
        if (guidePostMap.get(defaultCF) != null) {
                return guidePostMap.get(defaultCF);
        }
        return null;
    }

    private static String toString(List<byte[]> gps) {
        StringBuilder buf = new StringBuilder(gps.size() * 100);
        buf.append("[");
        for (int i = 0; i < gps.size(); i++) {
            buf.append(Bytes.toStringBinary(gps.get(i)));
            buf.append(",");
            if (i > 0 && i < gps.size()-1 && (i % 10) == 0) {
                buf.append("\n");
            }
        }
        buf.setCharAt(buf.length()-1, ']');
        return buf.toString();
    }
    
    private List<Scan> addNewScan(List<List<Scan>> parallelScans, List<Scan> scans, Scan scan, byte[] startKey, boolean crossedRegionBoundary, HRegionLocation regionLocation) {
        boolean startNewScan = scanGrouper.shouldStartNewScan(plan, scans, startKey, crossedRegionBoundary);
        if (scan != null) {
            scan.setAttribute(BaseScannerRegionObserver.SCAN_REGION_SERVER, regionLocation.getServerName().getVersionedBytes());
        	scans.add(scan);
        }
        if (startNewScan && !scans.isEmpty()) {
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
        TreeSet<byte[]> whereConditions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for(Pair<byte[], byte[]> where : context.getWhereConditionColumns()) {
            byte[] cf = where.getFirst();
            if (cf != null) {
                whereConditions.add(cf);
            }
        }
        GuidePostsInfo gps = getGuidePosts(whereConditions);
        hasGuidePosts = gps != GuidePostsInfo.NO_GUIDEPOST;
        boolean traverseAllRegions = isSalted || isLocalIndex;
        if (!traverseAllRegions) {
            byte[] scanStartRow = scan.getStartRow();
            if (scanStartRow.length != 0 && Bytes.compareTo(scanStartRow, startKey) > 0) {
                startKey = scanStartRow;
            }
            byte[] scanStopRow = scan.getStopRow();
            if (stopKey.length == 0
                    || (scanStopRow.length != 0 && Bytes.compareTo(scanStopRow, stopKey) < 0)) {
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
        
        ImmutableBytesWritable currentKey = new ImmutableBytesWritable(startKey);
        
        int gpsSize = gps.getGuidePostsCount();
        int estGuidepostsPerRegion = gpsSize == 0 ? 1 : gpsSize / regionLocations.size() + 1;
        int keyOffset = 0;
        ImmutableBytesWritable currentGuidePost = ByteUtil.EMPTY_IMMUTABLE_BYTE_ARRAY;
        List<Scan> scans = Lists.newArrayListWithExpectedSize(estGuidepostsPerRegion);
        ImmutableBytesWritable guidePosts = gps.getGuidePosts();
        ByteArrayInputStream stream = null;
        DataInput input = null;
        PrefixByteDecoder decoder = null;
        int guideIndex = 0;
        long estimatedRows = 0;
        long estimatedSize = 0;
        try {
            if (gpsSize > 0) {
                stream = new ByteArrayInputStream(guidePosts.get(), guidePosts.getOffset(), guidePosts.getLength());
                input = new DataInputStream(stream);
                decoder = new PrefixByteDecoder(gps.getMaxLength());
                try {
                    while (currentKey.compareTo(currentGuidePost = PrefixByteCodec.decode(decoder, input)) >= 0
                            && currentKey.getLength() != 0) {
                        guideIndex++;
                    }
                } catch (EOFException e) {}
            }
            byte[] currentKeyBytes = currentKey.copyBytes();
    
            // Merge bisect with guideposts for all but the last region
            while (regionIndex <= stopIndex) {
                byte[] currentGuidePostBytes = currentGuidePost.copyBytes();
                byte[] endKey, endRegionKey = EMPTY_BYTE_ARRAY;
                if (regionIndex == stopIndex) {
                    endKey = stopKey;
                } else {
                    endKey = regionBoundaries.get(regionIndex);
                }
                HRegionLocation regionLocation = regionLocations.get(regionIndex);
                if (isLocalIndex) {
                    HRegionInfo regionInfo = regionLocation.getRegionInfo();
                    endRegionKey = regionInfo.getEndKey();
                    keyOffset = ScanUtil.getRowKeyOffset(regionInfo.getStartKey(), endRegionKey);
                }
                try {
                    while (guideIndex < gpsSize && (currentGuidePost.compareTo(endKey) <= 0 || endKey.length == 0)) {
                        Scan newScan = scanRanges.intersectScan(scan, currentKeyBytes, currentGuidePostBytes, keyOffset,
                                false);
                        estimatedRows += gps.getRowCounts().get(guideIndex);
                        estimatedSize += gps.getByteCounts().get(guideIndex);
                        scans = addNewScan(parallelScans, scans, newScan, currentGuidePostBytes, false, regionLocation);
                        currentKeyBytes = currentGuidePost.copyBytes();
                        currentGuidePost = PrefixByteCodec.decode(decoder, input);
                        currentGuidePostBytes = currentGuidePost.copyBytes();
                        guideIndex++;
                    }
                } catch (EOFException e) {}
                Scan newScan = scanRanges.intersectScan(scan, currentKeyBytes, endKey, keyOffset, true);
                if (isLocalIndex) {
                    if (newScan != null) {
                        newScan.setAttribute(EXPECTED_UPPER_REGION_KEY, endRegionKey);
                    } else if (!scans.isEmpty()) {
                        scans.get(scans.size()-1).setAttribute(EXPECTED_UPPER_REGION_KEY, endRegionKey);
                    }
                }
                scans = addNewScan(parallelScans, scans, newScan, endKey, true, regionLocation);
                currentKeyBytes = endKey;
                regionIndex++;
            }
            if (hasGuidePosts) {
                this.estimatedRows = estimatedRows;
                this.estimatedSize = estimatedSize;
            } else {
                this.estimatedRows = null;
                this.estimatedSize = null;
            }
            if (!scans.isEmpty()) { // Add any remaining scans
                parallelScans.add(scans);
            }
        } finally {
            if (stream != null) Closeables.closeQuietly(stream);
        }
        return parallelScans;
    }

   
    public static <T> List<T> reverseIfNecessary(List<T> list, boolean reverse) {
        if (!reverse) {
            return list;
        }
        return Lists.reverse(list);
    }
    
    /**
     * Executes the scan in parallel across all regions, blocking until all scans are complete.
     * @return the result iterators for the scan of each region
     */
    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        Scan scan = context.getScan();
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("Getting iterators for " + this,
                    ScanUtil.getCustomAnnotations(scan)));
        }
        boolean isReverse = ScanUtil.isReversed(scan);
        boolean isLocalIndex = getTable().getIndexType() == IndexType.LOCAL;
        final ConnectionQueryServices services = context.getConnection().getQueryServices();
        // Get query time out from Statement
        final long startTime = System.currentTimeMillis();
        final long maxQueryEndTime = startTime + context.getStatement().getQueryTimeoutInMillis();
        int numScans = size();
        // Capture all iterators so that if something goes wrong, we close them all
        // The iterators list is based on the submission of work, so it may not
        // contain them all (for example if work was rejected from the queue)
        Queue<PeekingResultIterator> allIterators = new ConcurrentLinkedQueue<>();
        List<PeekingResultIterator> iterators = new ArrayList<PeekingResultIterator>(numScans);
        ScanWrapper previousScan = new ScanWrapper(null);
        return getIterators(scans, services, isLocalIndex, allIterators, iterators, isReverse, maxQueryEndTime,
                splits.size(), previousScan);
    }

    class ScanWrapper {
        Scan scan;

        public Scan getScan() {
            return scan;
        }

        public void setScan(Scan scan) {
            this.scan = scan;
        }

        public ScanWrapper(Scan scan) {
            this.scan = scan;
        }

    }

    private List<PeekingResultIterator> getIterators(List<List<Scan>> scan, ConnectionQueryServices services,
            boolean isLocalIndex, Queue<PeekingResultIterator> allIterators, List<PeekingResultIterator> iterators,
            boolean isReverse, long maxQueryEndTime, int splitSize, ScanWrapper previousScan) throws SQLException {
        boolean success = false;
        final List<List<Pair<Scan,Future<PeekingResultIterator>>>> futures = Lists.newArrayListWithExpectedSize(splitSize);
        allFutures.add(futures);
        SQLException toThrow = null;
        int queryTimeOut = context.getStatement().getQueryTimeoutInMillis();
        try {
            submitWork(scan, futures, allIterators, splitSize);
            boolean clearedCache = false;
            for (List<Pair<Scan,Future<PeekingResultIterator>>> future : reverseIfNecessary(futures,isReverse)) {
                List<PeekingResultIterator> concatIterators = Lists.newArrayListWithExpectedSize(future.size());
                for (Pair<Scan,Future<PeekingResultIterator>> scanPair : reverseIfNecessary(future,isReverse)) {
                    try {
                        long timeOutForScan = maxQueryEndTime - System.currentTimeMillis();
                        if (timeOutForScan < 0) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.OPERATION_TIMED_OUT).setMessage(". Query couldn't be completed in the alloted time: " + queryTimeOut + " ms").build().buildException(); 
                        }
                        if (isLocalIndex && previousScan != null && previousScan.getScan() != null
                                && ((!isReverse && Bytes.compareTo(scanPair.getFirst().getAttribute(SCAN_ACTUAL_START_ROW),
                                        previousScan.getScan().getStopRow()) < 0)
                                || (isReverse && Bytes.compareTo(scanPair.getFirst().getAttribute(SCAN_ACTUAL_START_ROW),
                                        previousScan.getScan().getStopRow()) > 0)
                                || (scanPair.getFirst().getAttribute(EXPECTED_UPPER_REGION_KEY) != null
                                        && previousScan.getScan().getAttribute(EXPECTED_UPPER_REGION_KEY) != null
                                        && Bytes.compareTo(scanPair.getFirst().getAttribute(EXPECTED_UPPER_REGION_KEY),
                                                previousScan.getScan()
                                                        .getAttribute(EXPECTED_UPPER_REGION_KEY)) == 0))) {
                            continue;
                        }
                        PeekingResultIterator iterator = scanPair.getSecond().get(timeOutForScan, TimeUnit.MILLISECONDS);
                        concatIterators.add(iterator);
                        previousScan.setScan(scanPair.getFirst());
                    } catch (ExecutionException e) {
                        try { // Rethrow as SQLException
                            throw ServerUtil.parseServerException(e);
                        } catch (StaleRegionBoundaryCacheException e2) {
                            // Catch only to try to recover from region boundary cache being out of date
                            if (!clearedCache) { // Clear cache once so that we rejigger job based on new boundaries
                                services.clearTableRegionCache(physicalTableName);
                                context.getOverallQueryMetrics().cacheRefreshedDueToSplits();
                            }
                            // Resubmit just this portion of work again
                            Scan oldScan = scanPair.getFirst();
                            byte[] startKey = oldScan.getAttribute(SCAN_ACTUAL_START_ROW);
                            byte[] endKey = oldScan.getStopRow();
                            if (isLocalIndex) {
                                endKey = oldScan.getAttribute(EXPECTED_UPPER_REGION_KEY);
                            }
                            
                            List<List<Scan>> newNestedScans = this.getParallelScans(startKey, endKey);
                            // Add any concatIterators that were successful so far
                            // as we need these to be in order
                            addIterator(iterators, concatIterators);
                            concatIterators = Lists.newArrayList();
                            getIterators(newNestedScans, services, isLocalIndex, allIterators, iterators, isReverse,
                                    maxQueryEndTime, newNestedScans.size(), previousScan);
                        }
                    }
                }
                addIterator(iterators, concatIterators);
            }
            success = true;
            return iterators;
        } catch (TimeoutException e) {
            context.getOverallQueryMetrics().queryTimedOut();
            GLOBAL_QUERY_TIMEOUT_COUNTER.increment();
            // thrown when a thread times out waiting for the future.get() call to return
            toThrow = new SQLExceptionInfo.Builder(SQLExceptionCode.OPERATION_TIMED_OUT)
                    .setMessage(". Query couldn't be completed in the alloted time: " + queryTimeOut + " ms")
                    .setRootCause(e).build().buildException();
        } catch (SQLException e) {
            toThrow = e;
        } catch (Exception e) {
            toThrow = ServerUtil.parseServerException(e);
        } finally {
            try {
                if (!success) {
                    try {
                        close();
                    } catch (Exception e) {
                        if (toThrow == null) {
                            toThrow = ServerUtil.parseServerException(e);
                        } else {
                            toThrow.setNextException(ServerUtil.parseServerException(e));
                        }
                    } finally {
                        try {
                            SQLCloseables.closeAll(allIterators);
                        } catch (Exception e) {
                            if (toThrow == null) {
                                toThrow = ServerUtil.parseServerException(e);
                            } else {
                                toThrow.setNextException(ServerUtil.parseServerException(e));
                            }
                        }
                    }
                }
            } finally {
                if (toThrow != null) {
                    GLOBAL_FAILED_QUERY_COUNTER.increment();
                    context.getOverallQueryMetrics().queryFailed();
                    throw toThrow;
                }
            }
        }
        return null; // Not reachable
    }
    

    @Override
    public void close() throws SQLException {
        if (allFutures.isEmpty()) {
            return;
        }
        // Don't call cancel on already started work, as it causes the HConnection
        // to get into a funk. Instead, just cancel queued work.
        boolean cancelledWork = false;
        try {
            List<Future<PeekingResultIterator>> futuresToClose = Lists.newArrayListWithExpectedSize(getSplits().size());
            for (List<List<Pair<Scan,Future<PeekingResultIterator>>>> futures : allFutures) {
                for (List<Pair<Scan,Future<PeekingResultIterator>>> futureScans : futures) {
                    for (Pair<Scan,Future<PeekingResultIterator>> futurePair : futureScans) {
                        // When work is rejected, we may have null futurePair entries, because
                        // we randomize these and set them as they're submitted.
                        if (futurePair != null) {
                            Future<PeekingResultIterator> future = futurePair.getSecond();
                            if (future != null) {
                                if (future.cancel(false)) {
                                    cancelledWork = true;
                                } else {
                                    futuresToClose.add(future);
                                }
                            }
                        }
                    }
                }
            }
            // Wait for already started tasks to complete as we can't interrupt them without
            // leaving our HConnection in a funky state.
            for (Future<PeekingResultIterator> future : futuresToClose) {
                try {
                    PeekingResultIterator iterator = future.get();
                    iterator.close();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    logger.info("Failed to execute task during cancel", e);
                    continue;
                }
            }
        } finally {
            if (cancelledWork) {
                context.getConnection().getQueryServices().getExecutor().purge();
            }
            allFutures.clear();
        }
    }

    private void addIterator(List<PeekingResultIterator> parentIterators, List<PeekingResultIterator> childIterators) throws SQLException {
        if (!childIterators.isEmpty()) {
            if (plan.useRoundRobinIterator()) {
                /*
                 * When using a round robin iterator we shouldn't concatenate the iterators together. This is because a
                 * round robin iterator should be calling next() on these iterators directly after selecting them in a 
                 * round robin fashion. This helps take advantage of loading the underlying scanners' caches in parallel
                 * as well as preventing errors arising out of scanner lease expirations.
                 */
                parentIterators.addAll(childIterators);
            } else {
                parentIterators.add(ConcatResultIterator.newIterator(childIterators));
            }
        }
    }

    protected static final class ScanLocator {
    	private final int outerListIndex;
    	private final int innerListIndex;
    	private final Scan scan;
    	
    	public ScanLocator(Scan scan, int outerListIndex, int innerListIndex) {
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
    

    abstract protected String getName();    
    abstract protected void submitWork(List<List<Scan>> nestedScans, List<List<Pair<Scan,Future<PeekingResultIterator>>>> nestedFutures,
            Queue<PeekingResultIterator> allIterators, int estFlattenedSize) throws SQLException;
    
    @Override
    public int size() {
        return this.scans.size();
    }

    @Override
    public void explain(List<String> planSteps) {
        boolean displayChunkCount = context.getConnection().getQueryServices().getProps().getBoolean(
                QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB,
                QueryServicesOptions.DEFAULT_EXPLAIN_CHUNK_COUNT);
        StringBuilder buf = new StringBuilder();
        buf.append("CLIENT ");
        if (displayChunkCount) {
            boolean displayRowCount = context.getConnection().getQueryServices().getProps().getBoolean(
                    QueryServices.EXPLAIN_ROW_COUNT_ATTRIB,
                    QueryServicesOptions.DEFAULT_EXPLAIN_ROW_COUNT);
            buf.append(this.splits.size()).append("-CHUNK ");
            if (displayRowCount && hasGuidePosts) {
                buf.append(estimatedRows).append(" ROWS ");
                buf.append(estimatedSize).append(" BYTES ");
            }
        }
        buf.append(getName()).append(" ").append(size()).append("-WAY ");
        try {
            if (plan.useRoundRobinIterator()) {
                buf.append("ROUND ROBIN ");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        explain(buf.toString(),planSteps);
    }

    public Long getEstimatedRowCount() {
        return this.estimatedRows;
    }
    
    public Long getEstimatedByteCount() {
        return this.estimatedSize;
    }
    
    @Override
    public String toString() {
        return "ResultIterators [name=" + getName() + ",id=" + scanId + ",scans=" + scans + "]";
    }

}