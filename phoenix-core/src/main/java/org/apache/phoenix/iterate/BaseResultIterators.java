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

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.LOCAL_INDEX_BUILD;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SCAN_ACTUAL_START_ROW;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SCAN_START_ROW_SUFFIX;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SCAN_STOP_ROW_SUFFIX;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_FAILED_QUERY_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.schema.PTable.IndexType.LOCAL;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;
import static org.apache.phoenix.util.EncodedColumnsUtil.isPossibleToUseEncodedCQFilter;
import static org.apache.phoenix.util.ScanUtil.hasDynamicColumns;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.HashJoinCacheNotFoundException;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.filter.BooleanExpressionFilter;
import org.apache.phoenix.filter.ColumnProjectionFilter;
import org.apache.phoenix.filter.DistinctPrefixFilter;
import org.apache.phoenix.filter.EncodedQualifiersColumnProjectionFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.join.HashCacheClient;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PrefixByteCodec;
import org.apache.phoenix.util.PrefixByteDecoder;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
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
	public static final Logger logger = LoggerFactory.getLogger(BaseResultIterators.class);
    private static final int ESTIMATED_GUIDEPOSTS_PER_REGION = 20;
    private static final int MIN_SEEK_TO_COLUMN_VERSION = VersionUtil.encodeVersion("0", "98", "12");
    private final List<List<Scan>> scans;
    private final List<KeyRange> splits;
    private final byte[] physicalTableName;
    protected final QueryPlan plan;
    protected final String scanId;
    protected final MutationState mutationState;
    protected final ParallelScanGrouper scanGrouper;
    // TODO: too much nesting here - breakup into new classes.
    private final List<List<List<Pair<Scan,Future<PeekingResultIterator>>>>> allFutures;
    private Long estimatedRows;
    private Long estimatedSize;
    private Long estimateInfoTimestamp;
    private boolean hasGuidePosts;
    private Scan scan;
    private final boolean useStatsForParallelization;
    protected Map<ImmutableBytesPtr,ServerCache> caches;
    private final QueryPlan dataPlan;
    
    static final Function<HRegionLocation, KeyRange> TO_KEY_RANGE = new Function<HRegionLocation, KeyRange>() {
        @Override
        public KeyRange apply(HRegionLocation region) {
            return KeyRange.getKeyRange(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
        }
    };

    private PTable getTable() {
        return plan.getTableRef().getTable();
    }
    
    abstract protected boolean isSerial();
    
    protected boolean useStats() {
        /*
         * Don't use guide posts:
         * 1) If we're collecting stats, as in this case we need to scan entire
         * regions worth of data to track where to put the guide posts.
         * 2) If the query is going to be executed serially.
         */
        if (ScanUtil.isAnalyzeTable(scan)) {
            return false;
        }
        return !isSerial();
    }
    
    private static void initializeScan(QueryPlan plan, Integer perScanLimit, Integer offset, Scan scan) throws SQLException {
        StatementContext context = plan.getContext();
        TableRef tableRef = plan.getTableRef();
        PTable table = tableRef.getTable();

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
                                context.addWhereConditionColumn(family.getName().getBytes(), null);
                            }
                        } else {
                            byte[] ecf = SchemaUtil.getEmptyColumnFamily(table);
                            // Project empty key value unless the column family containing it has
                            // been projected in its entirety.
                            if (!familyMap.containsKey(ecf) || familyMap.get(ecf) != null) {
                                scan.addColumn(ecf, EncodedColumnsUtil.getEmptyKeyValueInfo(table).getFirst());
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
            
            if(offset!=null){
                ScanUtil.addOffsetAttribute(scan, offset);
            }
            int cols = plan.getGroupBy().getOrderPreservingColumnCount();
            if (cols > 0 && keyOnlyFilter &&
                !plan.getStatement().getHint().hasHint(HintNode.Hint.RANGE_SCAN) &&
                cols < plan.getTableRef().getTable().getRowKeySchema().getFieldCount() &&
                plan.getGroupBy().isOrderPreserving() &&
                (context.getAggregationManager().isEmpty() || plan.getGroupBy().isUngroupedAggregate())) {
                
                ScanUtil.andFilterAtEnd(scan,
                        new DistinctPrefixFilter(plan.getTableRef().getTable().getRowKeySchema(),
                                cols));
                if (plan.getLimit() != null) { // We can push the limit to the server
                    ScanUtil.andFilterAtEnd(scan, new PageFilter(plan.getLimit()));
                }
            }
            scan.setAttribute(BaseScannerRegionObserver.QUALIFIER_ENCODING_SCHEME, new byte[]{table.getEncodingScheme().getSerializedMetadataValue()});
            scan.setAttribute(BaseScannerRegionObserver.IMMUTABLE_STORAGE_ENCODING_SCHEME, new byte[]{table.getImmutableStorageScheme().getSerializedMetadataValue()});
            // we use this flag on the server side to determine which value column qualifier to use in the key value we return from server.
            scan.setAttribute(BaseScannerRegionObserver.USE_NEW_VALUE_COLUMN_QUALIFIER, Bytes.toBytes(true));
            // When analyzing the table, there is no look up for key values being done.
            // So there is no point setting the range.
            if (!ScanUtil.isAnalyzeTable(scan)) {
                setQualifierRanges(keyOnlyFilter, table, scan, context);
            }
            if (optimizeProjection) {
                optimizeProjection(context, scan, table, statement);
            }
        }
    }
    
    private static void setQualifierRanges(boolean keyOnlyFilter, PTable table, Scan scan,
            StatementContext context) throws SQLException {
        if (EncodedColumnsUtil.useEncodedQualifierListOptimization(table, scan)) {
            Pair<Integer, Integer> minMaxQualifiers = new Pair<>();
            for (Pair<byte[], byte[]> whereCol : context.getWhereConditionColumns()) {
                byte[] cq = whereCol.getSecond();
                if (cq != null) {
                    int qualifier = table.getEncodingScheme().decode(cq);
                    adjustQualifierRange(qualifier, minMaxQualifiers);
                }
            }
            Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
            for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
                if (entry.getValue() != null) {
                    for (byte[] cq : entry.getValue()) {
                        if (cq != null) {
                            int qualifier = table.getEncodingScheme().decode(cq);
                            adjustQualifierRange(qualifier, minMaxQualifiers);
                        }
                    }
                } else {
                    byte[] cf = entry.getKey();
                    String family = Bytes.toString(cf);
                    if (table.getType() == INDEX && table.getIndexType() == LOCAL
                            && !IndexUtil.isLocalIndexFamily(family)) {
                        // TODO: samarth confirm with James why do we need this hack here :(
                        family = IndexUtil.getLocalIndexColumnFamily(family);
                    }
                    byte[] familyBytes = Bytes.toBytes(family);
                    NavigableSet<byte[]> qualifierSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
                    if (Bytes.equals(familyBytes, SchemaUtil.getEmptyColumnFamily(table))) {
                        // If the column family is also the empty column family, project the
                        // empty key value column
                        Pair<byte[], byte[]> emptyKeyValueInfo =
                                EncodedColumnsUtil.getEmptyKeyValueInfo(table);
                        qualifierSet.add(emptyKeyValueInfo.getFirst());
                    }
                    // In case of a keyOnlyFilter, we only need to project the 
                    // empty key value column
                    if (!keyOnlyFilter) {
                        Pair<Integer, Integer> qualifierRangeForFamily =
                                EncodedColumnsUtil.setQualifiersForColumnsInFamily(table, family,
                                    qualifierSet);
                        familyMap.put(familyBytes, qualifierSet);
                        if (qualifierRangeForFamily != null) {
                            adjustQualifierRange(qualifierRangeForFamily.getFirst(),
                                minMaxQualifiers);
                            adjustQualifierRange(qualifierRangeForFamily.getSecond(),
                                minMaxQualifiers);
                        }
                    }
                }
            }
            if (minMaxQualifiers.getFirst() != null) {
                scan.setAttribute(BaseScannerRegionObserver.MIN_QUALIFIER,
                    Bytes.toBytes(minMaxQualifiers.getFirst()));
                scan.setAttribute(BaseScannerRegionObserver.MAX_QUALIFIER,
                    Bytes.toBytes(minMaxQualifiers.getSecond()));
                ScanUtil.setQualifierRangesOnFilter(scan, minMaxQualifiers);
            }
        }
    }

    private static void adjustQualifierRange(Integer qualifier, Pair<Integer, Integer> minMaxQualifiers) {
        if (minMaxQualifiers.getFirst() == null) {
            minMaxQualifiers.setFirst(qualifier);
            minMaxQualifiers.setSecond(qualifier);
        } else {
            if (minMaxQualifiers.getFirst() > qualifier) {
                minMaxQualifiers.setFirst(qualifier);
            } else if (minMaxQualifiers.getSecond() < qualifier) {
                minMaxQualifiers.setSecond(qualifier);
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
        QualifierEncodingScheme encodingScheme = table.getEncodingScheme();
        ImmutableStorageScheme storageScheme = table.getImmutableStorageScheme();
        BitSet trackedColumnsBitset = isPossibleToUseEncodedCQFilter(encodingScheme, storageScheme) && !hasDynamicColumns(table) ? new BitSet(10) : null;
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
        boolean preventSeekToColumn = false;
        if (statement.getHint().hasHint(Hint.SEEK_TO_COLUMN)) {
            // Allow seeking to column during filtering
            preventSeekToColumn = false;
        } else if (!EncodedColumnsUtil.useEncodedQualifierListOptimization(table, scan)) {
            /*
             * preventSeekToColumn cannot be true, even if hinted, when encoded qualifier list
             * optimization is being used. When using the optimization, it is necessary that we
             * explicitly set the column qualifiers of the column family in the scan and not just
             * project the entire column family.
             */
            if (statement.getHint().hasHint(Hint.NO_SEEK_TO_COLUMN)) {
                // Prevent seeking to column during filtering
                preventSeekToColumn = true;
            } else {
                int hbaseServerVersion = context.getConnection().getQueryServices().getLowestClusterHBaseVersion();
                // When only a single column family is referenced, there are no hints, and HBase server version
                // is less than when the fix for HBASE-13109 went in (0.98.12), then we prevent seeking to a
                // column.
                preventSeekToColumn = referencedCfCount == 1 && hbaseServerVersion < MIN_SEEK_TO_COLUMN_VERSION;
            }
        }
        for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
            ImmutableBytesPtr cf = new ImmutableBytesPtr(entry.getKey());
            NavigableSet<byte[]> qs = entry.getValue();
            NavigableSet<ImmutableBytesPtr> cols = null;
            if (qs != null) {
                cols = new TreeSet<ImmutableBytesPtr>();
                for (byte[] q : qs) {
                    cols.add(new ImmutableBytesPtr(q));
                    if (trackedColumnsBitset != null) {
                        int qualifier = encodingScheme.decode(q);
                        trackedColumnsBitset.set(qualifier);
                    }
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
                ScanUtil.andFilterAtEnd(scan, 
                    trackedColumnsBitset != null ? new EncodedQualifiersColumnProjectionFilter(SchemaUtil.getEmptyColumnFamily(table), trackedColumnsBitset, conditionOnlyCfs, table.getEncodingScheme()) : new ColumnProjectionFilter(SchemaUtil.getEmptyColumnFamily(table),
                        columnsTracker, conditionOnlyCfs, EncodedColumnsUtil.usesEncodedColumnNames(table.getEncodingScheme())));
            }
        }
    }
    
    public BaseResultIterators(QueryPlan plan, Integer perScanLimit, Integer offset, ParallelScanGrouper scanGrouper, Scan scan, Map<ImmutableBytesPtr,ServerCache> caches, QueryPlan dataPlan) throws SQLException {
        super(plan.getContext(), plan.getTableRef(), plan.getGroupBy(), plan.getOrderBy(),
                plan.getStatement().getHint(), QueryUtil.getOffsetLimit(plan.getLimit(), plan.getOffset()), offset);
        this.plan = plan;
        this.scan = scan;
        this.caches = caches;
        this.scanGrouper = scanGrouper;
        this.dataPlan = dataPlan;
        StatementContext context = plan.getContext();
        // Clone MutationState as the one on the connection will change if auto commit is on
        // yet we need the original one with the original transaction from TableResultIterator.
        this.mutationState = new MutationState(context.getConnection().getMutationState());
        TableRef tableRef = plan.getTableRef();
        PTable table = tableRef.getTable();
        physicalTableName = table.getPhysicalName().getBytes();
        Long currentSCN = context.getConnection().getSCN();
        if (null == currentSCN) {
          currentSCN = HConstants.LATEST_TIMESTAMP;
        }
        // Used to tie all the scans together during logging
        scanId = new UUID(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()).toString();
        
        initializeScan(plan, perScanLimit, offset, scan);
        this.useStatsForParallelization = PhoenixConfigurationUtil.getStatsForParallelizationProp(context.getConnection(), table);
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

    private List<HRegionLocation> getRegionBoundaries(ParallelScanGrouper scanGrouper)
        throws SQLException{
        return scanGrouper.getRegionBoundaries(context, physicalTableName);
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

    private GuidePostsInfo getGuidePosts() throws SQLException {
        if (!useStats() || !StatisticsUtil.isStatsEnabled(TableName.valueOf(physicalTableName))) {
            return GuidePostsInfo.NO_GUIDEPOST;
        }

        TreeSet<byte[]> whereConditions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for(Pair<byte[], byte[]> where : context.getWhereConditionColumns()) {
            byte[] cf = where.getFirst();
            if (cf != null) {
                whereConditions.add(cf);
            }
        }
        PTable table = getTable();
        byte[] defaultCF = SchemaUtil.getEmptyColumnFamily(getTable());
        byte[] cf = null;
        if ( !table.getColumnFamilies().isEmpty() && !whereConditions.isEmpty() ) {
            for(Pair<byte[], byte[]> where : context.getWhereConditionColumns()) {
                byte[] whereCF = where.getFirst();
                if (Bytes.compareTo(defaultCF, whereCF) == 0) {
                    cf = defaultCF;
                    break;
                }
            }
            if (cf == null) {
                cf = context.getWhereConditionColumns().get(0).getFirst();
            }
        }
        if (cf == null) {
            cf = defaultCF;
        }
        GuidePostsKey key = new GuidePostsKey(physicalTableName, cf);
        return context.getConnection().getQueryServices().getTableStats(key);
    }

    private static void updateEstimates(GuidePostsInfo gps, int guideIndex, GuidePostEstimate estimate) {
        estimate.rowsEstimate += gps.getRowCounts()[guideIndex];
        estimate.bytesEstimate += gps.getByteCounts()[guideIndex];
        /*
         * It is possible that the timestamp of guideposts could be different.
         * So we report the time at which stats information was collected as the
         * minimum of timestamp of the guideposts that we will be going over.
         */
        estimate.lastUpdated =
                Math.min(estimate.lastUpdated,
                    gps.getGuidePostTimestamps()[guideIndex]);
    }
    
    private List<Scan> addNewScan(List<List<Scan>> parallelScans, List<Scan> scans, Scan scan,
            byte[] startKey, boolean crossedRegionBoundary, HRegionLocation regionLocation) {
        boolean startNewScan = scanGrouper.shouldStartNewScan(plan, scans, startKey, crossedRegionBoundary);
        if (scan != null) {
            if (regionLocation.getServerName() != null) {
                scan.setAttribute(BaseScannerRegionObserver.SCAN_REGION_SERVER, regionLocation.getServerName().getVersionedBytes());
            }
            if (useStatsForParallelization || crossedRegionBoundary) {
                scans.add(scan);
            }
        }
        if (startNewScan && !scans.isEmpty()) {
            parallelScans.add(scans);
            scans = Lists.newArrayListWithExpectedSize(1);
        }
        return scans;
    }

    private List<List<Scan>> getParallelScans() throws SQLException {
        // If the scan boundaries are not matching with scan in context that means we need to get
        // parallel scans for the chunk after split/merge.
        if (!ScanUtil.isContextScan(scan, context)) {
            return getParallelScans(scan);
        }
        return getParallelScans(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
    }

    /**
     * Get parallel scans of the specified scan boundaries. This can be used for getting parallel
     * scans when there is split/merges while scanning a chunk. In this case we need not go by all
     * the regions or guideposts.
     * @param scan
     * @return
     * @throws SQLException
     */
    private List<List<Scan>> getParallelScans(Scan scan) throws SQLException {
        List<HRegionLocation> regionLocations = getRegionBoundaries(scanGrouper);
        List<byte[]> regionBoundaries = toBoundaries(regionLocations);
        int regionIndex = 0;
        int stopIndex = regionBoundaries.size();
        if (scan.getStartRow().length > 0) {
            regionIndex = getIndexContainingInclusive(regionBoundaries, scan.getStartRow());
        }
        if (scan.getStopRow().length > 0) {
            stopIndex = Math.min(stopIndex, regionIndex + getIndexContainingExclusive(regionBoundaries.subList(regionIndex, stopIndex), scan.getStopRow()));
        }
        List<List<Scan>> parallelScans = Lists.newArrayListWithExpectedSize(stopIndex - regionIndex + 1);
        List<Scan> scans = Lists.newArrayListWithExpectedSize(2);
        while (regionIndex <= stopIndex) {
            HRegionLocation regionLocation = regionLocations.get(regionIndex);
            HRegionInfo regionInfo = regionLocation.getRegionInfo();
            Scan newScan = ScanUtil.newScan(scan);
            byte[] endKey;
            if (regionIndex == stopIndex) {
                endKey = scan.getStopRow();
            } else {
                endKey = regionBoundaries.get(regionIndex);
            }
            if(ScanUtil.isLocalIndex(scan)) {
                ScanUtil.setLocalIndexAttributes(newScan, 0, regionInfo.getStartKey(),
                    regionInfo.getEndKey(), newScan.getAttribute(SCAN_START_ROW_SUFFIX),
                    newScan.getAttribute(SCAN_STOP_ROW_SUFFIX));
            } else {
                if(Bytes.compareTo(scan.getStartRow(), regionInfo.getStartKey())<=0) {
                    newScan.setAttribute(SCAN_ACTUAL_START_ROW, regionInfo.getStartKey());
                    newScan.setStartRow(regionInfo.getStartKey());
                }
                if(scan.getStopRow().length == 0 || (regionInfo.getEndKey().length != 0 && Bytes.compareTo(scan.getStopRow(), regionInfo.getEndKey())>0)) {
                    newScan.setStopRow(regionInfo.getEndKey());
                }
            }
            scans = addNewScan(parallelScans, scans, newScan, endKey, true, regionLocation);
            regionIndex++;
        }
        if (!scans.isEmpty()) { // Add any remaining scans
            parallelScans.add(scans);
        }
        return parallelScans;
    }

    private static class GuidePostEstimate {
        private long bytesEstimate;
        private long rowsEstimate;
        private long lastUpdated = Long.MAX_VALUE;
    }

    private int computeColumnsInCommon() {
        PTable dataTable;
        if ((dataTable=dataPlan.getTableRef().getTable()).getBucketNum() != null) { // unable to compute prefix range for salted data table
            return 0;
        }

        PTable table = getTable();
        int nColumnsOffset = dataTable.isMultiTenant() ? 1 :0;
        int nColumnsInCommon = nColumnsOffset;
        List<PColumn> dataPKColumns = dataTable.getPKColumns();
        List<PColumn> indexPKColumns = table.getPKColumns();
        int nIndexPKColumns = indexPKColumns.size();
        int nDataPKColumns = dataPKColumns.size();
        // Skip INDEX_ID and tenant ID columns
        for (int i = 1 + nColumnsInCommon; i < nIndexPKColumns; i++) {
            PColumn indexColumn = indexPKColumns.get(i);
            String indexColumnName = indexColumn.getName().getString();
            String cf = IndexUtil.getDataColumnFamilyName(indexColumnName);
            if (cf.length() != 0) {
                break;
            }
            if (i > nDataPKColumns) {
                break;
            }
            PColumn dataColumn = dataPKColumns.get(i-1);
            String dataColumnName = dataColumn.getName().getString();
            // Ensure both name and type are the same. Because of the restrictions we have
            // on PK column types (namely that you can only have a fixed width nullable
            // column as your last column), the type check is more of a sanity check
            // since it wouldn't make sense to have an index with every column in common.
            if (indexColumn.getDataType() == dataColumn.getDataType() 
                    && dataColumnName.equals(IndexUtil.getDataColumnName(indexColumnName))) {
                nColumnsInCommon++;
                continue;
            }
            break;
        }
        return nColumnsInCommon;
    }
     
    // public for testing
    public static ScanRanges computePrefixScanRanges(ScanRanges dataScanRanges, int nColumnsInCommon) {
        if (nColumnsInCommon == 0) {
            return ScanRanges.EVERYTHING;
        }
        
        int offset = 0;
        List<List<KeyRange>> cnf = Lists.newArrayListWithExpectedSize(nColumnsInCommon);
        int[] slotSpan = new int[nColumnsInCommon];
        boolean useSkipScan = false;
        boolean hasRange = false;
        List<List<KeyRange>> rangesList = dataScanRanges.getRanges();
        int rangesListSize = rangesList.size();
        while (offset < nColumnsInCommon && offset < rangesListSize) {
            List<KeyRange> ranges = rangesList.get(offset);
            // We use a skip scan if we have multiple ranges or if
            // we have a non single key range before the last range.
            useSkipScan |= ranges.size() > 1 || hasRange;
            cnf.add(ranges);
            int rangeSpan = 1 + dataScanRanges.getSlotSpans()[offset];
            if (offset + rangeSpan > nColumnsInCommon) {
                rangeSpan = nColumnsInCommon - offset;
                // trim range to only be rangeSpan in length
                ranges = Lists.newArrayListWithExpectedSize(cnf.get(cnf.size()-1).size());
                for (KeyRange range : cnf.get(cnf.size()-1)) {
                    range = clipRange(dataScanRanges.getSchema(), offset, rangeSpan, range);
                    // trim range to be only rangeSpan in length
                    ranges.add(range);
                }
                cnf.set(cnf.size()-1, ranges);
            }
            for (KeyRange range : ranges) {
                if (!range.isSingleKey()) {
                    hasRange = true;
                }
            }
            slotSpan[offset] = rangeSpan - 1;
            offset = offset + rangeSpan;
        }
        useSkipScan &= dataScanRanges.useSkipScanFilter();
        slotSpan = slotSpan.length == cnf.size() ? slotSpan : Arrays.copyOf(slotSpan, cnf.size());
        ScanRanges commonScanRanges = ScanRanges.create(dataScanRanges.getSchema(), cnf, slotSpan, null, useSkipScan, -1);
        return commonScanRanges;
    }
        
    /**
     * Truncates range to be a max of rangeSpan fields
     * @param schema row key schema
     * @param fieldIndex starting index of field with in the row key schema
     * @param rangeSpan maximum field length
     * @return the same range if unchanged and otherwise a new range
     */
    public static KeyRange clipRange(RowKeySchema schema, int fieldIndex, int rangeSpan, KeyRange range) {
        if (range == KeyRange.EVERYTHING_RANGE) {
            return range;
        }
        if (range == KeyRange.EMPTY_RANGE) {
            return range;
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean newRange = false;
        boolean lowerUnbound = range.lowerUnbound();
        boolean lowerInclusive = range.isLowerInclusive();
        byte[] lowerRange = range.getLowerRange();
        if (!lowerUnbound && lowerRange.length > 0) {
            if (clipKeyRangeBytes(schema, fieldIndex, rangeSpan, lowerRange, ptr, true)) {
                // Make lower range inclusive since we're decreasing the range by chopping the last part off
                lowerInclusive = true;
                lowerRange = ptr.copyBytes();
                newRange = true;
            }
        }
        boolean upperUnbound = range.upperUnbound();
        boolean upperInclusive = range.isUpperInclusive();
        byte[] upperRange = range.getUpperRange();
        if (!upperUnbound && upperRange.length > 0) {
            if (clipKeyRangeBytes(schema, fieldIndex, rangeSpan, upperRange, ptr, false)) {
                // Make lower range inclusive since we're decreasing the range by chopping the last part off
                upperInclusive = true;
                upperRange = ptr.copyBytes();
                newRange = true;
            }
        }
        
        return newRange ? KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive) : range;
    }

    private static boolean clipKeyRangeBytes(RowKeySchema schema, int fieldIndex, int rangeSpan, byte[] rowKey, ImmutableBytesWritable ptr, boolean trimTrailingNulls) {
        int position = 0;
        int maxOffset = schema.iterator(rowKey, ptr);
        byte[] newRowKey = new byte[rowKey.length];
        int offset = 0;
        int trailingNullsToTrim = 0;
        do {
            if (schema.next(ptr, fieldIndex, maxOffset) == null) {
                break;
            }
            System.arraycopy(ptr.get(), ptr.getOffset(), newRowKey, offset, ptr.getLength());
            offset += ptr.getLength();
            Field field =  schema.getField(fieldIndex);
            if (field.getDataType().isFixedWidth()) {
                trailingNullsToTrim = 0;
            } else {
                boolean isNull = ptr.getLength() == 0;
                byte sepByte = SchemaUtil.getSeparatorByte(true, isNull, field);
                newRowKey[offset++] = sepByte;
                if (isNull) {
                    if (trimTrailingNulls) {
                        trailingNullsToTrim++;
                    } else {
                        trailingNullsToTrim = 0;
                    }
                } else {
                    // So that last zero separator byte is always trimmed
                    trailingNullsToTrim = 1;
                }
            }
            fieldIndex++;
        } while (++position < rangeSpan);
        // remove trailing nulls
        ptr.set(newRowKey, 0, offset - trailingNullsToTrim);
        // return true if we've clipped the rowKey
        return maxOffset != offset;
    }
    
    /**
     * Compute the list of parallel scans to run for a given query. The inner scans
     * may be concatenated together directly, while the other ones may need to be
     * merge sorted, depending on the query.
     * Also computes an estimated bytes scanned, rows scanned, and last update time
     * of statistics. To compute correctly, we need to handle a couple of edge cases:
     * 1) if a guidepost is equal to the start key of the scan.
     * 2) If a guidepost is equal to the end region key.
     * In both cases, we set a flag (delayAddingEst) which indicates that the previous
     * gp should be use in our stats calculation. The normal case is that a gp is
     * encountered which is in the scan range in which case it is simply added to
     * our calculation.
     * For the last update time, we use the min timestamp of the gp that are in
     * range of the scans that will be issued. If we find no gp in the range, we use
     * the gp in the first or last region of the scan. If we encounter a region with
     * no gp, then we return a null value as an indication that we don't know with
     * certainty when the stats were updated last. This handles the case of a split
     * occurring for a large ingest with stats never having been calculated for the
     * new region.
     * @return list of parallel scans to run for a given query.
     * @throws SQLException
     */
    private List<List<Scan>> getParallelScans(byte[] startKey, byte[] stopKey) throws SQLException {
        List<HRegionLocation> regionLocations = getRegionBoundaries(scanGrouper);
        List<byte[]> regionBoundaries = toBoundaries(regionLocations);
        ScanRanges scanRanges = context.getScanRanges();
        PTable table = getTable();
        boolean isSalted = table.getBucketNum() != null;
        boolean isLocalIndex = table.getIndexType() == IndexType.LOCAL;
        GuidePostsInfo gps = getGuidePosts();
        // case when stats wasn't collected
        hasGuidePosts = gps != GuidePostsInfo.NO_GUIDEPOST;
        // Case when stats collection did run but there possibly wasn't enough data. In such a
        // case we generate an empty guide post with the byte estimate being set as guide post
        // width. 
        boolean emptyGuidePost = gps.isEmptyGuidePost();
        byte[] startRegionBoundaryKey = startKey;
        byte[] stopRegionBoundaryKey = stopKey;
        int columnsInCommon = 0;
        ScanRanges prefixScanRanges = ScanRanges.EVERYTHING;
        boolean traverseAllRegions = isSalted || isLocalIndex;
        if (isLocalIndex) {
            // TODO: when implementing PHOENIX-4585, we should change this to an assert
            // as we should always have a data plan when a local index is being used.
            if (dataPlan != null && dataPlan.getTableRef().getTable().getType() != PTableType.INDEX) { // Sanity check
                prefixScanRanges = computePrefixScanRanges(dataPlan.getContext().getScanRanges(), columnsInCommon=computeColumnsInCommon());
                KeyRange prefixRange = prefixScanRanges.getScanRange();
                if (!prefixRange.lowerUnbound()) {
                    startRegionBoundaryKey = prefixRange.getLowerRange();
                }
                if (!prefixRange.upperUnbound()) {
                    stopRegionBoundaryKey = prefixRange.getUpperRange();
                }
            }
        } else if (!traverseAllRegions) {
            byte[] scanStartRow = scan.getStartRow();
            if (scanStartRow.length != 0 && Bytes.compareTo(scanStartRow, startKey) > 0) {
                startRegionBoundaryKey = startKey = scanStartRow;
            }
            byte[] scanStopRow = scan.getStopRow();
            if (stopKey.length == 0
                    || (scanStopRow.length != 0 && Bytes.compareTo(scanStopRow, stopKey) < 0)) {
                stopRegionBoundaryKey = stopKey = scanStopRow;
            }
        }
        
        int regionIndex = 0;
        int startRegionIndex = 0;
        int stopIndex = regionBoundaries.size();
        if (startRegionBoundaryKey.length > 0) {
            startRegionIndex = regionIndex = getIndexContainingInclusive(regionBoundaries, startRegionBoundaryKey);
        }
        if (stopRegionBoundaryKey.length > 0) {
            stopIndex = Math.min(stopIndex, regionIndex + getIndexContainingExclusive(regionBoundaries.subList(regionIndex, stopIndex), stopRegionBoundaryKey));
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
        GuidePostEstimate estimates = new GuidePostEstimate();
        boolean gpsForFirstRegion = false;
        boolean intersectWithGuidePosts = true;
        // Maintain min ts for gps in first or last region outside of
        // gps that are in the scan range. We'll use this if we find
        // no gps in range.
        long fallbackTs = Long.MAX_VALUE;
        // Determination of whether of not we found a guidepost in
        // every region between the start and stop key. If not, then
        // we cannot definitively say at what time the guideposts
        // were collected.
        boolean gpsAvailableForAllRegions = true;
        try {
            boolean delayAddingEst = false;
            ImmutableBytesWritable firstRegionStartKey = null;
            if (gpsSize > 0) {
                stream = new ByteArrayInputStream(guidePosts.get(), guidePosts.getOffset(), guidePosts.getLength());
                input = new DataInputStream(stream);
                decoder = new PrefixByteDecoder(gps.getMaxLength());
                firstRegionStartKey = new ImmutableBytesWritable(regionLocations.get(regionIndex).getRegionInfo().getStartKey());
                try {
                    int c;
                    // Continue walking guideposts until we get past the currentKey
                    while ((c=currentKey.compareTo(currentGuidePost = PrefixByteCodec.decode(decoder, input))) >= 0) {
                        // Detect if we found a guidepost that might be in the first region. This
                        // is for the case where the start key may be past the only guidepost in
                        // the first region.
                        if (!gpsForFirstRegion && firstRegionStartKey.compareTo(currentGuidePost) <= 0) {
                            gpsForFirstRegion = true;
                        }
                        // While we have gps in the region (but outside of start/stop key), track
                        // the min ts as a fallback for the time at which stas were calculated.
                        if (gpsForFirstRegion) {
                            fallbackTs =
                                    Math.min(fallbackTs,
                                        gps.getGuidePostTimestamps()[guideIndex]);
                        }
                        // Special case for gp == startKey in which case we want to
                        // count this gp (if it's in range) though we go past it.
                        delayAddingEst = (c == 0);
                        guideIndex++;
                    }
                } catch (EOFException e) {
                    // expected. Thrown when we have decoded all guide posts.
                    intersectWithGuidePosts = false;
                }
            }
            byte[] endRegionKey = regionLocations.get(stopIndex).getRegionInfo().getEndKey();
            byte[] currentKeyBytes = currentKey.copyBytes();
            intersectWithGuidePosts &= guideIndex < gpsSize;
            // Merge bisect with guideposts for all but the last region
            while (regionIndex <= stopIndex) {
                HRegionLocation regionLocation = regionLocations.get(regionIndex);
                HRegionInfo regionInfo = regionLocation.getRegionInfo();
                byte[] currentGuidePostBytes = currentGuidePost.copyBytes();
                byte[] endKey;
                if (regionIndex == stopIndex) {
                    endKey = stopKey;
                } else {
                    endKey = regionBoundaries.get(regionIndex);
                }
                if (isLocalIndex) {
                    // Only attempt further pruning if the prefix range is using
                    // a skip scan since we've already pruned the range of regions
                    // based on the start/stop key.
                    if (columnsInCommon > 0 && prefixScanRanges.useSkipScanFilter()) {
                        byte[] regionStartKey = regionInfo.getStartKey();
                        ImmutableBytesWritable ptr = context.getTempPtr();
                        clipKeyRangeBytes(prefixScanRanges.getSchema(), 0, columnsInCommon, regionStartKey, ptr, false);
                        regionStartKey = ByteUtil.copyKeyBytesIfNecessary(ptr);
                        // Prune this region if there's no intersection
                        if (!prefixScanRanges.intersectRegion(regionStartKey, regionInfo.getEndKey(), false)) {
                            currentKeyBytes = endKey;
                            regionIndex++;
                            continue;
                        }
                    }
                    keyOffset = ScanUtil.getRowKeyOffset(regionInfo.getStartKey(), regionInfo.getEndKey());
                }
                byte[] initialKeyBytes = currentKeyBytes;
                int gpsComparedToEndKey = -1;
                boolean everNotDelayed = false;
                while (intersectWithGuidePosts && (endKey.length == 0 || (gpsComparedToEndKey=currentGuidePost.compareTo(endKey)) <= 0)) {
                    Scan newScan = scanRanges.intersectScan(scan, currentKeyBytes, currentGuidePostBytes, keyOffset,
                        false);
                    if (newScan != null) {
                        ScanUtil.setLocalIndexAttributes(newScan, keyOffset,
                            regionInfo.getStartKey(), regionInfo.getEndKey(),
                            newScan.getStartRow(), newScan.getStopRow());
                        // If we've delaying adding estimates, add the previous
                        // gp estimates now that we know they are in range.
                        if (delayAddingEst) {
                            updateEstimates(gps, guideIndex-1, estimates);
                        }
                        // If we're not delaying adding estimates, add the
                        // current gp estimates.
                        if (! (delayAddingEst = gpsComparedToEndKey == 0) ) {
                            updateEstimates(gps, guideIndex, estimates);
                        }
                    } else {
                        delayAddingEst = false;
                    }
                    everNotDelayed |= !delayAddingEst;
                    scans = addNewScan(parallelScans, scans, newScan, currentGuidePostBytes, false, regionLocation);
                    currentKeyBytes = currentGuidePostBytes;
                    try {
                        currentGuidePost = PrefixByteCodec.decode(decoder, input);
                        currentGuidePostBytes = currentGuidePost.copyBytes();
                        guideIndex++;
                    } catch (EOFException e) {
                        // We have read all guide posts
                        intersectWithGuidePosts = false;
                    }
                }
                boolean gpsInThisRegion = initialKeyBytes != currentKeyBytes;
                if (!useStatsForParallelization) {
                    /*
                     * If we are not using stats for generating parallel scans, we need to reset the
                     * currentKey back to what it was at the beginning of the loop.
                     */
                    currentKeyBytes = initialKeyBytes;
                }
                Scan newScan = scanRanges.intersectScan(scan, currentKeyBytes, endKey, keyOffset, true);
                if(newScan != null) {
                    ScanUtil.setLocalIndexAttributes(newScan, keyOffset, regionInfo.getStartKey(),
                        regionInfo.getEndKey(), newScan.getStartRow(), newScan.getStopRow());
                    // Boundary case of no GP in region after delaying adding of estimates
                    if (!gpsInThisRegion && delayAddingEst) {
                        updateEstimates(gps, guideIndex-1, estimates);
                        gpsInThisRegion = true;
                        delayAddingEst = false;
                    }
                } else if (!gpsInThisRegion) {
                    delayAddingEst = false;
                }
                scans = addNewScan(parallelScans, scans, newScan, endKey, true, regionLocation);
                currentKeyBytes = endKey;
                // We have a guide post in the region if the above loop was entered
                // or if the current key is less than the region end key (since the loop
                // may not have been entered if our scan end key is smaller than the
                // first guide post in that region).
                boolean gpsAfterStopKey = false;
                gpsAvailableForAllRegions &= 
                    ( gpsInThisRegion && everNotDelayed) || // GP in this region
                    ( regionIndex == startRegionIndex && gpsForFirstRegion ) || // GP in first region (before start key)
                    ( gpsAfterStopKey = ( regionIndex == stopIndex && intersectWithGuidePosts && // GP in last region (after stop key)
                            ( endRegionKey.length == 0 || // then check if gp is in the region
                            currentGuidePost.compareTo(endRegionKey) < 0)  ) );            
                if (gpsAfterStopKey) {
                    // If gp after stop key, but still in last region, track min ts as fallback 
                    fallbackTs =
                            Math.min(fallbackTs,
                                gps.getGuidePostTimestamps()[guideIndex]);
                }
                regionIndex++;
            }
            if (!scans.isEmpty()) { // Add any remaining scans
                parallelScans.add(scans);
            }
            Long pageLimit = getUnfilteredPageLimit(scan);
            if (scanRanges.isPointLookup() || pageLimit != null) {
                // If run in parallel, the limit is pushed to each parallel scan so must be accounted for in all of them
                int parallelFactor = this.isSerial() ? 1 : parallelScans.size();
                if (scanRanges.isPointLookup() && pageLimit != null) {
                    this.estimatedRows = Long.valueOf(Math.min(scanRanges.getPointLookupCount(), pageLimit * parallelFactor));
                } else if (scanRanges.isPointLookup()) {
                    this.estimatedRows = Long.valueOf(scanRanges.getPointLookupCount());
                } else {
                    this.estimatedRows = Long.valueOf(pageLimit) * parallelFactor;
                }
                this.estimatedSize = this.estimatedRows * SchemaUtil.estimateRowSize(table);
                 // Indication to client that the statistics estimates were not
                 // calculated based on statistics but instead are based on row
                 // limits from the query.
               this.estimateInfoTimestamp = StatisticsUtil.NOT_STATS_BASED_TS;
            } else if (emptyGuidePost) {
                // In case of an empty guide post, we estimate the number of rows scanned by
                // using the estimated row size
                this.estimatedRows = (gps.getByteCounts()[0] / SchemaUtil.estimateRowSize(table));
                this.estimatedSize = gps.getByteCounts()[0];
                this.estimateInfoTimestamp = gps.getGuidePostTimestamps()[0];
            } else if (hasGuidePosts) {
                this.estimatedRows = estimates.rowsEstimate;
                this.estimatedSize = estimates.bytesEstimate;
                this.estimateInfoTimestamp = computeMinTimestamp(gpsAvailableForAllRegions, estimates, fallbackTs);
            } else {
                this.estimatedRows = null;
                this.estimatedSize = null;
                this.estimateInfoTimestamp = null;
            }
        } finally {
            if (stream != null) Closeables.closeQuietly(stream);
        }
        sampleScans(parallelScans,this.plan.getStatement().getTableSamplingRate());
        return parallelScans;
    }

    /**
     * Return row count limit of PageFilter if exists and there is no where
     * clause filter.
     * @return
     */
    private static Long getUnfilteredPageLimit(Scan scan) {
        Long pageLimit = null;
        Iterator<Filter> filters = ScanUtil.getFilterIterator(scan);
        while (filters.hasNext()) {
            Filter filter = filters.next();
            if (filter instanceof BooleanExpressionFilter) {
                return null;
            }
            if (filter instanceof PageFilter) {
                pageLimit = ((PageFilter)filter).getPageSize();
            }
        }
        return pageLimit;
    }

    private static Long computeMinTimestamp(boolean gpsAvailableForAllRegions, 
            GuidePostEstimate estimates,
            long fallbackTs) {
        if (gpsAvailableForAllRegions) {
            if (estimates.lastUpdated < Long.MAX_VALUE) {
                return estimates.lastUpdated;
            }
            if (fallbackTs < Long.MAX_VALUE) {
                return fallbackTs;
            }
        }
        return null;
    }

    /**
     * Loop through List<List<Scan>> parallelScans object, 
     * rolling dice on each scan based on startRowKey.
     * 
     * All FilterableStatement should have tableSamplingRate. 
     * In case it is delete statement, an unsupported message is raised. 
     * In case it is null tableSamplingRate, 100% sampling rate will be applied by default.
     *  
     * @param parallelScans
     */
    private void sampleScans(final List<List<Scan>> parallelScans, final Double tableSamplingRate){
    	if(tableSamplingRate==null||tableSamplingRate==100d) return;
    	final Predicate<byte[]> tableSamplerPredicate=TableSamplerPredicate.of(tableSamplingRate);
    	
    	for(Iterator<List<Scan>> is = parallelScans.iterator();is.hasNext();){
    		for(Iterator<Scan> i=is.next().iterator();i.hasNext();){
    			final Scan scan=i.next();
    			if(!tableSamplerPredicate.apply(scan.getStartRow())){i.remove();}
    		}
    	}
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
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("Getting iterators for " + this,
                    ScanUtil.getCustomAnnotations(scan)));
        }
        boolean isReverse = ScanUtil.isReversed(scan);
        boolean isLocalIndex = getTable().getIndexType() == IndexType.LOCAL;
        final ConnectionQueryServices services = context.getConnection().getQueryServices();
        // Get query time out from Statement
        final long startTime = EnvironmentEdgeManager.currentTimeMillis();
        final long maxQueryEndTime = startTime + context.getStatement().getQueryTimeoutInMillis();
        int numScans = size();
        // Capture all iterators so that if something goes wrong, we close them all
        // The iterators list is based on the submission of work, so it may not
        // contain them all (for example if work was rejected from the queue)
        Queue<PeekingResultIterator> allIterators = new ConcurrentLinkedQueue<>();
        List<PeekingResultIterator> iterators = new ArrayList<PeekingResultIterator>(numScans);
        ScanWrapper previousScan = new ScanWrapper(null);
        return getIterators(scans, services, isLocalIndex, allIterators, iterators, isReverse, maxQueryEndTime,
                splits.size(), previousScan, context.getConnection().getQueryServices().getConfiguration()
                        .getInt(QueryConstants.HASH_JOIN_CACHE_RETRIES, QueryConstants.DEFAULT_HASH_JOIN_CACHE_RETRIES));
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
            boolean isReverse, long maxQueryEndTime, int splitSize, ScanWrapper previousScan, int retryCount) throws SQLException {
        boolean success = false;
        final List<List<Pair<Scan,Future<PeekingResultIterator>>>> futures = Lists.newArrayListWithExpectedSize(splitSize);
        allFutures.add(futures);
        SQLException toThrow = null;
        final HashCacheClient hashCacheClient = new HashCacheClient(context.getConnection());
        int queryTimeOut = context.getStatement().getQueryTimeoutInMillis();
        try {
            submitWork(scan, futures, allIterators, splitSize, isReverse, scanGrouper);
            boolean clearedCache = false;
            for (List<Pair<Scan,Future<PeekingResultIterator>>> future : reverseIfNecessary(futures,isReverse)) {
                List<PeekingResultIterator> concatIterators = Lists.newArrayListWithExpectedSize(future.size());
                Iterator<Pair<Scan, Future<PeekingResultIterator>>> scanPairItr = reverseIfNecessary(future,isReverse).iterator();
                while (scanPairItr.hasNext()) {
                    Pair<Scan,Future<PeekingResultIterator>> scanPair = scanPairItr.next();
                    try {
                        long timeOutForScan = maxQueryEndTime - EnvironmentEdgeManager.currentTimeMillis();
                        if (timeOutForScan < 0) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.OPERATION_TIMED_OUT).setMessage(". Query couldn't be completed in the alloted time: " + queryTimeOut + " ms").build().buildException(); 
                        }
                        if (isLocalIndex && previousScan != null && previousScan.getScan() != null
                                && (((!isReverse && Bytes.compareTo(scanPair.getFirst().getAttribute(SCAN_ACTUAL_START_ROW),
                                        previousScan.getScan().getStopRow()) < 0)
                                || (isReverse && Bytes.compareTo(scanPair.getFirst().getAttribute(SCAN_ACTUAL_START_ROW),
                                        previousScan.getScan().getStopRow()) > 0)
                                || (Bytes.compareTo(scanPair.getFirst().getStopRow(), previousScan.getScan().getStopRow()) == 0)) 
                                    && Bytes.compareTo(scanPair.getFirst().getAttribute(SCAN_START_ROW_SUFFIX), previousScan.getScan().getAttribute(SCAN_START_ROW_SUFFIX))==0)) {
                            continue;
                        }
                        PeekingResultIterator iterator = scanPair.getSecond().get(timeOutForScan, TimeUnit.MILLISECONDS);
                        concatIterators.add(iterator);
                        previousScan.setScan(scanPair.getFirst());
                    } catch (ExecutionException e) {
                        try { // Rethrow as SQLException
                            throw ServerUtil.parseServerException(e);
                        } catch (StaleRegionBoundaryCacheException | HashJoinCacheNotFoundException e2){
                            // Catch only to try to recover from region boundary cache being out of date
                            if (!clearedCache) { // Clear cache once so that we rejigger job based on new boundaries
                                services.clearTableRegionCache(physicalTableName);
                                context.getOverallQueryMetrics().cacheRefreshedDueToSplits();
                            }
                            // Resubmit just this portion of work again
                            Scan oldScan = scanPair.getFirst();
                            byte[] startKey = oldScan.getAttribute(SCAN_ACTUAL_START_ROW);
                            if(e2 instanceof HashJoinCacheNotFoundException){
                                logger.debug(
                                        "Retrying when Hash Join cache is not found on the server ,by sending the cache again");
                                if(retryCount<=0){
                                    throw e2;
                                }
                                Long cacheId = ((HashJoinCacheNotFoundException)e2).getCacheId();
                                if (!hashCacheClient.addHashCacheToServer(startKey,
                                        caches.get(new ImmutableBytesPtr(Bytes.toBytes(cacheId))), plan.getTableRef().getTable())) { throw e2; }
                            }
                            concatIterators =
                                    recreateIterators(services, isLocalIndex, allIterators,
                                        iterators, isReverse, maxQueryEndTime, previousScan,
                                        clearedCache, concatIterators, scanPairItr, scanPair, retryCount-1);
                        } catch(ColumnFamilyNotFoundException cfnfe) {
                            if (scanPair.getFirst().getAttribute(LOCAL_INDEX_BUILD) != null) {
                                Thread.sleep(1000);
                                concatIterators =
                                        recreateIterators(services, isLocalIndex, allIterators,
                                            iterators, isReverse, maxQueryEndTime, previousScan,
                                            clearedCache, concatIterators, scanPairItr, scanPair, retryCount);
                            }
                            
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

    private List<PeekingResultIterator> recreateIterators(ConnectionQueryServices services,
            boolean isLocalIndex, Queue<PeekingResultIterator> allIterators,
            List<PeekingResultIterator> iterators, boolean isReverse, long maxQueryEndTime,
            ScanWrapper previousScan, boolean clearedCache,
            List<PeekingResultIterator> concatIterators,
            Iterator<Pair<Scan, Future<PeekingResultIterator>>> scanPairItr,
            Pair<Scan, Future<PeekingResultIterator>> scanPair, int retryCount) throws SQLException {
        scanPairItr.remove();
        // Resubmit just this portion of work again
        Scan oldScan = scanPair.getFirst();
        byte[] startKey = oldScan.getAttribute(SCAN_ACTUAL_START_ROW);
        byte[] endKey = oldScan.getStopRow();

        List<List<Scan>> newNestedScans = this.getParallelScans(startKey, endKey);
        // Add any concatIterators that were successful so far
        // as we need these to be in order
        addIterator(iterators, concatIterators);
        concatIterators = Lists.newArrayList();
        getIterators(newNestedScans, services, isLocalIndex, allIterators, iterators, isReverse,
                maxQueryEndTime, newNestedScans.size(), previousScan, retryCount);
        return concatIterators;
    }
    

    @Override
    public void close() throws SQLException {
       
        // Don't call cancel on already started work, as it causes the HConnection
        // to get into a funk. Instead, just cancel queued work.
        boolean cancelledWork = false;
        try {
            if (allFutures.isEmpty()) {
                return;
            }
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
            SQLCloseables.closeAllQuietly(caches.values());
            caches.clear();
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
    	private final boolean isFirstScan;
    	private final boolean isLastScan;
    	
    	public ScanLocator(Scan scan, int outerListIndex, int innerListIndex, boolean isFirstScan, boolean isLastScan) {
    		this.outerListIndex = outerListIndex;
    		this.innerListIndex = innerListIndex;
    		this.scan = scan;
    		this.isFirstScan = isFirstScan;
    		this.isLastScan = isLastScan;
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
    	public boolean isFirstScan()  {
    	    return isFirstScan;
    	}
    	public boolean isLastScan() {
    	    return isLastScan;
    	}
    }
    

    abstract protected String getName();    
    abstract protected void submitWork(List<List<Scan>> nestedScans, List<List<Pair<Scan,Future<PeekingResultIterator>>>> nestedFutures,
            Queue<PeekingResultIterator> allIterators, int estFlattenedSize, boolean isReverse, ParallelScanGrouper scanGrouper) throws SQLException;
    
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
            if (displayRowCount && estimatedRows != null) {
                buf.append(estimatedRows).append(" ROWS ");
                buf.append(estimatedSize).append(" BYTES ");
            }
        }
        buf.append(getName()).append(" ").append(size()).append("-WAY ");
        
        if(this.plan.getStatement().getTableSamplingRate()!=null){
        	buf.append(plan.getStatement().getTableSamplingRate()/100D).append("-").append("SAMPLED ");
        }
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

    public Long getEstimateInfoTimestamp() {
        return this.estimateInfoTimestamp;
    }

}
