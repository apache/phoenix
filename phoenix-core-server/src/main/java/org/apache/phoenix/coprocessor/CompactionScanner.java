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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.filter.RowKeyComparisonFilter.RowKeyTuple;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.coprocessorclient.RowKeyMatcher;
import org.apache.phoenix.coprocessorclient.TableTTLInfoCache;
import org.apache.phoenix.coprocessorclient.TableTTLInfo;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.coprocessor.CompactionScanner.MatcherType.GLOBAL_INDEXES;
import static org.apache.phoenix.coprocessor.CompactionScanner.MatcherType.GLOBAL_VIEWS;
import static org.apache.phoenix.coprocessor.CompactionScanner.MatcherType.TENANT_INDEXES;
import static org.apache.phoenix.coprocessor.CompactionScanner.MatcherType.TENANT_VIEWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAMESPACE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_NOT_DEFINED;
import static org.apache.phoenix.query.QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX;
import static org.apache.phoenix.query.QueryServices.PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;

/**
 * The store scanner that implements compaction for Phoenix. Phoenix coproc overrides the scan
 * options so that HBase store scanner retains all cells during compaction and flushes. Then this
 * store scanner decides which cells to retain. This is required to ensure rows do not expire
 * partially and to preserve all cells within Phoenix max lookback window.
 *
 * The compaction process is optimized for Phoenix. This optimization assumes that
 * . A given delete family or delete family version marker is inserted to all column families
 * . A given delete family version marker always delete a full version of a row. Please note
 *   delete family version markers are used only on index tables where mutations are always
 *   full row mutations.
 *
 *  During major compaction, minor compaction and memstore flush, all cells (and delete markers)
 *  that are visible through the max lookback window are retained. Outside the max lookback window,
 *  (1) extra put cell versions, (2) delete markers and deleted cells that are not supposed to be
 *  kept (by the KeepDeletedCell option), and (3) expired cells are removed during major compaction.
 *  During flushes and minor compaction, expired cells and delete markers are not removed however
 *  deleted cells that are not supposed to be kept (by the KeepDeletedCell option) and extra put
 *  cell versions are removed.
 *
 */
public class CompactionScanner implements InternalScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionScanner.class);
    public static final String SEPARATOR = ":";
    private final InternalScanner storeScanner;
    private final Region region;
    private final Store store;
    private final RegionCoprocessorEnvironment env;
    private long maxLookbackWindowStart;
    private final long maxLookbackInMillis;
    private int minVersion;
    private int maxVersion;
    private final boolean emptyCFStore;
    private final boolean localIndex;
    private final int familyCount;
    private KeepDeletedCells keepDeletedCells;
    private long compactionTime;
    private final byte[] emptyCF;
    private final byte[] emptyCQ;
    private final byte[] storeColumnFamily;
    private final String tableName;
    private final String columnFamilyName;
    private static Map<String, Long> maxLookbackMap = new ConcurrentHashMap<>();
    private PhoenixLevelRowCompactor phoenixLevelRowCompactor;
    private HBaseLevelRowCompactor hBaseLevelRowCompactor;
    private boolean major;
    private long inputCellCount = 0;
    private long outputCellCount = 0;
    private boolean phoenixLevelOnly = false;
    private boolean isCDCIndex;

    // Only for forcing minor compaction while testing
    private static boolean forceMinorCompaction = false;

    public CompactionScanner(RegionCoprocessorEnvironment env,
            Store store,
            InternalScanner storeScanner,
            long maxLookbackAgeInMillis,
            boolean major,
            boolean keepDeleted,
            PTable table) throws IOException {
        this.storeScanner = storeScanner;
        this.region = env.getRegion();
        this.store = store;
        this.env = env;
        // Empty column family and qualifier are always needed to compute which all empty cells to retain
        // even during minor compactions. If required empty cells are not retained during
        // minor compactions then we can run into the risk of partial row expiry on next major compaction.
        this.emptyCF = SchemaUtil.getEmptyColumnFamily(table);
        this.emptyCQ = SchemaUtil.getEmptyColumnQualifier(table);
        compactionTime = EnvironmentEdgeManager.currentTimeMillis();
        columnFamilyName = store.getColumnFamilyName();
        storeColumnFamily = columnFamilyName.getBytes();
        tableName = region.getRegionInfo().getTable().getNameAsString();
        String dataTableName = table.getName().toString();
        Long overriddenMaxLookback = maxLookbackMap.get(tableName + SEPARATOR + columnFamilyName);
        this.maxLookbackInMillis = overriddenMaxLookback == null ?
                maxLookbackAgeInMillis : Math.max(maxLookbackAgeInMillis, overriddenMaxLookback);
        // The oldest scn is current time - maxLookbackInMillis. Phoenix sets the scan time range
        // for scn queries [0, scn). This means that the maxlookback size should be
        // maxLookbackInMillis + 1 so that the oldest scn does not return empty row
        this.maxLookbackWindowStart = this.maxLookbackInMillis == 0 ? compactionTime : compactionTime - (this.maxLookbackInMillis + 1);
        ColumnFamilyDescriptor cfd = store.getColumnFamilyDescriptor();
        this.major = major && ! forceMinorCompaction;
        this.minVersion = cfd.getMinVersions();
        this.maxVersion = cfd.getMaxVersions();
        this.keepDeletedCells = keepDeleted ? KeepDeletedCells.TTL : cfd.getKeepDeletedCells();
        familyCount = region.getTableDescriptor().getColumnFamilies().length;
        localIndex = columnFamilyName.startsWith(LOCAL_INDEX_COLUMN_FAMILY_PREFIX);
        emptyCFStore = familyCount == 1 || columnFamilyName.equals(Bytes.toString(emptyCF))
                        || localIndex;

        isCDCIndex = table != null ? CDCUtil.isCDCIndex(table) : false;
        // Initialize the tracker that computes the TTL for the compacting table.
        // The TTL tracker can be
        // simple (one single TTL for the table) when the compacting table is not Partitioned
        // complex when the TTL can vary per row when the compacting table is Partitioned.
        TTLTracker
                ttlTracker =
                this.major ?
                        createTTLTrackerFor(env, store, table):
                        new TableTTLTrackerForFlushesAndMinor(tableName);

        phoenixLevelRowCompactor = new PhoenixLevelRowCompactor(ttlTracker);
        hBaseLevelRowCompactor = new HBaseLevelRowCompactor(ttlTracker);

        LOGGER.info("Starting CompactionScanner for table " + tableName + " store "
                + columnFamilyName + (this.major ? " major " : " not major ") + "compaction ttl "
                + ttlTracker.getRowContext().getTTL() + "ms " + "max lookback " + this.maxLookbackInMillis + "ms");
        LOGGER.info(String.format("CompactionScanner params:- (" +
                        "physical-data-tablename = %s, compaction-tablename = %s, region = %s, " +
                        "start-key = %s, end-key = %s, " +
                        "emptyCF = %s, emptyCQ = %s, " +
                        "minVersion = %d, maxVersion = %d, keepDeletedCells = %s, " +
                        "familyCount = %d, localIndex = %s, emptyCFStore = %s, " +
                        "compactionTime = %d, maxLookbackWindowStart = %d, maxLookbackInMillis = %d, major = %s)",
                dataTableName, tableName, region.getRegionInfo().getEncodedName(),
                Bytes.toStringBinary(region.getRegionInfo().getStartKey()),
                Bytes.toStringBinary(region.getRegionInfo().getEndKey()),
                Bytes.toString(this.emptyCF), Bytes.toString(emptyCQ),
                this.minVersion, this.maxVersion, this.keepDeletedCells.name(),
                this.familyCount, this.localIndex, this.emptyCFStore,
                compactionTime, maxLookbackWindowStart, maxLookbackInMillis, this.major));

    }

    @VisibleForTesting
    public static void setForceMinorCompaction(boolean doMinorCompaction) {
        forceMinorCompaction = doMinorCompaction;
    }

    @VisibleForTesting
    public static boolean getForceMinorCompaction() {
        return forceMinorCompaction;
    }

    /**
     * Helper method to create TTL tracker for various phoenix data model objects
     * i.e views, view indexes ...
     * @param env
     * @param store
     * @param baseTable
     * @return
     */
    private TTLTracker createTTLTrackerFor(RegionCoprocessorEnvironment env,
            Store store,  PTable baseTable) throws IOException {

        boolean isViewTTLEnabled =
                env.getConfiguration().getBoolean(QueryServices.PHOENIX_VIEW_TTL_ENABLED,
                        QueryServicesOptions.DEFAULT_PHOENIX_VIEW_TTL_ENABLED);
        boolean isLongViewIndexEnabled =
                env.getConfiguration().getBoolean(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB,
                        QueryServicesOptions.DEFAULT_LONG_VIEW_INDEX_ENABLED);

        int viewTTLTenantViewsPerScanLimit = -1;
        if (isViewTTLEnabled) {
            // if view ttl enabled then we need to limit the number of rows scanned
            // when querying syscat for views with TTL enabled/set
            viewTTLTenantViewsPerScanLimit = env.getConfiguration().getInt(
                    PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT,
                    DEFAULT_PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT);
        }
        // If VIEW TTL is not enabled then return TTL tracker for base HBase tables.
        // since TTL can be set only at the table level.
        if (!isViewTTLEnabled) {
            return new NonPartitionedTableTTLTracker(baseTable, store);
        }

        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        String compactionTableName = env.getRegion().getRegionInfo().getTable().getNameAsString();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(baseTable.getName().toString());
        String tableName = SchemaUtil.getTableNameFromFullName(baseTable.getName().toString());

        boolean isSharedIndex = false;
        if (compactionTableName.startsWith(MetaDataUtil.VIEW_INDEX_TABLE_PREFIX)) {
            isSharedIndex = true;
        }

        // NonPartitioned: Salt bucket property can be separately set for base tables and indexes.
        // Partitioned: Salt bucket property can be set only for the base table.
        // Global views, Tenant views, view indexes inherit the salt bucket property from their
        // base table.
        boolean isSalted = baseTable.getBucketNum() != null;
        try (PhoenixConnection serverConnection = QueryUtil.getConnectionOnServer(new Properties(),
                env.getConfiguration()).unwrap(PhoenixConnection.class)) {

            byte[] childLinkTableNameBytes = SchemaUtil.isNamespaceMappingEnabled(
                    PTableType.SYSTEM, env.getConfiguration()) ?
                    SYSTEM_CHILD_LINK_NAMESPACE_BYTES :
                    SYSTEM_CHILD_LINK_NAME_BYTES;
            Table childLinkHTable = serverConnection.getQueryServices().getTable(childLinkTableNameBytes);
            // If there is at least one child view for this table then it is a partitioned table.
            boolean isPartitioned = ViewUtil.hasChildViews(
                    childLinkHTable,
                    EMPTY_BYTE_ARRAY,
                    Bytes.toBytes(schemaName),
                    Bytes.toBytes(tableName),
                    currentTime);

            return isPartitioned ?
                    new PartitionedTableTTLTracker(baseTable, isSalted, isSharedIndex,
                            isLongViewIndexEnabled, viewTTLTenantViewsPerScanLimit) :
                    new NonPartitionedTableTTLTracker(baseTable, store);

        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    /**
     * Any coprocessors within a JVM can extend the max lookback window for a column family
     * by calling this static method.
     */
    public static void overrideMaxLookback(String tableName, String columnFamilyName,
            long maxLookbackInMillis) {
        if (tableName == null || columnFamilyName == null) {
            return;
        }
        Long old = maxLookbackMap.putIfAbsent(tableName + SEPARATOR + columnFamilyName,
                maxLookbackInMillis);
        if (old != null) {
            maxLookbackMap.put(tableName + SEPARATOR + columnFamilyName, maxLookbackInMillis);
        }
    }

    public static long getMaxLookbackInMillis(String tableName, String columnFamilyName,
            long maxLookbackInMillis) {
        if (tableName == null || columnFamilyName == null) {
            return maxLookbackInMillis;
        }
        Long value = maxLookbackMap.get(tableName + CompactionScanner.SEPARATOR + columnFamilyName);
        return value == null
                ? maxLookbackInMillis
                : maxLookbackMap.get(tableName + CompactionScanner.SEPARATOR + columnFamilyName);
    }
    static class CellTimeComparator implements Comparator<Cell> {
        public static final CellTimeComparator COMPARATOR = new CellTimeComparator();
        @Override public int compare(Cell o1, Cell o2) {
            long ts1 = o1.getTimestamp();
            long ts2 = o2.getTimestamp();
            if (ts1 == ts2) return 0;
            if (ts1 > ts2) return -1;
            return 1;
        }

        @Override public boolean equals(Object obj) {
            return false;
        }
    }
    /*
    private void printRow(List<Cell> result, String title, boolean sort) {
        List<Cell> row;
        if (sort) {
            row = new ArrayList<>(result);
            Collections.sort(row, CellTimeComparator.COMPARATOR);
        } else {
            row = result;
        }
        System.out.println("---- " + title + " ----");
        System.out.println((major ? "Major " : "Not major ")
                + "compaction time: " + compactionTime);
        System.out.println("Max lookback window start time: " + maxLookbackWindowStart);
        System.out.println("Max lookback in ms: " + maxLookbackInMillis);
        System.out.println("TTL in ms: " + ttlInMillis);
        boolean maxLookbackLine = false;
        boolean ttlLine = false;
        for (Cell cell : row) {
            if (!maxLookbackLine && cell.getTimestamp() < maxLookbackWindowStart) {
                System.out.println("-----> Max lookback window start time: " + maxLookbackWindowStart);
                maxLookbackLine = true;
            } else if (!ttlLine && cell.getTimestamp() < ttlWindowStart) {
                System.out.println("-----> TTL window start time: " + ttlWindowStart);
                ttlLine = true;
            }
            System.out.println(cell);
        }
    }
     */

    @Override
    public boolean next(List<Cell> result) throws IOException {
        boolean hasMore = storeScanner.next(result);
        inputCellCount += result.size();
        if (!result.isEmpty()) {
            // printRow(result, "Input for " + tableName + " " + columnFamilyName, true); // This is for debugging
            phoenixLevelRowCompactor.compact(result, false);
            outputCellCount += result.size();
            // printRow(result, "Output for " + tableName + " " + columnFamilyName, true); // This is for debugging
        }
        return hasMore;
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing CompactionScanner for table " + tableName + " store "
                + columnFamilyName + (major ? " major " : " not major ") + "compaction retained "
                + outputCellCount + " of " + inputCellCount + " cells"
                + (phoenixLevelOnly ? " phoenix level only" : ""));
        if (forceMinorCompaction) {
            forceMinorCompaction = false;
        }
        storeScanner.close();
    }

    enum MatcherType {
        GLOBAL_VIEWS, GLOBAL_INDEXES, TENANT_VIEWS, TENANT_INDEXES
    }

    /**
     * Helper class for managing various RowKeyMatchers and TTLCaches
     * For matcher types =>
     * GLOBAL_VIEWS:
     * GLOBAL_INDEXES:
     *      Assumption is that the number of Global views in a system is bounded.(can fit in memory)
     *      Reads the ROW_KEY_MATCHER and TTL attribute from SYSCAT for all global views.
     *      SYSCAT query is not batched.
     *
     * TENANT_VIEWS:
     *      Reads the ROW_KEY_MATCHER and TTL attribute from SYSCAT in a batched fashion.
     *      TENANT_ID from the region startKey is used to further filter the SYSCAT query.
     *      The batch size is controlled by viewTTLTenantViewsPerScanLimit attribute.
     *      Special case: when the data type of the TENANT_ID is not a VARCHAR/CHAR,
     *      then SYSCAT queries cannot be bounded or batched using the TENANT_ID,
     *      since the TENANT_ID attribute in SYSCAT is a VARCHAR.
     *
     * TENANT_INDEXES:
     *      Reads the ROW_KEY_MATCHER and TTL attribute from SYSCAT in a batched fashion.
     *      Since TENANT_ID is not the leading part of the row key and thus not in a
     *      lexicographic order for a given region,
     *      the TENANT_ID cannot be used to query SYSCAT in a batch of more than one.
     *      The batch size is configured to one => uses the TENANT_ID of the current row.
     *
     */
    /// TODO : Needs to convert debug to trace logging.
    private class PartitionedTableRowKeyMatcher  {

        private static final int NO_BATCH = -1;

        private boolean isSharedIndex = false;
        private boolean isMultiTenant = false;
        private boolean isSalted = false;
        private boolean shouldBatchCatalogAccess = true;
        private RowKeyParser rowKeyParser;
        private PTable baseTable;
        private RowKeyMatcher globalViewMatcher;
        private RowKeyMatcher tenantViewMatcher;
        private RowKeyMatcher globalIndexMatcher;
        private RowKeyMatcher tenantIndexMatcher;
        private TableTTLInfoCache globalViewTTLCache;
        private TableTTLInfoCache tenantViewTTLCache;
        private TableTTLInfoCache globalIndexTTLCache;
        private TableTTLInfoCache tenantIndexTTLCache;
        private String startTenantId = "";
        private String endTenantId = "";
        private String lastTenantId = "";
        private String currentTenantId = "";
        private int viewTTLTenantViewsPerScanLimit;

        public PartitionedTableRowKeyMatcher(
                PTable table,
                boolean isSalted,
                boolean isSharedIndex,
                boolean isLongViewIndexEnabled,
                int viewTTLTenantViewsPerScanLimit) throws SQLException {
            this.baseTable = table;
            this.globalViewTTLCache = new TableTTLInfoCache();
            this.globalIndexTTLCache = new TableTTLInfoCache();
            this.tenantViewTTLCache = new TableTTLInfoCache();
            this.tenantIndexTTLCache = new TableTTLInfoCache();
            this.rowKeyParser = new RowKeyParser(baseTable, isLongViewIndexEnabled);
            PDataType tenantIdType = this.rowKeyParser.getTenantIdDataType();
            this.shouldBatchCatalogAccess = (tenantIdType.getSqlType() == Types.VARCHAR ||
                    tenantIdType.getSqlType() == Types.CHAR);
            this.isSharedIndex = isSharedIndex || localIndex ;
            this.isSalted = isSalted;
            this.isMultiTenant = table.isMultiTenant();
            this.viewTTLTenantViewsPerScanLimit = viewTTLTenantViewsPerScanLimit;
            initializeMatchers();
        }


        /**
         * Initialize the various matchers
         * Case : multi-tenant
         * @throws SQLException
         */

        private void initializeMatchers() throws SQLException {

            if (this.isSharedIndex) {
                this.globalIndexMatcher = initializeMatcher(GLOBAL_INDEXES);
                // Matcher for TENANT_INDEXES will be created/refreshed when processing the rows.
            } else if (this.isMultiTenant) {
                this.globalViewMatcher = initializeMatcher(GLOBAL_VIEWS);
                this.tenantViewMatcher = initializeMatcher(TENANT_VIEWS);
            } else {
                this.globalViewMatcher = initializeMatcher(GLOBAL_VIEWS);
            }
        }

        // Queries SYSCAT to find the ROW_KEY_MATCHER and TTL attributes for various matcher types.
        // The attributes are populated/initialized into local cache objects.
        // TTL => TableTTLInfoCache
        // ROW_KEY_MATCHER => RowKeyMatcher (TrieIndex)
        private RowKeyMatcher initializeMatcher(MatcherType type) throws SQLException {
            List<TableTTLInfo> tableList = null;
            RowKeyMatcher matcher  = new RowKeyMatcher();
            String regionName = region.getRegionInfo().getEncodedName();

            switch (type) {
            case GLOBAL_INDEXES:
                tableList = getMatchPatternsForGlobalPartitions(
                        this.baseTable.getName().getString(),
                        env.getConfiguration(),
                        false, true);
                break;
            case TENANT_INDEXES:
                try {
                    startTenantId = rowKeyParser.getTenantIdFromRowKey(
                            region.getRegionInfo().getStartKey());
                    endTenantId = rowKeyParser.getTenantIdFromRowKey(
                            region.getRegionInfo().getEndKey()
                    );
                } catch (SQLException sqle) {
                    LOGGER.error(sqle.getMessage());
                    throw sqle;
                }
                if (startTenantId != null && !startTenantId.isEmpty()) {
                    tableList = getMatchPatternsForTenant(
                            this.baseTable.getName().getString(),
                            env.getConfiguration(), true, false,
                            regionName, startTenantId);
                }
                break;
            case GLOBAL_VIEWS:
                tableList = getMatchPatternsForGlobalPartitions(
                        this.baseTable.getName().getString(),
                        env.getConfiguration(),
                        true, false);
                break;
            case TENANT_VIEWS:
                try {
                    startTenantId = rowKeyParser.getTenantIdFromRowKey(
                            region.getRegionInfo().getStartKey());
                    endTenantId = rowKeyParser.getTenantIdFromRowKey(
                            region.getRegionInfo().getEndKey()
                    );
                } catch (SQLException sqle) {
                    LOGGER.error(sqle.getMessage());
                    throw sqle;
                }

                if (shouldBatchCatalogAccess) {
                    tableList = getMatchPatternsForTenantBatch(
                            this.baseTable.getName().getString(),
                            env.getConfiguration(),
                            regionName, startTenantId, viewTTLTenantViewsPerScanLimit);

                } else if (startTenantId != null && !startTenantId.isEmpty()) {
                    tableList = getMatchPatternsForTenant(
                            this.baseTable.getName().getString(),
                            env.getConfiguration(), true, false,
                            regionName, startTenantId);
                }
                break;
            default:
                tableList = new ArrayList<>();
                break;
            }

            if (tableList != null && !tableList.isEmpty()) {
                tableList.forEach(m -> {
                    if (m.getTTL() != TTL_NOT_DEFINED) {
                        // add the ttlInfo to the cache.
                        // each new/unique ttlInfo object added returns a unique tableId.
                        int tableId = -1;
                        switch (type) {
                        case GLOBAL_INDEXES:
                            tableId = globalIndexTTLCache.addTable(m);
                            break;
                        case TENANT_INDEXES:
                            tableId = tenantIndexTTLCache.addTable(m);
                            break;
                        case GLOBAL_VIEWS:
                            tableId = globalViewTTLCache.addTable(m);
                            break;
                        case TENANT_VIEWS:
                            tableId = tenantViewTTLCache.addTable(m);
                            break;
                        }

                        // map the match pattern to the tableId using matcher index.
                        matcher.put(m.getMatchPattern(), tableId);
                        LOGGER.debug("Matcher updated (init) {} : {}", type.toString(), m);
                    }
                });
            }

            LOGGER.debug(String.format("Initialized matcher for type r=%s, t=%s :- " +
                            "s=%s, e=%s, c=%s, l=%s",
                    regionName,
                    type,
                    startTenantId,
                    endTenantId,
                    currentTenantId,
                    lastTenantId));

            return matcher;
        }

        // The tenant views/indexes that have TTL set are queried in batches.
        // Refresh the tenant view/index matcher with the next batch of tenant views
        // that have ttl set.
        private void refreshMatcher(MatcherType type) throws SQLException {
            List<TableTTLInfo> tableList = null;
            String regionName = region.getRegionInfo().getEncodedName();
            int catalogAccessBatchSize = NO_BATCH;
            switch (type) {
            case TENANT_INDEXES:
                this.tenantIndexMatcher = new RowKeyMatcher();
                this.tenantIndexTTLCache = new TableTTLInfoCache();
                if (currentTenantId != null && !currentTenantId.isEmpty()) {
                    tableList = getMatchPatternsForTenant(
                            this.baseTable.getName().getString(),
                            env.getConfiguration(), false, true,
                            regionName, currentTenantId);
                }
                break;
            case TENANT_VIEWS:
                this.tenantViewMatcher  = new RowKeyMatcher();
                this.tenantViewTTLCache = new TableTTLInfoCache();

                if (shouldBatchCatalogAccess) {
                    tableList = getMatchPatternsForTenantBatch(
                            this.baseTable.getName().getString(),
                            env.getConfiguration(),
                            regionName, currentTenantId, viewTTLTenantViewsPerScanLimit);

                } else if (currentTenantId != null && !currentTenantId.isEmpty()) {
                    tableList = getMatchPatternsForTenant(
                            this.baseTable.getName().getString(),
                            env.getConfiguration(), true, false,
                            regionName, currentTenantId);
                }

                break;
            default:
                throw new SQLException("Refresh for type " + type.toString() + " is not supported");
            }

            if (tableList != null && !tableList.isEmpty()) {
                tableList.forEach(m -> {
                    if (m.getTTL() != TTL_NOT_DEFINED) {
                        // add the ttlInfo to the cache.
                        // each new/unique ttlInfo object added returns a unique tableId.
                        int tableId = -1;
                        switch (type) {
                        case TENANT_INDEXES:
                            tableId = tenantIndexTTLCache.addTable(m);
                            // map the match pattern to the tableId using matcher index.
                            this.tenantIndexMatcher.put(m.getMatchPattern(), tableId);
                            break;
                        case TENANT_VIEWS:
                            tableId = tenantViewTTLCache.addTable(m);
                            // map the match pattern to the tableId using matcher index.
                            this.tenantViewMatcher.put(m.getMatchPattern(), tableId);
                            break;
                        }
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Refreshed matcher for type (updated) {}, {} : {}",
                                    regionName, type.toString(), m);
                        }

                    }
                });
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Refreshed matcher for type  r={}, t={}:- " +
                                    "rs={}, re={}, s={}, e={}, c={}, l={}",
                            regionName,
                            type,
                            Bytes.toStringBinary(region.getRegionInfo().getStartKey()),
                            Bytes.toStringBinary(region.getRegionInfo().getEndKey()),
                            startTenantId,
                            endTenantId,
                            currentTenantId,
                            lastTenantId);
                }
            }
        }


        // Match row key using the appropriate matcher
        private TableTTLInfo match(byte[] rowkey, int offset, MatcherType matcherType)
                throws SQLException {
            Integer tableId = null;
            TableTTLInfoCache tableTTLInfoCache = null;
            RowKeyMatcher matcher = null;

            if (this.isSharedIndex && matcherType.compareTo(TENANT_INDEXES) == 0) {
                currentTenantId = rowKeyParser.getTenantIdFromRowKey(rowkey, true);
                if (Bytes.BYTES_COMPARATOR.compare(
                        Bytes.toBytes(currentTenantId),
                        Bytes.toBytes(lastTenantId)) != 0) {
                    refreshMatcher(TENANT_INDEXES);
                }
                matcher = this.tenantIndexMatcher;
                tableTTLInfoCache = this.tenantIndexTTLCache;
            } else if (this.isSharedIndex &&
                    (matcherType.compareTo(GLOBAL_INDEXES) == 0)) {
                matcher = this.globalIndexMatcher;
                tableTTLInfoCache = this.globalIndexTTLCache;
            } else if (this.isMultiTenant &&
                    (matcherType.compareTo(TENANT_VIEWS) == 0)) {
                // Check whether we need to retrieve the next batch of tenants
                // If the current tenant from the row is greater than the last tenant row
                // in the tenantViewTTLCache/tenantViewMatcher then refresh the cache.
                currentTenantId = rowKeyParser.getTenantIdFromRowKey(rowkey);
                if (((shouldBatchCatalogAccess) && (Bytes.BYTES_COMPARATOR.compare(
                        Bytes.toBytes(currentTenantId),
                        Bytes.toBytes(lastTenantId)) > 0))
                    || ((!shouldBatchCatalogAccess) && (Bytes.BYTES_COMPARATOR.compare(
                        Bytes.toBytes(currentTenantId),
                        Bytes.toBytes(lastTenantId)) != 0))) {
                    refreshMatcher(TENANT_VIEWS);
                }

                matcher = this.tenantViewMatcher;
                tableTTLInfoCache = this.tenantViewTTLCache;
            } else if (matcherType.compareTo(GLOBAL_VIEWS) == 0) {
                matcher = this.globalViewMatcher;
                tableTTLInfoCache = this.globalViewTTLCache;
            } else {
                matcher = null;
                tableTTLInfoCache = null;
            }
            tableId = matcher != null ? matcher.match(rowkey, offset) : null;
            TableTTLInfo tableTTLInfo = tableTTLInfoCache != null ?
                    tableTTLInfoCache.getTableById(tableId) : null;

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Matched matcher for type r=%s, t=%s, r=%s:- " +
                                "s=%s, e=%s, c=%s, l=%s",
                        region.getRegionInfo().getEncodedName(),
                        matcherType,
                        Bytes.toStringBinary(rowkey),
                        startTenantId,
                        endTenantId,
                        currentTenantId,
                        lastTenantId));
            }

            return tableTTLInfo;

        }

        private List<TableTTLInfo> getMatchPatternsForGlobalPartitions(String physicalTableName,
                Configuration configuration, boolean globalViews, boolean globalIndexes)
                throws SQLException {

            List<TableTTLInfo> tableTTLInfoList = Lists.newArrayList();
            if (globalViews || globalIndexes) {
                Set<TableInfo> globalViewSet = getGlobalViews(physicalTableName, configuration);
                if (globalViewSet.size() > 0) {
                    getTTLInfo(physicalTableName, globalViewSet, configuration, globalIndexes,
                            tableTTLInfoList);
                }
            }
            return tableTTLInfoList;
        }

        private List<TableTTLInfo> getMatchPatternsForTenantBatch(String physicalTableName,
                Configuration configuration, String regionName,
                String startTenantId, int batchSize) throws SQLException {

            List<TableTTLInfo> tableTTLInfoList = Lists.newArrayList();

            // Batching is enabled only for TENANT_VIEWS.
            Set<TableInfo> tenantViewSet = getNextTenantViews(physicalTableName, configuration,
                    regionName, startTenantId, batchSize);
            if (tenantViewSet.size() > 0) {
                getTTLInfo(physicalTableName, tenantViewSet, configuration,
                        false, tableTTLInfoList);
            }
            return tableTTLInfoList;
        }

        private List<TableTTLInfo> getMatchPatternsForTenant(String physicalTableName,
                Configuration configuration, boolean tenantViews, boolean tenantIndexes,
                String regionName, String tenantId) throws SQLException {

            List<TableTTLInfo> tableTTLInfoList = Lists.newArrayList();
            if (tenantViews || tenantIndexes) {
                // Get all TENANT_VIEWS AND TENANT_INDEXES.
                Set<TableInfo> tenantViewSet = getNextTenantViews(physicalTableName, configuration,
                        regionName, tenantId, NO_BATCH);
                if (tenantViewSet.size() > 0) {
                    getTTLInfo(physicalTableName, tenantViewSet, configuration,
                            tenantIndexes, tableTTLInfoList);
                }
            }

            return tableTTLInfoList;
        }

        /**
         * Get the ROW_KEY_MATCHER AND TTL field values for various view related entities -
         * GLOBAL_VIEWS AND GLOBAL_INDEXES for a given HBase table (Physical Phoenix table)
         *
         * @param physicalTableName
         * @param configuration
         * @return
         * @throws SQLException
         */
        private Set<TableInfo> getGlobalViews(String physicalTableName, Configuration configuration)
                throws SQLException {

            Set<TableInfo> globalViewSet = new HashSet<>();
            try (Connection serverConnection = QueryUtil.getConnectionOnServer(new Properties(),
                    configuration)) {
                String
                        globalViewsSQLFormat =
                        "SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, " +
                                "COLUMN_NAME AS PHYSICAL_TABLE_TENANT_ID, " +
                                "COLUMN_FAMILY AS PHYSICAL_TABLE_FULL_NAME " +
                                "FROM SYSTEM.CATALOG " +
                                "WHERE " + "LINK_TYPE = 2 " +
                                "AND TABLE_TYPE IS NULL " +
                                "AND COLUMN_FAMILY = '%s' " +
                                "AND TENANT_ID IS NULL";
                String globalViewSQL = String.format(globalViewsSQLFormat, physicalTableName);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("globalViewSQL: {}", globalViewSQL);
                }

                try (PhoenixPreparedStatement globalViewStmt = serverConnection.prepareStatement(
                        globalViewSQL).unwrap(PhoenixPreparedStatement.class)) {
                    try (ResultSet globalViewRS = globalViewStmt.executeQuery()) {
                        while (globalViewRS.next()) {
                            String tid = globalViewRS.getString("TENANT_ID");
                            String schem = globalViewRS.getString("TABLE_SCHEM");
                            String tName = globalViewRS.getString("TABLE_NAME");
                            String tenantId = tid == null || tid.isEmpty() ? "NULL" : "'" + tid + "'";
                            String
                                    schemCol =
                                    schem == null || schem.isEmpty() ? "NULL" : "'" + schem + "'";
                            TableInfo
                                    tableInfo =
                                    new TableInfo(tenantId.getBytes(), schemCol.getBytes(),
                                            tName.getBytes());
                            globalViewSet.add(tableInfo);
                        }
                    }
                }
            }
            return globalViewSet;
        }

        /**
         * Get the ROW_KEY_MATCHER AND TTL field values for various view related entities -
         * TENANT_VIEWS AND TENANT_INDEXES for a given HBase table (Physical Phoenix table)
         *
         * when batch <= 0
         *  Get all the tenant views defined for a given tenant
         * when batch > 0
         *  Get the tenant views defined for tenants starting from passed in tenantId
         *  in a query more/batch style
         *
         * @param physicalTableName
         * @param configuration
         * @param regionName
         * @param fromTenantId
         * @param batchSize
         * @return
         * @throws SQLException
         */
        private  Set<TableInfo> getNextTenantViews(
                String physicalTableName,
                Configuration configuration,
                String regionName,
                String fromTenantId,
                int batchSize
        ) throws SQLException {

            Set<TableInfo> tenantViewSet = new HashSet<>();
            try (Connection serverConnection = QueryUtil.getConnectionOnServer(new Properties(),
                    configuration)) {
                String
                        tenantViewsSQLFormat =
                        "SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME," +
                                "COLUMN_NAME AS PHYSICAL_TABLE_TENANT_ID, " +
                                "COLUMN_FAMILY AS PHYSICAL_TABLE_FULL_NAME " +
                                "FROM SYSTEM.CATALOG " +
                                "WHERE LINK_TYPE = 2 " +
                                "AND COLUMN_FAMILY = '%s' " +
                                "AND TENANT_ID IS NOT NULL ";
                                if (batchSize <= 0) {
                                    tenantViewsSQLFormat +=
                                            ((fromTenantId != null && fromTenantId.length() > 0)
                                            ? "AND TENANT_ID = ? "
                                            : "");

                                } else {
                                    tenantViewsSQLFormat +=
                                            ((fromTenantId != null && fromTenantId.length() > 0)
                                            ? "AND TENANT_ID >= ? " + "LIMIT " + batchSize
                                            : "");
                                }

                String tenantViewSQL = String.format(tenantViewsSQLFormat, physicalTableName);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(String.format("tenantViewSQL " +
                                    "region-name = %s, " +
                                    "start-tenant-id = %s, " +
                                    "batch = %d, " +
                                    "sql = %s ",
                            regionName,
                            fromTenantId,
                            batchSize,
                            tenantViewSQL));
                }

                try (PhoenixPreparedStatement tenantViewStmt = serverConnection.prepareStatement(
                        tenantViewSQL).unwrap(PhoenixPreparedStatement.class)) {
                    int paramPos = 1;
                    if (fromTenantId != null && fromTenantId.length() > 0) {
                        tenantViewStmt.setString(paramPos, fromTenantId);
                    }
                    try (ResultSet tenantViewRS = tenantViewStmt.executeQuery()) {
                        while (tenantViewRS.next()) {
                            String tid = tenantViewRS.getString("TENANT_ID");
                            String schem = tenantViewRS.getString("TABLE_SCHEM");
                            String tName = tenantViewRS.getString("TABLE_NAME");
                            String tenantId = tid == null || tid.isEmpty() ? "NULL" : "'" + tid + "'";
                            String schemCol = schem == null || schem.isEmpty()
                                    ? "NULL" : "'" + schem + "'";
                            TableInfo
                                    tableInfo =
                                    new TableInfo(tenantId.getBytes(), schemCol.getBytes(),
                                            tName.getBytes());
                            lastTenantId = tid == null || tid.isEmpty() ? "" : tid;
                            tenantViewSet.add(tableInfo);
                        }
                    }
                }
            }
            return tenantViewSet;
        }

        /**
         * Get the view/shared-index details (TTL, ROW_KEY_MATCHER) for a given set of views
         * @param physicalTableName
         * @param viewSet
         * @param configuration
         * @param isSharedIndex
         * @param tableTTLInfoList
         * @throws SQLException
         */
        private void getTTLInfo(String physicalTableName,
                Set<TableInfo> viewSet, Configuration configuration,
                boolean isSharedIndex, List<TableTTLInfo> tableTTLInfoList)
                throws SQLException {

            if (viewSet.size() == 0) {
                return;
            }
            String
                    viewsClause =
                    new StringBuilder(viewSet.stream()
                            .map((v) -> String.format("(%s, %s,'%s')",
                                    Bytes.toString(v.getTenantId()),
                                    Bytes.toString(v.getSchemaName()),
                                    Bytes.toString(v.getTableName())))
                            .collect(Collectors.joining(","))).toString();
            String
                    viewsWithTTLSQL =
                    "SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, " +
                            "TTL, ROW_KEY_MATCHER " +
                            "FROM SYSTEM.CATALOG " +
                            "WHERE TABLE_TYPE = 'v' AND " +
                            "(TENANT_ID, TABLE_SCHEM, TABLE_NAME) IN " +
                            "(" + viewsClause.toString() + ")";
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                        String.format("ViewsWithTTLSQL : %s", viewsWithTTLSQL));
            }

            try (Connection serverConnection = QueryUtil.getConnectionOnServer(new Properties(),
                    configuration)) {

                try (
                        PhoenixPreparedStatement
                                viewTTLStmt =
                                serverConnection.prepareStatement(viewsWithTTLSQL)
                                        .unwrap(PhoenixPreparedStatement.class)) {

                    try (ResultSet viewTTLRS = viewTTLStmt.executeQuery()) {
                        while (viewTTLRS.next()) {
                            String tid = viewTTLRS.getString("TENANT_ID");
                            String schem = viewTTLRS.getString("TABLE_SCHEM");
                            String tName = viewTTLRS.getString("TABLE_NAME");
                            String viewTTLStr = viewTTLRS.getString("TTL");
                            int viewTTL = viewTTLStr == null || viewTTLStr.isEmpty() ?
                                    TTL_NOT_DEFINED : Integer.valueOf(viewTTLStr);
                            byte[] rowKeyMatcher = viewTTLRS.getBytes("ROW_KEY_MATCHER");
                            byte[]
                                    tenantIdBytes =
                                    tid == null || tid.isEmpty() ? EMPTY_BYTE_ARRAY : tid.getBytes();

                            String fullTableName = SchemaUtil.getTableName(schem, tName);
                            Properties tenantProps = new Properties();
                            if (tid != null) {
                                tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tid);
                            }

                            if (isSharedIndex) {
                                try (Connection
                                        tableConnection =
                                        QueryUtil.getConnectionOnServer(tenantProps, configuration)) {

                                    PTable
                                            pTable =
                                            PhoenixRuntime.getTableNoCache(
                                                    tableConnection, fullTableName);
                                    for (PTable index : pTable.getIndexes()) {
                                        // Handling the case when it is a table level index.
                                        // In those cases view-index-id = null
                                        if (index.getViewIndexId() == null) {
                                            continue;
                                        }
                                        PDataType viewIndexIdType = index.getviewIndexIdType();
                                        byte[]
                                                viewIndexIdBytes =
                                                PSmallint.INSTANCE.toBytes(index.getViewIndexId());
                                        if (viewIndexIdType.compareTo(PLong.INSTANCE) == 0) {
                                            viewIndexIdBytes =
                                                    PLong.INSTANCE.toBytes(index.getViewIndexId());
                                        }
                                        tableTTLInfoList.add(
                                                new TableTTLInfo(pTable.getPhysicalName().getBytes(),
                                                        tenantIdBytes, index.getTableName().getBytes(),
                                                        viewIndexIdBytes, index.getTTL()));
                                    }

                                }
                            } else {
                                tableTTLInfoList.add(
                                        new TableTTLInfo(physicalTableName.getBytes(),
                                                tenantIdBytes, fullTableName.getBytes(),
                                                rowKeyMatcher, viewTTL));
                            }
                        }
                    }
                }
            }
        }


        public boolean isSharedIndex() {
            return isSharedIndex;
        }

        public boolean isMultiTenant() {
            return isMultiTenant;
        }

        public boolean isSalted() {
            return isSalted;
        }

        public RowKeyParser getRowKeyParser() {
            return rowKeyParser;
        }

        public PTable getBaseTable() {
            return baseTable;
        }

        public RowKeyMatcher getGlobalViewMatcher() {
            return globalViewMatcher;
        }

        public RowKeyMatcher getTenantViewMatcher() {
            return tenantViewMatcher;
        }

        public RowKeyMatcher getGlobalIndexMatcher() {
            return globalIndexMatcher;
        }

        public RowKeyMatcher getTenantIndexMatcher() {
            return tenantIndexMatcher;
        }


        public TableTTLInfoCache getGlobalViewTTLCache() {
            return globalViewTTLCache;
        }

        public TableTTLInfoCache getTenantViewTTLCache() {
            return tenantViewTTLCache;
        }

        public TableTTLInfoCache getGlobalIndexTTLCache() {
            return globalIndexTTLCache;
        }

        public TableTTLInfoCache getTenantIndexTTLCache() {
            return tenantIndexTTLCache;
        }

        public int getNumGlobalEntries() {
            return globalViewMatcher == null ? 0 : globalViewMatcher.getNumEntries();
        }

        public int getNumTenantEntries() {
            return tenantViewMatcher == null ? 0 : tenantViewMatcher.getNumEntries();
        }

        public int getNumGlobalIndexEntries() {
            return globalIndexMatcher == null ? 0 : globalIndexMatcher.getNumEntries();
        }

        public int getNumTenantIndexEntries() {
            return tenantIndexMatcher == null ? 0 : tenantIndexMatcher.getNumEntries();
        }

        public int getNumTablesInGlobalCache() {
            return globalViewTTLCache == null ? 0 : globalViewTTLCache.getNumTablesInCache();
        }

        public int getNumTablesInTenantCache() {
            return tenantViewTTLCache == null ? 0 : tenantViewTTLCache.getNumTablesInCache();
        }

        public int getNumTablesInGlobalIndexCache() {
            return globalIndexTTLCache == null ? 0 : globalIndexTTLCache.getNumTablesInCache();
        }

        public int getNumTablesInTenantIndexCache() {
            return tenantIndexTTLCache == null ? 0 : tenantIndexTTLCache.getNumTablesInCache();
        }

        public int getNumTablesInCache() {
            int totalNumTables = 0;
            totalNumTables +=
                    globalViewTTLCache == null ? 0 : globalViewTTLCache.getNumTablesInCache();
            totalNumTables +=
                    tenantViewTTLCache == null ? 0 : tenantViewTTLCache.getNumTablesInCache();
            totalNumTables +=
                    globalIndexTTLCache == null ? 0 : globalIndexTTLCache.getNumTablesInCache();
            totalNumTables +=
                    tenantIndexTTLCache == null ? 0 : tenantIndexTTLCache.getNumTablesInCache();
            return totalNumTables;
        }

    }

    /**
     * The implementation classes will track TTL for various Phoenix Objects.
     * Tables - Partitioned (HBase Tables with Views and View-Indexes)
     *          and Non-Partitioned (Simple HBase Tables And Indexes)
     *          For Flushes and Minor compaction we do not need to track the TTL.
     */
    private interface TTLTracker {
        // Set the TTL for the given row in the row-context being tracked.
        void setTTL(Cell firstCell) throws IOException;
        // get the row context for the current row.
        RowContext getRowContext();
        // set the row context for the current row.
        void setRowContext(RowContext rowContext);
    }

    /**
     * This tracker will be used for memstore flushes and minor compaction where we do not need to
     * track the TTL.
     */
    private class TableTTLTrackerForFlushesAndMinor implements TTLTracker {

        private long ttl;
        private RowContext rowContext;

        public TableTTLTrackerForFlushesAndMinor(String tableName) {

            ttl = DEFAULT_TTL;
            LOGGER.info(String.format(
                    "TableTTLTrackerForFlushesAndMinor params:- " +
                            "(table-name=%s, ttl=%d)",
                    tableName, ttl*1000));
        }

        @Override
        public void setTTL(Cell firstCell) {
            if (this.rowContext == null) {
                this.rowContext = new RowContext();
            }
            this.rowContext.setTTL(ttl);

        }

        @Override
        public RowContext getRowContext() {
            if (this.rowContext == null) {
                this.rowContext = new RowContext();
                this.rowContext.setTTL(ttl);
            }
            return rowContext;
        }

        @Override
        public void setRowContext(RowContext rowContext) {
            this.rowContext = rowContext;
            this.rowContext.setTTL(ttl);
        }
    }

    private class NonPartitionedTableTTLTracker implements TTLTracker {

        private long ttl;
        private RowContext rowContext;

        public NonPartitionedTableTTLTracker(
                PTable pTable,
                Store store) {

            boolean isSystemTable = pTable.getType() == PTableType.SYSTEM;
            if (isSystemTable) {
                ColumnFamilyDescriptor cfd = store.getColumnFamilyDescriptor();
                ttl = cfd.getTimeToLive();
            } else {
                ttl = pTable.getTTL() != TTL_NOT_DEFINED ? pTable.getTTL() : DEFAULT_TTL;
            }
            LOGGER.info(String.format(
                    "NonPartitionedTableTTLTracker params:- " +
                            "(physical-name=%s, ttl=%d, isSystemTable=%s)",
                    pTable.getName().toString(), ttl*1000, isSystemTable));
        }

        @Override
        public void setTTL(Cell firstCell) {
            if (this.rowContext == null) {
                this.rowContext = new RowContext();
            }
            this.rowContext.setTTL(ttl);

        }

        @Override
        public RowContext getRowContext() {
            if (this.rowContext == null) {
                this.rowContext = new RowContext();
                this.rowContext.setTTL(ttl);
            }
            return rowContext;
        }

        @Override
        public void setRowContext(RowContext rowContext) {
            this.rowContext = rowContext;
            this.rowContext.setTTL(ttl);
        }
    }

    private class PartitionedTableTTLTracker implements TTLTracker {
        private final Logger LOGGER = LoggerFactory.getLogger(
                PartitionedTableTTLTracker.class);

        // Default or Table-Level TTL
        private long ttl;
        private RowContext rowContext;

        private boolean isSharedIndex = false;
        private boolean isMultiTenant = false;
        private boolean isSalted = false;
        private boolean isLongViewIndexEnabled = false;
        private int startingPKPosition;
        private PartitionedTableRowKeyMatcher tableRowKeyMatcher;

        public PartitionedTableTTLTracker(
                PTable table,
                boolean isSalted,
                boolean isSharedIndex,
                boolean isLongViewIndexEnabled,
                int viewTTLTenantViewsPerScanLimit
        ) throws IOException {

            try {
                // Initialize the various matcher indexes
                this.tableRowKeyMatcher =
                        new PartitionedTableRowKeyMatcher(table, isSalted, isSharedIndex,
                                isLongViewIndexEnabled, viewTTLTenantViewsPerScanLimit);
                this.ttl = table.getTTL() != TTL_NOT_DEFINED ? table.getTTL() : DEFAULT_TTL;
                this.isSharedIndex = isSharedIndex || localIndex;
                this.isLongViewIndexEnabled = isLongViewIndexEnabled;
                this.isSalted = isSalted;
                this.isMultiTenant = table.isMultiTenant();

                this.startingPKPosition = getStartingPKPosition();;
                LOGGER.info(String.format(
                        "PartitionedTableTTLTracker params:- " + 
                                "region-name = %s, table-name = %s,  " +
                                "multi-tenant = %s, shared-index = %s, salted = %s, " +
                                "default-ttl = %d, startingPKPosition = %d",
                        region.getRegionInfo().getEncodedName(),
                        region.getRegionInfo().getTable().getNameAsString(), this.isMultiTenant,
                        this.isSharedIndex, this.isSalted, this.ttl, this.startingPKPosition));

            } catch (SQLException e) {
                LOGGER.error(String.format("Failed to read from catalog: " + e.getMessage()));
                throw new IOException(e);
            } finally {
                if (tableRowKeyMatcher != null) {
                    LOGGER.info(String.format(
                            "PartitionedTableTTLTracker stats " +
                                    "(index-entries, table-entries) for region = %s:-" +
                                    "global-views = %d, %d, " +
                                    "tenant-views = %d, %d, " +
                                    "global-indexes = %d, %d " +
                                    "tenant-indexes = %d, %d ",
                            region.getRegionInfo().getEncodedName(),
                            tableRowKeyMatcher.getNumGlobalEntries(),
                            tableRowKeyMatcher.getNumTablesInGlobalCache(),
                            tableRowKeyMatcher.getNumTenantEntries(),
                            tableRowKeyMatcher.getNumTablesInTenantCache(),
                            tableRowKeyMatcher.getNumGlobalIndexEntries(),
                            tableRowKeyMatcher.getNumTablesInGlobalIndexCache(),
                            tableRowKeyMatcher.getNumTenantIndexEntries(),
                            tableRowKeyMatcher.getNumTablesInTenantIndexCache()));
                } else {
                    LOGGER.error(String.format("Failed to initialize: tableRowKeyMatcher is null"));
                }
            }
        }

        private int getStartingPKPosition() {
            int startingPKPosition = 0;
            if (this.isMultiTenant && this.isSalted && this.isSharedIndex) {
                // case multi-tenanted, salted, is a shared-index =>
                // startingPKPosition = 1 skip the salt-byte and starting at the viewIndexId
                startingPKPosition = 1;
            } else if (this.isMultiTenant && this.isSalted && !this.isSharedIndex) {
                // case multi-tenanted, salted, not a shared-index =>
                // startingPKPosition = 2 skip salt byte + tenant-id to search the global space
                // if above search returned no results
                // then search using the following start position
                // startingPKPosition = 1 skip salt-byte to search the tenant space
                startingPKPosition = 2;
            } else if (this.isMultiTenant && !this.isSalted && this.isSharedIndex) {
                // case multi-tenanted, not-salted, is a shared-index =>
                // startingPKPosition = 0, the first key will the viewIndexId
                startingPKPosition = 0;
            } else if (this.isMultiTenant && !this.isSalted && !this.isSharedIndex) {
                // case multi-tenanted, not-salted, not a shared-index =>
                // startingPKPosition = 1 skip tenant-id to search the global space
                // if above search returned no results
                // then search using the following start position
                // startingPKPosition = 0 to search the tenant space
                startingPKPosition = 1;
            } else if (!this.isMultiTenant && this.isSalted && this.isSharedIndex) {
                // case non-multi-tenanted, salted, shared-index =>
                // startingPKPosition = 1 skip salt-byte search using the viewIndexId
                startingPKPosition = 1;
            } else if (!this.isMultiTenant && this.isSalted && !this.isSharedIndex) {
                // case non-multi-tenanted, salted, not a shared-index =>
                // start at the global pk position after skipping the salt byte
                // startingPKPosition = 1 skip salt-byte
                startingPKPosition = 1;
            } else if (!this.isMultiTenant && !this.isSalted && this.isSharedIndex) {
                // case non-multi-tenanted, not-salted, is a shared-index =>
                // startingPKPosition = 0 the first key will the viewIndexId
                startingPKPosition = 0;
            } else {
                // case non-multi-tenanted, not-salted, not a view-index-table  =>
                // startingPKPosition = 0
                startingPKPosition = 0;
            }
            return startingPKPosition;
        }

        @Override
        public void setTTL(Cell firstCell) throws IOException {

            boolean matched = false;
            TableTTLInfo tableTTLInfo = null;
            List<Integer> pkPositions = null;
            long rowTTLInSecs = ttl;
            long matchedOffset = -1;
            int pkPosition = startingPKPosition;
            MatcherType matchedType = null;
            try {
                // pkPositions holds the byte offsets for the PKs of the base table
                // for the current row
                pkPositions = isSharedIndex ?
                        (isSalted ?
                                Arrays.asList(0, 1) :
                                Arrays.asList(0)) :
                        tableRowKeyMatcher.getRowKeyParser().parsePKPositions(firstCell);
                // The startingPKPosition was initialized in the following manner =>
                // see getStartingPKPosition()
                // case multi-tenant, salted, is-shared-index  => startingPKPosition = 1
                // case multi-tenant, salted, not-shared-index => startingPKPosition = 2
                // case multi-tenant, not-salted, is-shared-index => startingPKPosition = 0
                // case multi-tenant, not-salted, not-shared-index => startingPKPosition = 1
                // case non-multi-tenant, salted, is-shared-index => startingPKPosition = 1
                // case non-multi-tenant, salted, not-shared-index => startingPKPosition = 1
                // case non-multi-tenant, not-salted, is-shared-index => startingPKPosition = 0
                // case non-multi-tenant, not-salted, not-shared-index => startingPKPosition = 0
                int offset = pkPositions.get(pkPosition);
                byte[] rowKey = CellUtil.cloneRow(firstCell);
                Integer tableId = null;
                // Search using the starting offset (startingPKPosition offset)
                if (isSharedIndex) {
                    // case index table, first check the global indexes
                    matchedType = GLOBAL_INDEXES;
                    tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, GLOBAL_INDEXES);
                    if (tableTTLInfo == null) {
                        matchedType = TENANT_INDEXES;
                        tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, TENANT_INDEXES);
                    }

                } else if (isMultiTenant) {
                    // case multi-tenant, non-index tables, global space
                    matchedType = GLOBAL_VIEWS;
                    tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, GLOBAL_VIEWS);
                    if (tableTTLInfo == null) {
                        // search returned no results, determine the new pkPosition(offset) to use
                        // Search using the new offset
                        pkPosition = this.isSalted ? 1 : 0;
                        offset = pkPositions.get(pkPosition);
                        // case multi-tenant, non-index tables, tenant space
                        matchedType = TENANT_VIEWS;
                        tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, TENANT_VIEWS);
                    }
                } else {
                    // case non-multi-tenant and non-index tables, global space
                    matchedType = GLOBAL_VIEWS;
                    tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, GLOBAL_VIEWS);
                }
                matched = tableTTLInfo != null;
                matchedOffset = matched ? offset : -1;
                rowTTLInSecs = matched ? tableTTLInfo.getTTL() : ttl; /* in secs */
                if (this.rowContext == null) {
                    this.rowContext = new RowContext();
                }
                this.rowContext.setTTL(rowTTLInSecs);
            } catch (SQLException e) {
                LOGGER.error(String.format("Exception when visiting table: " + e.getMessage()));
                throw new IOException(e);
            } finally {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(String.format("visiting row-key = %s, region = %s, " +
                                    "table-ttl-info=%s, " +
                                    "matched = %s, matched-type = %s, match-pattern = %s, " +
                                    "ttl = %d, matched-offset = %d, " +
                                    "pk-pos = %d, pk-pos-list = %s",
                            CellUtil.getCellKeyAsString(firstCell),
                            CompactionScanner.this.store.getRegionInfo().getEncodedName(),
                            matched ? tableTTLInfo : "NULL",
                            matched,
                            matchedType,
                            matched ? Bytes.toStringBinary(tableTTLInfo.getMatchPattern()) : "NULL",
                            rowTTLInSecs,
                            matchedOffset,
                            pkPosition,
                            pkPositions != null ? pkPositions.stream()
                                    .map((p) -> String.valueOf(p))
                                    .collect(Collectors.joining(",")) : ""));
                }
            }

        }

        @Override
        public RowContext getRowContext() {
            if (this.rowContext == null) {
                this.rowContext = new RowContext();
                this.rowContext.setTTL(ttl);
            }
            return rowContext;
        }

        @Override
        public void setRowContext(RowContext rowContext) {
            this.rowContext = rowContext;
            this.rowContext.setTTL(ttl);
        }
    }

    public class RowKeyParser {
        private final RowKeyColumnExpression[] baseTableColExprs;
        private final List<PColumn> baseTablePKColumns;
        private final PColumn[] sharedIndexPKColumns;
        private final boolean isSalted;
        private final boolean isLongViewIndexEnabled;
        private final boolean isMultiTenant;
        private final PDataType tenantDataType;

        public RowKeyParser(PTable table, boolean isLongViewIndexEnabled) {
            this.isLongViewIndexEnabled = isLongViewIndexEnabled;
            isSalted = table.getBucketNum() != null;
            isMultiTenant = table.isMultiTenant();

            // Get the TENANT_ID data type, this will be used to determine if the queries to
            // SYSCAT will be batched.
            tenantDataType = table.getRowKeySchema().getField(isSalted ? 1 : 0).getDataType();

            // Initialize the ColumnExpressions for the base table PK Columns
            baseTablePKColumns = table.getPKColumns();
            baseTableColExprs = new RowKeyColumnExpression[baseTablePKColumns.size()];
            int saltPos = isSalted ? 0 : -1;
            for (int i = 0; i < baseTablePKColumns.size(); i++) {
                PColumn column = baseTablePKColumns.get(i);
                baseTableColExprs[i] = new RowKeyColumnExpression(
                        column,
                        new RowKeyValueAccessor(baseTablePKColumns, i));
            }

            // Initialize the shared index PK columns to be used in getTenantIdFromRowKey()
            // to create a RowKeyColumnExpression for tenantId parsing.
            // position 0 : salt byte if salted else index_id
            // position 1 : index_id if salted else tenant_id
            // position 2 : tenant_id if salted and multi-tenanted else empty
            sharedIndexPKColumns = new PColumn[3];
            if (saltPos == 0) {
                sharedIndexPKColumns[saltPos] = baseTablePKColumns.get(saltPos);
            }
            final int tenantPos = isMultiTenant ? (saltPos + 1) : -1;
            if ((tenantPos == 0) ||  (tenantPos == 1)) {
                sharedIndexPKColumns[tenantPos] = new PColumn() {

                    @Override
                    public PName getName() {
                        return PNameFactory.newName("_INDEX_ID");
                    }

                    @Override
                    public PName getFamilyName() {
                        return null;
                    }

                    @Override
                    public int getPosition() {
                        return tenantPos;
                    }

                    @Override
                    public Integer getArraySize() {
                        return 0;
                    }

                    @Override
                    public byte[] getViewConstant() {
                        return new byte[0];
                    }

                    @Override
                    public boolean isViewReferenced() {
                        return false;
                    }

                    @Override
                    public int getEstimatedSize() {
                        return 0;
                    }

                    @Override
                    public String getExpressionStr() {
                        return "";
                    }

                    @Override
                    public long getTimestamp() {
                        return 0;
                    }

                    @Override
                    public boolean isDerived() {
                        return false;
                    }

                    @Override
                    public boolean isExcluded() {
                        return false;
                    }

                    @Override
                    public boolean isRowTimestamp() {
                        return false;
                    }

                    @Override
                    public boolean isDynamic() {
                        return false;
                    }

                    @Override
                    public byte[] getColumnQualifierBytes() {
                        return new byte[0];
                    }

                    @Override
                    public boolean isNullable() {
                        return false;
                    }

                    @Override
                    public PDataType getDataType() {
                        return RowKeyParser.this.isLongViewIndexEnabled ?
                                PLong.INSTANCE : PSmallint.INSTANCE;
                    }

                    @Override
                    public Integer getMaxLength() {
                        return 0;
                    }

                    @Override
                    public Integer getScale() {
                        return 0;
                    }

                    @Override
                    public SortOrder getSortOrder() {
                        return SortOrder.ASC;
                    }
                };
                sharedIndexPKColumns[tenantPos+1] = baseTablePKColumns.get(tenantPos);
            }

        }

        // accessor method for tenantDataType
        public PDataType getTenantIdDataType() {
            return tenantDataType;
        }

        // Parse the row key cell to find the PK position boundaries
        public List<Integer> parsePKPositions(Cell inputCell)  {
            RowKeyTuple inputTuple = new RowKeyTuple();
            inputTuple.setKey(inputCell.getRowArray(),
                    inputCell.getRowOffset(),
                    inputCell.getRowLength());

            int lastPos = 0;
            List<Integer> pkPositions = new ArrayList<>();
            pkPositions.add(lastPos);
            // Use the RowKeyColumnExpression to parse the PK positions
            for (int i = 0; i < baseTableColExprs.length; i++) {
                RowKeyColumnExpression expr = baseTableColExprs[i];
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                expr.evaluate(inputTuple, ptr);
                int separatorLength = baseTablePKColumns.get(i).getDataType().isFixedWidth() ? 0 : 1;
                int endPos = lastPos + ptr.getLength() + separatorLength;
                pkPositions.add(endPos);
                lastPos = endPos;
            }
            return pkPositions;
        }

        // Parse the row key to extract the TENANT_ID as a String
        private String getTenantIdFromRowKey(byte[] rowKey) throws SQLException {
            return getTenantIdFromRowKey(rowKey, false);
        }

        // Parse the row key to extract the TENANT_ID as a String
        private String getTenantIdFromRowKey(byte[] rowKey, boolean isSharedIndex) throws SQLException {
            // case: when it is the start of the first region or end of the last region
            if ((rowKey != null && ByteUtil.isEmptyOrNull(rowKey, 0, rowKey.length)) || (!isMultiTenant)) {
                return "";
            }
            // Construct a cell from the rowKey for us evaluate the tenantId
            Cell rowKeyCell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                    .setRow(rowKey)
                    .setFamily(emptyCF)
                    .setQualifier(emptyCQ)
                    .setTimestamp(EnvironmentEdgeManager.currentTimeMillis())
                    .setType(Cell.Type.Put)
                    .setValue(HConstants.EMPTY_BYTE_ARRAY)
                    .build();
            // Evaluating and converting a byte ptr to tenantId
            // Sometimes the underlying byte ptr is padded with null bytes (0x0)
            // in case of salted regions.
            int tenantIdPosition = (isSalted ? 1 : 0) + (isSharedIndex ? 1 : 0);
            RowKeyColumnExpression expr;
            PDataType dataType;
            RowKeyTuple inputTuple = new RowKeyTuple();
            if (isSharedIndex) {
                expr = new RowKeyColumnExpression(
                        sharedIndexPKColumns[tenantIdPosition],
                        new RowKeyValueAccessor(Arrays.asList(sharedIndexPKColumns), tenantIdPosition));
                dataType = sharedIndexPKColumns[tenantIdPosition].getDataType();

                // Constructing a RowKeyTuple for expression evaluation
                inputTuple.setKey(rowKeyCell.getRowArray(),
                        rowKeyCell.getRowOffset(),
                        rowKeyCell.getRowLength());
            } else {
                expr = baseTableColExprs[tenantIdPosition];
                dataType = baseTablePKColumns.get(tenantIdPosition).getDataType();
                inputTuple.setKey(rowKeyCell.getRowArray(),
                        rowKeyCell.getRowOffset(),
                        rowKeyCell.getRowLength());
            }
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            String tenantId = "";
            try {
                expr.evaluate(inputTuple, ptr);
                dataType.pad(ptr, expr.getMaxLength(), expr.getSortOrder());
                tenantId = ByteUtil.isEmptyOrNull(ptr.get(), ptr.getOffset(),
                        ptr.getLength()) ? "" : dataType.toObject(ptr).toString();
            } catch(IllegalDataException ex) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TENANTID_IS_OF_WRONG_TYPE)
                        .build().buildException();
            }
            finally {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("TenantId in getTenantIdFromRowKey: {}, {}", CompactionScanner.this.store.getRegionInfo().getEncodedName(), tenantId);
                }
            }
            return tenantId;
        }
    }


    /**
     * The context for a given row during compaction. A row may have multiple compaction row
     * versions. CompactionScanner uses the same row context for these versions.
     */
    class RowContext {
        Cell familyDeleteMarker = null;
        Cell familyVersionDeleteMarker = null;
        List<Cell> columnDeleteMarkers = new ArrayList<>();
        int version = 0;
        long maxTimestamp;
        long minTimestamp;
        long ttl;
        long ttlWindowStart;
        long maxLookbackWindowStartForRow;

        private void init() {
            familyDeleteMarker = null;
            familyVersionDeleteMarker = null;
            columnDeleteMarkers.clear();
            version = 0;
        }

        public void setTTL(long ttlInSecs) {
            this.ttl = ttlInSecs*1000;
            this.ttlWindowStart = ttlInSecs == HConstants.FOREVER ? 1 : compactionTime - ttl ;
            this.maxLookbackWindowStartForRow = Math.max(ttlWindowStart, maxLookbackWindowStart);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("RowContext:- (ttlWindowStart=%d, maxLookbackWindowStart=%d)",
                        ttlWindowStart, maxLookbackWindowStart));
            }

        }
        public long getTTL() {
            return ttl;
        }
        public long getTtlWindowStart() {
            return ttlWindowStart;
        }
        public long getMaxLookbackWindowStart() {
            return maxLookbackWindowStartForRow;
        }


        private void addColumnDeleteMarker(Cell deleteMarker) {
            if (columnDeleteMarkers.isEmpty()) {
                columnDeleteMarkers.add(deleteMarker);
                return;
            }
            int i = 0;
            // Replace the existing delete marker for the same column
            for (Cell cell : columnDeleteMarkers) {
                if (cell.getType() == deleteMarker.getType() &&
                        CellUtil.matchingColumn(cell, deleteMarker)) {
                    columnDeleteMarkers.remove(i);
                    break;
                }
                i++;
            }
            columnDeleteMarkers.add(deleteMarker);
        }

        private void retainFamilyDeleteMarker(List<Cell> retainedCells) {
            if (familyVersionDeleteMarker != null) {
                retainedCells.add(familyVersionDeleteMarker);
                // Set it to null so it will be used once
                familyVersionDeleteMarker = null;
            } else {
                // The same delete family marker may be retained multiple times. Duplicates will be
                // removed later
                retainedCells.add(familyDeleteMarker);
            }
        }
        /**
         * Based on the column delete markers decide if the cells should be retained. If a
         * deleted cell is retained, the delete marker is also retained.
         */
        private void retainCell(Cell cell, List<Cell> retainedCells,
                KeepDeletedCells keepDeletedCells, long ttlWindowStart) {
            int i = 0;
            for (Cell dm : columnDeleteMarkers) {
                if (cell.getTimestamp() > dm.getTimestamp()) {
                    continue;
                }
                if ((CellUtil.matchingFamily(cell, dm)) &&
                        CellUtil.matchingQualifier(cell, dm)) {
                    if (dm.getType() == Cell.Type.Delete) {
                        if (cell.getTimestamp() == dm.getTimestamp()) {
                            // Delete is for deleting a specific cell version. Thus, it can be used
                            // to delete only one cell.
                            columnDeleteMarkers.remove(i);
                        } else {
                            continue;
                        }
                    }
                    if (maxTimestamp >= ttlWindowStart) {
                        // Inside the TTL window
                        if (keepDeletedCells != KeepDeletedCells.FALSE ) {
                            retainedCells.add(cell);
                            retainedCells.add(dm);
                        }
                    } else if (keepDeletedCells == KeepDeletedCells.TTL &&
                            dm.getTimestamp() >= ttlWindowStart) {
                        retainedCells.add(cell);
                        retainedCells.add(dm);
                    }
                    return;
                }
                i++;
            }
            // No delete marker for this cell
            retainedCells.add(cell);
        }
        /**
         * This method finds out the maximum and minimum timestamp of the cells of the next row
         * version. Cells are organized into columns based on the pair of family name and column
         * qualifier. This means that the delete family markers for a column family will have their
         * own column. However, the delete column markers will be packed with the put cells. The cells
         * within a column are ordered in descending timestamps.
         */
        private void getNextRowVersionTimestamps(LinkedList<LinkedList<Cell>> columns,
                byte[] columnFamily) {
            maxTimestamp = 0;
            minTimestamp = Long.MAX_VALUE;
            Cell firstCell;
            LinkedList<Cell> deleteColumn = null;
            long ts;
            // The next row version is formed by the first cell of each column. Similarly, the min
            // max timestamp of the cells of a row version is determined by looking at just first
            // cell of the columns
            for (LinkedList<Cell> column : columns) {
                firstCell = column.getFirst();
                ts = firstCell.getTimestamp();
                if ((firstCell.getType() == Cell.Type.DeleteFamily ||
                        firstCell.getType() == Cell.Type.DeleteFamilyVersion) &&
                        CellUtil.matchingFamily(firstCell, columnFamily)) {
                    deleteColumn = column;
                }
                if (maxTimestamp < ts) {
                    maxTimestamp = ts;
                }
                if (minTimestamp > ts) {
                    minTimestamp = ts;
                }
            }
            if (deleteColumn != null) {
                // A row version cannot cross a family delete marker by definition. This means
                // min timestamp cannot be lower than the delete markers timestamp
                for (Cell cell : deleteColumn) {
                    ts = cell.getTimestamp();
                    if (ts < maxTimestamp) {
                        minTimestamp = ts + 1;
                        break;
                    }
                }
            }
        }

        /**
         * This is used for Phoenix level compaction
         */
        private void getNextRowVersionTimestamps(List<Cell> row, byte[] columnFamily) {
            maxTimestamp = 0;
            minTimestamp = Long.MAX_VALUE;
            Cell deleteFamily = null;
            long ts;
            // The next row version is formed by the first cell of each column. Similarly, the min
            // max timestamp of the cells of a row version is determined by looking at just first
            // cell of the columns
            for (Cell cell : row) {
                ts = cell.getTimestamp();
                if ((cell.getType() == Cell.Type.DeleteFamily ||
                        cell.getType() == Cell.Type.DeleteFamilyVersion) &&
                        CellUtil.matchingFamily(cell, columnFamily)) {
                    deleteFamily = cell;
                }
                if (maxTimestamp < ts) {
                    maxTimestamp = ts;
                }
                if (minTimestamp > ts) {
                    minTimestamp = ts;
                }
            }
            if (deleteFamily != null) {
                // A row version cannot cross a family delete marker by definition. This means
                // min timestamp cannot be lower than the delete markers timestamp
                ts = deleteFamily.getTimestamp();
                if (ts < maxTimestamp) {
                    minTimestamp = ts + 1;
                }
            }
        }
    }

    /**
     * HBaseLevelRowCompactor ensures that the cells of a given row are retained according to the
     * HBase data retention rules.
     *
     */
    class HBaseLevelRowCompactor {
        private RowContext rowContext = new RowContext();
        private CompactionRowVersion rowVersion = new CompactionRowVersion();
        private TTLTracker rowTracker;

        HBaseLevelRowCompactor(TTLTracker rowTracker) {
            this.rowTracker = rowTracker;
        }

        /**
         * A compaction row version includes the latest put cell versions from each column such that
         * the cell versions do not cross delete family markers. In other words, the compaction row
         * versions are built from cell versions that are all either before or after the next delete
         * family or delete family version maker if family delete markers exist. Also, when the cell
         * timestamps are ordered for a given row version, the difference between two subsequent
         * timestamps has to be less than the ttl value. This is taken care before calling
         * HBaseLevelRowCompactor#compact().
         *
         * Compaction row versions are disjoint sets. A compaction row version does not share a cell
         * version with the next compaction row version. A compaction row version includes at most
         * one cell version from a column.
         *
         * After creating the first compaction row version, we form the next compaction row version
         * from the remaining cell versions.
         *
         * Compaction row versions are used for compaction purposes to efficiently determine which
         * cell versions to retain based on the HBase data retention parameters.
         */
        class CompactionRowVersion {
            // Cells included in the row version
            List<Cell> cells = new ArrayList<>();
            // The timestamp of the row version
            long ts = 0;
            // The version of a row version. It is the minimum of the versions of the cells included
            // in the row version
            int version = 0;

            private void init() {
                cells.clear();
            }
            @Override
            public String toString() {
                StringBuilder output = new StringBuilder();
                output.append("Cell count: " + cells.size() + "\n");
                for (Cell cell : cells) {
                    output.append(cell + "\n");
                }
                output.append("ts:" + ts + " v:" + version);
                return output.toString();
            }
        }

        /**
         * Decide if compaction row versions inside the TTL window should be retained. The
         * versions are retained if one of the following conditions holds
         * 1. The compaction row version is alive and its version is less than VERSIONS
         * 2. The compaction row version is deleted and KeepDeletedCells is not FALSE
         *
         */
        private void retainInsideTTLWindow(CompactionRowVersion rowVersion, RowContext rowContext,
                List<Cell> retainedCells) {
            if (rowContext.familyDeleteMarker == null
                    && rowContext.familyVersionDeleteMarker == null) {
                // The compaction row version is alive
                if (rowVersion.version < maxVersion) {
                    // Rule 1
                    retainCells(rowVersion, rowContext, retainedCells);
                }
            } else {
                // Deleted
                if (rowVersion.version < maxVersion && keepDeletedCells != KeepDeletedCells.FALSE) {
                    // Retain based on rule 2
                    retainCells(rowVersion, rowContext, retainedCells);
                    rowContext.retainFamilyDeleteMarker(retainedCells);
                }
            }
        }

        /**
         * Decide if compaction row versions outside the TTL window should be retained. The
         * versions are retained if one of the following conditions holds
         *
         * 1. Live row versions less than MIN_VERSIONS are retained
         * 2. Delete row versions whose delete markers are inside the TTL window and
         *    KeepDeletedCells is TTL are retained
         */
        private void retainOutsideTTLWindow(CompactionRowVersion rowVersion, RowContext rowContext,
                List<Cell> retainedCells) {
            if (rowContext.familyDeleteMarker == null
                    && rowContext.familyVersionDeleteMarker == null) {
                // Live compaction row version
                if (rowVersion.version < minVersion) {
                    // Rule 1
                    retainCells(rowVersion, rowContext, retainedCells);
                }
            } else {
                // Deleted compaction row version
                if (keepDeletedCells == KeepDeletedCells.TTL
                        && rowContext.familyDeleteMarker != null
                        && rowContext.familyDeleteMarker.getTimestamp() > rowContext.getTtlWindowStart()) {
                    // Rule 2
                    retainCells(rowVersion, rowContext, retainedCells);
                    rowContext.retainFamilyDeleteMarker(retainedCells);
                }
            }
        }

        private void retainCells(CompactionRowVersion rowVersion, RowContext rowContext,
                List<Cell> retainedCells) {
            if (rowContext.columnDeleteMarkers == null) {
                retainedCells.addAll(rowVersion.cells);
                return;
            }
            for (Cell cell : rowVersion.cells) {
                rowContext.retainCell(cell, retainedCells, keepDeletedCells, rowContext.getTtlWindowStart());
            }
        }

        /**
         * Form the next compaction row version by picking (removing) the first cell from each
         * column. Put cells are used to form the next compaction row version. Delete markers
         * are added to the row context which are processed to decide which row versions
         * or cell version to delete.
         */
        private void formNextCompactionRowVersion(LinkedList<LinkedList<Cell>> columns,
                RowContext rowContext, List<Cell> retainedCells) {
            rowVersion.init();
            rowContext.getNextRowVersionTimestamps(columns, storeColumnFamily);
            rowVersion.ts = rowContext.maxTimestamp;
            for (LinkedList<Cell> column : columns) {
                Cell cell = column.getFirst();
                if (column.getFirst().getTimestamp() < rowContext.minTimestamp) {
                    continue;
                }
                if (cell.getType() == Cell.Type.DeleteFamily) {
                    if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                        rowContext.familyDeleteMarker = cell;
                        column.removeFirst();
                        break;
                    }
                    continue;
                }
                else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
                    if (cell.getTimestamp() == rowVersion.ts) {
                        rowContext.familyVersionDeleteMarker = cell;
                        column.removeFirst();
                        break;
                    }
                    continue;
                }
                column.removeFirst();
                if (cell.getType() == Cell.Type.DeleteColumn ||
                        cell.getType() == Cell.Type.Delete) {
                    rowContext.addColumnDeleteMarker(cell);
                    continue;
                }
                rowVersion.cells.add(cell);
            }
            if (rowVersion.cells.isEmpty()) {
                return;
            }
            rowVersion.version = rowContext.version++;
            if (rowVersion.ts >= rowContext.getTtlWindowStart()) {
                retainInsideTTLWindow(rowVersion, rowContext, retainedCells);
            } else {
                retainOutsideTTLWindow(rowVersion, rowContext, retainedCells);
            }
        }

        private void formCompactionRowVersions(LinkedList<LinkedList<Cell>> columns,
                List<Cell> result) {
            rowContext.init();
            rowTracker.setRowContext(rowContext);
            while (!columns.isEmpty()) {
                formNextCompactionRowVersion(columns, rowContext, result);
                // Remove the columns that are empty
                Iterator<LinkedList<Cell>> iterator = columns.iterator();
                while (iterator.hasNext()) {
                    LinkedList<Cell> column = iterator.next();
                    if (column.isEmpty()) {
                        iterator.remove();
                    }
                }
            }
        }

        /**
         * Group the cells that are ordered lexicographically into columns based on
         * the pair of family name and column qualifier. While doing that also add the delete
         * markers to a separate list.
         */
        private void formColumns(List<Cell> result, LinkedList<LinkedList<Cell>> columns) {
            Cell currentColumnCell = null;
            LinkedList<Cell> currentColumn = null;
            for (Cell cell : result) {
                if (currentColumnCell == null) {
                    currentColumn = new LinkedList<>();
                    currentColumnCell = cell;
                    currentColumn.add(cell);
                } else if (!CellUtil.matchingColumn(cell, currentColumnCell)) {
                    columns.add(currentColumn);
                    currentColumn = new LinkedList<>();
                    currentColumnCell = cell;
                    currentColumn.add(cell);
                } else {
                    currentColumn.add(cell);
                }
            }
            if (currentColumn != null) {
                columns.add(currentColumn);
            }
        }

        /**
         * Compacts a single row at the HBase level. The result parameter is the input row and
         * modified to be the output of the compaction.
         */
        private void compact(List<Cell> result) {
            if (result.isEmpty()) {
                return;
            }
            LinkedList<LinkedList<Cell>> columns = new LinkedList<>();
            formColumns(result, columns);
            result.clear();
            formCompactionRowVersions(columns, result);
        }
    }

    /**
     * PhoenixLevelRowCompactor ensures that the cells of the latest row version and the
     * row versions that are visible through the max lookback window are retained including delete
     * markers placed after these cells. This is the complete set of cells that Phoenix
     * needs for its queries. Beyond these cells, HBase retention rules may require more
     * cells to be retained. These cells are identified by the HBase level compaction implemented
     * by HBaseLevelRowCompactor.
     *
     */
    class PhoenixLevelRowCompactor {
        private RowContext rowContext = new RowContext();
        List<Cell> lastRowVersion = new ArrayList<>();
        List<Cell> emptyColumn = new ArrayList<>();
        List<Cell> phoenixResult = new ArrayList<>();
        List<Cell> trimmedRow = new ArrayList<>();
        List<Cell> trimmedEmptyColumn = new ArrayList<>();
        private TTLTracker rowTracker;

        PhoenixLevelRowCompactor(TTLTracker rowTracker) {
            this.rowTracker = rowTracker;
        }

        /**
         * The cells of the row (i.e., result) read from HBase store are lexicographically ordered
         * for tables using the key part of the cells which includes row, family, qualifier,
         * timestamp and type. The cells belong of a column are ordered from the latest to
         * the oldest. The method leverages this ordering and groups the cells into their columns
         * based on the pair of family name and column qualifier.
         *
         * The cells within the max lookback window except the once at the lower edge of the
         * max lookback window (the last row of the max lookback window) are retained immediately.
         *
         * This method also returned the remaining cells (outside the max lookback window) of
         * the empty colum
         */
        private void getLastRowVersionInMaxLookbackWindow(List<Cell> result,
                List<Cell> lastRowVersion, List<Cell> retainedCells, List<Cell> emptyColumn) {
            Cell currentColumnCell = null;
            boolean isEmptyColumn = false;
            for (Cell cell : result) {
                long maxLookbackWindowStart = rowTracker.getRowContext().getMaxLookbackWindowStart();
                if (cell.getTimestamp() > maxLookbackWindowStart) {
                    retainedCells.add(cell);
                    continue;
                }
                if (!major && cell.getType() != Cell.Type.Put) {
                    retainedCells.add(cell);
                }
                if (currentColumnCell == null ||
                        !CellUtil.matchingColumn(cell, currentColumnCell)) {
                    currentColumnCell = cell;
                    isEmptyColumn = ScanUtil.isEmptyColumn(cell, emptyCF, emptyCQ);
                    if ((cell.getType() != Cell.Type.Delete
                            && cell.getType() != Cell.Type.DeleteColumn)
                            || cell.getTimestamp() == maxLookbackWindowStart) {
                        // Include only delete family markers and put cells
                        // The last row version can also be the cells with timestamp
                        // same as timestamp of start of max lookback window
                        lastRowVersion.add(cell);
                    }
                } else if (isEmptyColumn) {
                    // We only need to keep one cell for every column for the last row version.
                    // So here we just form the empty column beyond the last row version.
                    // Empty column needs to be collected during minor compactions also
                    // else we will see partial row expiry.
                    emptyColumn.add(cell);
                }
            }
        }

        /**
         * Close the gap between the two timestamps, max and min, with the minimum number of cells
         * from the input list such that the timestamp difference between two cells should
         * not more than ttl. The cells that are used to close the gap are added to the output
         * list. The input list is a list of empty cells in decreasing order of timestamp.
         */
        private void closeGap(long max, long min, long ttl, List<Cell> input, List<Cell> output) {
            int  previous = -1;
            long ts;
            for (Cell cell : input) {
                ts = cell.getTimestamp();
                if (ts >= max) {
                    previous++;
                    continue;
                }
                if (previous == -1 && max - ts > ttl) {
                    // Means even the first empty cells in the input list which is closest to
                    // max timestamp can't close the gap. So, gap can't be closed by empty cells at all.
                    break;
                }
                if (max - ts > ttl) {
                    max = input.get(previous).getTimestamp();
                    output.add(input.remove(previous));
                    if (max - min > ttl) {
                        closeGap(max, min, ttl, input, output);
                    }
                    return;
                }
                previous++;
            }
            if (previous > -1 && max - min > ttl) {
                // This covers the case we need to retain the last empty cell in the input list. The close gap
                // algorithm is such that if we need to retain the i th empty cell in the input list then we
                // will get to know that once we are iterating on i+1 th empty cell. So, to retain last empty cell
                // in input list we need to check the min timestamp.
                output.add(input.remove(previous));
            }
        }

        /**
         * Retains minimum empty cells needed during minor compaction to not loose data/partial row expiry
         * on next major compaction.
         * @param emptyColumn Empty column cells in decreasing order of timestamp.
         * @param retainedCells Cells to be retained.
         */
        private void retainEmptyCellsInMinorCompaction(List<Cell> emptyColumn, List<Cell> retainedCells) {
            if (emptyColumn.isEmpty()) {
                return;
            }
            else if (familyCount == 1 || localIndex) {
                // We are compacting empty column family store and its single column family so
                // just need to retain empty cells till min timestamp of last row version. Can't
                // minimize the retained empty cells further as we don't know actual TTL during
                // minor compactions.
                long minRowTimestamp = rowContext.minTimestamp;
                for (Cell emptyCell: emptyColumn) {
                    if (emptyCell.getTimestamp() > minRowTimestamp) {
                        retainedCells.add(emptyCell);
                    }
                }
                return;
            }
            // For multi-column family, w/o doing region level scan we can't put a bound on timestamp
            // till which we should retain the empty cells. The empty cells can be needed to close the gap
            // b/w empty column family cell and non-empty column family cell.
            retainedCells.addAll(emptyColumn);
        }

        /**
         * Retain the last row version visible through the max lookback window
         */
        private void retainCellsOfLastRowVersion(List<Cell> lastRow,
                List<Cell> emptyColumn, List<Cell> retainedCells) {
            if (lastRow.isEmpty()) {
                return;
            }
            rowContext.init();
            rowTracker.setRowContext(rowContext);
            long ttl = rowContext.getTTL();
            rowContext.getNextRowVersionTimestamps(lastRow, storeColumnFamily);
            Cell firstCell = lastRow.get(0);
            if (firstCell.getType() == Cell.Type.DeleteFamily ||
                    firstCell.getType() == Cell.Type.DeleteFamilyVersion) {
                if (firstCell.getTimestamp() >= rowContext.maxTimestamp) {
                    // This means that the row version outside the max lookback window is
                    // deleted and thus should not be visible to the scn queries
                    return;
                }
            }

            if (major && compactionTime - rowContext.maxTimestamp > maxLookbackInMillis + ttl) {
                // Only do this check for major compaction as for minor compactions we don't expire cells.
                // The row version should not be visible via the max lookback window. Nothing to do
                return;
            }
            retainedCells.addAll(lastRow);
            // If the gap between two back to back mutations is more than ttl then the older
            // mutation will be considered expired and masked. If the length of the time range of
            // a row version is not more than ttl, then we know the cells covered by the row
            // version are not apart from each other more than ttl and will not be masked.
            if (major && rowContext.maxTimestamp - rowContext.minTimestamp <= ttl) {
                // Skip this check for minor compactions as we don't compute actual TTL for
                // minor compactions and don't expire cells.
                return;
            }
            // The quick time range check did not pass. We need get at least one empty cell to cover
            // the gap so that the row version will not be masked by PhoenixTTLRegionScanner.
            if (emptyColumn.isEmpty()) {
                return;
            }
            else if (! major) {
                retainEmptyCellsInMinorCompaction(emptyColumn, retainedCells);
                return;
            }
            int size = lastRow.size();
            long tsArray[] = new long[size];
            int i = 0;
            for (Cell cell : lastRow) {
                tsArray[i++] = cell.getTimestamp();
            }
            Arrays.sort(tsArray);
            for (i = size - 1; i > 0; i--) {
                if (tsArray[i] - tsArray[i - 1] > ttl) {
                    closeGap(tsArray[i], tsArray[i - 1], ttl, emptyColumn, retainedCells);
                }
            }
        }

        /**
         * For a CDC index, we retain all cells within the max lookback window as opposed to
         * retaining all row versions visible through max lookback window we do for other tables
         */
        private boolean retainCellsForCDCIndex(List<Cell> result, List<Cell> retainedCells) {
            for (Cell cell : result) {
                if (cell.getTimestamp() >= rowTracker.getRowContext().getMaxLookbackWindowStart()) {
                    retainedCells.add(cell);
                }
            }
            return true;
        }

        /**
         * The retained cells includes the cells that are visible through the max lookback
         * window and the additional empty column cells that are needed to reduce large time
         * between the cells of the last row version.
         */
        private boolean retainCellsForMaxLookback(List<Cell> result, boolean regionLevel,
                List<Cell> retainedCells) {

            lastRowVersion.clear();
            emptyColumn.clear();
            if (isCDCIndex) {
                return retainCellsForCDCIndex(result, retainedCells);
            }
            getLastRowVersionInMaxLookbackWindow(result, lastRowVersion, retainedCells,
                    emptyColumn);
            if (lastRowVersion.isEmpty()) {
                return true;
            }
            if (!major) {
                // We do not expire cells for minor compaction and memstore flushes
                retainCellsOfLastRowVersion(lastRowVersion, emptyColumn, retainedCells);
                return true;
            }
            long ttl = rowTracker.getRowContext().getTTL();
            long maxTimestamp = 0;
            long minTimestamp = Long.MAX_VALUE;
            long ts;
            for (Cell cell : lastRowVersion) {
                ts =cell.getTimestamp();
                if (ts > maxTimestamp) {
                    maxTimestamp = ts;
                }
                ts = cell.getTimestamp();
                if (ts < minTimestamp) {
                    minTimestamp = ts;
                }
            }
            if (compactionTime - maxTimestamp > maxLookbackInMillis + ttl) {
                if (!emptyCFStore && !regionLevel) {
                    // The row version is more than maxLookbackInMillis + ttl old. We cannot decide
                    // if we should retain it with the store level compaction when the current
                    // store is not the empty column family store.
                    return false;
                }
                return true;
            }
            // If the time gap between two back to back mutations is more than ttl then we know
            // that the row is expired within the time gap.
            if (maxTimestamp - minTimestamp > ttl) {
                if ((familyCount > 1 && !regionLevel && !localIndex)) {
                    // When there are more than one column family for a given table and a row
                    // version constructed at the store level covers a time span larger than ttl,
                    // we need region level compaction to see if the other stores have more cells
                    // for any of these large time gaps. A store level compaction may incorrectly
                    // remove some cells due to a large time gap which may not there at the region
                    // level.
                    return false;
                }
                // We either have one column family or are doing region level compaction. In both
                // case, we can safely trim the cells beyond the first time gap larger ttl.
                // Here we are interested in the gaps between the cells of the last row version
                // amd thus we need to examine the gaps between these cells and the empty column.
                // Please note that empty column is always updated for every mutation and so we
                // just need empty column cells for the gap analysis.
                int size = lastRowVersion.size();
                size += emptyColumn.size();
                long tsArray[] = new long[size];
                int i = 0;
                for (Cell cell : lastRowVersion) {
                    tsArray[i++] = cell.getTimestamp();
                }
                for (Cell cell : emptyColumn) {
                    tsArray[i++] = cell.getTimestamp();
                }
                Arrays.sort(tsArray);
                boolean gapFound = false;
                // Since timestamps are sorted in ascending order, traverse them in reverse order
                for (i = size - 1; i > 0; i--) {
                    if (tsArray[i] - tsArray[i - 1] > ttl) {
                        minTimestamp = tsArray[i];
                        gapFound = true;
                        break;
                    }
                }
                if (gapFound) {
                    trimmedRow.clear();
                    for (Cell cell : lastRowVersion) {
                        if (cell.getTimestamp() >= minTimestamp) {
                            trimmedRow.add(cell);
                        }
                    }
                    lastRowVersion = trimmedRow;
                    trimmedEmptyColumn.clear();;
                    for (Cell cell : emptyColumn) {
                        if (cell.getTimestamp() >= minTimestamp) {
                            trimmedEmptyColumn.add(cell);
                        }
                    }
                    emptyColumn = trimmedEmptyColumn;
                }
            }
            retainCellsOfLastRowVersion(lastRowVersion, emptyColumn, retainedCells);
            return true;
        }
        private void removeDuplicates(List<Cell> input, List<Cell> output) {
            Cell previousCell = null;
            for (Cell cell : input) {
                if (previousCell == null ||
                        cell.getTimestamp() != previousCell.getTimestamp() ||
                        cell.getType() != previousCell.getType() ||
                        !CellUtil.matchingColumn(cell, previousCell)) {
                    output.add(cell);
                }
                previousCell = cell;
            }
        }
        /**
         * Compacts a single row at the Phoenix level. The result parameter is the input row and
         * modified to be the output of the compaction process.
         */
        private void compact(List<Cell> result, boolean regionLevel) throws IOException {
            if (result.isEmpty()) {
                return;
            }
            phoenixResult.clear();
            rowTracker.setTTL(result.get(0));
            // For multi-CF case, always do region level scan for empty CF store during major compaction else
            // we could end-up removing some empty cells which are needed to close the gap b/w empty CF cell and
            // non-empty CF cell to prevent partial row expiry. This can happen when last row version of non-empty
            // CF cell outside max lookback window is older than last row version of empty CF cell.
            if (major && familyCount > 1 && ! localIndex && emptyCFStore && ! regionLevel) {
                compactRegionLevel(result, phoenixResult);
            }
            else if (!retainCellsForMaxLookback(result, regionLevel, phoenixResult)) {
                if (familyCount == 1 || regionLevel) {
                    throw new RuntimeException("UNEXPECTED");
                }
                phoenixResult.clear();
                compactRegionLevel(result, phoenixResult);
            }
            if (maxVersion == 1
                    && (!major
                        || (minVersion == 0 && keepDeletedCells == KeepDeletedCells.FALSE))) {
                // We need Phoenix level compaction only
                Collections.sort(phoenixResult, CellComparator.getInstance());
                result.clear();
                removeDuplicates(phoenixResult, result);
                phoenixLevelOnly = true;
                return;
            }
            // We may need to retain more cells, and so we need to run HBase level compaction
            // too. The result of two compactions will be merged and duplicate cells are removed.
            int phoenixResultSize = phoenixResult.size();
            List<Cell> hbaseResult = new ArrayList<>(result);
            hBaseLevelRowCompactor.compact(hbaseResult);
            phoenixResult.addAll(hbaseResult);
            Collections.sort(phoenixResult, CellComparator.getInstance());
            result.clear();
            removeDuplicates(phoenixResult, result);
            if (result.size() > phoenixResultSize) {
                LOGGER.debug("HBase level compaction retained " +
                        (result.size() - phoenixResultSize) + " more cells");
            }
        }

        private int compareTypes(Cell a, Cell b) {
            Cell.Type aType = a.getType();
            Cell.Type bType = b.getType();

            if (aType == bType) {
                return 0;
            }
            if (aType == Cell.Type.DeleteFamily) {
                return -1;
            }
            if (bType == Cell.Type.DeleteFamily) {
                return 1;
            }
            if (aType == Cell.Type.DeleteFamilyVersion) {
                return -1;
            }
            if (bType == Cell.Type.DeleteFamilyVersion) {
                return 1;
            }
            if (aType == Cell.Type.DeleteColumn) {
                return -1;
            }
            return 1;
        }

        private int compare(Cell a, Cell b) {
            int result;
            result = Bytes.compareTo(a.getFamilyArray(), a.getFamilyOffset(),
                    a.getFamilyLength(),
                    b.getFamilyArray(), b.getFamilyOffset(), b.getFamilyLength());
            if (result != 0) {
                return result;
            }
            result = Bytes.compareTo(a.getQualifierArray(), a.getQualifierOffset(),
                    a.getQualifierLength(),
                    b.getQualifierArray(), b.getQualifierOffset(), b.getQualifierLength());
            if (result != 0) {
                return result;
            }
            if (a.getTimestamp() > b.getTimestamp()) {
                return -1;
            }
            if (a.getTimestamp() < b.getTimestamp()) {
                return 1;
            }
            return compareTypes(a, b);
        }

        /**
         * The generates the intersection of regionResult and input. The result is the resulting
         * intersection.
         */
        private void trimRegionResult(List<Cell> regionResult, List<Cell> input,
                List<Cell> result) {
            if (regionResult.isEmpty()) {
                return;
            }
            int index = 0;
            int size = regionResult.size();
            int compare;
            for (Cell originalCell : input) {
                Cell regionCell = regionResult.get(index);
                compare = compare(originalCell, regionCell);
                while (compare > 0) {
                    index++;
                    if (index == size) {
                        break;
                    }
                    regionCell = regionResult.get(index);
                    compare = compare(originalCell, regionCell);
                }
                if (compare == 0) {
                    result.add(originalCell);
                    index++;
                }
                if (index == size) {
                    break;
                }
            }
        }

        /**
         * This is used only when the Phoenix level compaction cannot be done at the store level.
         */
        private void compactRegionLevel(List<Cell> input, List<Cell> result) throws IOException {
            byte[] rowKey = CellUtil.cloneRow(input.get(0));
            Scan scan = new Scan();
            scan.setRaw(true);
            scan.readAllVersions();
            // compaction + 1 because the upper limit of the time range is not inclusive
            scan.setTimeRange(0, compactionTime + 1);
            scan.withStartRow(rowKey, true);
            scan.withStopRow(rowKey, true);
            RegionScanner scanner = region.getScanner(scan);

            List<Cell> regionResult = new ArrayList<>(result.size());
            scanner.next(regionResult);
            scanner.close();
            Collections.sort(regionResult, CellComparator.getInstance());
            compact(regionResult, true);
            result.clear();
            trimRegionResult(regionResult, input, result);
        }
    }
}
