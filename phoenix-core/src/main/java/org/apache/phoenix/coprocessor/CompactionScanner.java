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
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.filter.RowKeyComparisonFilter.RowKeyTuple;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.matcher.RowKeyMatcher;
import org.apache.phoenix.util.matcher.TableTTLInfoCache;
import org.apache.phoenix.util.matcher.TableTTLInfo;
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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAMESPACE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_NOT_DEFINED;
import static org.apache.phoenix.query.QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;

/**
 * The store scanner that implements Phoenix TTL and Max Lookback. Phoenix overrides the
 * HBase implementation of data retention policies which is built at the cell level, and implements
 * its row level data retention within this store scanner.
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
    private static Map<String, Long> maxLookbackMap = new ConcurrentHashMap<>();
    private PhoenixLevelRowCompactor phoenixLevelRowCompactor;
    private HBaseLevelRowCompactor hBaseLevelRowCompactor;

    public CompactionScanner(RegionCoprocessorEnvironment env,
            Store store,
            InternalScanner storeScanner,
            long maxLookbackInMillis,
            PTable table) throws IOException {
        this.storeScanner = storeScanner;
        this.region = env.getRegion();
        this.store = store;
        this.env = env;
        this.emptyCF = SchemaUtil.getEmptyColumnFamily(table);
        this.emptyCQ = SchemaUtil.getEmptyColumnQualifier(table);
        compactionTime = EnvironmentEdgeManager.currentTimeMillis();
        String columnFamilyName = store.getColumnFamilyName();
        storeColumnFamily = columnFamilyName.getBytes();
        String tableName = region.getRegionInfo().getTable().getNameAsString();
        Long overriddenMaxLookback =
                maxLookbackMap.remove(tableName + SEPARATOR + columnFamilyName);
        maxLookbackInMillis = overriddenMaxLookback == null ?
                maxLookbackInMillis : Math.max(maxLookbackInMillis, overriddenMaxLookback);

        this.maxLookbackInMillis = maxLookbackInMillis;
        // The oldest scn is current time - maxLookbackInMillis. Phoenix sets the scan time range
        // for scn queries [0, scn). This means that the maxlookback size should be
        // maxLookbackInMillis + 1 so that the oldest scn does not return empty row
        this.maxLookbackWindowStart = maxLookbackInMillis == 0 ? compactionTime : compactionTime - (maxLookbackInMillis + 1);
        ColumnFamilyDescriptor cfd = store.getColumnFamilyDescriptor();
        this.minVersion = cfd.getMinVersions();
        this.maxVersion = cfd.getMaxVersions();
        this.keepDeletedCells = cfd.getKeepDeletedCells();
        familyCount = region.getTableDescriptor().getColumnFamilies().length;
        localIndex = columnFamilyName.startsWith(LOCAL_INDEX_COLUMN_FAMILY_PREFIX);
        emptyCFStore = familyCount == 1 || columnFamilyName.equals(Bytes.toString(emptyCF)) || localIndex;
        LOGGER.info(String.format("CompactionScanner params:- (" +
                        "physical-data-tablename = %s, compaction-tablename = %s, region = %s, " +
                        "start-key = %s, end-key = %s, " +
                        "emptyCF = %s, emptyCQ = %s, " +
                        "minVersion = %d, maxVersion = %d, keepDeletedCells = %s, " +
                        "familyCount = %d, localIndex = %s, emptyCFStore = %s, " +
                        "compactionTime = %d, maxLookbackWindowStart = %d, maxLookbackInMillis = %d)",
                table.getName().toString(), tableName, region.getRegionInfo().getEncodedName(),
                Bytes.toStringBinary(region.getRegionInfo().getStartKey()),
                Bytes.toStringBinary(region.getRegionInfo().getEndKey()),
                Bytes.toString(this.emptyCF), Bytes.toString(emptyCQ),
                this.minVersion, this.maxVersion, this.keepDeletedCells.name(),
                this.familyCount, this.localIndex, this.emptyCFStore,
                compactionTime, maxLookbackWindowStart, maxLookbackInMillis));

        // Initialize the tracker that computes the TTL for the compacting table.
        // The TTL tracker can be
        // simple (one single TTL for the table) when the compacting table is not Partitioned
        // complex when the TTL can vary per row when the compacting table is Partitioned.
        TTLTracker ttlTracker = createTTLTrackerFor(env, store, table);
        phoenixLevelRowCompactor = new PhoenixLevelRowCompactor(ttlTracker);
        hBaseLevelRowCompactor = new HBaseLevelRowCompactor(ttlTracker);

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
        // If VIEW TTL is not enabled then return TTL tracker for base HBase tables.
        // since TTL can be set only at the table level.
        if (!isViewTTLEnabled) {
            return new NonPartitionedTableTTLTracker(baseTable, store);
        }

        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        String compactionTableName = env.getRegion().getRegionInfo().getTable().getNameAsString();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(baseTable.getName().toString());
        String tableName = SchemaUtil.getTableNameFromFullName(baseTable.getName().toString());

        boolean isPartitionedViewIndexTable = false;
        if (compactionTableName.startsWith(MetaDataUtil.VIEW_INDEX_TABLE_PREFIX)) {
            isPartitionedViewIndexTable = true;
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
            // If there is atleast one child view for this table then it is a partitioned table.
            boolean isPartitioned = ViewUtil.hasChildViews(
                    childLinkHTable,
                    EMPTY_BYTE_ARRAY,
                    Bytes.toBytes(schemaName),
                    Bytes.toBytes(tableName),
                    currentTime);

            return isPartitioned ?
                    new PartitionedTableTTLTracker(baseTable, isSalted, isPartitionedViewIndexTable) :
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
        if (old != null && old < maxLookbackInMillis) {
            maxLookbackMap.put(tableName + SEPARATOR + columnFamilyName, maxLookbackInMillis);
        }
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        boolean hasMore = storeScanner.next(result);
        if (!result.isEmpty()) {
            phoenixLevelRowCompactor.compact(result, false);
        }
        return hasMore;
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }

    @Override
    public void close() throws IOException {
        storeScanner.close();
    }

    private enum MatcherType {
        VIEW_INDEXES, GLOBAL_VIEWS, TENANT_VIEWS
    }
    private class PartitionedTableRowKeyMatcher  {

        private boolean isViewIndexTable = false;
        private boolean isMultiTenant = false;
        private boolean isSalted = false;
        private RowKeyParser rowKeyParser;
        private PTable baseTable;
        private RowKeyMatcher globalViewMatcher;
        private RowKeyMatcher tenantViewMatcher;
        private RowKeyMatcher viewIndexMatcher;
        private TableTTLInfoCache viewIndexTTLCache;
        private TableTTLInfoCache globalViewTTLCache;
        private TableTTLInfoCache tenantViewTTLCache;

        public PartitionedTableRowKeyMatcher(
                PTable table,
                boolean isSalted,
                boolean isViewIndexTable) throws SQLException {
            this.baseTable = table;
            this.viewIndexTTLCache = new TableTTLInfoCache();
            this.globalViewTTLCache = new TableTTLInfoCache();
            this.tenantViewTTLCache = new TableTTLInfoCache();
            this.rowKeyParser = new RowKeyParser(baseTable);
            this.isViewIndexTable = isViewIndexTable || localIndex ;
            this.isSalted = isSalted;
            this.isMultiTenant = table.isMultiTenant();
            initializeMatchers();
        }

        // Initialize the various matchers
        private void initializeMatchers() throws SQLException {

            if (this.isViewIndexTable) {
                this.viewIndexMatcher = initializeMatcher(MatcherType.VIEW_INDEXES);
            } else if (this.isMultiTenant) {
                this.globalViewMatcher = initializeMatcher(MatcherType.GLOBAL_VIEWS);
                this.tenantViewMatcher = initializeMatcher(MatcherType.TENANT_VIEWS);
            } else {
                this.globalViewMatcher = initializeMatcher(MatcherType.GLOBAL_VIEWS);
            }
        }
        // Initialize matcher as per type
        private RowKeyMatcher initializeMatcher(MatcherType type) throws SQLException {
            List<TableTTLInfo> tableList;
            RowKeyMatcher matcher  = new RowKeyMatcher();
            String regionName = region.getRegionInfo().getEncodedName();
            String startTenantId = "";
            String endTenantId = "";
            switch (type) {
            case VIEW_INDEXES:
                tableList = getMatchPatternsForPartitionedTables(
                        this.baseTable.getName().getString(),
                        env.getConfiguration(),
                        false, false, true,
                        regionName, startTenantId, endTenantId);
                break;
            case GLOBAL_VIEWS:
                tableList = getMatchPatternsForPartitionedTables(
                        this.baseTable.getName().getString(),
                        env.getConfiguration(),
                        true, false, false,
                        regionName, startTenantId, endTenantId);
                break;
            case TENANT_VIEWS:
                try {
                    startTenantId = rowKeyParser.getStartTenantId();
                    endTenantId = rowKeyParser.getEndTenantId();
                    LOGGER.info(String.format("TenantId information: %s, %s",
                            startTenantId,
                            endTenantId));
                } catch (SQLException sqle) {
                    LOGGER.error(sqle.getMessage());
                }
                tableList = getMatchPatternsForPartitionedTables(
                        this.baseTable.getName().getString(),
                        env.getConfiguration(),
                        false, true, false,
                        regionName, startTenantId, endTenantId);

                break;
            default:
                tableList = new ArrayList<>();
                break;
            }

            tableList.forEach(m -> {
                if (m.getTTL() != TTL_NOT_DEFINED) {
                    // add the ttlInfo to the cache.
                    // each new/unique ttlInfo object added returns a unique tableId.
                    int tableId = -1;
                    switch (type) {
                    case VIEW_INDEXES:
                         tableId = viewIndexTTLCache.addTable(m);
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
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format("Updated %s : %s", type.toString(), m));
                    }

                }
            });
            return matcher;
        }

        // Match row key using the appropriate matcher
        private TableTTLInfo match(byte[] rowkey, int offset, MatcherType matcherType) {
            RowKeyMatcher matcher = null;
            if (this.isViewIndexTable && matcherType.compareTo(MatcherType.VIEW_INDEXES) == 0) {
                Integer tableId = this.viewIndexMatcher.match(rowkey, offset);
                return viewIndexTTLCache.getTableById(tableId);
            } else if (this.isMultiTenant && matcherType.compareTo(MatcherType.GLOBAL_VIEWS) == 0) {
                Integer tableId = this.globalViewMatcher.match(rowkey, offset);
                return globalViewTTLCache.getTableById(tableId);
            } else if (this.isMultiTenant && matcherType.compareTo(MatcherType.TENANT_VIEWS) == 0) {
                Integer tableId = this.tenantViewMatcher.match(rowkey, offset);
                return tenantViewTTLCache.getTableById(tableId);
            } else if (matcherType.compareTo(MatcherType.GLOBAL_VIEWS) == 0) {
                Integer tableId = this.globalViewMatcher.match(rowkey, offset);
                return globalViewTTLCache.getTableById(tableId);
            } else {
                return null;
            }
        }


        /// TODO : Needs optimization and remove logging
        private List<TableTTLInfo> getMatchPatternsForPartitionedTables(String physicalTableName,
                Configuration configuration, boolean globalViews, boolean tenantViews,
                boolean viewIndexes,
                String regionName,
                String startRegionKey, String endRegionKey) throws SQLException {

            List<TableTTLInfo> tableTTLInfoList = Lists.newArrayList();
            if (globalViews || viewIndexes) {
                Set<TableInfo> globalViewSet = getGlobalViews(physicalTableName, configuration);
                if (globalViewSet.size() > 0) {
                    getTTLInfo(physicalTableName, globalViewSet, configuration,
                            viewIndexes, tableTTLInfoList);
                }
            }

            if (tenantViews || viewIndexes) {
                Set<TableInfo> tenantViewSet = getTenantViews(physicalTableName, configuration,
                        regionName, startRegionKey, endRegionKey);
                if (tenantViewSet.size() > 0) {
                    getTTLInfo(physicalTableName, tenantViewSet, configuration,
                            viewIndexes, tableTTLInfoList);
                }
            }

            return tableTTLInfoList;
        }

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
                LOGGER.debug("globalViewSQL:" + globalViewSQL);
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

        // TODO Bound it by tenantId's from reqion boundaries
        // TODO Batch getTenantViews() to handle tenant explosions
        private  Set<TableInfo> getTenantViews(
                String physicalTableName,
                Configuration configuration,
                String regionName,
                String startRegionKey,
                String endRegionKey
        ) throws SQLException {

            Set<TableInfo> tenantViewSet = new HashSet<>();
            try (Connection serverConnection = QueryUtil.getConnectionOnServer(new Properties(),
                    configuration)) {
                // TODO: need to investigate why the SKIP_SCAN filter does not work
                String
                        tenantViewsSQLFormat =
                        "SELECT /*+ RANGE_SCAN */ TENANT_ID,TABLE_SCHEM,TABLE_NAME," +
                                "COLUMN_NAME AS PHYSICAL_TABLE_TENANT_ID, " +
                                "COLUMN_FAMILY AS PHYSICAL_TABLE_FULL_NAME " +
                                "FROM SYSTEM.CATALOG " +
                                "WHERE LINK_TYPE = 2 " +
                                "AND COLUMN_FAMILY = '%s' " +
                                "AND TENANT_ID IS NOT NULL " +
                                ((startRegionKey != null && startRegionKey.length() > 0) ? "AND TENANT_ID >= ? "  : "") +
                                ((endRegionKey != null && endRegionKey.length() > 0) ? "AND TENANT_ID < ? "  : "");

                String tenantViewSQL = String.format(tenantViewsSQLFormat, physicalTableName);
                LOGGER.debug("tenantViewSQL:" + tenantViewSQL);
                LOGGER.debug(String.format("getTenantViews region-name = %s, start-key = %s, end-key = %s",
                        regionName,
                        startRegionKey,
                        endRegionKey));

                try (PhoenixPreparedStatement tenantViewStmt = serverConnection.prepareStatement(
                        tenantViewSQL).unwrap(PhoenixPreparedStatement.class)) {
                    int paramPos = 1;
                    if (startRegionKey != null && startRegionKey.length() > 0) {
                        tenantViewStmt.setString(paramPos++, startRegionKey);
                    }
                    if (endRegionKey != null && endRegionKey.length() > 0) {
                        tenantViewStmt.setString(paramPos, endRegionKey);
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
                            LOGGER.debug("tableInfo: " + tableInfo);
                            tenantViewSet.add(tableInfo);
                        }
                    }
                }
            }
            return tenantViewSet;
        }

        private void getTTLInfo(String physicalTableName,
                Set<TableInfo> viewSet, Configuration configuration,
                boolean isIndexTable, List<TableTTLInfo> tableTTLInfoList)
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
            LOGGER.debug(
                    String.format("ViewsWithTTLSQL : %s", viewsWithTTLSQL));

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
                            int viewTTL = viewTTLRS.getInt("TTL");
                            byte[] rowKeyMatcher = viewTTLRS.getBytes("ROW_KEY_MATCHER");
                            byte[]
                                    tenantIdBytes =
                                    tid == null || tid.isEmpty() ? EMPTY_BYTE_ARRAY : tid.getBytes();

                            String fullTableName = SchemaUtil.getTableName(schem, tName);
                            Properties tenantProps = new Properties();
                            if (tid != null) {
                                tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tid);
                            }

                            if (isIndexTable) {
                                try (Connection
                                        tableConnection =
                                        QueryUtil.getConnectionOnServer(tenantProps, configuration)) {

                                    PTable
                                            pTable =
                                            PhoenixRuntime.getTableNoCache(
                                                    tableConnection, fullTableName);
                                    for (PTable index : pTable.getIndexes()) {
                                        LOGGER.debug(String.format(
                                                "index-name = %s, ttl = %d, row-key-prefix = %d",
                                                index.getName(), index.getTTL(),
                                                index.getViewIndexId()));
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
                                        LOGGER.debug(String.format(
                                                "index-name = %s, index-id-type = %s, " +
                                                        "index-id-bytes = %s",
                                                index.getName(), viewIndexIdType.getSqlTypeName(),
                                                Bytes.toStringBinary(viewIndexIdBytes)));

                                        tableTTLInfoList.add(
                                                new TableTTLInfo(pTable.getPhysicalName().getBytes(),
                                                        tenantIdBytes, index.getTableName().getBytes(),
                                                        viewIndexIdBytes, index.getTTL()));
                                    }

                                }
                            } else {
                                LOGGER.debug(
                                        String.format("table-name = %s, ttl = %d, row-key-prefix = %s",
                                                fullTableName, viewTTL,
                                                Bytes.toStringBinary(rowKeyMatcher)));
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


        public boolean isViewIndexTable() {
            return isViewIndexTable;
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

        public RowKeyMatcher getViewIndexMatcher() {
            return viewIndexMatcher;
        }

        public TableTTLInfoCache getViewIndexTTLCache() {
            return viewIndexTTLCache;
        }

        public TableTTLInfoCache getGlobalViewTTLCache() {
            return globalViewTTLCache;
        }

        public TableTTLInfoCache getTenantViewTTLCache() {
            return tenantViewTTLCache;
        }

        public int getNumGlobalEntries() {
            return globalViewMatcher == null ? 0 : globalViewMatcher.getNumEntries();
        }

        public int getNumTenantEntries() {
            return tenantViewMatcher == null ? 0 : tenantViewMatcher.getNumEntries();
        }

        public int getNumViewIndexEntries() {
            return viewIndexMatcher == null ? 0 : viewIndexMatcher.getNumEntries();
        }

        public int getNumTablesInGlobalCache() {
            return globalViewTTLCache == null ? 0 : globalViewTTLCache.getNumTablesInCache();
        }

        public int getNumTablesInTenantCache() {
            return tenantViewTTLCache == null ? 0 : tenantViewTTLCache.getNumTablesInCache();
        }

        public int getNumTablesInViewIndexCache() {
            return viewIndexTTLCache == null ? 0 : viewIndexTTLCache.getNumTablesInCache();
        }

        public int getNumTablesInCache() {
            int totalNumTables = 0;
            totalNumTables +=
                    globalViewTTLCache == null ? 0 : globalViewTTLCache.getNumTablesInCache();
            totalNumTables +=
                    tenantViewTTLCache == null ? 0 : tenantViewTTLCache.getNumTablesInCache();
            totalNumTables +=
                    viewIndexTTLCache == null ? 0 : viewIndexTTLCache.getNumTablesInCache();
            return totalNumTables;
        }

    }

    /**
     * The implementation classes will track TTL for various Phoenix Objects.
     * Tables - Partitioned (HBase Tables with Views and View-Indexes)
     *          and Non-Partitioned (Simple HBase Tables And Indexes)
     */
    private interface TTLTracker {
        void setTTL(Cell firstCell);
        RowContext getRowContext();
        void setRowContext(RowContext rowContext);
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
            this.rowContext = new RowContext();
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

        private boolean isViewIndexTable = false;
        private boolean isMultiTenant = false;
        private boolean isSalted = false;
        private int startingPKPosition;
        private PartitionedTableRowKeyMatcher tableRowKeyMatcher;

        public PartitionedTableTTLTracker(
                PTable table,
                boolean isSalted,
                boolean isViewIndexTable) {

            try {
                // Initialize the various matcher indexes
                this.tableRowKeyMatcher =
                        new PartitionedTableRowKeyMatcher(table, isSalted, isViewIndexTable);
                this.ttl = table.getTTL() != TTL_NOT_DEFINED ? table.getTTL() : DEFAULT_TTL;
                this.isViewIndexTable = isViewIndexTable || localIndex ;
                this.isSalted = isSalted;
                this.isMultiTenant = table.isMultiTenant();

                int startingPKPosition = 0;
                if (this.isMultiTenant && this.isSalted && this.isViewIndexTable) {
                    // case multi-tenanted, salted, is a view-index-table =>
                    // startingPKPosition = 1 skip the salt-byte and starting at the viewIndexId
                    startingPKPosition = 1;
                } else if (this.isMultiTenant && this.isSalted && !this.isViewIndexTable) {
                    // case multi-tenanted, salted, not a view-index-table  =>
                    // startingPKPosition = 2 skip salt byte + tenant-id to search the global space
                    // if above search returned no results
                    // then search using the following start position
                    // startingPKPosition = 1 skip salt-byte to search the tenant space
                    startingPKPosition = 2;
                } else if (this.isMultiTenant && !this.isSalted && this.isViewIndexTable) {
                    // case multi-tenanted, not-salted, is a view-index-table  =>
                    // startingPKPosition = 0, the first key will the viewIndexId
                    startingPKPosition = 0;
                } else if (this.isMultiTenant && !this.isSalted && !this.isViewIndexTable) {
                    // case multi-tenanted, not-salted, not a view-index-table  =>
                    // startingPKPosition = 1 skip tenant-id to search the global space
                    // if above search returned no results
                    // then search using the following start position
                    // startingPKPosition = 0 to search the tenant space
                    startingPKPosition = 1;
                } else if (!this.isMultiTenant && this.isSalted && this.isViewIndexTable) {
                    // case non-multi-tenanted, salted, index-table =>
                    // startingPKPosition = 1 skip salt-byte search using the viewIndexId
                    startingPKPosition = 1;
                } else if (!this.isMultiTenant && this.isSalted && !this.isViewIndexTable) {
                    // case non-multi-tenanted, salted, not a view-index-table =>
                    // start at the view-index-id position after skipping the salt byte
                    // startingPKPosition = 1 skip salt-byte
                    startingPKPosition = 1;
                } else if (!this.isMultiTenant && !this.isSalted && this.isViewIndexTable) {
                    // case non-multi-tenanted, not-salted, is a view-index-table =>
                    // startingPKPosition = 0 the first key will the viewIndexId
                    startingPKPosition = 0;
                } else {
                    // case non-multi-tenanted, not-salted, not a view-index-table  =>
                    // startingPKPosition = 0
                    startingPKPosition = 0;
                }

                this.startingPKPosition = startingPKPosition;
                LOGGER.info(String.format("PartitionedTableTTLTracker params:- " +
                                "region-name = %s, table-name = %s,  " +
                                "multi-tenant = %s, index-table = %s, salted = %s, " +
                                "default-ttl = %d, startingPKPosition = %d",
                        region.getRegionInfo().getEncodedName(),
                        region.getRegionInfo().getTable().getNameAsString(),
                        this.isMultiTenant,
                        this.isViewIndexTable,
                        this.isSalted,
                        this.ttl,
                        this.startingPKPosition));

            } catch (SQLException e) {
                LOGGER.error(String.format("Failed to read from catalog: " + e.getMessage()));
            } finally {
                LOGGER.info(String.format("PartitionedTableTTLTracker " +
                                "global-entries = %d, %d, " +
                                "tenant-entries = %d, %d, " +
                                "viewindex-entries = %d, %d, " +
                                "for region = %s",
                        tableRowKeyMatcher.getNumGlobalEntries(),
                        tableRowKeyMatcher.getNumTablesInGlobalCache(),
                        tableRowKeyMatcher.getNumTenantEntries(),
                        tableRowKeyMatcher.getNumTablesInTenantCache(),
                        tableRowKeyMatcher.getNumViewIndexEntries(),
                        tableRowKeyMatcher.getNumTablesInViewIndexCache(),
                        CompactionScanner.this.store.getRegionInfo().getEncodedName()));
            }

        }

        @Override
        public void setTTL(Cell firstCell) {

            boolean matched = false;
            TableTTLInfo tableTTLInfo = null;
            List<Integer> pkPositions = null;
            long rowTTLInSecs = ttl;
            long matchedOffset = -1;
            int pkPosition = startingPKPosition;
            try {
                if (tableRowKeyMatcher.getNumTablesInCache() == 0) {
                    // case: no views in the hierarchy had TTL set
                    // Use the default TTL
                    this.rowContext = new RowContext();
                    this.rowContext.setTTL(rowTTLInSecs);
                    return;
                }
                // pkPositions holds the byte offsets for the PKs of the base table
                // for the current row
                pkPositions = isViewIndexTable ?
                        (isSalted ?
                                Arrays.asList(0, 1) :
                                Arrays.asList(0)) :
                        tableRowKeyMatcher.getRowKeyParser().parsePKPositions(firstCell);
                // The startingPKPosition was initialized(constructor) in the following manner
                // case multi-tenant, salted, is-view-index-table  => startingPKPosition = 1
                // case multi-tenant, salted, not-view-index-table => startingPKPosition = 2
                // case multi-tenant, not-salted, is-view-index-table => startingPKPosition = 0
                // case multi-tenant, not-salted, not-view-index-table => startingPKPosition = 1
                // case non-multi-tenant, salted, is-view-index-table => startingPKPosition = 1
                // case non-multi-tenant, salted, not-view-index-table => startingPKPosition = 1
                // case non-multi-tenant, not-salted, is-view-index-table => startingPKPosition = 0
                // case non-multi-tenant, not-salted, not-view-index-table => startingPKPosition = 0
                int offset = pkPositions.get(pkPosition);
                byte[] rowKey = CellUtil.cloneRow(firstCell);
                Integer tableId = null;
                // Search using the starting offset (startingPKPosition offset)
                if (isViewIndexTable) {
                    // case index table
                    tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, MatcherType.VIEW_INDEXES);
                } else if (isMultiTenant) {
                    // case multi-tenant, non-index tables, global space
                    tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, MatcherType.GLOBAL_VIEWS);
                    if (tableTTLInfo == null) {
                        // search returned no results, determine the new pkPosition(offset) to use
                        // Search using the new offset
                        pkPosition = this.isSalted ? 1 : 0;
                        offset = pkPositions.get(pkPosition);
                        // case multi-tenant, non-index tables, tenant space
                        tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, MatcherType.TENANT_VIEWS);
                    }
                } else {
                    // case non-multi-tenant and non-index tables, global space
                    tableTTLInfo = tableRowKeyMatcher.match(rowKey, offset, MatcherType.GLOBAL_VIEWS);
                }
                //tableTTLInfo = ttlCache.getTableById(tableId);
                matched = tableTTLInfo != null;
                matchedOffset = matched ? offset : -1;
                rowTTLInSecs = matched ? tableTTLInfo.getTTL() : ttl; /* in secs */
                this.rowContext = new RowContext();
                this.rowContext.setTTL(rowTTLInSecs);
            } catch (Exception e) {
                LOGGER.error(String.format("Exception when visiting table: " + e.getMessage()));
            } finally {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("visiting row-key = %s, region = %s, " +
                                    "table-ttl-info=%s, " +
                                    "matched = %s, match-pattern = %s, " +
                                    "ttl = %d, matched-offset = %d, " +
                                    "pk-pos = %d, pk-pos-list = %s",
                            CellUtil.getCellKeyAsString(firstCell),
                            CompactionScanner.this.store.getRegionInfo().getEncodedName(),
                            matched ? tableTTLInfo : "NULL",
                            matched,
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
        private final RowKeyColumnExpression[] colExprs;
        private final List<PColumn> pkColumns;
        private final boolean isSalted;
        private final boolean isMultiTenant;

        public RowKeyParser(PTable table) {
            isSalted = table.getBucketNum() != null;
            isMultiTenant = table.isMultiTenant();

            pkColumns = table.getPKColumns();
            colExprs = new RowKeyColumnExpression[pkColumns.size()];
            for (int i = 0; i < pkColumns.size(); i++) {
                PColumn column = pkColumns.get(i);
                colExprs[i] = new RowKeyColumnExpression(column, new RowKeyValueAccessor(pkColumns, i));
            }
        }

        public List<Integer> parsePKPositions(Cell inputCell)  {
            RowKeyTuple inputTuple = new RowKeyTuple();
            inputTuple.setKey(inputCell.getRowArray(),
                    inputCell.getRowOffset(),
                    inputCell.getRowLength());

            int lastPos = 0;
            List<Integer> pkPositions = new ArrayList<>();
            pkPositions.add(lastPos);
            for (int i = 0; i < colExprs.length; i++) {
                RowKeyColumnExpression expr = colExprs[i];
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                expr.evaluate(inputTuple, ptr);
                int separatorLength = pkColumns.get(i).getDataType().isFixedWidth() ? 0 : 1;
                int endPos = lastPos + ptr.getLength() + separatorLength;
                pkPositions.add(endPos);
                lastPos = endPos;
            }
            return pkPositions;
        }

        public String getStartTenantId() throws SQLException {
            byte[] startKey = region.getRegionInfo().getStartKey();
            if (Bytes.compareTo(startKey, HConstants.EMPTY_BYTE_ARRAY) == 0 ) {
                return "";
            }

            Cell startKeyCell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                    .setRow(startKey)
                    .setFamily(emptyCF)
                    .setQualifier(emptyCQ)
                    .setTimestamp(EnvironmentEdgeManager.currentTimeMillis())
                    .setType(Cell.Type.Put)
                    .setValue(HConstants.EMPTY_BYTE_ARRAY)
                    .build();

            RowKeyTuple inputTuple = new RowKeyTuple();
            inputTuple.setKey(startKeyCell.getRowArray(),
                    startKeyCell.getRowOffset(),
                    startKeyCell.getRowLength());


            int tenantIdPosition = isSalted ? 1 : 0;
            RowKeyColumnExpression expr = colExprs[tenantIdPosition];
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            byte[] convertedValue;
            String tenantId = "";
            try {
                expr.evaluate(inputTuple, ptr);
                PDataType dataType = pkColumns.get(tenantIdPosition).getDataType();
                dataType.pad(ptr, expr.getMaxLength(), expr.getSortOrder());
                convertedValue = ByteUtil.copyKeyBytesIfNecessary(ptr);
                tenantId = dataType.toObject(convertedValue).toString();

            } catch(IllegalDataException ex) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TENANTID_IS_OF_WRONG_TYPE)
                        .build().buildException();
            }

//            String tenantId = pkColumns.get(tenantIdPosition).getDataType().toObject(ptr).toString();
            return tenantId;
        }
        public String getEndTenantId() throws SQLException {
            byte[] endKey = region.getRegionInfo().getEndKey();
            if (Bytes.compareTo(endKey, HConstants.EMPTY_BYTE_ARRAY) == 0 ) {
                return "";
            }

            Cell endKeyCell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                    .setRow(endKey)
                    .setFamily(emptyCF)
                    .setQualifier(emptyCQ)
                    .setTimestamp(EnvironmentEdgeManager.currentTimeMillis())
                    .setType(Cell.Type.Put)
                    .setValue(HConstants.EMPTY_BYTE_ARRAY)
                    .build();

            RowKeyTuple inputTuple = new RowKeyTuple();
            inputTuple.setKey(endKeyCell.getRowArray(),
                    endKeyCell.getRowOffset(),
                    endKeyCell.getRowLength());

            int tenantIdPosition = isSalted ? 1 : 0;
            RowKeyColumnExpression expr = colExprs[tenantIdPosition];
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            byte[] convertedValue;
            String tenantId = "";
            try {
                expr.evaluate(inputTuple, ptr);
                PDataType dataType = pkColumns.get(tenantIdPosition).getDataType();
                dataType.pad(ptr, expr.getMaxLength(), expr.getSortOrder());
                //convertedValue = ByteUtil.copyKeyBytesIfNecessary(ptr);
                tenantId = dataType.toObject(ptr).toString();

            } catch(IllegalDataException ex) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TENANTID_IS_OF_WRONG_TYPE)
                        .build().buildException();
            }

            //String tenantId = pkColumns.get(tenantIdPosition).getDataType().toObject(ptr).toString();
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
        List<Cell> columnDeleteMarkers = null;
        int version = 0;
        long maxTimestamp;
        long minTimestamp;
        long ttl;
        long ttlWindowStart;
        long maxLookbackWindowStartForRow;

        public void setTTL(long ttlInSecs) {
            this.ttl = ttlInSecs*1000;
            this.ttlWindowStart = ttlInSecs == HConstants.FOREVER ? 1 : compactionTime - ttl ;
            this.maxLookbackWindowStartForRow = Math.max(ttlWindowStart, maxLookbackWindowStart);
            LOGGER.info(String.format("RowContext:- (ttlWindowStart=%d, maxLookbackWindowStart=%d)",
                    ttlWindowStart, maxLookbackWindowStart));

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
            if (columnDeleteMarkers == null) {
                columnDeleteMarkers = new ArrayList<>();
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
                        // Delete is for deleting a specific cell version. Thus, it can be used
                        // to delete only one cell.
                        columnDeleteMarkers.remove(i);
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
                // A row version do not cross a family delete marker. This means
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
    }

    /**
     * HBaseLevelRowCompactor ensures that the cells of a given row are retained according to the
     * HBase data retention rules.
     *
     */
    class HBaseLevelRowCompactor {

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
         * 2. The compaction row version is deleted and KeepDeletedCells is TTL
         * 3. The compaction row version is deleted, its version is less than MIN_VERSIONS and
         * KeepDeletedCells is TRUE
         *
         */
        private void retainInsideTTLWindow(CompactionRowVersion rowVersion, RowContext rowContext,
                List<Cell> retainedCells) {
            if (rowContext.familyDeleteMarker == null && rowContext.familyVersionDeleteMarker == null) {
                // The compaction row version is alive
                if (rowVersion.version < maxVersion) {
                    // Rule 1
                    retainCells(rowVersion, rowContext, retainedCells);
                }
            } else {
                // Deleted
                if ((rowVersion.version < maxVersion && keepDeletedCells == KeepDeletedCells.TRUE) ||
                        keepDeletedCells == KeepDeletedCells.TTL) {
                    // Retain based on rule 2 or 3
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
                if (keepDeletedCells == KeepDeletedCells.TTL && (
                        (rowContext.familyVersionDeleteMarker != null &&
                                rowContext.familyVersionDeleteMarker.getTimestamp() > rowContext.getTtlWindowStart()) ||
                                (rowContext.familyDeleteMarker != null &&
                                        rowContext.familyDeleteMarker.getTimestamp() > rowContext.getTtlWindowStart())
                )) {
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
            CompactionRowVersion rowVersion = new CompactionRowVersion();
            rowContext.getNextRowVersionTimestamps(columns, storeColumnFamily);
            rowVersion.ts = rowContext.maxTimestamp;
            rowVersion.version = rowContext.version++;
            for (LinkedList<Cell> column : columns) {
                Cell cell = column.getFirst();
                if (column.getFirst().getTimestamp() < rowContext.minTimestamp) {
                    continue;
                }
                if (cell.getType() == Cell.Type.DeleteFamily) {
                    if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                        rowContext.familyDeleteMarker = cell;
                        column.removeFirst();
                    }
                    continue;
                }
                else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
                    if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                        rowContext.familyVersionDeleteMarker = cell;
                        column.removeFirst();
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
            if (rowVersion.ts >= rowContext.getTtlWindowStart()) {
                retainInsideTTLWindow(rowVersion, rowContext, retainedCells);
            } else {
                retainOutsideTTLWindow(rowVersion, rowContext, retainedCells);
            }
        }

        private void formCompactionRowVersions(LinkedList<LinkedList<Cell>> columns,
                List<Cell> result) {
            RowContext rowContext = new RowContext();
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
        private void formColumns(List<Cell> result, LinkedList<LinkedList<Cell>> columns,
                List<Cell> deleteMarkers) {
            Cell currentColumnCell = null;
            LinkedList<Cell> currentColumn = null;
            for (Cell cell : result) {
                if (cell.getType() != Cell.Type.Put) {
                    deleteMarkers.add(cell);
                }
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
            List<Cell> deleteMarkers = new ArrayList<>();
            formColumns(result, columns, deleteMarkers);
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
         * max lookback window are retained immediately and not included in the constructed columns.
         */
        private void getColumns(List<Cell> result, LinkedList<LinkedList<Cell>> columns,
                List<Cell> retainedCells) {
            Cell currentColumnCell = null;
            LinkedList<Cell> currentColumn = null;
            for (Cell cell : result) {
                if (cell.getTimestamp() > rowTracker.getRowContext().getMaxLookbackWindowStart()) {
                    retainedCells.add(cell);
                    continue;
                }
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
         * Close the gap between the two timestamps, max and min, with the minimum number of cells
         * from the input list such that the timestamp difference between two cells should
         * not more than ttl. The cells that are used to close the gap are added to the output
         * list.
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
                if (previous == -1) {
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
        }

        /**
         * Retain the last row version visible through the max lookback window
         */
        private void retainCellsOfLastRowVersion(LinkedList<LinkedList<Cell>> columns,
                List<Cell> retainedCells) {
            if (columns.isEmpty()) {
                return;
            }

            RowContext rowContext = new RowContext();
            rowTracker.setRowContext(rowContext);
            long ttl = rowContext.getTTL();
            rowContext.getNextRowVersionTimestamps(columns, storeColumnFamily);
            List<Cell> retainedPutCells = new ArrayList<>();
            for (LinkedList<Cell> column : columns) {
                Cell cell = column.getFirst();
                if (cell.getTimestamp() < rowContext.minTimestamp) {
                    continue;
                }
                if (cell.getType() == Cell.Type.Put) {
                    retainedPutCells.add(cell);
                } else if (cell.getType() == Cell.Type.DeleteFamily ||
                        cell.getType() == Cell.Type.DeleteFamilyVersion) {
                    if (cell.getTimestamp() >= rowContext.maxTimestamp) {
                        // This means that the row version outside the max lookback window is
                        // deleted and thus should not be visible to the scn queries
                        if (cell.getTimestamp() == rowTracker.getRowContext().getMaxLookbackWindowStart()) {
                            // Include delete markers at maxLookbackWindowStart
                            retainedCells.add(cell);
                        }
                        return;
                    }
                } else if (cell.getTimestamp() == rowTracker.getRowContext().getMaxLookbackWindowStart()) {
                    // Include delete markers at maxLookbackWindowStart
                    retainedCells.add(cell);
                }
            }
            if (compactionTime - rowContext.maxTimestamp > maxLookbackInMillis + ttl) {
                // The row version should not be visible via the max lookback window. Nothing to do
                return;
            }
            retainedCells.addAll(retainedPutCells);
            // If the gap between two back to back mutations is more than ttl then the older
            // mutation will be considered expired and masked. If the length of the time range of
            // a row version is not more than ttl, then we know the cells covered by the row
            // version are not apart from each other more than ttl and will not be masked.
            if (rowContext.maxTimestamp - rowContext.minTimestamp <= ttl) {
                return;
            }
            // The quick time range check did not pass. We need get at least one empty cell to cover
            // the gap so that the row version will not be masked by PhoenixTTLRegionScanner.
            List<Cell> emptyCellColumn = null;
            for (LinkedList<Cell> column : columns) {
                if (ScanUtil.isEmptyColumn(column.getFirst(), emptyCF, emptyCQ)) {
                    emptyCellColumn = column;
                    break;
                }
            }
            if (emptyCellColumn == null) {
                return;
            }
            int size = retainedPutCells.size();
            long tsArray[] = new long[size];
            int i = 0;
            for (Cell cell : retainedPutCells) {
                tsArray[i++] = cell.getTimestamp();
            }
            Arrays.sort(tsArray);
            for (i = size - 1; i > 0; i--) {
                if (tsArray[i] - tsArray[i - 1] > ttl) {
                    closeGap(tsArray[i], tsArray[i - 1], ttl, emptyCellColumn, retainedCells);
                }
            }
        }

        private boolean retainCellsForMaxLookback(List<Cell> result, boolean regionLevel,
                List<Cell> retainedCells) {

            long ttl = rowTracker.getRowContext().getTTL();
            LinkedList<LinkedList<Cell>> columns = new LinkedList<>();
            getColumns(result, columns, retainedCells);
            long maxTimestamp = 0;
            long minTimestamp = Long.MAX_VALUE;
            long ts;
            for (LinkedList<Cell> column : columns) {
                ts = column.getFirst().getTimestamp();
                if (ts > maxTimestamp) {
                    maxTimestamp = ts;
                }
                ts = column.getLast().getTimestamp();
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
                // case, we can safely trim the cells beyond the first time gap larger ttl
                int size = result.size();
                long tsArray[] = new long[size];
                int i = 0;
                for (Cell cell : result) {
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
                    List<Cell> trimmedResult = new ArrayList<>(size - i);
                    for (Cell cell : result) {
                        if (cell.getTimestamp() >= minTimestamp) {
                            trimmedResult.add(cell);
                        }
                    }
                    columns.clear();
                    retainedCells.clear();
                    getColumns(trimmedResult, columns, retainedCells);
                }
            }
            retainCellsOfLastRowVersion(columns, retainedCells);
            return true;
        }
        /**
         * Compacts a single row at the Phoenix level. The result parameter is the input row and
         * modified to be the output of the compaction process.
         */
        private void compact(List<Cell> result, boolean regionLevel) throws IOException {
            if (result.isEmpty()) {
                return;
            }
            rowTracker.setTTL(result.get(0));
            List<Cell> phoenixResult = new ArrayList<>(result.size());
            if (!retainCellsForMaxLookback(result, regionLevel, phoenixResult)) {
                if (familyCount == 1 || regionLevel) {
                    throw new RuntimeException("UNEXPECTED");
                }
                phoenixResult.clear();
                compactRegionLevel(result, phoenixResult);
            }
            if (maxVersion == 1 &&  minVersion == 0 && keepDeletedCells == KeepDeletedCells.FALSE) {
                // We need to Phoenix level compaction only
                Collections.sort(phoenixResult, CellComparator.getInstance());
                result.clear();
                result.addAll(phoenixResult);
                return;
            }
            // We may need to do retain more cells, and so we need to run HBase level compaction
            // too. The result of two compactions will be merged and duplicate cells are removed.
            int phoenixResultSize = phoenixResult.size();
            List<Cell> hbaseResult = new ArrayList<>(result);
            hBaseLevelRowCompactor.compact(hbaseResult);
            phoenixResult.addAll(hbaseResult);
            Collections.sort(phoenixResult, CellComparator.getInstance());
            result.clear();
            Cell previousCell = null;
            // Eliminate duplicates
            for (Cell cell : phoenixResult) {
                if (previousCell == null ||
                        cell.getTimestamp() != previousCell.getTimestamp() ||
                        cell.getType() != previousCell.getType() ||
                        !CellUtil.matchingColumn(cell, previousCell)) {
                    result.add(cell);
                }
                previousCell = cell;
            }

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
