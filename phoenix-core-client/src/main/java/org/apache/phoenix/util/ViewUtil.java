/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.phoenix.coprocessorclient.MetaDataEndpointImplConstants;
import org.apache.phoenix.thirdparty.com.google.common.base.Objects;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnNameTrackingExpressionCompiler;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.coprocessorclient.WhereConstantParser;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.MIN_SPLITTABLE_SYSTEM_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PARENT_TENANT_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.schema.PTableImpl.getColumnsToClone;
import static org.apache.phoenix.util.PhoenixRuntime.CURRENT_SCN_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

public class ViewUtil {

    private static final Logger logger = LoggerFactory.getLogger(ViewUtil.class);

    /**
     * Find all the descendant views of a given table or view in a depth-first fashion.
     * Note that apart from scanning the {@code parent->child } links, we also validate each view
     * by trying to resolve it.
     * Use {@link ViewUtil#findAllRelatives(Table, byte[], byte[], byte[], LinkType,
     * TableViewFinderResult)} if you want to find other links and don't care about orphan results.
     *
     * @param sysCatOrsysChildLink Table corresponding to either SYSTEM.CATALOG or SYSTEM.CHILD_LINK
     * @param serverSideConfig server-side configuration
     * @param tenantId tenantId of the view (null if it is a table or global view)
     * @param schemaName schema name of the table/view
     * @param tableOrViewName name of the table/view
     * @param clientTimeStamp client timestamp
     * @param findJustOneLegitimateChildView if true, we are only interested in knowing if there is
     *                                       at least one legitimate child view, so we return early.
     *                                       If false, we want to find all legitimate child views
     *                                       and all orphan views (views that no longer exist)
     *                                       stemming from this table/view and all of its legitimate
     *                                       child views.
     *
     * @return a Pair where the first element is a list of all legitimate child views (or just 1
     * child view in case findJustOneLegitimateChildView is true) and where the second element is
     * a list of all orphan views stemming from this table/view and all of its legitimate child
     * views (in case findJustOneLegitimateChildView is true, this list will be incomplete since we
     * are not interested in it anyhow)
     *
     * @throws IOException thrown if there is an error scanning SYSTEM.CHILD_LINK or SYSTEM.CATALOG
     * @throws SQLException thrown if there is an error getting a connection to the server or an
     * error retrieving the PTable for a child view
     */
    public static Pair<List<PTable>, List<TableInfo>> findAllDescendantViews(
            Table sysCatOrsysChildLink, Configuration serverSideConfig, byte[] tenantId,
            byte[] schemaName, byte[] tableOrViewName, long clientTimeStamp,
            boolean findJustOneLegitimateChildView)
            throws IOException, SQLException {
        List<PTable> legitimateChildViews = new ArrayList<>();
        List<TableInfo> orphanChildViews = new ArrayList<>();

        findAllDescendantViews(sysCatOrsysChildLink, serverSideConfig, tenantId, schemaName,
                tableOrViewName, clientTimeStamp, legitimateChildViews, orphanChildViews,
                findJustOneLegitimateChildView);
        return new Pair<>(legitimateChildViews, orphanChildViews);
    }

    private static void findAllDescendantViews(Table sysCatOrsysChildLink,
            Configuration serverSideConfig, byte[] parentTenantId, byte[] parentSchemaName,
            byte[] parentTableOrViewName, long clientTimeStamp, List<PTable> legitimateChildViews,
            List<TableInfo> orphanChildViews, boolean findJustOneLegitimateChildView)
            throws IOException, SQLException {
        TableViewFinderResult currentResult =
                findImmediateRelatedViews(sysCatOrsysChildLink, parentTenantId, parentSchemaName,
                        parentTableOrViewName, LinkType.CHILD_TABLE, clientTimeStamp);
        for (TableInfo viewInfo : currentResult.getLinks()) {
            byte[] viewTenantId = viewInfo.getTenantId();
            byte[] viewSchemaName = viewInfo.getSchemaName();
            byte[] viewName = viewInfo.getTableName();
            PTable view;
            Properties props = new Properties();
            if (viewTenantId != null) {
                props.setProperty(TENANT_ID_ATTRIB, Bytes.toString(viewTenantId));
            }
            if (clientTimeStamp != HConstants.LATEST_TIMESTAMP) {
                props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(clientTimeStamp));
            }
            try (PhoenixConnection connection =
                    QueryUtil.getConnectionOnServer(props, serverSideConfig)
                            .unwrap(PhoenixConnection.class)) {
                try {
                    view = connection.getTableNoCache(
                            SchemaUtil.getTableName(viewSchemaName, viewName));
                } catch (TableNotFoundException ex) {
                    logger.error("Found an orphan parent->child link keyed by this parent."
                            + " Parent Tenant Id: '" + Bytes.toString(parentTenantId)
                            + "'. Parent Schema Name: '" + Bytes.toString(parentSchemaName)
                            + "'. Parent Table/View Name: '" + Bytes.toString(parentTableOrViewName)
                            + "'. The child view which could not be resolved has ViewInfo: '"
                            + viewInfo + "'.", ex);
                    orphanChildViews.add(viewInfo);
                    // Prune orphan branches
                    continue;
                }

                if (isLegitimateChildView(view, parentSchemaName, parentTableOrViewName)) {
                    legitimateChildViews.add(view);
                    // return early since we're only interested in knowing if there is at least one
                    // valid child view
                    if (findJustOneLegitimateChildView) {
                        break;
                    }
                    // Note that we only explore this branch if the current view is a legitimate
                    // child view, else we ignore it and move on to the next potential child view
                    findAllDescendantViews(sysCatOrsysChildLink, serverSideConfig,
                            viewInfo.getTenantId(), viewInfo.getSchemaName(),
                            viewInfo.getTableName(), clientTimeStamp, legitimateChildViews,
                            orphanChildViews, findJustOneLegitimateChildView);
                } else {
                    logger.error("Found an orphan parent->child link keyed by this parent."
                            + " Parent Tenant Id: '" + Bytes.toString(parentTenantId)
                            + "'. Parent Schema Name: '" + Bytes.toString(parentSchemaName)
                            + "'. Parent Table/View Name: '" + Bytes.toString(parentTableOrViewName)
                            + "'. There currently exists a legitimate view of the same name which"
                            + " is not a descendant of this table/view. View Info: '" + viewInfo
                            + "'. Ignoring this view and not counting it as a child view.");
                    // Prune unrelated view branches left around due to orphan parent->child links
                }
            }
        }
    }

    private static boolean isLegitimateChildView(PTable view, byte[] parentSchemaName,
            byte[] parentTableOrViewName) {
        return view != null && view.getParentSchemaName() != null &&
                view.getParentTableName() != null &&
                (Arrays.equals(view.getParentSchemaName().getBytes(), parentSchemaName) &&
                        Arrays.equals(view.getParentTableName().getBytes(), parentTableOrViewName));
    }

    /**
     * Returns relatives in a breadth-first fashion. Note that this is not resilient to orphan
     * linking rows and we also do not try to resolve any of the views to ensure they are valid.
     * Use {@link ViewUtil#findAllDescendantViews(Table, Configuration, byte[], byte[], byte[],
     * long, boolean)} if you are only interested in {@link LinkType#CHILD_TABLE} and need to be
     * resilient to orphan linking rows.
     *
     * @param sysCatOrsysChildLink Table corresponding to either SYSTEM.CATALOG or SYSTEM.CHILD_LINK
     * @param tenantId tenantId of the key (null if it is a table or global view)
     * @param schema schema name to use in the key
     * @param table table/view name to use in the key
     * @param linkType link type
     * @param result containing all linked entities
     *
     * @throws IOException thrown if there is an error scanning SYSTEM.CHILD_LINK or SYSTEM.CATALOG
     */
    public static void findAllRelatives(Table sysCatOrsysChildLink, byte[] tenantId, byte[] schema,
            byte[] table, PTable.LinkType linkType, TableViewFinderResult result)
            throws IOException {
        findAllRelatives(sysCatOrsysChildLink, tenantId, schema, table, linkType,
                HConstants.LATEST_TIMESTAMP, result);
    }

    private static void findAllRelatives(Table sysCatOrsysChildLink, byte[] tenantId, byte[] schema,
            byte[] table, PTable.LinkType linkType, long timestamp, TableViewFinderResult result)
            throws IOException {
        TableViewFinderResult currentResult = findImmediateRelatedViews(sysCatOrsysChildLink,
                tenantId, schema, table, linkType, timestamp);
        result.addResult(currentResult);
        for (TableInfo viewInfo : currentResult.getLinks()) {
            findAllRelatives(sysCatOrsysChildLink, viewInfo.getTenantId(), viewInfo.getSchemaName(),
                    viewInfo.getTableName(), linkType, timestamp, result);
        }
    }

    /**
     * Runs a scan on SYSTEM.CATALOG or SYSTEM.CHILD_LINK to get the immediate related tables/views.
     */
    protected static TableViewFinderResult findImmediateRelatedViews(Table sysCatOrsysChildLink,
            byte[] tenantId, byte[] schema, byte[] table, PTable.LinkType linkType, long timestamp)
            throws IOException {
        if (linkType==PTable.LinkType.INDEX_TABLE || linkType==PTable.LinkType.EXCLUDED_COLUMN) {
            throw new IllegalArgumentException("findAllRelatives does not support link type "
                    + linkType);
        }
        byte[] key = SchemaUtil.getTableKey(tenantId, schema, table);
		Scan scan = MetaDataUtil.newTableRowsScan(key, MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                timestamp);
        SingleColumnValueFilter linkFilter = new SingleColumnValueFilter(TABLE_FAMILY_BYTES,
                LINK_TYPE_BYTES, CompareOperator.EQUAL,
                linkType.getSerializedValueAsByteArray());
        linkFilter.setFilterIfMissing(true);
        scan.setFilter(linkFilter);
        scan.addColumn(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES);
        if (linkType==PTable.LinkType.PARENT_TABLE)
            scan.addColumn(TABLE_FAMILY_BYTES, PARENT_TENANT_ID_BYTES);
        if (linkType==PTable.LinkType.PHYSICAL_TABLE)
            scan.addColumn(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
        List<TableInfo> tableInfoList = Lists.newArrayList();
        try (ResultScanner scanner = sysCatOrsysChildLink.getScanner(scan))  {
            for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                byte[][] rowKeyMetaData = new byte[5][];
                byte[] viewTenantId = null;
                getVarChars(result.getRow(), 5, rowKeyMetaData);
                if (linkType==PTable.LinkType.PARENT_TABLE) {
                    viewTenantId = result.getValue(TABLE_FAMILY_BYTES, PARENT_TENANT_ID_BYTES);
                } else if (linkType==PTable.LinkType.CHILD_TABLE) {
                    viewTenantId = rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
                } else if (linkType==PTable.LinkType.VIEW_INDEX_PARENT_TABLE) {
                    viewTenantId = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
                } 
                else if (linkType==PTable.LinkType.PHYSICAL_TABLE &&
                        result.getValue(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES)!=null) {
                    // do not links from indexes to their physical table
                    continue;
                }
                byte[] viewSchemaName = SchemaUtil.getSchemaNameFromFullName(
                        rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX])
                        .getBytes(StandardCharsets.UTF_8);
                byte[] viewName = SchemaUtil.getTableNameFromFullName(
                        rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX])
                        .getBytes(StandardCharsets.UTF_8);
                tableInfoList.add(new TableInfo(viewTenantId, viewSchemaName, viewName));
            }
            return new TableViewFinderResult(tableInfoList);
        } 
    }

    public static TableViewFinderResult findChildViews(PhoenixConnection connection, String tenantId, String schema,
                                                       String tableName) throws IOException, SQLException {
        // Find child views
        TableViewFinderResult childViewsResult = new TableViewFinderResult();
        ReadOnlyProps readOnlyProps = connection.getQueryServices().getProps();
        for (int i=0; i<2; i++) {
            try (Table sysCatOrSysChildLinkTable = connection.getQueryServices()
                    .getTable(SchemaUtil.getPhysicalName(
                            i==0 ? SYSTEM_CHILD_LINK_NAME_BYTES : SYSTEM_CATALOG_TABLE_BYTES,
                            readOnlyProps).getName())) {
                byte[] tenantIdBytes = tenantId != null ? tenantId.getBytes() : null;
                ViewUtil.findAllRelatives(sysCatOrSysChildLinkTable, tenantIdBytes,
                        schema == null?null:schema.getBytes(),
                        tableName.getBytes(), PTable.LinkType.CHILD_TABLE, childViewsResult);
                break;
            } catch (TableNotFoundException ex) {
                // try again with SYSTEM.CATALOG in case the schema is old
                if (i == 1) {
                    // This means even SYSTEM.CATALOG was not found, so this is bad, rethrow
                    throw ex;
                }
            }
        }
        return childViewsResult;
    }

    /**
     * Check metadata to find if a given table/view has any immediate child views. Note that this
     * is not resilient to orphan {@code parent->child } links.
     * @param sysCatOrsysChildLink For older (pre-4.15.0) clients, we look for child links inside
     *                             SYSTEM.CATALOG, otherwise we look for them inside
     *                             SYSTEM.CHILD_LINK
     * @param tenantId tenantId
     * @param schemaName table schema name
     * @param tableName table name
     * @param timestamp passed client-side timestamp
     * @return true if the given table has at least one child view
     * @throws IOException thrown if there is an error scanning SYSTEM.CHILD_LINK or SYSTEM.CATALOG
     */
    public static boolean hasChildViews(Table sysCatOrsysChildLink, byte[] tenantId,
            byte[] schemaName, byte[] tableName, long timestamp) throws IOException {
        byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
        Scan scan = MetaDataUtil.newTableRowsScan(key, MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                timestamp);
        SingleColumnValueFilter linkFilter =
                new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES,
                        CompareOperator.EQUAL,
                        LinkType.CHILD_TABLE.getSerializedValueAsByteArray()) {
                    // if we found a row with the CHILD_TABLE link type we are done and can
                    // terminate the scan
                    @Override
                    public boolean filterAllRemaining() {
                        return matchedColumn;
                    }
                };
        linkFilter.setFilterIfMissing(true);
        scan.setFilter(linkFilter);
        scan.addColumn(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES);
        try (ResultScanner scanner = sysCatOrsysChildLink.getScanner(scan)) {
            Result result = scanner.next();
            return result!=null; 
        }
    }

    /**
     * Determines whether we should use SYSTEM.CATALOG or SYSTEM.CHILD_LINK to find
     * {@code parent->child } links i.e. {@link LinkType#CHILD_TABLE}.
     * If the client is older than 4.15.0 and the SYSTEM.CHILD_LINK table does not exist, we use
     * the SYSTEM.CATALOG table. In all other cases, we use the SYSTEM.CHILD_LINK table.
     * This is required for backwards compatibility.
     * @param clientVersion client version
     * @param conf server-side configuration
     * @return name of the system table to be used
     * @throws SQLException thrown if there is an error connecting to the server
     */
    public static TableName getSystemTableForChildLinks(int clientVersion,
                                                        Configuration conf) throws SQLException, IOException {
        byte[] fullTableName = SYSTEM_CHILD_LINK_NAME_BYTES;
        if (clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG) {

            try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(conf).
                    unwrap(PhoenixConnection.class)) {
                connection.getTableNoCache(SYSTEM_CHILD_LINK_NAME);
            } catch (TableNotFoundException e) {
                // If this is an old client and the CHILD_LINK table doesn't exist i.e. metadata
                // hasn't been updated since there was never a connection from a 4.15 client
                fullTableName = SYSTEM_CATALOG_NAME_BYTES;
            } catch (SQLException e) {
                logger.error("Error getting a connection on the server : " + e);
                throw e;
            }
        }
        return SchemaUtil.getPhysicalTableName(fullTableName, conf);
    }
    public static boolean isDivergedView(PTable view) {
        return view.getBaseColumnCount() == QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;
    }

    public static boolean isViewDiverging(PColumn columnToDelete, PTable view,
            long clientVersion) {
        // If we are dropping a column from a pre-4.15 client, the only way to know if the
        // view is diverging is by comparing the base column count
        return !isDivergedView(view) && (clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG ?
                columnToDelete.getPosition() < view.getBaseColumnCount() :
                columnToDelete.isDerived());
    }

    /**
     * Adds indexes of the parent table to inheritedIndexes if the index contains all required
     * columns
     */
    public static void addIndexesFromParent(PhoenixConnection connection, PTable view,
            PTable parentTable, List<PTable> inheritedIndexes) throws SQLException {
        List<PTable> parentTableIndexes = parentTable.getIndexes();
        for (PTable index : parentTableIndexes) {
            boolean containsAllReqdCols = true;
            // Ensure that all columns required to create index exist in the view too,
            // since view columns may be removed.
            IndexMaintainer indexMaintainer = index.getIndexMaintainer(parentTable, connection);
            // Check that the columns required for the index pk are present in the view
            Set<Pair<String, String>> indexedColInfos = indexMaintainer.getIndexedColumnInfo();
            for (Pair<String, String> colInfo : indexedColInfos) {
                try {
                    String colFamily = colInfo.getFirst();
                    String colName = colInfo.getSecond();
                    if (colFamily == null) {
                        view.getColumnForColumnName(colName);
                    } else {
                        view.getColumnFamily(colFamily).getPColumnForColumnName(colName);
                    }
                } catch (ColumnNotFoundException e) {
                    containsAllReqdCols = false;
                    break;
                }
            }

            // Ensure that constant columns (i.e. columns matched in the view WHERE clause)
            // all exist in the index on the parent table.
            for (PColumn col : view.getColumns()) {
                if (col.isViewReferenced() || col.getViewConstant() != null) {
                    try {
                        // It'd be possible to use a local index that doesn't have all view
                        // constants, but the WHERE clause for the view statement (which is added to
                        // the index below) would fail to compile.
                        String indexColumnName = IndexUtil.getIndexColumnName(col);
                        index.getColumnForColumnName(indexColumnName);
                    } catch (ColumnNotFoundException e1) {
                        PColumn indexCol = null;
                        try {
                            String cf = col.getFamilyName()!=null ?
                                    col.getFamilyName().getString() : null;
                            String colName = col.getName().getString();
                            if (cf != null) {
                                indexCol = parentTable.getColumnFamily(cf)
                                        .getPColumnForColumnName(colName);
                            }
                            else {
                                indexCol = parentTable.getColumnForColumnName(colName);
                            }
                        } catch (ColumnNotFoundException e2) {
                            // Ignore this index and continue with others
                            containsAllReqdCols = false;
                            break;
                        }
                        if (indexCol.getViewConstant()==null || Bytes.compareTo(
                                indexCol.getViewConstant(), col.getViewConstant())!=0) {
                            containsAllReqdCols = false;
                            break;
                        }
                    }
                }
            }
            if (containsAllReqdCols) {
                // Tack on view statement to index to get proper filtering for view
                String viewStatement = IndexUtil.rewriteViewStatement(connection, index,
                        parentTable, view.getViewStatement());
                PName modifiedIndexName = PNameFactory.newName(view.getName().getString()
                        + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR +
                        index.getName().getString());
                // add the index table with a new name so that it does not conflict with the
                // existing index table and set update cache frequency to that of the view
                if (Objects.equal(viewStatement, index.getViewStatement())) {
                    inheritedIndexes.add(index);
                } else {
                    inheritedIndexes.add(PTableImpl.builderWithColumns(index,
                            getColumnsToClone(index))
                            .setTableName(modifiedIndexName)
                            .setViewStatement(viewStatement)
                            .setUpdateCacheFrequency(view.getUpdateCacheFrequency())
                            .setTenantId(view.getTenantId())
                            .setPhysicalNames(Collections.singletonList(index.getPhysicalName()))
                            .build());
                }
            }
        }
    }

    public static PTable addDerivedColumnsAndIndexesFromAncestors(PhoenixConnection connection,
        PTable table) throws SQLException {
        List<PTable> ancestorList = Lists.newArrayList(table);
        //First generate a list of tables from child to base table. First element will be the
        //ultimate descendant, last element will be the base table.
        PName parentName = table.getParentName();
        while (parentName != null && parentName.getString().length() > 0) {
            PTable currentTable = ancestorList.get(ancestorList.size() -1);
            String parentTableName = SchemaUtil.getTableName(currentTable.getParentSchemaName(),
                currentTable.getParentTableName()).getString();
            PTable parentTable;
            try {
                parentTable = connection.getTable(parentTableName);
            } catch (TableNotFoundException tnfe) {
                //check to see if there's a tenant-owned parent
                parentTable = connection.getTable(table.getTenantId().getString(), parentTableName);
            }
            ancestorList.add(parentTable);
            parentName = parentTable.getParentName();
        }
        //now add the columns from all ancestors up from the base table to the top-most view
        if (ancestorList.size() > 1) {
            for (int k = ancestorList.size() -2; k >= 0; k--) {
                ancestorList.set(k, addDerivedColumnsAndIndexesFromParent(connection,
                    ancestorList.get(k), ancestorList.get(k +1)));
            }
            return ancestorList.get(0);
        } else {
            return table;
        }
    }
    /**
     * Inherit all indexes and columns from the parent 
     * @return table with inherited columns and indexes
     */
    public static PTable addDerivedColumnsAndIndexesFromParent(PhoenixConnection connection,
            PTable table, PTable parentTable) throws SQLException {
        PTable pTable = addDerivedColumnsFromParent(connection, table, parentTable);
        boolean hasIndexId = table.getViewIndexId() != null;
        // For views :
        if (!hasIndexId) {
            // 1. need to resolve the views's own indexes so that any columns added by ancestors
            // are included
            List<PTable> allIndexes = Lists.newArrayList();
            if (pTable !=null && pTable.getIndexes() !=null && !pTable.getIndexes().isEmpty()) {
                for (PTable viewIndex : pTable.getIndexes()) {
                    PTable resolvedViewIndex = ViewUtil.addDerivedColumnsAndIndexesFromParent(
                            connection, viewIndex, pTable);
                    if (resolvedViewIndex!=null)
                        allIndexes.add(resolvedViewIndex);
                }
            }
            
            // 2. include any indexes from ancestors that can be used by this view
            List<PTable> inheritedIndexes = Lists.newArrayList();
            addIndexesFromParent(connection, pTable, parentTable, inheritedIndexes);
            allIndexes.addAll(inheritedIndexes);
            if (!allIndexes.isEmpty()) {
                pTable = PTableImpl.builderWithColumns(pTable, getColumnsToClone(pTable))
                        .setIndexes(allIndexes).build();
            }
        }
        return pTable;
    }

    /**
     * Inherit all columns from the parent unless it's an excluded column.
     * If the same column is present in the parent and child (for table metadata created before
     * PHOENIX-3534) we choose the child column over the parent column
     * @return table with inherited columns
     */
    public static PTable addDerivedColumnsFromParent(PhoenixConnection connection,
        PTable view, PTable parentTable)
        throws SQLException {
        return addDerivedColumnsFromParent(connection, view, parentTable, true);
    }
    /**
     * Inherit all columns from the parent unless it's an excluded column.
     * If the same column is present in the parent and child (for table metadata created before
     * PHOENIX-3534) we choose the child column over the parent column
     * @return table with inherited columns
     */
    public static PTable addDerivedColumnsFromParent(PhoenixConnection connection,
        PTable view, PTable parentTable,
        boolean recalculateBaseColumnCount)
            throws SQLException {
        // combine columns for view and view indexes
        boolean hasIndexId = view.getViewIndexId() != null;
        boolean isSalted = view.getBucketNum() != null;
        boolean isDiverged = isDivergedView(view);
        boolean isDivergedViewCreatedPre4_15 = isDiverged;
        List<PColumn> allColumns = Lists.newArrayList();
        List<PColumn> excludedColumns = Lists.newArrayList();
        // add my own columns first in reverse order
        List<PColumn> myColumns = view.getColumns();
        // skip salted column as it will be created automatically
        myColumns = myColumns.subList(isSalted ? 1 : 0, myColumns.size());
        for (int i = myColumns.size() - 1; i >= 0; i--) {
            PColumn pColumn = myColumns.get(i);
            if (pColumn.isExcluded()) {
                // Diverged views created pre-4.15 will not have EXCLUDED_COLUMN linking rows
                isDivergedViewCreatedPre4_15 = false;
                excludedColumns.add(pColumn);
            }
            allColumns.add(pColumn);
        }

        // initialize map from with indexed expression to list of required data columns
        // then remove the data columns that have not been dropped, so that we get the columns that
        // have been dropped
        Map<PColumn, List<String>> indexRequiredDroppedDataColMap =
                Maps.newHashMapWithExpectedSize(view.getColumns().size());
        if (hasIndexId) {
            int indexPosOffset = (isSalted ? 1 : 0) + (view.isMultiTenant() ? 1 : 0) + 1;
            ColumnNameTrackingExpressionCompiler expressionCompiler =
                    new ColumnNameTrackingExpressionCompiler();
            for (int i = indexPosOffset; i < view.getPKColumns().size(); i++) {
                PColumn indexColumn = view.getPKColumns().get(i);
                try {
                    expressionCompiler.reset();
                    String expressionStr = IndexUtil.getIndexColumnExpressionStr(indexColumn);
                    ParseNode parseNode = SQLParser.parseCondition(expressionStr);
                    parseNode.accept(expressionCompiler);
                    indexRequiredDroppedDataColMap.put(indexColumn,
                            Lists.newArrayList(expressionCompiler.getDataColumnNames()));
                } catch (SQLException e) {
                    throw new RuntimeException(e); // Impossible
                }
            }
        }

        long maxTableTimestamp = view.getTimeStamp();
        long maxDDLTimestamp = view.getLastDDLTimestamp() != null ? view.getLastDDLTimestamp() : 0L;
        int numPKCols = view.getPKColumns().size();
        // set the final table timestamp and DDL timestamp as the respective max timestamps of the
        // view/view index or its ancestors
        maxTableTimestamp = Math.max(maxTableTimestamp, parentTable.getTimeStamp());
        //Diverged views no longer inherit ddl timestamps from their ancestors because they don't
        // inherit column changes
        maxDDLTimestamp = Math.max(maxDDLTimestamp,
            parentTable.getLastDDLTimestamp() != null ? parentTable.getLastDDLTimestamp() : 0L);

        if (hasIndexId) {
            // add all pk columns of parent tables to indexes
            // skip salted column as it will be added from the base table columns
            int startIndex = parentTable.getBucketNum() != null ? 1 : 0;
            for (int index=startIndex; index<parentTable.getPKColumns().size(); index++) {
                PColumn pkColumn = parentTable.getPKColumns().get(index);
                // don't add the salt column of ancestor tables for view indexes, or deleted columns
                // or constant columns from the view where statement
                if (pkColumn.equals(SaltingUtil.SALTING_COLUMN) || pkColumn.isExcluded()
                        || pkColumn.getViewConstant()!=null) {
                    continue;
                }
                pkColumn = IndexUtil.getIndexPKColumn(++numPKCols, pkColumn);
                int existingColumnIndex = allColumns.indexOf(pkColumn);
                if (existingColumnIndex == -1) {
                    allColumns.add(0, pkColumn);
                }
            }
            for (int j = 0; j < parentTable.getColumns().size(); j++) {
                PColumn tableColumn = parentTable.getColumns().get(j);
                if (tableColumn.isExcluded()) {
                    continue;
                }
                String dataColumnName = tableColumn.getName().getString();
                // remove from list of columns since it has not been dropped
                for (Map.Entry<PColumn, List<String>> entry : indexRequiredDroppedDataColMap
                        .entrySet()) {
                    entry.getValue().remove(dataColumnName);
                }
            }
        } else if (!isDivergedViewCreatedPre4_15) {
            // For diverged views created by a pre-4.15 client, we don't need to inherit columns
            // from its ancestors
            inheritColumnsFromParent(view, parentTable, isDiverged, excludedColumns, allColumns);
        }
        // at this point indexRequiredDroppedDataColMap only contain the columns required by a view
        // index that have dropped
        for (Map.Entry<PColumn, List<String>> entry : indexRequiredDroppedDataColMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                PColumn indexColumnToBeDropped = entry.getKey();
                if (SchemaUtil.isPKColumn(indexColumnToBeDropped)) {
                    // if an indexed column was dropped in an ancestor then we
                    // cannot use this index an more
                    // TODO figure out a way to actually drop this view index
                    return null;
                } else {
                    allColumns.remove(indexColumnToBeDropped);
                }
            }
        }

        List<PColumn> columnsToAdd = Lists.newArrayList();
        int position = isSalted ? 1 : 0;
        // allColumns contains the columns in the reverse order
        for (int i = allColumns.size() - 1; i >= 0; i--) {
            PColumn column = allColumns.get(i);
            if (view.getColumns().contains(column)) {
                // for views this column is not derived from an ancestor
                columnsToAdd.add(new PColumnImpl(column, position++));
            } else {
                columnsToAdd.add(new PColumnImpl(column, true, position++));
            }
        }
        // we need to include the salt column when setting the base table column count in order to
        // maintain b/w compatibility
        int baseTableColumnCount = view.getBaseColumnCount();
        if (recalculateBaseColumnCount) {
            baseTableColumnCount = isDiverged ?
                QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT :
                columnsToAdd.size() - myColumns.size() + (isSalted ? 1 : 0);
        }
        // Inherit view-modifiable properties from the parent table/view if the current view has
        // not previously modified this property
        long updateCacheFreq = (view.getType() != PTableType.VIEW ||
                view.hasViewModifiedUpdateCacheFrequency()) ?
                view.getUpdateCacheFrequency() : parentTable.getUpdateCacheFrequency();
        Boolean useStatsForParallelization = (view.getType() != PTableType.VIEW ||
                view.hasViewModifiedUseStatsForParallelization()) ?
                view.useStatsForParallelization() : parentTable.useStatsForParallelization();

        // When creating a PTable for views or view indexes, use the baseTable PTable for attributes
        // inherited from the physical base table.
        // if a TableProperty is not valid on a view we set it to the base table value
        // if a TableProperty is valid on a view and is not mutable on a view we set it to the base
        // table value
        // if a TableProperty is valid on a view and is mutable on a view, we use the value set
        // on the view if the view had previously modified the property, otherwise we propagate the
        // value from the base table (see PHOENIX-4763)
        PTable pTable = PTableImpl.builderWithColumns(view, columnsToAdd)
                .setImmutableRows(parentTable.isImmutableRows())
                .setDisableWAL(parentTable.isWALDisabled())
                .setMultiTenant(parentTable.isMultiTenant())
                .setStoreNulls(parentTable.getStoreNulls())
                .setTransactionProvider(parentTable.getTransactionProvider())
                .setAutoPartitionSeqName(parentTable.getAutoPartitionSeqName())
                .setAppendOnlySchema(parentTable.isAppendOnlySchema())
                .setBaseColumnCount(baseTableColumnCount)
                .setBaseTableLogicalName(parentTable.getBaseTableLogicalName())
                .setTimeStamp(maxTableTimestamp)
                .setExcludedColumns(ImmutableList.copyOf(excludedColumns))
                .setUpdateCacheFrequency(updateCacheFreq)
                .setUseStatsForParallelization(useStatsForParallelization)
                .setLastDDLTimestamp(maxDDLTimestamp)
                .build();
        pTable = WhereConstantParser.addViewInfoToPColumnsIfNeeded(pTable);

        return pTable;
    }

    /**
     * Inherit all columns from the parent unless it's an excluded column.
     * If the same column is present in the parent and child
     * (for table metadata created before PHOENIX-3534 or when
     * {@link org.apache.phoenix.query.QueryServices#ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK} is
     * enabled) we choose the latest column.
     * Note that we don't need to call this method for views created before 4.15 since they
     * already contain all the columns from their ancestors.
     * @param view PTable of the view
     * @param parentTable PTable of the view's parent
     * @param isDiverged true if it is a diverged view
     * @param excludedColumns list of excluded columns
     * @param allColumns list of all columns. Initially this contains just the columns in the view.
     *                   We will populate inherited columns by adding them to this list
     */
    static void inheritColumnsFromParent(PTable view, PTable parentTable,
            boolean isDiverged, List<PColumn> excludedColumns, List<PColumn> allColumns) {
        List<PColumn> currAncestorTableCols = PTableImpl.getColumnsToClone(parentTable);
        if (currAncestorTableCols != null) {
            // add the ancestor columns in reverse order so that the final column list
            // (reversed outside of this method invocation)
            // contains ancestor columns and then the view columns in the right order
            for (int j = currAncestorTableCols.size() - 1; j >= 0; j--) {
                PColumn ancestorColumn = currAncestorTableCols.get(j);
                // for diverged views we always include pk columns of the base table. We
                // have to include these pk columns to be able to support adding pk
                // columns to the diverged view.
                // We only include regular columns that were created before the view
                // diverged.
                if (isDiverged && ancestorColumn.getFamilyName() != null
                        && ancestorColumn.getTimestamp() > view.getTimeStamp()) {
                    // If this is a diverged view, the ancestor column is not a PK and the ancestor
                    // column was added after the view diverged, ignore this ancestor column.
                    continue;
                }
                // need to check if this ancestor column is in the list of excluded (dropped)
                // columns of the view
                int existingExcludedIndex = excludedColumns.indexOf(ancestorColumn);
                if (existingExcludedIndex != -1) {
                    // if it is, only exclude the ancestor column if it was created before the
                    // column was dropped in the view in order to handle the case where
                    // a base table column is dropped in a view, then dropped in the
                    // base table and then added back to the base table
                    if (ancestorColumn.getTimestamp() <= excludedColumns.get(existingExcludedIndex)
                            .getTimestamp()) {
                        continue;
                    }
                }
                // A diverged view from a pre-4.15 client won't ever go in this case since
                // isExcluded was introduced in 4.15. If this is a 4.15+ client, excluded columns
                // will be identifiable via PColumn#isExcluded()
                if (ancestorColumn.isExcluded()) {
                    excludedColumns.add(ancestorColumn);
                } else {
                    int existingColumnIndex = allColumns.indexOf(ancestorColumn);
                    if (existingColumnIndex != -1) {
                        // For non-diverged views, if the same column exists in a parent and child,
                        // we keep the latest column.
                        PColumn existingColumn = allColumns.get(existingColumnIndex);
                        if (!isDiverged && ancestorColumn.getTimestamp() >
                                existingColumn.getTimestamp()) {
                            allColumns.remove(existingColumnIndex);
                            // Remove the existing column and add the ancestor
                            // column at the end and make sure to mark it as
                            // derived
                            allColumns.add(new PColumnImpl(ancestorColumn, true,
                                    ancestorColumn.getPosition()));
                        } else {
                            // Since this is a column from the ancestor,
                            // mark it as derived
                            allColumns.set(existingColumnIndex,
                                    new PColumnImpl(existingColumn, true,
                                            existingColumn.getPosition()));
                        }
                    } else {
                        // Since this is a column from the ancestor,
                        // mark it as derived
                        allColumns.add(new PColumnImpl(ancestorColumn, true,
                                ancestorColumn.getPosition()));
                    }
                }
            }
        }
        // remove the excluded columns if the timestamp of the excludedColumn is newer
        for (PColumn excludedColumn : excludedColumns) {
            int index = allColumns.indexOf(excludedColumn);
            if (index != -1) {
                if (allColumns.get(index).getTimestamp() <= excludedColumn.getTimestamp()) {
                    allColumns.remove(excludedColumn);
                }
            }
        }
    }

    /**
     * See PHOENIX-4763. If we are modifying any table-level properties that are mutable on a view,
     * we mark these cells in SYSTEM.CATALOG with tags to indicate that this view property should
     * not be kept in-sync with the base table and so we shouldn't propagate the base table's
     * property value when resolving the view
     * @param tableMetaData list of mutations on the view
     * @param parent PTable of the parent or null
     */
    public static void addTagsToPutsForViewAlteredProperties(List<Mutation> tableMetaData,
            PTable parent, ExtendedCellBuilder extendedCellBuilder) {
        byte[] parentUpdateCacheFreqBytes = null;
        byte[] parentUseStatsForParallelizationBytes = null;
        byte[] parentPhoenixTTLBytes = null;
        if (parent != null) {
            parentUpdateCacheFreqBytes = new byte[PLong.INSTANCE.getByteSize()];
            PLong.INSTANCE.getCodec().encodeLong(parent.getUpdateCacheFrequency(),
                    parentUpdateCacheFreqBytes, 0);
            if (parent.useStatsForParallelization() != null) {
                parentUseStatsForParallelizationBytes =
                        PBoolean.INSTANCE.toBytes(parent.useStatsForParallelization());
            }
            parentPhoenixTTLBytes = new byte[PLong.INSTANCE.getByteSize()];
            PLong.INSTANCE.getCodec().encodeLong(parent.getPhoenixTTL(),
                    parentPhoenixTTLBytes, 0);
        }
        for (Mutation m: tableMetaData) {
            if (m instanceof Put) {
                MetaDataUtil.conditionallyAddTagsToPutCells((Put)m,
                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES,
                        extendedCellBuilder,
                        parentUpdateCacheFreqBytes,
                        MetaDataEndpointImplConstants.VIEW_MODIFIED_PROPERTY_BYTES);
                MetaDataUtil.conditionallyAddTagsToPutCells((Put)m,
                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION_BYTES,
                        extendedCellBuilder,
                        parentUseStatsForParallelizationBytes,
                        MetaDataEndpointImplConstants.VIEW_MODIFIED_PROPERTY_BYTES);
                MetaDataUtil.conditionallyAddTagsToPutCells((Put)m,
                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.PHOENIX_TTL_BYTES,
                        extendedCellBuilder,
                        parentPhoenixTTLBytes,
                        MetaDataEndpointImplConstants.VIEW_MODIFIED_PROPERTY_BYTES);
            }

        }
    }
}
