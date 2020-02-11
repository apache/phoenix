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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnNameTrackingExpressionCompiler;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.TableInfo;
import org.apache.phoenix.coprocessor.WhereConstantParser;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.coprocessor.MetaDataProtocol.MIN_SPLITTABLE_SYSTEM_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PARENT_TENANT_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.schema.PTableImpl.getColumnsToClone;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

public class ViewUtil {

    private static final Logger logger = LoggerFactory.getLogger(ViewUtil.class);

    public static void findAllRelatives(Table sysCatOrsysChildLink, byte[] tenantId, byte[] schema, byte[] table,
        PTable.LinkType linkType, TableViewFinderResult result) throws IOException {
        findAllRelatives(sysCatOrsysChildLink, tenantId, schema, table, linkType, HConstants.LATEST_TIMESTAMP, result);
    }

    private static void findAllRelatives(Table sysCatOrsysChildLink, byte[] tenantId, byte[] schema, byte[] table,
        PTable.LinkType linkType, long timestamp, TableViewFinderResult result) throws IOException {
        TableViewFinderResult currentResult =
            findRelatedViews(sysCatOrsysChildLink, tenantId, schema, table, linkType, timestamp);
        result.addResult(currentResult);
        for (TableInfo viewInfo : currentResult.getLinks()) {
            findAllRelatives(sysCatOrsysChildLink, viewInfo.getTenantId(), viewInfo.getSchemaName(), viewInfo.getTableName(), linkType, timestamp, result);
        }
    }

    /**
     * Runs a scan on SYSTEM.CATALOG or SYSTEM.CHILD_LINK to get the related tables/views
     */
    private static TableViewFinderResult findRelatedViews(Table sysCatOrsysChildLink, byte[] tenantId, byte[] schema, byte[] table,
        PTable.LinkType linkType, long timestamp) throws IOException {
        if (linkType==PTable.LinkType.INDEX_TABLE || linkType==PTable.LinkType.EXCLUDED_COLUMN) {
            throw new IllegalArgumentException("findAllRelatives does not support link type "+linkType);
        }
        byte[] key = SchemaUtil.getTableKey(tenantId, schema, table);
		Scan scan = MetaDataUtil.newTableRowsScan(key, MetaDataProtocol.MIN_TABLE_TIMESTAMP, timestamp);
        SingleColumnValueFilter linkFilter =
            new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES, CompareFilter.CompareOp.EQUAL,
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
                else if (linkType==PTable.LinkType.PHYSICAL_TABLE && result.getValue(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES)!=null) {
                    // do not links from indexes to their physical table
                    continue;
                }
                byte[] viewSchemaName = SchemaUtil.getSchemaNameFromFullName(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
                byte[] viewName = SchemaUtil.getTableNameFromFullName(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
                tableInfoList.add(new TableInfo(viewTenantId, viewSchemaName, viewName));
            }
            return new TableViewFinderResult(tableInfoList);
        } 
    }
    
    /**
     * Check metadata to find all child views for a given table/view
     * @param sysCatOrsysChildLink For older (pre-4.15.0) clients, we look for child links inside SYSTEM.CATALOG,
     *                             otherwise we look for them inside SYSTEM.CHILD_LINK
     * @param tenantId tenantId
     * @param schemaName table schema name
     * @param tableName table name
     * @param timestamp passed client-side timestamp
     * @return true if the given table has at least one child view
     * @throws IOException
     */
    public static boolean hasChildViews(Table sysCatOrsysChildLink, byte[] tenantId, byte[] schemaName,
                                        byte[] tableName, long timestamp) throws IOException {
        byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
        Scan scan = MetaDataUtil.newTableRowsScan(key, MetaDataProtocol.MIN_TABLE_TIMESTAMP, timestamp);
        SingleColumnValueFilter linkFilter =
                new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES,
                        CompareFilter.CompareOp.EQUAL,
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

    public static void dropChildViews(RegionCoprocessorEnvironment env, byte[] tenantIdBytes,
            byte[] schemaName, byte[] tableName, byte[] sysCatOrSysChildLink)
            throws IOException, SQLException {
        Table hTable = null;
        try {
            hTable = ServerUtil.getHTableForCoprocessorScan(env, sysCatOrSysChildLink);
        }
        catch (Exception e){
        }
        // if the SYSTEM.CATALOG or SYSTEM.CHILD_LINK doesn't exist just return
        if (hTable==null) {
            return;
        }

        TableViewFinderResult childViewsResult = findRelatedViews(hTable, tenantIdBytes, schemaName, tableName,
                LinkType.CHILD_TABLE, HConstants.LATEST_TIMESTAMP);

        for (TableInfo viewInfo : childViewsResult.getLinks()) {
            byte[] viewTenantId = viewInfo.getTenantId();
            byte[] viewSchemaName = viewInfo.getSchemaName();
            byte[] viewName = viewInfo.getTableName();
            if (logger.isDebugEnabled()) {
                logger.debug("dropChildViews :" + Bytes.toString(schemaName) + "." + Bytes.toString(tableName) +
                        " -> " + Bytes.toString(viewSchemaName) + "." + Bytes.toString(viewName) +
                        "with tenant id :" + Bytes.toString(viewTenantId));
            }
            Properties props = new Properties();
            if (viewTenantId != null && viewTenantId.length != 0)
                props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, Bytes.toString(viewTenantId));
            try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(props, env.getConfiguration())
                    .unwrap(PhoenixConnection.class)) {
                MetaDataClient client = new MetaDataClient(connection);
                org.apache.phoenix.parse.TableName viewTableName = org.apache.phoenix.parse.TableName
                        .create(Bytes.toString(viewSchemaName), Bytes.toString(viewName));
                try {
                    client.dropTable(
                            new DropTableStatement(viewTableName, PTableType.VIEW, true, true, true));
                }
                catch (TableNotFoundException e) {
                    logger.info("Ignoring view "+viewTableName+" as it has already been dropped");
                }
            }
        }
    }


    /**
     * Determines whether we should use SYSTEM.CATALOG or SYSTEM.CHILD_LINK to find parent->child
     * links i.e. {@link LinkType#CHILD_TABLE}.
     * If the client is older than 4.15.0 and the SYSTEM.CHILD_LINK table does not exist, we use
     * the SYSTEM.CATALOG table. In all other cases, we use the SYSTEM.CHILD_LINK table.
     * This is required for backwards compatibility.
     * @param clientVersion client version
     * @param conf server-side configuration
     * @return name of the system table to be used
     * @throws SQLException
     */
    public static TableName getSystemTableForChildLinks(int clientVersion,
            Configuration conf) throws SQLException, IOException {
        byte[] fullTableName = SYSTEM_CHILD_LINK_NAME_BYTES;
        if (clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG) {
            try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(
                    conf).unwrap(PhoenixConnection.class);
                    HBaseAdmin admin = connection.getQueryServices().getAdmin()) {

                // If this is an old client and the CHILD_LINK table doesn't exist i.e. metadata
                // hasn't been updated since there was never a connection from a 4.15 client
                if (!admin.tableExists(SchemaUtil.getPhysicalTableName(
                        SYSTEM_CHILD_LINK_NAME_BYTES, conf))) {
                    fullTableName = SYSTEM_CATALOG_NAME_BYTES;
                }
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

    /**
     * Adds indexes of the parent table to inheritedIndexes if the index contains all required columns
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
                        // It'd be possible to use a local index that doesn't have all view constants,
                        // but the WHERE clause for the view statement (which is added to the index below)
                        // would fail to compile.
                        String indexColumnName = IndexUtil.getIndexColumnName(col);
                        index.getColumnForColumnName(indexColumnName);
                    } catch (ColumnNotFoundException e1) {
                        PColumn indexCol = null;
                        try {
                            String cf = col.getFamilyName()!=null ? col.getFamilyName().getString() : null;
                            String colName = col.getName().getString();
                            if (cf != null) {
                                indexCol = parentTable.getColumnFamily(cf).getPColumnForColumnName(colName);
                            }
                            else {
                                indexCol = parentTable.getColumnForColumnName(colName);
                            }
                        } catch (ColumnNotFoundException e2) { // Ignore this index and continue with others
                            containsAllReqdCols = false;
                            break;
                        }
                        if (indexCol.getViewConstant()==null || Bytes.compareTo(indexCol.getViewConstant(), col.getViewConstant())!=0) {
                            containsAllReqdCols = false;
                            break;
                        }
                    }
                }
            }
            if (containsAllReqdCols) {
                // Tack on view statement to index to get proper filtering for view
                String viewStatement = IndexUtil.rewriteViewStatement(connection, index, parentTable, view.getViewStatement());
                PName modifiedIndexName = PNameFactory.newName(view.getName().getString()
                        + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR + index.getName().getString());
                // add the index table with a new name so that it does not conflict with the existing index table
                // and set update cache frequency to that of the view
                if (Objects.equal(viewStatement, index.getViewStatement())) {
                    inheritedIndexes.add(index);
                } else {
                    inheritedIndexes.add(PTableImpl.builderWithColumns(index, getColumnsToClone(index))
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

    /**
     * Inherit all columns from the parent unless its an excluded column if the same columns is present in the parent
     * and child (for table metadata created before PHOENIX-3534) we chose the child column over the parent column
     * @return table with inherited columns and indexes
     */
    public static PTable addDerivedColumnsAndIndexesFromParent(PhoenixConnection connection,
                                                               PTable table, PTable parentTable) throws SQLException {
        // combine columns for view and view indexes
        boolean hasIndexId = table.getViewIndexId() != null;
        boolean isSalted = table.getBucketNum() != null;
        boolean isDiverged = isDivergedView(table);
        List<PColumn> allColumns = Lists.newArrayList();
        List<PColumn> excludedColumns = Lists.newArrayList();
        // add my own columns first in reverse order
        List<PColumn> myColumns = table.getColumns();
        // skip salted column as it will be created automatically
        myColumns = myColumns.subList(isSalted ? 1 : 0, myColumns.size());
        for (int i = myColumns.size() - 1; i >= 0; i--) {
            PColumn pColumn = myColumns.get(i);
            if (pColumn.isExcluded()) {
                excludedColumns.add(pColumn);
            }
            allColumns.add(pColumn);
        }

        // initialize map from with indexed expression to list of required data columns
        // then remove the data columns that have not been dropped, so that we get the columns that
        // have been dropped
        Map<PColumn, List<String>> indexRequiredDroppedDataColMap =
                Maps.newHashMapWithExpectedSize(table.getColumns().size());
        if (hasIndexId) {
            int indexPosOffset = (isSalted ? 1 : 0) + (table.isMultiTenant() ? 1 : 0) + 1;
            ColumnNameTrackingExpressionCompiler expressionCompiler =
                    new ColumnNameTrackingExpressionCompiler();
            for (int i = indexPosOffset; i < table.getPKColumns().size(); i++) {
                PColumn indexColumn = table.getPKColumns().get(i);
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

        long maxTableTimestamp = table.getTimeStamp();
        int numPKCols = table.getPKColumns().size();
        // set the final table timestamp as the max timestamp of the view/view index or its ancestors
        maxTableTimestamp = Math.max(maxTableTimestamp, parentTable.getTimeStamp());
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
        } else {
            List<PColumn> currAncestorTableCols = PTableImpl.getColumnsToClone(parentTable);
            if (currAncestorTableCols != null) {
                // add the ancestor columns in reverse order so that the final column list
                // contains ancestor columns and then the view columns in the right order
                for (int j = currAncestorTableCols.size() - 1; j >= 0; j--) {
                    PColumn column = currAncestorTableCols.get(j);
                    // for diverged views we always include pk columns of the base table. We
                    // have to include these pk columns to be able to support adding pk
                    // columns to the diverged view
                    // we only include regular columns that were created before the view
                    // diverged
                    if (isDiverged && column.getFamilyName() != null
                            && column.getTimestamp() > table.getTimeStamp()) {
                        continue;
                    }
                    // need to check if this column is in the list of excluded (dropped)
                    // columns of the view
                    int existingIndex = excludedColumns.indexOf(column);
                    if (existingIndex != -1) {
                        // if it is, only exclude the column if was created before the
                        // column was dropped in the view in order to handle the case where
                        // a base table column is dropped in a view, then dropped in the
                        // base table and then added back to the base table
                        if (column.getTimestamp() <= excludedColumns.get(existingIndex)
                                .getTimestamp()) {
                            continue;
                        }
                    }
                    if (column.isExcluded()) {
                        excludedColumns.add(column);
                    } else {
                        int existingColumnIndex = allColumns.indexOf(column);
                        if (existingColumnIndex != -1) {
                            // for diverged views if the view was created before
                            // PHOENIX-3534 the parent table columns will be present in the
                            // view PTable (since the base column count is
                            // QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT we can't
                            // filter them out) so we always pick the parent column
                            // for non diverged views if the same column exists in a parent
                            // and child, we keep the latest column
                            PColumn existingColumn = allColumns.get(existingColumnIndex);
                            if (isDiverged || column.getTimestamp() > existingColumn.getTimestamp()) {
                                allColumns.remove(existingColumnIndex);
                                allColumns.add(column);
                            }
                        } else {
                            allColumns.add(column);
                        }
                    }
                }
            }
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
        // remove the excluded columns if the timestamp of the excludedColumn is newer
        for (PColumn excludedColumn : excludedColumns) {
            int index = allColumns.indexOf(excludedColumn);
            if (index != -1) {
                if (allColumns.get(index).getTimestamp() <= excludedColumn.getTimestamp()) {
                    allColumns.remove(excludedColumn);
                }
            }
        }
        List<PColumn> columnsToAdd = Lists.newArrayList();
        int position = isSalted ? 1 : 0;
        // allColumns contains the columns in the reverse order
        for (int i = allColumns.size() - 1; i >= 0; i--) {
            PColumn column = allColumns.get(i);
            if (table.getColumns().contains(column)) {
                // for views this column is not derived from an ancestor
                columnsToAdd.add(new PColumnImpl(column, position++));
            } else {
                columnsToAdd.add(new PColumnImpl(column, true, position++));
            }
        }
        // we need to include the salt column when setting the base table column count in order to
        // maintain b/w compatibility
        int baseTableColumnCount =
                isDiverged ? QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT
                        : columnsToAdd.size() - myColumns.size() + (isSalted ? 1 : 0);
        // Inherit view-modifiable properties from the parent table/view if the current view has
        // not previously modified this property
        Long updateCacheFreq = (table.getType() != PTableType.VIEW ||
                table.hasViewModifiedUpdateCacheFrequency()) ?
                table.getUpdateCacheFrequency() : parentTable.getUpdateCacheFrequency();
        Boolean useStatsForParallelization = (table.getType() != PTableType.VIEW ||
                table.hasViewModifiedUseStatsForParallelization()) ?
                table.useStatsForParallelization() : parentTable.useStatsForParallelization();

        // When creating a PTable for views or view indexes, use the baseTable PTable for attributes
        // inherited from the physical base table.
        // if a TableProperty is not valid on a view we set it to the base table value
        // if a TableProperty is valid on a view and is not mutable on a view we set it to the base table value
        // if a TableProperty is valid on a view and is mutable on a view, we use the value set
        // on the view if the view had previously modified the property, otherwise we propagate the
        // value from the base table (see PHOENIX-4763)
        PTable pTable = PTableImpl.builderWithColumns(table, columnsToAdd)
                .setImmutableRows(parentTable.isImmutableRows())
                .setDisableWAL(parentTable.isWALDisabled())
                .setMultiTenant(parentTable.isMultiTenant())
                .setStoreNulls(parentTable.getStoreNulls())
                .setTransactionProvider(parentTable.getTransactionProvider())
                .setAutoPartitionSeqName(parentTable.getAutoPartitionSeqName())
                .setAppendOnlySchema(parentTable.isAppendOnlySchema())
                .setImmutableStorageScheme(parentTable.getImmutableStorageScheme() == null ?
                        PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : parentTable.getImmutableStorageScheme())
                .setQualifierEncodingScheme(parentTable.getEncodingScheme() == null ?
                        PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : parentTable.getEncodingScheme())
                .setBaseColumnCount(baseTableColumnCount)
                .setTimeStamp(maxTableTimestamp)
                .setExcludedColumns(excludedColumns == null ?
                        ImmutableList.<PColumn>of() : ImmutableList.copyOf(excludedColumns))
                .setUpdateCacheFrequency(updateCacheFreq)
                .setUseStatsForParallelization(useStatsForParallelization)
                .build();
        pTable = WhereConstantParser.addViewInfoToPColumnsIfNeeded(pTable);

        // For views :
        if (!hasIndexId) {
            // 1. need to resolve the views's own indexes so that any columns added by ancestors are included
            List<PTable> allIndexes = Lists.newArrayList();
            if (!pTable.getIndexes().isEmpty()) {
                for (PTable viewIndex : pTable.getIndexes()) {
                    PTable resolvedViewIndex =
                            ViewUtil.addDerivedColumnsAndIndexesFromParent(connection, viewIndex, pTable);
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
     * See PHOENIX-4763. If we are modifying any table-level properties that are mutable on a view,
     * we mark these cells in SYSTEM.CATALOG with tags to indicate that this view property should
     * not be kept in-sync with the base table and so we shouldn't propagate the base table's
     * property value when resolving the view
     * @param tableMetaData list of mutations on the view
     * @param parent PTable of the parent or null
     */
    public static void addTagsToPutsForViewAlteredProperties(List<Mutation> tableMetaData,
            PTable parent) {
        byte[] parentUpdateCacheFreqBytes = null;
        byte[] parentUseStatsForParallelizationBytes = null;
        byte[] parentViewTTLBytes = null;
        if (parent != null) {
            parentUpdateCacheFreqBytes = new byte[PLong.INSTANCE.getByteSize()];
            PLong.INSTANCE.getCodec().encodeLong(parent.getUpdateCacheFrequency(),
                    parentUpdateCacheFreqBytes, 0);
            if (parent.useStatsForParallelization() != null) {
                parentUseStatsForParallelizationBytes =
                        PBoolean.INSTANCE.toBytes(parent.useStatsForParallelization());
            }
            parentViewTTLBytes = new byte[PLong.INSTANCE.getByteSize()];
            PLong.INSTANCE.getCodec().encodeLong(parent.getViewTTL(),
                    parentViewTTLBytes, 0);
        }
        for (Mutation m: tableMetaData) {
            if (m instanceof Put) {
                MetaDataUtil.conditionallyAddTagsToPutCells((Put)m,
                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES,
                        parentUpdateCacheFreqBytes,
                        MetaDataEndpointImpl.VIEW_MODIFIED_PROPERTY_BYTES);
                MetaDataUtil.conditionallyAddTagsToPutCells((Put)m,
                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION_BYTES,
                        parentUseStatsForParallelizationBytes,
                        MetaDataEndpointImpl.VIEW_MODIFIED_PROPERTY_BYTES);
                MetaDataUtil.conditionallyAddTagsToPutCells((Put)m,
                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.VIEW_TTL_BYTES,
                        parentViewTTLBytes,
                        MetaDataEndpointImpl.VIEW_MODIFIED_PROPERTY_BYTES);
            }

        }
    }
}
