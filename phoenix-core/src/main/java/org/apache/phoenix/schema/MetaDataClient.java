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
package org.apache.phoenix.schema;

import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.google.common.collect.Sets.newLinkedHashSetWithExpectedSize;
import static org.apache.hadoop.hbase.HColumnDescriptor.TTL;
import static org.apache.phoenix.exception.SQLExceptionCode.INSUFFICIENT_MULTI_TENANT_COLUMNS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PARENT_TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REGION_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORE_NULLS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexExpressionCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.PostDDLCompiler;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementNormalizer;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixParameterMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AddColumnStatement;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DropColumnStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.IndexKeyConstraint;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.UpdateStatisticsStatement;
import org.apache.phoenix.query.ConnectionQueryServices.Feature;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.stats.PTableStats;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

public class MetaDataClient {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataClient.class);

    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    private static final String CREATE_TABLE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            TABLE_TYPE + "," +
            TABLE_SEQ_NUM + "," +
            COLUMN_COUNT + "," +
            SALT_BUCKETS + "," +
            PK_NAME + "," +
            DATA_TABLE_NAME + "," +
            INDEX_STATE + "," +
            IMMUTABLE_ROWS + "," +
            DEFAULT_COLUMN_FAMILY_NAME + "," +
            VIEW_STATEMENT + "," +
            DISABLE_WAL + "," +
            MULTI_TENANT + "," +
            VIEW_TYPE + "," +
            VIEW_INDEX_ID + "," +
            INDEX_TYPE + "," +
            STORE_NULLS +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String CREATE_LINK =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_FAMILY + "," +
            LINK_TYPE +
            ") VALUES (?, ?, ?, ?, ?)";
    private static final String CREATE_VIEW_LINK =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_FAMILY + "," +
            LINK_TYPE + "," +
            PARENT_TENANT_ID + " " + PVarchar.INSTANCE.getSqlTypeName() + // Dynamic column for now to prevent schema change
            ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String INCREMENT_SEQ_NUM =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            TABLE_SEQ_NUM  +
            ") VALUES (?, ?, ?, ?)";
    private static final String MUTATE_TABLE =
        "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
        TENANT_ID + "," +
        TABLE_SCHEM + "," +
        TABLE_NAME + "," +
        TABLE_TYPE + "," +
        TABLE_SEQ_NUM + "," +
        COLUMN_COUNT +
        ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_INDEX_STATE =
        "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
        TENANT_ID + "," +
        TABLE_SCHEM + "," +
        TABLE_NAME + "," +
        INDEX_STATE +
        ") VALUES (?, ?, ?, ?)";
    private static final String UPDATE_INDEX_STATE_TO_ACTIVE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            INDEX_STATE + "," +
            INDEX_DISABLE_TIMESTAMP +
            ") VALUES (?, ?, ?, ?, ?)";
    private static final String INSERT_COLUMN =
        "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
        TENANT_ID + "," +
        TABLE_SCHEM + "," +
        TABLE_NAME + "," +
        COLUMN_NAME + "," +
        COLUMN_FAMILY + "," +
        DATA_TYPE + "," +
        NULLABLE + "," +
        COLUMN_SIZE + "," +
        DECIMAL_DIGITS + "," +
        ORDINAL_POSITION + "," +
        SORT_ORDER + "," +
        DATA_TABLE_NAME + "," + // write this both in the column and table rows for access by metadata APIs
        ARRAY_SIZE + "," +
        VIEW_CONSTANT + "," +
        IS_VIEW_REFERENCED + "," +
        PK_NAME + "," +  // write this both in the column and table rows for access by metadata APIs
        KEY_SEQ + "," +
        COLUMN_DEF +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_COLUMN_POSITION =
        "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\" ( " +
        TENANT_ID + "," +
        TABLE_SCHEM + "," +
        TABLE_NAME + "," +
        COLUMN_NAME + "," +
        COLUMN_FAMILY + "," +
        ORDINAL_POSITION +
        ") VALUES (?, ?, ?, ?, ?, ?)";

    private final PhoenixConnection connection;

    public MetaDataClient(PhoenixConnection connection) {
        this.connection = connection;
    }

    public PhoenixConnection getConnection() {
        return connection;
    }

    public long getCurrentTime(String schemaName, String tableName) throws SQLException {
        MetaDataMutationResult result = updateCache(schemaName, tableName, true);
        return result.getMutationTime();
    }

    /**
     * Update the cache with the latest as of the connection scn.
     * @param schemaName
     * @param tableName
     * @return the timestamp from the server, negative if the table was added to the cache and positive otherwise
     * @throws SQLException
     */
    public MetaDataMutationResult updateCache(String schemaName, String tableName) throws SQLException {
        return updateCache(schemaName, tableName, false);
    }

    private MetaDataMutationResult updateCache(String schemaName, String tableName, boolean alwaysHitServer) throws SQLException {
        return updateCache(connection.getTenantId(), schemaName, tableName, alwaysHitServer);
    }

    public MetaDataMutationResult updateCache(PName tenantId, String schemaName, String tableName) throws SQLException {
        return updateCache(tenantId, schemaName, tableName, false);
    }

    private long getClientTimeStamp() {
        Long scn = connection.getSCN();
        long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        return clientTimeStamp;
    }

    private MetaDataMutationResult updateCache(PName tenantId, String schemaName, String tableName,
            boolean alwaysHitServer) throws SQLException { // TODO: pass byte[] herez
        long clientTimeStamp = getClientTimeStamp();
        boolean systemTable = SYSTEM_CATALOG_SCHEMA.equals(schemaName);
        // System tables must always have a null tenantId
        tenantId = systemTable ? null : tenantId;
        PTable table = null;
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        long tableTimestamp = HConstants.LATEST_TIMESTAMP;
        try {
            table = connection.getMetaDataCache().getTable(new PTableKey(tenantId, fullTableName));
            tableTimestamp = table.getTimeStamp();
        } catch (TableNotFoundException e) {
        }
        // Don't bother with server call: we can't possibly find a newer table
        if (table != null && !alwaysHitServer && (systemTable || tableTimestamp == clientTimeStamp - 1)) {
            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,QueryConstants.UNSET_TIMESTAMP,table);
        }

        int maxTryCount = tenantId == null ? 1 : 2;
        int tryCount = 0;
        MetaDataMutationResult result;

        do {
            final byte[] schemaBytes = PVarchar.INSTANCE.toBytes(schemaName);
            final byte[] tableBytes = PVarchar.INSTANCE.toBytes(tableName);
            result = connection.getQueryServices().getTable(tenantId, schemaBytes, tableBytes, tableTimestamp, clientTimeStamp);

            if (SYSTEM_CATALOG_SCHEMA.equals(schemaName)) {
                return result;
            }
            MutationCode code = result.getMutationCode();
            PTable resultTable = result.getTable();
            // We found an updated table, so update our cache
            if (resultTable != null) {
                // Cache table, even if multi-tenant table found for null tenant_id
                // These may be accessed by tenant-specific connections, as the
                // tenant_id will always be added to mask other tenants data.
                // Otherwise, a tenant would be required to create a VIEW first
                // which is not really necessary unless you want to filter or add
                // columns
                addTableToCache(result);
                return result;
            } else {
                // if (result.getMutationCode() == MutationCode.NEWER_TABLE_FOUND) {
                // TODO: No table exists at the clientTimestamp, but a newer one exists.
                // Since we disallow creation or modification of a table earlier than the latest
                // timestamp, we can handle this such that we don't ask the
                // server again.
                if (table != null) {
                    // Ensures that table in result is set to table found in our cache.
                    result.setTable(table);
                    if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                        // Although this table is up-to-date, the parent table may not be.
                        // In this case, we update the parent table which may in turn pull
                        // in indexes to add to this table.
                        if (addIndexesFromPhysicalTable(result)) {
                            connection.addTable(result.getTable());
                        }
                        return result;
                    }
                    // If table was not found at the current time stamp and we have one cached, remove it.
                    // Otherwise, we're up to date, so there's nothing to do.
                    if (code == MutationCode.TABLE_NOT_FOUND && tryCount + 1 == maxTryCount) {
                        connection.removeTable(tenantId, fullTableName, table.getParentName() == null ? null : table.getParentName().getString(), table.getTimeStamp());
                    }
                }
            }
            tenantId = null; // Try again with global tenantId
        } while (++tryCount < maxTryCount);

        return result;
    }

    /**
     * Fault in the physical table to the cache and add any indexes it has to the indexes
     * of the table for which we just updated.
     * TODO: combine this round trip with the one that updates the cache for the child table.
     * @param result the result from updating the cache for the current table.
     * @return true if the PTable contained by result was modified and false otherwise
     * @throws SQLException if the physical table cannot be found
     */
    private boolean addIndexesFromPhysicalTable(MetaDataMutationResult result) throws SQLException {
        PTable table = result.getTable();
        // If not a view or if a view directly over an HBase table, there's nothing to do
        if (table.getType() != PTableType.VIEW || table.getViewType() == ViewType.MAPPED) {
            return false;
        }
        String physicalName = table.getPhysicalName().getString();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(physicalName);
        String tableName = SchemaUtil.getTableNameFromFullName(physicalName);
        MetaDataMutationResult parentResult = updateCache(null, schemaName, tableName, false);
        PTable physicalTable = parentResult.getTable();
        if (physicalTable == null) {
            throw new TableNotFoundException(schemaName, tableName);
        }
        if (!result.wasUpdated() && !parentResult.wasUpdated()) {
            return false;
        }
        List<PTable> indexes = physicalTable.getIndexes();
        if (indexes.isEmpty()) {
            return false;
        }
        // Filter out indexes if column doesn't exist in view
        List<PTable> allIndexes = Lists.newArrayListWithExpectedSize(indexes.size() + table.getIndexes().size());
        if (result.wasUpdated()) { // Table from server never contains inherited indexes
            allIndexes.addAll(table.getIndexes());
        } else { // Only add original ones, as inherited ones may have changed
            for (PTable index : indexes) {
                if (index.getViewIndexId() != null) {
                    allIndexes.add(index);
                }
            }
        }
        for (PTable index : indexes) {
            if (index.getViewIndexId() == null) {
                boolean containsAllReqdCols = true;
                // Ensure that all columns required to create index
                // exist in the view too (since view columns may be removed)
                IndexMaintainer indexMaintainer = index.getIndexMaintainer(physicalTable, connection);
                // check that the columns required for the index pk (not including the pk columns of the data table)
                // are present in the view
                Set<ColumnReference> indexColRefs = indexMaintainer.getIndexedColumns();
                for (ColumnReference colRef : indexColRefs) {
                    try {
                        byte[] cf= colRef.getFamily();
                        byte[] cq= colRef.getQualifier();
                        if (cf!=null) {
                            table.getColumnFamily(cf).getColumn(cq);
                        }
                        else {
                            table.getColumn( Bytes.toString(cq));
                        }
                    } catch (ColumnNotFoundException e) { // Ignore this index and continue with others
                        containsAllReqdCols = false;
                        break;
                    }
                }
                // check that pk columns of the data table (which are also present in the index pk) are present in the view
                List<PColumn> pkColumns = physicalTable.getPKColumns();
                for (int i = physicalTable.getBucketNum() == null ? 0 : 1; i < pkColumns.size(); i++) {
                    try {
                        PColumn pkColumn = pkColumns.get(i);
                        table.getColumn(pkColumn.getName().getString());
                    } catch (ColumnNotFoundException e) { // Ignore this index and continue with others
                        containsAllReqdCols = false;
                        break;
                    }
                }
                // Ensure that constant columns (i.e. columns matched in the view WHERE clause)
                // all exist in the index on the physical table.
                for (PColumn col : table.getColumns()) {
                    if (col.getViewConstant() != null) {
                        try {
                            // TODO: it'd be possible to use a local index that doesn't have all view constants
                            String indexColumnName = IndexUtil.getIndexColumnName(col);
                            index.getColumn(indexColumnName);
                        } catch (ColumnNotFoundException e) { // Ignore this index and continue with others
                            containsAllReqdCols = false;
                            break;
                        }
                    }
                }
                if (containsAllReqdCols) {
                    // Tack on view statement to index to get proper filtering for view
                    String viewStatement = IndexUtil.rewriteViewStatement(connection, index, physicalTable, table.getViewStatement());
                    index = PTableImpl.makePTable(index, viewStatement);
                    allIndexes.add(index);
                }
            }
        }
        PTable allIndexesTable = PTableImpl.makePTable(table, table.getTimeStamp(), allIndexes);
        result.setTable(allIndexesTable);
        return true;
    }


    private void addColumnMutation(String schemaName, String tableName, PColumn column, PreparedStatement colUpsert, String parentTableName, String pkName, Short keySeq, boolean isSalted) throws SQLException {
        colUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
        colUpsert.setString(2, schemaName);
        colUpsert.setString(3, tableName);
        colUpsert.setString(4, column.getName().getString());
        colUpsert.setString(5, column.getFamilyName() == null ? null : column.getFamilyName().getString());
        colUpsert.setInt(6, column.getDataType().getSqlType());
        colUpsert.setInt(7, column.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
        if (column.getMaxLength() == null) {
            colUpsert.setNull(8, Types.INTEGER);
        } else {
            colUpsert.setInt(8, column.getMaxLength());
        }
        if (column.getScale() == null) {
            colUpsert.setNull(9, Types.INTEGER);
        } else {
            colUpsert.setInt(9, column.getScale());
        }
        colUpsert.setInt(10, column.getPosition() + (isSalted ? 0 : 1));
        colUpsert.setInt(11, column.getSortOrder().getSystemValue());
        colUpsert.setString(12, parentTableName);
        if (column.getArraySize() == null) {
            colUpsert.setNull(13, Types.INTEGER);
        } else {
            colUpsert.setInt(13, column.getArraySize());
        }
        colUpsert.setBytes(14, column.getViewConstant());
        colUpsert.setBoolean(15, column.isViewReferenced());
        colUpsert.setString(16, pkName);
        if (keySeq == null) {
            colUpsert.setNull(17, Types.SMALLINT);
        } else {
            colUpsert.setShort(17, keySeq);
        }
        if (column.getExpressionStr() == null) {
            colUpsert.setNull(18, Types.VARCHAR);
        } else {
            colUpsert.setString(18, column.getExpressionStr());
        }
        colUpsert.execute();
    }

    private PColumn newColumn(int position, ColumnDef def, PrimaryKeyConstraint pkConstraint, String defaultColumnFamily, boolean addingToPK) throws SQLException {
        try {
            ColumnName columnDefName = def.getColumnDefName();
            SortOrder sortOrder = def.getSortOrder();
            boolean isPK = def.isPK();
            if (pkConstraint != null) {
                Pair<ColumnName, SortOrder> pkSortOrder = pkConstraint.getColumn(columnDefName);
                if (pkSortOrder != null) {
                    isPK = true;
                    sortOrder = pkSortOrder.getSecond();
                }
            }

            String columnName = columnDefName.getColumnName();
            PName familyName = null;
            if (def.isPK() && !pkConstraint.getColumnNames().isEmpty() ) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                    .setColumnName(columnName).build().buildException();
            }
            boolean isNull = def.isNull();
            if (def.getColumnDefName().getFamilyName() != null) {
                String family = def.getColumnDefName().getFamilyName();
                if (isPK) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME)
                        .setColumnName(columnName).setFamilyName(family).build().buildException();
                } else if (!def.isNull()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.KEY_VALUE_NOT_NULL)
                        .setColumnName(columnName).setFamilyName(family).build().buildException();
                }
                familyName = PNameFactory.newName(family);
            } else if (!isPK) {
                familyName = PNameFactory.newName(defaultColumnFamily == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : defaultColumnFamily);
            }

            if (isPK && !addingToPK && pkConstraint.getColumnNames().size() <= 1) {
                if (def.isNull() && def.isNullSet()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_PK_MAY_NOT_BE_NULL)
                    .setColumnName(columnName).build().buildException();
                }
                isNull = false;
            }

            PColumn column = new PColumnImpl(PNameFactory.newName(columnName), familyName, def.getDataType(),
                    def.getMaxLength(), def.getScale(), isNull, position, sortOrder, def.getArraySize(), null, false, def.getExpression());
            return column;
        } catch (IllegalArgumentException e) { // Based on precondition check in constructor
            throw new SQLException(e);
        }
    }

    public MutationState createTable(CreateTableStatement statement, byte[][] splits, PTable parent, String viewStatement, ViewType viewType, byte[][] viewColumnConstants, BitSet isViewColumnReferenced) throws SQLException {
        PTable table = createTableInternal(statement, splits, parent, viewStatement, viewType, viewColumnConstants, isViewColumnReferenced, null, null);
        if (table == null || table.getType() == PTableType.VIEW) {
            return new MutationState(0,connection);
        }
        // Hack to get around the case when an SCN is specified on the connection.
        // In this case, we won't see the table we just created yet, so we hack
        // around it by forcing the compiler to not resolve anything.
        PostDDLCompiler compiler = new PostDDLCompiler(connection);
        //connection.setAutoCommit(true);
        // Execute any necessary data updates
        Long scn = connection.getSCN();
        long ts = (scn == null ? table.getTimeStamp() : scn);
        // Getting the schema through the current connection doesn't work when the connection has an scn specified
        // Since the table won't be added to the current connection.
        TableRef tableRef = new TableRef(null, table, ts, false);
        byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table);
        MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), emptyCF, null, null, tableRef.getTimeStamp());
        return connection.getQueryServices().updateData(plan);
    }

    public MutationState updateStatistics(UpdateStatisticsStatement updateStatisticsStmt)
            throws SQLException {
        // Don't mistakenly commit pending rows
        connection.rollback();
        // Check before updating the stats if we have reached the configured time to reupdate the stats once again
        ColumnResolver resolver = FromCompiler.getResolver(updateStatisticsStmt, connection);
        PTable table = resolver.getTables().get(0).getTable();
        long rowCount = 0;
        if (updateStatisticsStmt.updateColumns()) {
            rowCount += updateStatisticsInternal(table.getPhysicalName(), table, updateStatisticsStmt.getProps());
        }
        if (updateStatisticsStmt.updateIndex()) {
            // TODO: If our table is a VIEW with multiple indexes or a TABLE with local indexes,
            // we may be doing more work that we have to here. We should union the scan ranges
            // across all indexes in that case so that we don't re-calculate the same stats
            // multiple times.
            for (PTable index : table.getIndexes()) {
                rowCount += updateStatisticsInternal(index.getPhysicalName(), index, updateStatisticsStmt.getProps());
            }
            // If analyzing the indexes of a multi-tenant table or a table with view indexes
            // then analyze all of those indexes too.
            if (table.getType() != PTableType.VIEW) {
                List<PName> names = Lists.newArrayListWithExpectedSize(2);
                if (table.isMultiTenant() || MetaDataUtil.hasViewIndexTable(connection, table.getName())) {
                    names.add(PNameFactory.newName(SchemaUtil.getTableName(
                            MetaDataUtil.getViewIndexSchemaName(table.getSchemaName().getString()),
                            MetaDataUtil.getViewIndexTableName(table.getTableName().getString()))));
                }
                if (MetaDataUtil.hasLocalIndexTable(connection, table.getName())) {
                    names.add(PNameFactory.newName(SchemaUtil.getTableName(
                            MetaDataUtil.getLocalIndexSchemaName(table.getSchemaName().getString()),
                            MetaDataUtil.getLocalIndexTableName(table.getTableName().getString()))));
                }

                for (final PName name : names) {
                    PTable indexLogicalTable = new DelegateTable(table) {
                        @Override
                        public PName getPhysicalName() {
                            return name;
                        }
                        @Override
                        public PTableStats getTableStats() {
                            return PTableStats.EMPTY_STATS;
                        }
                    };
                    rowCount += updateStatisticsInternal(name, indexLogicalTable, updateStatisticsStmt.getProps());
                }
            }
        }
        return new MutationState((int)rowCount, connection);
    }

    private long updateStatisticsInternal(PName physicalName, PTable logicalTable, Map<String, Object> statsProps) throws SQLException {
        ReadOnlyProps props = connection.getQueryServices().getProps();
        final long msMinBetweenUpdates = props
                .getLong(QueryServices.MIN_STATS_UPDATE_FREQ_MS_ATTRIB,
                        props.getLong(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB,
                                QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS) / 2);
        byte[] tenantIdBytes = ByteUtil.EMPTY_BYTE_ARRAY;
        Long scn = connection.getSCN();
        // Always invalidate the cache
        long clientTimeStamp = connection.getSCN() == null ? HConstants.LATEST_TIMESTAMP : scn;
        String query = "SELECT CURRENT_DATE()," + LAST_STATS_UPDATE_TIME + " FROM " + PhoenixDatabaseMetaData.SYSTEM_STATS_NAME
                + " WHERE " + PHYSICAL_NAME + "='" + physicalName.getString() + "' AND " + COLUMN_FAMILY
                + " IS NULL AND " + REGION_NAME + " IS NULL AND " + LAST_STATS_UPDATE_TIME + " IS NOT NULL";
        ResultSet rs = connection.createStatement().executeQuery(query);
        long msSinceLastUpdate = Long.MAX_VALUE;
        if (rs.next()) {
            msSinceLastUpdate = rs.getLong(1) - rs.getLong(2);
        }
        long rowCount = 0;
        if (msSinceLastUpdate >= msMinBetweenUpdates) {
            /*
             * Execute a COUNT(*) through PostDDLCompiler as we need to use the logicalTable passed through,
             * since it may not represent a "real" table in the case of the view indexes of a base table.
             */
            PostDDLCompiler compiler = new PostDDLCompiler(connection);
            TableRef tableRef = new TableRef(null, logicalTable, clientTimeStamp, false);
            MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), null, null, null, clientTimeStamp);
            Scan scan = plan.getContext().getScan();
            scan.setCacheBlocks(false);
            scan.setAttribute(BaseScannerRegionObserver.ANALYZE_TABLE, PDataType.TRUE_BYTES);
            if (statsProps != null) {
                Object gp_width = statsProps.get(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB);
                if (gp_width != null) {
                    scan.setAttribute(BaseScannerRegionObserver.GUIDEPOST_WIDTH_BYTES, PLong.INSTANCE.toBytes(gp_width));
                }
                Object gp_per_region = statsProps.get(QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB);
                if (gp_per_region != null) {
                    scan.setAttribute(BaseScannerRegionObserver.GUIDEPOST_PER_REGION, PInteger.INSTANCE.toBytes(gp_per_region));
                }
            }
            MutationState mutationState = plan.execute();
            rowCount = mutationState.getUpdateCount();
        }

        /*
         *  Update the stats table so that client will pull the new one with the updated stats.
         *  Even if we don't run the command due to the last update time, invalidate the cache.
         *  This supports scenarios in which a major compaction was manually initiated and the
         *  client wants the modified stats to be reflected immediately.
         */
        connection.getQueryServices().clearTableFromCache(tenantIdBytes,
                Bytes.toBytes(SchemaUtil.getSchemaNameFromFullName(physicalName.getString())),
                Bytes.toBytes(SchemaUtil.getTableNameFromFullName(physicalName.getString())), clientTimeStamp);
        return rowCount;
    }

    private MutationState buildIndexAtTimeStamp(PTable index, NamedTableNode dataTableNode) throws SQLException {
        // If our connection is at a fixed point-in-time, we need to open a new
        // connection so that our new index table is visible.
        Properties props = new Properties(connection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(connection.getSCN()+1));
        PhoenixConnection conn = DriverManager.getConnection(connection.getURL(), props).unwrap(PhoenixConnection.class);
        MetaDataClient newClientAtNextTimeStamp = new MetaDataClient(conn);

        // Re-resolve the tableRef from the now newer connection
        conn.setAutoCommit(true);
        ColumnResolver resolver = FromCompiler.getResolver(dataTableNode, conn);
        TableRef tableRef = resolver.getTables().get(0);
        boolean success = false;
        SQLException sqlException = null;
        try {
            MutationState state = newClientAtNextTimeStamp.buildIndex(index, tableRef);
            success = true;
            return state;
        } catch (SQLException e) {
            sqlException = e;
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                if (sqlException == null) {
                    // If we're not in the middle of throwing another exception
                    // then throw the exception we got on close.
                    if (success) {
                        sqlException = e;
                    }
                } else {
                    sqlException.setNextException(e);
                }
            }
            if (sqlException != null) {
                throw sqlException;
            }
        }
        throw new IllegalStateException(); // impossible
    }

    private MutationState buildIndex(PTable index, TableRef dataTableRef) throws SQLException {
        AlterIndexStatement indexStatement = null;
        boolean wasAutoCommit = connection.getAutoCommit();
        connection.rollback();
        try {
            connection.setAutoCommit(true);
            MutationPlan mutationPlan;

            // For local indexes, we optimize the initial index population by *not* sending Puts over
            // the wire for the index rows, as we don't need to do that. Instead, we tap into our
            // region observer to generate the index rows based on the data rows as we scan
            if (index.getIndexType() == IndexType.LOCAL) {
                final PhoenixStatement statement = new PhoenixStatement(connection);
                String tableName = getFullTableName(dataTableRef);
                String query = "SELECT count(*) FROM " + tableName;
                final QueryPlan plan = statement.compileQuery(query);
                TableRef tableRef = plan.getTableRef();
                // Set attribute on scan that UngroupedAggregateRegionObserver will switch on.
                // We'll detect that this attribute was set the server-side and write the index
                // rows per region as a result. The value of the attribute will be our persisted
                // index maintainers.
                // Define the LOCAL_INDEX_BUILD as a new static in BaseScannerRegionObserver
                Scan scan = plan.getContext().getScan();
                try {
                    if(plan.getContext().getScanTimeRange()==null) {
                        Long scn = connection.getSCN();
                        if (scn == null) {
                            scn = plan.getContext().getCurrentTime();
                            // Add one to server time since max of time range is exclusive
                            // and we need to account of OSs with lower resolution clocks.
                            if (scn < HConstants.LATEST_TIMESTAMP) {
                                scn++;
                            }
                        }
                        plan.getContext().setScanTimeRange(new TimeRange(dataTableRef.getLowerBoundTimeStamp(),scn));
                    }
                } catch (IOException e) {
                    throw new SQLException(e);
                }
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                PTable dataTable = tableRef.getTable();
                for(PTable idx: dataTable.getIndexes()) {
                    if(idx.getName().equals(index.getName())) {
                        index = idx;
                        break;
                    }
                }
                List<PTable> indexes = Lists.newArrayListWithExpectedSize(1);
                // Only build newly created index.
                indexes.add(index);
                IndexMaintainer.serialize(dataTable, ptr, indexes, plan.getContext().getConnection());
                scan.setAttribute(BaseScannerRegionObserver.LOCAL_INDEX_BUILD, ByteUtil.copyKeyBytesIfNecessary(ptr));
                // By default, we'd use a FirstKeyOnly filter as nothing else needs to be projected for count(*).
                // However, in this case, we need to project all of the data columns that contribute to the index.
                IndexMaintainer indexMaintainer = index.getIndexMaintainer(dataTable, connection);
                for (ColumnReference columnRef : indexMaintainer.getAllColumns()) {
                    scan.addColumn(columnRef.getFamily(), columnRef.getQualifier());
                }

                // Go through MutationPlan abstraction so that we can create local indexes
                // with a connectionless connection (which makes testing easier).
                mutationPlan = new MutationPlan() {

                    @Override
                    public StatementContext getContext() {
                        return plan.getContext();
                    }

                    @Override
                    public ParameterMetaData getParameterMetaData() {
                        return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                    }

                    @Override
                    public ExplainPlan getExplainPlan() throws SQLException {
                        return ExplainPlan.EMPTY_PLAN;
                    }

                    @Override
                    public PhoenixConnection getConnection() {
                        return connection;
                    }

                    @Override
                    public MutationState execute() throws SQLException {
                        Cell kv = plan.iterator().next().getValue(0);
                        ImmutableBytesWritable tmpPtr = new ImmutableBytesWritable(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
                        // A single Cell will be returned with the count(*) - we decode that here
                        long rowCount = PLong.INSTANCE.getCodec().decodeLong(tmpPtr, SortOrder.getDefault());
                        // The contract is to return a MutationState that contains the number of rows modified. In this
                        // case, it's the number of rows in the data table which corresponds to the number of index
                        // rows that were added.
                        return new MutationState(0, connection, rowCount);
                    }

                };
            } else {
                PostIndexDDLCompiler compiler = new PostIndexDDLCompiler(connection, dataTableRef);
                mutationPlan = compiler.compile(index);
                try {
                    mutationPlan.getContext().setScanTimeRange(new TimeRange(dataTableRef.getLowerBoundTimeStamp(), Long.MAX_VALUE));
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
            MutationState state = connection.getQueryServices().updateData(mutationPlan);
            indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null,
                TableName.create(index.getSchemaName().getString(), index.getTableName().getString())),
                dataTableRef.getTable().getTableName().getString(), false, PIndexState.ACTIVE);
            alterIndex(indexStatement);

            return state;
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private String getFullTableName(TableRef dataTableRef) {
        String schemaName = dataTableRef.getTable().getSchemaName().getString();
        String tableName = dataTableRef.getTable().getTableName().getString();
        String fullName =
                schemaName == null ? ("\"" + tableName + "\"") : ("\"" + schemaName + "\""
                        + QueryConstants.NAME_SEPARATOR + "\"" + tableName + "\"");
        return fullName;
    }

    /**
     * Rebuild indexes from a timestamp which is the value from hbase row key timestamp field
     */
    public void buildPartialIndexFromTimeStamp(PTable index, TableRef dataTableRef) throws SQLException {
        boolean needRestoreIndexState = false;
        // Need to change index state from Disable to InActive when build index partially so that
        // new changes will be indexed during index rebuilding
        AlterIndexStatement indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null,
            TableName.create(index.getSchemaName().getString(), index.getTableName().getString())),
            dataTableRef.getTable().getTableName().getString(), false, PIndexState.INACTIVE);
        alterIndex(indexStatement);
        needRestoreIndexState = true;
        try {
            buildIndex(index, dataTableRef);
            needRestoreIndexState = false;
        } finally {
            if(needRestoreIndexState) {
                // reset index state to disable
                indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null,
                    TableName.create(index.getSchemaName().getString(), index.getTableName().getString())),
                    dataTableRef.getTable().getTableName().getString(), false, PIndexState.DISABLE);
                alterIndex(indexStatement);
            }
        }
    }

    /**
     * Create an index table by morphing the CreateIndexStatement into a CreateTableStatement and calling
     * MetaDataClient.createTable. In doing so, we perform the following translations:
     * 1) Change the type of any columns being indexed to types that support null if the column is nullable.
     *    For example, a BIGINT type would be coerced to a DECIMAL type, since a DECIMAL type supports null
     *    when it's in the row key while a BIGINT does not.
     * 2) Append any row key column from the data table that is not in the indexed column list. Our indexes
     *    rely on having a 1:1 correspondence between the index and data rows.
     * 3) Change the name of the columns to include the column family. For example, if you have a column
     *    named "B" in a column family named "A", the indexed column name will be "A:B". This makes it easy
     *    to translate the column references in a query to the correct column references in an index table
     *    regardless of whether the column reference is prefixed with the column family name or not. It also
     *    has the side benefit of allowing the same named column in different column families to both be
     *    listed as an index column.
     * @param statement
     * @param splits
     * @return MutationState from population of index table from data table
     * @throws SQLException
     */
    public MutationState createIndex(CreateIndexStatement statement, byte[][] splits) throws SQLException {
        IndexKeyConstraint ik = statement.getIndexConstraint();
        TableName indexTableName = statement.getIndexTableName();
        
        List<Pair<ParseNode, SortOrder>> indexParseNodeAndSortOrderList = ik.getParseNodeAndSortOrderList();
        List<ColumnName> includedColumns = statement.getIncludeColumns();
        TableRef tableRef = null;
        PTable table = null;
        boolean retry = true;
        Short indexId = null;
        boolean allocateIndexId = false;
        boolean isLocalIndex = statement.getIndexType() == IndexType.LOCAL;
        int hbaseVersion = connection.getQueryServices().getLowestClusterHBaseVersion();
        if (isLocalIndex) {
            if (!connection.getQueryServices().getProps().getBoolean(QueryServices.ALLOW_LOCAL_INDEX_ATTRIB, QueryServicesOptions.DEFAULT_ALLOW_LOCAL_INDEX)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNALLOWED_LOCAL_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
            }
            if (!connection.getQueryServices().supportsFeature(Feature.LOCAL_INDEX)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_LOCAL_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
            }
        }
        while (true) {
            try {
                ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                tableRef = resolver.getTables().get(0);
                PTable dataTable = tableRef.getTable();
                boolean isTenantConnection = connection.getTenantId() != null;
                if (isTenantConnection) {
                    if (dataTable.getType() != PTableType.VIEW) {
                        throw new SQLFeatureNotSupportedException("An index may only be created for a VIEW through a tenant-specific connection");
                    }
                }
                if (!dataTable.isImmutableRows()) {
                    if (hbaseVersion < PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
                    }
                    if (connection.getQueryServices().hasInvalidIndexConfiguration()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG).setTableName(indexTableName.getTableName()).build().buildException();
                    }
                }
                int posOffset = 0;
                List<PColumn> pkColumns = dataTable.getPKColumns();
                Set<RowKeyColumnExpression> unusedPkColumns;
                if (dataTable.getBucketNum() != null) { // Ignore SALT column
                	unusedPkColumns = Sets.newLinkedHashSetWithExpectedSize(pkColumns.size()-1);
                	posOffset++;
                } else {
                	unusedPkColumns = Sets.newLinkedHashSetWithExpectedSize(pkColumns.size());
                }
                for (int i = posOffset; i < pkColumns.size(); i++) {
                    PColumn column = pkColumns.get(i);
					unusedPkColumns.add(new RowKeyColumnExpression(column, new RowKeyValueAccessor(pkColumns, i), "\""+column.getName().getString()+"\""));
                }
                List<Pair<ColumnName, SortOrder>> allPkColumns = Lists.newArrayListWithExpectedSize(unusedPkColumns.size());
                List<ColumnDef> columnDefs = Lists.newArrayListWithExpectedSize(includedColumns.size() + indexParseNodeAndSortOrderList.size());
                
                if (dataTable.isMultiTenant()) {
                    // Add tenant ID column as first column in index
                    PColumn col = dataTable.getPKColumns().get(posOffset);
                    RowKeyColumnExpression columnExpression = new RowKeyColumnExpression(col, new RowKeyValueAccessor(pkColumns, posOffset), col.getName().getString());
					unusedPkColumns.remove(columnExpression);
                    PDataType dataType = IndexUtil.getIndexColumnDataType(col);
                    ColumnName colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                    allPkColumns.add(new Pair<ColumnName, SortOrder>(colName, col.getSortOrder()));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, SortOrder.getDefault(), col.getName().getString()));
                }
                /*
                 * Allocate an index ID in two circumstances:
                 * 1) for a local index, as all local indexes will reside in the same HBase table
                 * 2) for a view on an index.
                 */
                if (isLocalIndex || (dataTable.getType() == PTableType.VIEW && dataTable.getViewType() != ViewType.MAPPED)) {
                    allocateIndexId = true;
                    // Next add index ID column
                    PDataType dataType = MetaDataUtil.getViewIndexIdDataType();
                    ColumnName colName = ColumnName.caseSensitiveColumnName(MetaDataUtil.getViewIndexIdColumnName());
                    allPkColumns.add(new Pair<ColumnName, SortOrder>(colName, SortOrder.getDefault()));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), false, null, null, false, SortOrder.getDefault(), null));
                }
                
                PhoenixStatement phoenixStatment = new PhoenixStatement(connection);
                StatementContext context = new StatementContext(phoenixStatment, resolver);
                IndexExpressionCompiler expressionIndexCompiler = new IndexExpressionCompiler(context);
                Set<ColumnName> indexedColumnNames = Sets.newHashSetWithExpectedSize(indexParseNodeAndSortOrderList.size());
                for (Pair<ParseNode, SortOrder> pair : indexParseNodeAndSortOrderList) {
                	ParseNode parseNode = pair.getFirst();
                    // normalize the parse node
                    parseNode = StatementNormalizer.normalize(parseNode, resolver);
                    // compile the parseNode to get an expression
                    expressionIndexCompiler.reset();
                    Expression expression = parseNode.accept(expressionIndexCompiler);   
                    if (expressionIndexCompiler.isAggregate()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_INDEX).build().buildException();
                    }
                    if (expression.getDeterminism() != Determinism.ALWAYS) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.NON_DETERMINISTIC_EXPRESSION_NOT_ALLOWED_IN_INDEX).build().buildException();
                    }
                    if (expression.isStateless()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.STATELESS_EXPRESSION_NOT_ALLOWED_IN_INDEX).build().buildException();
                    }
                    unusedPkColumns.remove(expression);
                    
                    // Go through parse node to get string as otherwise we
                    // can lose information during compilation
                    StringBuilder buf = new StringBuilder();
                    parseNode.toSQL(resolver, buf);
                    // need to escape backslash as this expression will be re-parsed later
                    String expressionStr = StringUtil.escapeBackslash(buf.toString());
                    
                    ColumnName colName = null;
                    ColumnRef colRef = expressionIndexCompiler.getColumnRef();
					if (colRef!=null) { 
						// if this is a regular column
					    PColumn column = colRef.getColumn();
					    String columnFamilyName = column.getFamilyName()!=null ? column.getFamilyName().getString() : null;
					    colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(columnFamilyName, column.getName().getString()));
					}
					else { 
						// if this is an expression
					    // TODO column names cannot have double quotes, remove this once this PHOENIX-1621 is fixed
						String name = expressionStr.replaceAll("\"", "'");
                        colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(null, name));
					}
					indexedColumnNames.add(colName);
                	PDataType dataType = IndexUtil.getIndexColumnDataType(expression.isNullable(), expression.getDataType());
                    allPkColumns.add(new Pair<ColumnName, SortOrder>(colName, pair.getSecond()));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), expression.isNullable(), expression.getMaxLength(), expression.getScale(), false, pair.getSecond(), expressionStr));
                }

                // Next all the PK columns from the data table that aren't indexed
                if (!unusedPkColumns.isEmpty()) {
                    for (RowKeyColumnExpression colExpression : unusedPkColumns) {
                        PColumn col = dataTable.getPKColumns().get(colExpression.getPosition());
                        // Don't add columns with constant values from updatable views, as
                        // we don't need these in the index
                        if (col.getViewConstant() == null) {
                            ColumnName colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                            allPkColumns.add(new Pair<ColumnName, SortOrder>(colName, colExpression.getSortOrder()));
                            PDataType dataType = IndexUtil.getIndexColumnDataType(colExpression.isNullable(), colExpression.getDataType());
                            columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(),
                                    colExpression.isNullable(), colExpression.getMaxLength(), colExpression.getScale(),
                                    false, colExpression.getSortOrder(), colExpression.toString()));
                        }
                    }
                }
                
                // Last all the included columns (minus any PK columns)
                for (ColumnName colName : includedColumns) {
                    PColumn col = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName()).getColumn();
                    colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                    // Check for duplicates between indexed and included columns
                    if (indexedColumnNames.contains(colName)) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_EXIST_IN_DEF).build().buildException();
                    }
                    if (!SchemaUtil.isPKColumn(col) && col.getViewConstant() == null) {
                        // Need to re-create ColumnName, since the above one won't have the column family name
                        colName = ColumnName.caseSensitiveColumnName(col.getFamilyName().getString(), IndexUtil.getIndexColumnName(col));
                        columnDefs.add(FACTORY.columnDef(colName, col.getDataType().getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, col.getSortOrder(), null));
                    }
                }

                // Don't re-allocate indexId on ConcurrentTableMutationException,
                // as there's no need to burn another sequence value.
                if (allocateIndexId && indexId == null) {
                    Long scn = connection.getSCN();
                    long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                    PName tenantId = connection.getTenantId();
                    String tenantIdStr = tenantId == null ? null : connection.getTenantId().getString();
                    PName physicalName = dataTable.getPhysicalName();
                    int nSequenceSaltBuckets = connection.getQueryServices().getSequenceSaltBuckets();
                    SequenceKey key = MetaDataUtil.getViewIndexSequenceKey(tenantIdStr, physicalName, nSequenceSaltBuckets);
                    // Create at parent timestamp as we know that will be earlier than now
                    // and earlier than any SCN if one is set.
                    createSequence(key.getTenantId(), key.getSchemaName(), key.getSequenceName(),
                        true, Short.MIN_VALUE, 1, 1, false, Long.MIN_VALUE, Long.MAX_VALUE,
                        dataTable.getTimeStamp());
                    long[] seqValues = new long[1];
                    SQLException[] sqlExceptions = new SQLException[1];
                    connection.getQueryServices().incrementSequences(Collections.singletonList(key),
                            Math.max(timestamp, dataTable.getTimeStamp()), seqValues, sqlExceptions);
                    if (sqlExceptions[0] != null) {
                        throw sqlExceptions[0];
                    }
                    long seqValue = seqValues[0];
                    if (seqValue > Short.MAX_VALUE) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.TOO_MANY_INDEXES)
                        .setSchemaName(SchemaUtil.getSchemaNameFromFullName(physicalName.getString())).setTableName(SchemaUtil.getTableNameFromFullName(physicalName.getString())).build().buildException();
                    }
                    indexId = (short) seqValue;
                }
                // Set DEFAULT_COLUMN_FAMILY_NAME of index to match data table
                // We need this in the props so that the correct column family is created
                if (dataTable.getDefaultFamilyName() != null && dataTable.getType() != PTableType.VIEW && indexId == null) {
                    statement.getProps().put("", new Pair<String,Object>(DEFAULT_COLUMN_FAMILY_NAME,dataTable.getDefaultFamilyName().getString()));
                }
                PrimaryKeyConstraint pk = FACTORY.primaryKey(null, allPkColumns);
                CreateTableStatement tableStatement = FACTORY.createTable(indexTableName, statement.getProps(), columnDefs, pk, statement.getSplitNodes(), PTableType.INDEX, statement.ifNotExists(), null, null, statement.getBindCount());
                table = createTableInternal(tableStatement, splits, dataTable, null, null, null, null, indexId, statement.getIndexType());
                break;
            } catch (ConcurrentTableMutationException e) { // Can happen if parent data table changes while above is in progress
                if (retry) {
                    retry = false;
                    continue;
                }
                throw e;
            }
        }
        if (table == null) {
            return new MutationState(0,connection);
        }

        // In async process, we return immediately as the MR job needs to be triggered .
        if(statement.isAsync()) {
            return new MutationState(0, connection);
        }
        
        // If our connection is at a fixed point-in-time, we need to open a new
        // connection so that our new index table is visible.
        if (connection.getSCN() != null) {
            return buildIndexAtTimeStamp(table, statement.getTable());
        }
        return buildIndex(table, tableRef);
    }

    public MutationState dropSequence(DropSequenceStatement statement) throws SQLException {
        Long scn = connection.getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        String schemaName = statement.getSequenceName().getSchemaName();
        String sequenceName = statement.getSequenceName().getTableName();
        String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
        try {
            connection.getQueryServices().dropSequence(tenantId, schemaName, sequenceName, timestamp);
        } catch (SequenceNotFoundException e) {
            if (statement.ifExists()) {
                return new MutationState(0, connection);
            }
            throw e;
        }
        return new MutationState(1, connection);
    }

    public MutationState createSequence(CreateSequenceStatement statement, long startWith,
            long incrementBy, long cacheSize, long minValue, long maxValue) throws SQLException {
        Long scn = connection.getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        String tenantId =
                connection.getTenantId() == null ? null : connection.getTenantId().getString();
        return createSequence(tenantId, statement.getSequenceName().getSchemaName(), statement
                .getSequenceName().getTableName(), statement.ifNotExists(), startWith, incrementBy,
            cacheSize, statement.getCycle(), minValue, maxValue, timestamp);
    }

    private MutationState createSequence(String tenantId, String schemaName, String sequenceName,
            boolean ifNotExists, long startWith, long incrementBy, long cacheSize, boolean cycle,
            long minValue, long maxValue, long timestamp) throws SQLException {
        try {
            connection.getQueryServices().createSequence(tenantId, schemaName, sequenceName,
                startWith, incrementBy, cacheSize, minValue, maxValue, cycle, timestamp);
        } catch (SequenceAlreadyExistsException e) {
            if (ifNotExists) {
                return new MutationState(0, connection);
            }
            throw e;
        }
        return new MutationState(1, connection);
    }

    private static ColumnDef findColumnDefOrNull(List<ColumnDef> colDefs, ColumnName colName) {
        for (ColumnDef colDef : colDefs) {
            if (colDef.getColumnDefName().getColumnName().equals(colName.getColumnName())) {
                return colDef;
            }
        }
        return null;
    }

    private PTable createTableInternal(CreateTableStatement statement, byte[][] splits, final PTable parent, String viewStatement, ViewType viewType, final byte[][] viewColumnConstants, final BitSet isViewColumnReferenced, Short indexId, IndexType indexType) throws SQLException {
        final PTableType tableType = statement.getTableType();
        boolean wasAutoCommit = connection.getAutoCommit();
        connection.rollback();
        try {
            connection.setAutoCommit(false);
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(statement.getColumnDefs().size() + 3);

            TableName tableNameNode = statement.getTableName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            String parentTableName = null;
            PName tenantId = connection.getTenantId();
            String tenantIdStr = tenantId == null ? null : connection.getTenantId().getString();
            boolean multiTenant = false;
            boolean storeNulls = false;
            Integer saltBucketNum = null;
            String defaultFamilyName = null;
            boolean isImmutableRows = false;
            List<PName> physicalNames = Collections.emptyList();
            boolean addSaltColumn = false;
            if (parent != null && tableType == PTableType.INDEX) {
                // Index on view
                // TODO: Can we support a multi-tenant index directly on a multi-tenant
                // table instead of only a view? We don't have anywhere to put the link
                // from the table to the index, though.
                if (indexType == IndexType.LOCAL || (parent.getType() == PTableType.VIEW && parent.getViewType() != ViewType.MAPPED)) {
                    PName physicalName = parent.getPhysicalName();
                    saltBucketNum = parent.getBucketNum();
                    addSaltColumn = (saltBucketNum != null && indexType != IndexType.LOCAL);
                    defaultFamilyName = parent.getDefaultFamilyName() == null ? null : parent.getDefaultFamilyName().getString();
                    if (indexType == IndexType.LOCAL) {
                        saltBucketNum = null;
                        // Set physical name of local index table
                        physicalNames = Collections.singletonList(PNameFactory.newName(MetaDataUtil.getLocalIndexPhysicalName(physicalName.getBytes())));
                    } else {
                        // Set physical name of view index table
                        physicalNames = Collections.singletonList(PNameFactory.newName(MetaDataUtil.getViewIndexPhysicalName(physicalName.getBytes())));
                    }
                }

                multiTenant = parent.isMultiTenant();
                storeNulls = parent.getStoreNulls();
                parentTableName = parent.getTableName().getString();
                // Pass through data table sequence number so we can check it hasn't changed
                PreparedStatement incrementStatement = connection.prepareStatement(INCREMENT_SEQ_NUM);
                incrementStatement.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                incrementStatement.setString(2, schemaName);
                incrementStatement.setString(3, parentTableName);
                incrementStatement.setLong(4, parent.getSequenceNumber());
                incrementStatement.execute();
                // Get list of mutations and add to table meta data that will be passed to server
                // to guarantee order. This row will always end up last
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();

                // Add row linking from data table row to index table row
                PreparedStatement linkStatement = connection.prepareStatement(CREATE_LINK);
                linkStatement.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                linkStatement.setString(2, schemaName);
                linkStatement.setString(3, parentTableName);
                linkStatement.setString(4, tableName);
                linkStatement.setByte(5, LinkType.INDEX_TABLE.getSerializedValue());
                linkStatement.execute();
            }

            PrimaryKeyConstraint pkConstraint = statement.getPrimaryKeyConstraint();
            String pkName = null;
            List<Pair<ColumnName,SortOrder>> pkColumnsNames = Collections.<Pair<ColumnName,SortOrder>>emptyList();
            Iterator<Pair<ColumnName,SortOrder>> pkColumnsIterator = Iterators.emptyIterator();
            if (pkConstraint != null) {
                pkColumnsNames = pkConstraint.getColumnNames();
                pkColumnsIterator = pkColumnsNames.iterator();
                pkName = pkConstraint.getName();
            }

            Map<String,Object> tableProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());
            Map<String,Object> commonFamilyProps = Collections.emptyMap();
            // Somewhat hacky way of determining if property is for HColumnDescriptor or HTableDescriptor
            HColumnDescriptor defaultDescriptor = new HColumnDescriptor(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
            if (!statement.getProps().isEmpty()) {
                commonFamilyProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());

                Collection<Pair<String,Object>> props = statement.getProps().get(QueryConstants.ALL_FAMILY_PROPERTIES_KEY);
                for (Pair<String,Object> prop : props) {
                    if (defaultDescriptor.getValue(prop.getFirst()) == null) {
                        tableProps.put(prop.getFirst(), prop.getSecond());
                    } else {
                        commonFamilyProps.put(prop.getFirst(), prop.getSecond());
                    }
                }
            }

            // Although unusual, it's possible to set a mapped VIEW as having immutable rows.
            // This tells Phoenix that you're managing the index maintenance yourself.
            if (tableType != PTableType.INDEX && (tableType != PTableType.VIEW || viewType == ViewType.MAPPED)) {
                Boolean isImmutableRowsProp = (Boolean) tableProps.remove(PTable.IS_IMMUTABLE_ROWS_PROP_NAME);
                if (isImmutableRowsProp == null) {
                    isImmutableRows = connection.getQueryServices().getProps().getBoolean(QueryServices.IMMUTABLE_ROWS_ATTRIB, QueryServicesOptions.DEFAULT_IMMUTABLE_ROWS);
                } else {
                    isImmutableRows = isImmutableRowsProp;
                }
            }

            // Can't set any of these on views or shared indexes on views
            if (tableType != PTableType.VIEW && indexId == null) {
                saltBucketNum = (Integer) tableProps.remove(PhoenixDatabaseMetaData.SALT_BUCKETS);
                if (saltBucketNum != null) {
                    if (saltBucketNum < 0 || saltBucketNum > SaltingUtil.MAX_BUCKET_NUM) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_BUCKET_NUM).build().buildException();
                    }
                }
                // Salt the index table if the data table is salted
                if (saltBucketNum == null) {
                    if (parent != null) {
                        saltBucketNum = parent.getBucketNum();
                    }
                } else if (saltBucketNum.intValue() == 0) {
                    saltBucketNum = null; // Provides a way for an index to not be salted if its data table is salted
                }
                addSaltColumn = (saltBucketNum != null);
            }

            boolean removedProp = false;
            // Can't set MULTI_TENANT or DEFAULT_COLUMN_FAMILY_NAME on an INDEX or a non mapped VIEW
            if (tableType != PTableType.INDEX && (tableType != PTableType.VIEW || viewType == ViewType.MAPPED)) {
                Boolean multiTenantProp = (Boolean) tableProps.remove(PhoenixDatabaseMetaData.MULTI_TENANT);
                multiTenant = Boolean.TRUE.equals(multiTenantProp);
                defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);
                removedProp = (defaultFamilyName != null);
            }

            boolean disableWAL = false;
            Boolean disableWALProp = (Boolean) tableProps.remove(PhoenixDatabaseMetaData.DISABLE_WAL);
            if (disableWALProp != null) {
                disableWAL = disableWALProp;
            }

            Boolean storeNullsProp = (Boolean) tableProps.remove(PhoenixDatabaseMetaData.STORE_NULLS);
            storeNulls = storeNullsProp == null
                    ? connection.getQueryServices().getProps().getBoolean(
                            QueryServices.DEFAULT_STORE_NULLS_ATTRIB,
                            QueryServicesOptions.DEFAULT_STORE_NULLS)
                    : storeNullsProp;

            // Delay this check as it is supported to have IMMUTABLE_ROWS and SALT_BUCKETS defined on views
            if ((statement.getTableType() == PTableType.VIEW || indexId != null) && !tableProps.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build()
                        .buildException();
            }
            if (removedProp) {
                tableProps.put(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME, defaultFamilyName);
            }

            List<ColumnDef> colDefs = statement.getColumnDefs();
            List<PColumn> columns;
            LinkedHashSet<PColumn> pkColumns;

            if (tenantId != null && (tableType != PTableType.VIEW && indexId == null)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_TENANT_SPECIFIC_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            }

            if (tableType == PTableType.VIEW) {
                physicalNames = Collections.singletonList(PNameFactory.newName(parent.getPhysicalName().getString()));
                if (viewType == ViewType.MAPPED) {
                    columns = newArrayListWithExpectedSize(colDefs.size());
                    pkColumns = newLinkedHashSetWithExpectedSize(colDefs.size());
                } else {
                    // Propagate property values to VIEW.
                    // TODO: formalize the known set of these properties
                    multiTenant = parent.isMultiTenant();
                    saltBucketNum = parent.getBucketNum();
                    isImmutableRows = parent.isImmutableRows();
                    disableWAL = (disableWALProp == null ? parent.isWALDisabled() : disableWALProp);
                    defaultFamilyName = parent.getDefaultFamilyName() == null ? null : parent.getDefaultFamilyName().getString();
                    List<PColumn> allColumns = parent.getColumns();
                    if (saltBucketNum != null) { // Don't include salt column in columns, as it should not have it when created
                        allColumns = allColumns.subList(1, allColumns.size());
                    }
                    columns = newArrayListWithExpectedSize(allColumns.size() + colDefs.size());
                    columns.addAll(allColumns);
                    pkColumns = newLinkedHashSet(parent.getPKColumns());

                    // Add row linking from view to its parent table
                    // FIXME: not currently used, but see PHOENIX-1367
                    // as fixing that will require it's usage.
                    PreparedStatement linkStatement = connection.prepareStatement(CREATE_VIEW_LINK);
                    linkStatement.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                    linkStatement.setString(2, schemaName);
                    linkStatement.setString(3, tableName);
                    linkStatement.setString(4, parent.getName().getString());
                    linkStatement.setByte(5, LinkType.PARENT_TABLE.getSerializedValue());
                    linkStatement.setString(6, parent.getTenantId() == null ? null : parent.getTenantId().getString());
                    linkStatement.execute();
                }
            } else {
                columns = newArrayListWithExpectedSize(colDefs.size());
                pkColumns = newLinkedHashSetWithExpectedSize(colDefs.size() + 1); // in case salted
            }

            // Don't add link for mapped view, as it just points back to itself and causes the drop to
            // fail because it looks like there's always a view associated with it.
            if (!physicalNames.isEmpty()) {
                // Upsert physical name for mapped view only if the full physical table name is different than the full table name
                // Otherwise, we end up with a self-referencing link and then cannot ever drop the view.
                if (viewType != ViewType.MAPPED
                        || !physicalNames.get(0).getString().equals(SchemaUtil.getTableName(schemaName, tableName))) {
                    // Add row linking from data table row to physical table row
                    PreparedStatement linkStatement = connection.prepareStatement(CREATE_LINK);
                    for (PName physicalName : physicalNames) {
                        linkStatement.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                        linkStatement.setString(2, schemaName);
                        linkStatement.setString(3, tableName);
                        linkStatement.setString(4, physicalName.getString());
                        linkStatement.setByte(5, LinkType.PHYSICAL_TABLE.getSerializedValue());
                        linkStatement.execute();
                    }
                }
            }

            PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN);
            Map<String, PName> familyNames = Maps.newLinkedHashMap();
            boolean isPK = false;

            int positionOffset = columns.size();
            if (saltBucketNum != null) {
                positionOffset++;
                if (addSaltColumn) {
                    pkColumns.add(SaltingUtil.SALTING_COLUMN);
                }
            }
            int position = positionOffset;

            for (ColumnDef colDef : colDefs) {
                if (colDef.isPK()) {
                    if (isPK) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                            .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                    }
                    isPK = true;
                } else {
                    // do not allow setting NOT-NULL constraint on non-primary columns.
                    if (  Boolean.FALSE.equals(colDef.isNull()) &&
                        ( isPK || ( pkConstraint != null && !pkConstraint.contains(colDef.getColumnDefName())))) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_NOT_NULL_CONSTRAINT)
                                .setSchemaName(schemaName)
                                .setTableName(tableName)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                    }
                }

                PColumn column = newColumn(position++, colDef, pkConstraint, defaultFamilyName, false);
                if (SchemaUtil.isPKColumn(column)) {
                    // TODO: remove this constraint?
                    if (pkColumnsIterator.hasNext() && !column.getName().getString().equals(pkColumnsIterator.next().getFirst().getColumnName())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_OUT_OF_ORDER)
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .setColumnName(column.getName().getString())
                            .build().buildException();
                    }
                    if (tableType == PTableType.VIEW && viewType != ViewType.MAPPED) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_DEFINE_PK_FOR_VIEW)
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .setColumnName(colDef.getColumnDefName().getColumnName())
                            .build().buildException();
                    }
                    if (!pkColumns.add(column)) {
                        throw new ColumnAlreadyExistsException(schemaName, tableName, column.getName().getString());
                    }
                }
                if (tableType == PTableType.VIEW && hasColumnWithSameNameAndFamily(columns, column)) {
                    // we only need to check for dup columns for views because they inherit columns from parent
                    throw new ColumnAlreadyExistsException(schemaName, tableName, column.getName().getString());
                }
                columns.add(column);
                if ((colDef.getDataType() == PVarbinary.INSTANCE || colDef.getDataType().isArrayType())
                        && SchemaUtil.isPKColumn(column)
                        && pkColumnsIterator.hasNext()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_IN_ROW_KEY)
                        .setSchemaName(schemaName)
                        .setTableName(tableName)
                        .setColumnName(column.getName().getString())
                        .build().buildException();
                }
                if (column.getFamilyName() != null) {
                    familyNames.put(column.getFamilyName().getString(),column.getFamilyName());
                }
            }
            // We need a PK definition for a TABLE or mapped VIEW
            if (!isPK && pkColumnsNames.isEmpty() && tableType != PTableType.VIEW && viewType != ViewType.MAPPED) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                    .setSchemaName(schemaName)
                    .setTableName(tableName)
                    .build().buildException();
            }
            if (!pkColumnsNames.isEmpty() && pkColumnsNames.size() != pkColumns.size() - positionOffset) { // Then a column name in the primary key constraint wasn't resolved
                Iterator<Pair<ColumnName,SortOrder>> pkColumnNamesIterator = pkColumnsNames.iterator();
                while (pkColumnNamesIterator.hasNext()) {
                    ColumnName colName = pkColumnNamesIterator.next().getFirst();
                    ColumnDef colDef = findColumnDefOrNull(colDefs, colName);
                    if (colDef == null) {
                        throw new ColumnNotFoundException(schemaName, tableName, null, colName.getColumnName());
                    }
                    if (colDef.getColumnDefName().getFamilyName() != null) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME)
                        .setSchemaName(schemaName)
                        .setTableName(tableName)
                        .setColumnName(colDef.getColumnDefName().getColumnName() )
                        .setFamilyName(colDef.getColumnDefName().getFamilyName())
                        .build().buildException();
                    }
                }
                // The above should actually find the specific one, but just in case...
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_PRIMARY_KEY_CONSTRAINT)
                    .setSchemaName(schemaName)
                    .setTableName(tableName)
                    .build().buildException();
            }

            List<Pair<byte[],Map<String,Object>>> familyPropList = Lists.newArrayListWithExpectedSize(familyNames.size());
            if (!statement.getProps().isEmpty()) {
                for (String familyName : statement.getProps().keySet()) {
                    if (!familyName.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                        if (familyNames.get(familyName) == null) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROPERTIES_FOR_FAMILY)
                                .setFamilyName(familyName).build().buildException();
                        } else if (statement.getTableType() == PTableType.VIEW) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build().buildException();
                        }
                    }
                }
            }
            throwIfInsufficientColumns(schemaName, tableName, pkColumns, saltBucketNum!=null, multiTenant);

            for (PName familyName : familyNames.values()) {
                Collection<Pair<String,Object>> props = statement.getProps().get(familyName.getString());
                if (props.isEmpty()) {
                    familyPropList.add(new Pair<byte[],Map<String,Object>>(familyName.getBytes(),commonFamilyProps));
                } else {
                    Map<String,Object> combinedFamilyProps = Maps.newHashMapWithExpectedSize(props.size() + commonFamilyProps.size());
                    combinedFamilyProps.putAll(commonFamilyProps);
                    for (Pair<String,Object> prop : props) {
                        // Don't allow specifying column families for TTL. TTL can only apply for the all the column families of the table
                        // i.e. it can't be column family specific.
                        if (!familyName.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY) && prop.getFirst().equals(TTL)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL).build().buildException();
                        }
                        combinedFamilyProps.put(prop.getFirst(), prop.getSecond());
                    }
                    familyPropList.add(new Pair<byte[],Map<String,Object>>(familyName.getBytes(),combinedFamilyProps));
                }
            }

            if (familyNames.isEmpty()) {
            	//if there are no family names, use the default column family name. This also takes care of the case when
            	//the table ddl has only PK cols present (which means familyNames is empty).
                byte[] cf = defaultFamilyName == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : Bytes.toBytes(defaultFamilyName);
                familyPropList.add(new Pair<byte[],Map<String,Object>>(cf, commonFamilyProps));
            }

            // Bootstrapping for our SYSTEM.TABLE that creates itself before it exists
            if (SchemaUtil.isMetaTable(schemaName,tableName)) {
                // TODO: what about stats for system catalog?
                PName newSchemaName = PNameFactory.newName(schemaName);
                PTable table = PTableImpl.makePTable(tenantId,newSchemaName, PNameFactory.newName(tableName), tableType,
                        null, MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM,
                        PNameFactory.newName(QueryConstants.SYSTEM_TABLE_PK_NAME), null, columns, null, null,
                        Collections.<PTable>emptyList(), isImmutableRows,
                        Collections.<PName>emptyList(), defaultFamilyName == null ? null :
                                PNameFactory.newName(defaultFamilyName), null,
                        Boolean.TRUE.equals(disableWAL), false, false, null, indexId, indexType);
                connection.addTable(table);
            } else if (tableType == PTableType.INDEX && indexId == null) {
                if (tableProps.get(HTableDescriptor.MAX_FILESIZE) == null) {
                    int nIndexRowKeyColumns = isPK ? 1 : pkColumnsNames.size();
                    int nIndexKeyValueColumns = columns.size() - nIndexRowKeyColumns;
                    int nBaseRowKeyColumns = parent.getPKColumns().size() - (parent.getBucketNum() == null ? 0 : 1);
                    int nBaseKeyValueColumns = parent.getColumns().size() - parent.getPKColumns().size();
                    /*
                     * Approximate ratio between index table size and data table size:
                     * More or less equal to the ratio between the number of key value columns in each. We add one to
                     * the key value column count to take into account our empty key value. We add 1/4 for any key
                     * value data table column that was moved into the index table row key.
                     */
                    double ratio = (1+nIndexKeyValueColumns + (nIndexRowKeyColumns - nBaseRowKeyColumns)/4d)/(1+nBaseKeyValueColumns);
                    HTableDescriptor descriptor = connection.getQueryServices().getTableDescriptor(parent.getPhysicalName().getBytes());
                    if (descriptor != null) { // Is null for connectionless
                        long maxFileSize = descriptor.getMaxFileSize();
                        if (maxFileSize == -1) { // If unset, use default
                            maxFileSize = HConstants.DEFAULT_MAX_FILE_SIZE;
                        }
                        tableProps.put(HTableDescriptor.MAX_FILESIZE, (long)(maxFileSize * ratio));
                    }
                }
            }

            short nextKeySeq = 0;
            for (int i = 0; i < columns.size(); i++) {
                PColumn column = columns.get(i);
                final int columnPosition = column.getPosition();
                // For client-side cache, we need to update the column
                if (isViewColumnReferenced != null) {
                    if (viewColumnConstants != null && columnPosition < viewColumnConstants.length) {
                        columns.set(i, column = new DelegateColumn(column) {
                            @Override
                            public byte[] getViewConstant() {
                                return viewColumnConstants[columnPosition];
                            }
                            @Override
                            public boolean isViewReferenced() {
                                return isViewColumnReferenced.get(columnPosition);
                            }
                        });
                    } else {
                        columns.set(i, column = new DelegateColumn(column) {
                            @Override
                            public boolean isViewReferenced() {
                                return isViewColumnReferenced.get(columnPosition);
                            }
                        });
                    }
                }
                Short keySeq = SchemaUtil.isPKColumn(column) ? ++nextKeySeq : null;
                addColumnMutation(schemaName, tableName, column, colUpsert, parentTableName, pkName, keySeq, saltBucketNum != null);
            }

            tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
            connection.rollback();

            String dataTableName = parent == null || tableType == PTableType.VIEW ? null : parent.getTableName().getString();
            PIndexState indexState = parent == null || tableType == PTableType.VIEW  ? null : PIndexState.BUILDING;
            PreparedStatement tableUpsert = connection.prepareStatement(CREATE_TABLE);
            tableUpsert.setString(1, tenantIdStr);
            tableUpsert.setString(2, schemaName);
            tableUpsert.setString(3, tableName);
            tableUpsert.setString(4, tableType.getSerializedValue());
            tableUpsert.setLong(5, PTable.INITIAL_SEQ_NUM);
            tableUpsert.setInt(6, position);
            if (saltBucketNum != null) {
                tableUpsert.setInt(7, saltBucketNum);
            } else {
                tableUpsert.setNull(7, Types.INTEGER);
            }
            tableUpsert.setString(8, pkName);
            tableUpsert.setString(9, dataTableName);
            tableUpsert.setString(10, indexState == null ? null : indexState.getSerializedValue());
            tableUpsert.setBoolean(11, isImmutableRows);
            tableUpsert.setString(12, defaultFamilyName);
            tableUpsert.setString(13, viewStatement);
            tableUpsert.setBoolean(14, disableWAL);
            tableUpsert.setBoolean(15, multiTenant);
            if (viewType == null) {
                tableUpsert.setNull(16, Types.TINYINT);
            } else {
                tableUpsert.setByte(16, viewType.getSerializedValue());
            }
            if (indexId == null) {
                tableUpsert.setNull(17, Types.SMALLINT);
            } else {
                tableUpsert.setShort(17, indexId);
            }
            if (indexType == null) {
                tableUpsert.setNull(18, Types.TINYINT);
            } else {
                tableUpsert.setByte(18, indexType.getSerializedValue());
            }
            tableUpsert.setBoolean(19, storeNulls);
            tableUpsert.execute();

            tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
            connection.rollback();

            /*
             * The table metadata must be in the following order:
             * 1) table header row
             * 2) everything else
             * 3) parent table header row
             */
            Collections.reverse(tableMetaData);

            if (parent != null && tableType == PTableType.INDEX && indexType == IndexType.LOCAL) {
                tableProps.put(MetaDataUtil.PARENT_TABLE_KEY, parent.getPhysicalName().getString());
                tableProps.put(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_NAME, Boolean.TRUE);
                splits = getSplitKeys(connection.getQueryServices().getAllTableRegions(parent.getPhysicalName().getBytes()));
            } else {
                splits = SchemaUtil.processSplits(splits, pkColumns, saltBucketNum, connection.getQueryServices().getProps().getBoolean(
                    QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, QueryServicesOptions.DEFAULT_ROW_KEY_ORDER_SALTED_TABLE));
            }
            MetaDataMutationResult result = connection.getQueryServices().createTable(
                    tableMetaData,
                    viewType == ViewType.MAPPED || indexId != null ? physicalNames.get(0).getBytes() : null,
                    tableType, tableProps, familyPropList, splits);
            MutationCode code = result.getMutationCode();
            switch(code) {
            case TABLE_ALREADY_EXISTS:
                addTableToCache(result);
                if (!statement.ifNotExists()) {
                    throw new TableAlreadyExistsException(schemaName, tableName, result.getTable());
                }
                return null;
            case PARENT_TABLE_NOT_FOUND:
                throw new TableNotFoundException(schemaName, parent.getName().getString());
            case NEWER_TABLE_FOUND:
                // Add table to ConnectionQueryServices so it's cached, but don't add
                // it to this connection as we can't see it.
                throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            case CONCURRENT_TABLE_MUTATION:
                addTableToCache(result);
                throw new ConcurrentTableMutationException(schemaName, tableName);
            default:
                PName newSchemaName = PNameFactory.newName(schemaName);
                PTable table =  PTableImpl.makePTable(
                        tenantId, newSchemaName, PNameFactory.newName(tableName), tableType, indexState, result.getMutationTime(),
                        PTable.INITIAL_SEQ_NUM, pkName == null ? null : PNameFactory.newName(pkName), saltBucketNum, columns,
                        dataTableName == null ? null : newSchemaName, dataTableName == null ? null : PNameFactory.newName(dataTableName), Collections.<PTable>emptyList(), isImmutableRows,
                        physicalNames, defaultFamilyName == null ? null : PNameFactory.newName(defaultFamilyName), viewStatement, Boolean.TRUE.equals(disableWAL), multiTenant, storeNulls, viewType,
                        indexId, indexType);
                result = new MetaDataMutationResult(code, result.getMutationTime(), table, true);
                addTableToCache(result);
                return table;
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private byte[][] getSplitKeys(List<HRegionLocation> allTableRegions) {
        if(allTableRegions.size() == 1) return null;
        byte[][] splitKeys = new byte[allTableRegions.size()-1][];
        int i = 0;
        for (HRegionLocation region : allTableRegions) {
            if (region.getRegionInfo().getStartKey().length != 0) {
                splitKeys[i] = region.getRegionInfo().getStartKey();
                i++;
            }
        }
        return splitKeys;
    }

    private static boolean hasColumnWithSameNameAndFamily(Collection<PColumn> columns, PColumn column) {
        for (PColumn currColumn : columns) {
           if (Objects.equal(currColumn.getFamilyName(), column.getFamilyName()) &&
               Objects.equal(currColumn.getName(), column.getName())) {
               return true;
           }
        }
        return false;
    }

    /**
     * A table can be a parent table to tenant-specific tables if all of the following conditions are true:
     * <p>
     * FOR TENANT-SPECIFIC TABLES WITH TENANT_TYPE_ID SPECIFIED:
     * <ol>
     * <li>It has 3 or more PK columns AND
     * <li>First PK (tenant id) column is not nullible AND
     * <li>Firsts PK column's data type is either VARCHAR or CHAR AND
     * <li>Second PK (tenant type id) column is not nullible AND
     * <li>Second PK column data type is either VARCHAR or CHAR
     * </ol>
     * FOR TENANT-SPECIFIC TABLES WITH NO TENANT_TYPE_ID SPECIFIED:
     * <ol>
     * <li>It has 2 or more PK columns AND
     * <li>First PK (tenant id) column is not nullible AND
     * <li>Firsts PK column's data type is either VARCHAR or CHAR
     * </ol>
     */
    private static void throwIfInsufficientColumns(String schemaName, String tableName, Collection<PColumn> columns, boolean isSalted, boolean isMultiTenant) throws SQLException {
        if (!isMultiTenant) {
            return;
        }
        int nPKColumns = columns.size() - (isSalted ? 1 : 0);
        if (nPKColumns < 2) {
            throw new SQLExceptionInfo.Builder(INSUFFICIENT_MULTI_TENANT_COLUMNS).setSchemaName(schemaName).setTableName(tableName).build().buildException();
        }
        Iterator<PColumn> iterator = columns.iterator();
        if (isSalted) {
            iterator.next();
        }
        // Tenant ID must be VARCHAR or CHAR and be NOT NULL
        // NOT NULL is a requirement, since otherwise the table key would conflict
        // potentially with the global table definition.
        PColumn tenantIdCol = iterator.next();
        if (!tenantIdCol.getDataType().isCoercibleTo(PVarchar.INSTANCE) || tenantIdCol.isNullable()) {
            throw new SQLExceptionInfo.Builder(INSUFFICIENT_MULTI_TENANT_COLUMNS).setSchemaName(schemaName).setTableName(tableName).build().buildException();
        }
    }

    public MutationState dropTable(DropTableStatement statement) throws SQLException {
        String schemaName = statement.getTableName().getSchemaName();
        String tableName = statement.getTableName().getTableName();
        return dropTable(schemaName, tableName, null, statement.getTableType(), statement.ifExists(), statement.cascade());
    }

    public MutationState dropIndex(DropIndexStatement statement) throws SQLException {
        String schemaName = statement.getTableName().getSchemaName();
        String tableName = statement.getIndexName().getName();
        String parentTableName = statement.getTableName().getTableName();
        return dropTable(schemaName, tableName, parentTableName, PTableType.INDEX, statement.ifExists(), false);
    }

    private MutationState dropTable(String schemaName, String tableName, String parentTableName, PTableType tableType,
            boolean ifExists, boolean cascade) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            PName tenantId = connection.getTenantId();
            String tenantIdStr = tenantId == null ? null : tenantId.getString();
            byte[] key = SchemaUtil.getTableKey(tenantIdStr, schemaName, tableName);
            Long scn = connection.getSCN();
            long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
            Delete tableDelete = new Delete(key, clientTimeStamp);
            tableMetaData.add(tableDelete);
            boolean hasViewIndexTable = false;
            boolean hasLocalIndexTable = false;
            if (parentTableName != null) {
                byte[] linkKey = MetaDataUtil.getParentLinkKey(tenantIdStr, schemaName, parentTableName, tableName);
                Delete linkDelete = new Delete(linkKey, clientTimeStamp);
                tableMetaData.add(linkDelete);
            } else {
                hasViewIndexTable = MetaDataUtil.hasViewIndexTable(connection, schemaName, tableName);
                hasLocalIndexTable = MetaDataUtil.hasLocalIndexTable(connection, schemaName, tableName);
            }

            MetaDataMutationResult result = connection.getQueryServices().dropTable(tableMetaData, tableType, cascade);
            MutationCode code = result.getMutationCode();
            switch (code) {
            case TABLE_NOT_FOUND:
                if (!ifExists) { throw new TableNotFoundException(schemaName, tableName); }
                break;
            case NEWER_TABLE_FOUND:
                throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)

                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            default:
                connection.removeTable(tenantId, SchemaUtil.getTableName(schemaName, tableName), parentTableName,
                        result.getMutationTime());

                if (result.getTable() != null && tableType != PTableType.VIEW) {
                    connection.setAutoCommit(true);
                    PTable table = result.getTable();
                    boolean dropMetaData = result.getTable().getViewIndexId() == null &&
                            connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                    long ts = (scn == null ? result.getMutationTime() : scn);
                    // Create empty table and schema - they're only used to get the name from
                    // PName name, PTableType type, long timeStamp, long sequenceNumber, List<PColumn> columns
                    List<TableRef> tableRefs = Lists.newArrayListWithExpectedSize(2 + table.getIndexes().size());
                    // All multi-tenant tables have a view index table, so no need to check in that case
                    if (tableType == PTableType.TABLE
                            && (table.isMultiTenant() || hasViewIndexTable || hasLocalIndexTable)) {

                        MetaDataUtil.deleteViewIndexSequences(connection, table.getPhysicalName());
                        if (hasViewIndexTable) {
                            String viewIndexSchemaName = null;
                            String viewIndexTableName = null;
                            if (schemaName != null) {
                                viewIndexSchemaName = MetaDataUtil.getViewIndexTableName(schemaName);
                                viewIndexTableName = tableName;
                            } else {
                                viewIndexTableName = MetaDataUtil.getViewIndexTableName(tableName);
                            }
                            PTable viewIndexTable = new PTableImpl(null, viewIndexSchemaName, viewIndexTableName, ts,
                                    table.getColumnFamilies());
                            tableRefs.add(new TableRef(null, viewIndexTable, ts, false));
                        }
                        if (hasLocalIndexTable) {
                            String localIndexSchemaName = null;
                            String localIndexTableName = null;
                            if (schemaName != null) {
                                localIndexSchemaName = MetaDataUtil.getLocalIndexTableName(schemaName);
                                localIndexTableName = tableName;
                            } else {
                                localIndexTableName = MetaDataUtil.getLocalIndexTableName(tableName);
                            }
                            PTable localIndexTable = new PTableImpl(null, localIndexSchemaName, localIndexTableName,
                                    ts, Collections.<PColumnFamily> emptyList());
                            tableRefs.add(new TableRef(null, localIndexTable, ts, false));
                        }
                    }
                    tableRefs.add(new TableRef(null, table, ts, false));
                    // TODO: Let the standard mutable secondary index maintenance handle this?
                    for (PTable index : table.getIndexes()) {
                        tableRefs.add(new TableRef(null, index, ts, false));
                    }
                    deleteFromStatsTable(tableRefs, ts);
                    if (!dropMetaData) {
                        MutationPlan plan = new PostDDLCompiler(connection).compile(tableRefs, null, null,
                                Collections.<PColumn> emptyList(), ts);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        return connection.getQueryServices().updateData(plan);
                    }
                }
                break;
            }
            return new MutationState(0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private void deleteFromStatsTable(List<TableRef> tableRefs, long ts) throws SQLException {
        Properties props = new Properties(connection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(connection.getURL(), props);
        conn.setAutoCommit(true);
        boolean success = false;
        SQLException sqlException = null;
        try {
            StringBuilder buf = new StringBuilder("DELETE FROM SYSTEM.STATS WHERE PHYSICAL_NAME IN (");
            for (TableRef ref : tableRefs) {
                buf.append("'" + ref.getTable().getName().getString() + "',");
            }
            buf.setCharAt(buf.length() - 1, ')');
            conn.createStatement().execute(buf.toString());
            success = true;
        } catch (SQLException e) {
            sqlException = e;
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                if (sqlException == null) {
                    // If we're not in the middle of throwing another exception
                    // then throw the exception we got on close.
                    if (success) {
                        sqlException = e;
                    }
                } else {
                    sqlException.setNextException(e);
                }
            }
            if (sqlException != null) { throw sqlException; }
        }
    }

    private MutationCode processMutationResult(String schemaName, String tableName, MetaDataMutationResult result) throws SQLException {
        final MutationCode mutationCode = result.getMutationCode();
        PName tenantId = connection.getTenantId();
        switch (mutationCode) {
        case TABLE_NOT_FOUND:
            // Only called for add/remove column so parentTableName will always be null
            connection.removeTable(tenantId, SchemaUtil.getTableName(schemaName, tableName), null, HConstants.LATEST_TIMESTAMP);
            throw new TableNotFoundException(schemaName, tableName);
        case UNALLOWED_TABLE_MUTATION:
            String columnName = null;
            String familyName = null;
            String msg = null;
            // TODO: better to return error code
            if (result.getColumnName() != null) {
                familyName = result.getFamilyName() == null ? null : Bytes.toString(result.getFamilyName());
                columnName = Bytes.toString(result.getColumnName());
                msg = "Cannot drop column referenced by VIEW";
            }
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                .setSchemaName(schemaName).setTableName(tableName).setFamilyName(familyName).setColumnName(columnName).setMessage(msg).build().buildException();
        case NO_OP:
        case COLUMN_ALREADY_EXISTS:
        case COLUMN_NOT_FOUND:
            break;
        case CONCURRENT_TABLE_MUTATION:
            addTableToCache(result);
            if (logger.isDebugEnabled()) {
                logger.debug(LogUtil.addCustomAnnotations("CONCURRENT_TABLE_MUTATION for table " + SchemaUtil.getTableName(schemaName, tableName), connection));
            }
            throw new ConcurrentTableMutationException(schemaName, tableName);
        case NEWER_TABLE_FOUND:
            // TODO: update cache?
//            if (result.getTable() != null) {
//                connection.addTable(result.getTable());
//            }
            throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
        case NO_PK_COLUMNS:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
        case TABLE_ALREADY_EXISTS:
            break;
        default:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNEXPECTED_MUTATION_CODE).setSchemaName(schemaName)
                .setTableName(tableName).setMessage("mutation code: " + mutationCode).build().buildException();
        }
        return mutationCode;
    }

    private  long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta) throws SQLException {
        return incrementTableSeqNum(table, expectedType, columnCountDelta, null, null, null, null);
    }

    private long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta,
            Boolean isImmutableRows, Boolean disableWAL, Boolean isMultiTenant, Boolean storeNulls)
            throws SQLException {
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        // Ordinal position is 1-based and we don't count SALT column in ordinal position
        int totalColumnCount = table.getColumns().size() + (table.getBucketNum() == null ? 0 : -1);
        final long seqNum = table.getSequenceNumber() + 1;
        PreparedStatement tableUpsert = connection.prepareStatement(MUTATE_TABLE);
        String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
        try {
            tableUpsert.setString(1, tenantId);
            tableUpsert.setString(2, schemaName);
            tableUpsert.setString(3, tableName);
            tableUpsert.setString(4, expectedType.getSerializedValue());
            tableUpsert.setLong(5, seqNum);
            tableUpsert.setInt(6, totalColumnCount + columnCountDelta);
            tableUpsert.execute();
        } finally {
            tableUpsert.close();
        }
        if (isImmutableRows != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, IMMUTABLE_ROWS, isImmutableRows);
        }
        if (disableWAL != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, DISABLE_WAL, disableWAL);
        }
        if (isMultiTenant != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, MULTI_TENANT, isMultiTenant);
        }
        if (storeNulls != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, STORE_NULLS, storeNulls);
        }
        return seqNum;
    }

    private void mutateBooleanProperty(String tenantId, String schemaName, String tableName,
            String propertyName, boolean propertyValue) throws SQLException {
        String updatePropertySql = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                        TENANT_ID + "," +
                        TABLE_SCHEM + "," +
                        TABLE_NAME + "," +
                        propertyName +
                        ") VALUES (?, ?, ?, ?)";
        PreparedStatement tableBoolUpsert = connection.prepareStatement(updatePropertySql);
        tableBoolUpsert.setString(1, tenantId);
        tableBoolUpsert.setString(2, schemaName);
        tableBoolUpsert.setString(3, tableName);
        tableBoolUpsert.setBoolean(4, propertyValue);
        tableBoolUpsert.execute();
    }

    public MutationState addColumn(AddColumnStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            PName tenantId = connection.getTenantId();
            TableName tableNameNode = statement.getTable().getName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();

            Boolean isImmutableRowsProp = null;
            Boolean multiTenantProp = null;
            Boolean disableWALProp = null;
            Boolean storeNullsProp = null;

            ListMultimap<String,Pair<String,Object>> stmtProperties = statement.getProps();
            Map<String, List<Pair<String, Object>>> properties = new HashMap<>(stmtProperties.size());
            PTable table = FromCompiler.getResolver(statement, connection).getTables().get(0).getTable();
            List<ColumnDef> columnDefs = statement.getColumnDefs();
            if (columnDefs == null) {
                columnDefs = Collections.emptyList();
            }
            for (String family : stmtProperties.keySet()) {
                List<Pair<String, Object>> propsList = stmtProperties.get(family);
                for (Pair<String, Object> prop : propsList) {
                    String propName = prop.getFirst();
                    if (TableProperty.isPhoenixTableProperty(propName)) {
                        TableProperty.valueOf(propName).validate(true, !family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY), table.getType());
                        if (propName.equals(PTable.IS_IMMUTABLE_ROWS_PROP_NAME)) {
                            isImmutableRowsProp = (Boolean)prop.getSecond();
                        } else if (propName.equals(PhoenixDatabaseMetaData.MULTI_TENANT)) {
                            multiTenantProp = (Boolean)prop.getSecond();
                        } else if (propName.equals(DISABLE_WAL)) {
                            disableWALProp = (Boolean)prop.getSecond();
                        } else if (propName.equals(STORE_NULLS)) {
                            storeNullsProp = (Boolean)prop.getSecond();
                        }
                    } 
                }
                properties.put(family, propsList);
            }
            boolean retried = false;
            boolean changingPhoenixTableProperty = false;
            while (true) {
                ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                table = resolver.getTables().get(0).getTable();
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
                if (logger.isDebugEnabled()) {
                    logger.debug(LogUtil.addCustomAnnotations("Resolved table to " + table.getName().getString() + " with seqNum " + table.getSequenceNumber() + " at timestamp " + table.getTimeStamp() + " with " + table.getColumns().size() + " columns: " + table.getColumns(), connection));
                }

                int position = table.getColumns().size();

                List<PColumn> currentPKs = table.getPKColumns();
                PColumn lastPK = currentPKs.get(currentPKs.size()-1);
                // Disallow adding columns if the last column is VARBIANRY.
                if (lastPK.getDataType() == PVarbinary.INSTANCE || lastPK.getDataType().isArrayType()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_LAST_PK)
                    .setColumnName(lastPK.getName().getString()).build().buildException();
                }
                // Disallow adding columns if last column is fixed width and nullable.
                if (lastPK.isNullable() && lastPK.getDataType().isFixedWidth()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NULLABLE_FIXED_WIDTH_LAST_PK)
                    .setColumnName(lastPK.getName().getString()).build().buildException();
                }

                Boolean isImmutableRows = null;
                if (isImmutableRowsProp != null) {
                    if (isImmutableRowsProp.booleanValue() != table.isImmutableRows()) {
                        isImmutableRows = isImmutableRowsProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                Boolean multiTenant = null;
                if (multiTenantProp != null) {
                    if (multiTenantProp.booleanValue() != table.isMultiTenant()) {
                        multiTenant = multiTenantProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                Boolean disableWAL = null;
                if (disableWALProp != null) {
                    if (disableWALProp.booleanValue() != table.isWALDisabled()) {
                        disableWAL = disableWALProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                Boolean storeNulls = null;
                if (storeNullsProp != null) {
                    if (storeNullsProp.booleanValue() != table.getStoreNulls()) {
                        storeNulls = storeNullsProp;
                        changingPhoenixTableProperty = true;
                    }
                }

                int numPkColumnsAdded = 0;
                PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN);

                List<PColumn> columns = Lists.newArrayListWithExpectedSize(columnDefs.size());
                Set<String> colFamiliesForPColumnsToBeAdded = new LinkedHashSet<>();
                Set<String> families = new LinkedHashSet<>();
                if (columnDefs.size() > 0 ) {
                    short nextKeySeq = SchemaUtil.getMaxKeySeq(table);
                    for( ColumnDef colDef : columnDefs) {
                        if (colDef != null && !colDef.isNull()) {
                            if(colDef.isPK()) {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                            } else {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ADD_NOT_NULLABLE_COLUMN)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                            }
                        }
                        throwIfAlteringViewPK(colDef, table);
                        PColumn column = newColumn(position++, colDef, PrimaryKeyConstraint.EMPTY, table.getDefaultFamilyName() == null ? null : table.getDefaultFamilyName().getString(), true);
                        columns.add(column);
                        String pkName = null;
                        Short keySeq = null;

                        // TODO: support setting properties on other families?
                        if (column.getFamilyName() == null) {
                            ++numPkColumnsAdded;
                            pkName = table.getPKName() == null ? null : table.getPKName().getString();
                            keySeq = ++nextKeySeq;
                        } else {
                            families.add(column.getFamilyName().getString());
                        }
                        colFamiliesForPColumnsToBeAdded.add(column.getFamilyName() == null ? null : column.getFamilyName().getString());
                        addColumnMutation(schemaName, tableName, column, colUpsert, null, pkName, keySeq, table.getBucketNum() != null);
                    }

                    // Add any new PK columns to end of index PK
                    if (numPkColumnsAdded>0) {
                    	// create PK column list that includes the newly created columns
                    	List<PColumn> pkColumns = Lists.newArrayListWithExpectedSize(table.getPKColumns().size()+numPkColumnsAdded);
                    	pkColumns.addAll(table.getPKColumns());
                    	for (int i=0; i<columnDefs.size(); ++i) {
                    		if (columnDefs.get(i).isPK()) {
                    			pkColumns.add(columns.get(i));
                    		}
                    	}
                    	int pkSlotPosition = table.getPKColumns().size()-1;
                        for (PTable index : table.getIndexes()) {
                            short nextIndexKeySeq = SchemaUtil.getMaxKeySeq(index);
                            int indexPosition = index.getColumns().size();
                            for (int i=0; i<columnDefs.size(); ++i) {
                            	ColumnDef colDef = columnDefs.get(i);
                                if (colDef.isPK()) {
                                    PDataType indexColDataType = IndexUtil.getIndexColumnDataType(colDef.isNull(), colDef.getDataType());
                                    ColumnName indexColName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(null, colDef.getColumnDefName().getColumnName()));
                                    Expression expression = new RowKeyColumnExpression(columns.get(i), new RowKeyValueAccessor(pkColumns, ++pkSlotPosition));
                                    ColumnDef indexColDef = FACTORY.columnDef(indexColName, indexColDataType.getSqlTypeName(), colDef.isNull(), colDef.getMaxLength(), colDef.getScale(), true, colDef.getSortOrder(), expression.toString());
                                    PColumn indexColumn = newColumn(indexPosition++, indexColDef, PrimaryKeyConstraint.EMPTY, null, true);
                                    addColumnMutation(schemaName, index.getTableName().getString(), indexColumn, colUpsert, index.getParentTableName().getString(), index.getPKName() == null ? null : index.getPKName().getString(), ++nextIndexKeySeq, index.getBucketNum() != null);
                                }
                            }
                        }
                    }

                    tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                    connection.rollback();
                } else {
                    // Check that HBase configured properly for mutable secondary indexing
                    // if we're changing from an immutable table to a mutable table and we
                    // have existing indexes.
                    if (Boolean.FALSE.equals(isImmutableRows) && !table.getIndexes().isEmpty()) {
                        int hbaseVersion = connection.getQueryServices().getLowestClusterHBaseVersion();
                        if (hbaseVersion < PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES).setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                        if (connection.getQueryServices().hasInvalidIndexConfiguration()) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG).setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                    }
                    if (Boolean.TRUE.equals(multiTenant)) {
                        throwIfInsufficientColumns(schemaName, tableName, table.getPKColumns(), table.getBucketNum()!=null, multiTenant);
                    }
                }

                if (numPkColumnsAdded>0 && !table.getIndexes().isEmpty()) {
                    for (PTable index : table.getIndexes()) {
                        incrementTableSeqNum(index, index.getType(), 1);
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                    connection.rollback();
                }
                long seqNum = table.getSequenceNumber();
                if (changingPhoenixTableProperty || columnDefs.size() > 0) { 
                    seqNum = incrementTableSeqNum(table, statement.getTableType(), 1, isImmutableRows, disableWAL, multiTenant, storeNulls);
                    tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                    connection.rollback();
                }
                
                // Force the table header row to be first
                Collections.reverse(tableMetaData);

                byte[] family = families.size() > 0 ? families.iterator().next().getBytes() : null;

                // Figure out if the empty column family is changing as a result of adding the new column
                byte[] emptyCF = null;
                byte[] projectCF = null;
                if (table.getType() != PTableType.VIEW && family != null) {
                    if (table.getColumnFamilies().isEmpty()) {
                        emptyCF = family;
                    } else {
                        try {
                            table.getColumnFamily(family);
                        } catch (ColumnFamilyNotFoundException e) {
                            projectCF = family;
                            emptyCF = SchemaUtil.getEmptyColumnFamily(table);
                        }
                    }
                }

                MetaDataMutationResult result = connection.getQueryServices().addColumn(tableMetaData, table, properties, colFamiliesForPColumnsToBeAdded);
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_ALREADY_EXISTS) {
                        addTableToCache(result);
                        if (!statement.ifNotExists()) {
                            throw new ColumnAlreadyExistsException(schemaName, tableName, SchemaUtil.findExistingColumn(result.getTable(), columns));
                        }
                        return new MutationState(0,connection);
                    }

                    // Only update client side cache if we aren't adding a PK column to a table with indexes.
                    // We could update the cache manually then too, it'd just be a pain.
                    if (numPkColumnsAdded==0 || table.getIndexes().isEmpty()) {
                        connection.addColumn(tenantId, SchemaUtil.getTableName(schemaName, tableName), columns, result.getMutationTime(), seqNum, isImmutableRows == null ? table.isImmutableRows() : isImmutableRows, disableWAL == null ? table.isWALDisabled() : disableWAL, multiTenant == null ? table.isMultiTenant() : multiTenant, storeNulls == null ? table.getStoreNulls() : storeNulls);
                    }
                    // Delete rows in view index if we haven't dropped it already
                    // We only need to do this if the multiTenant transitioned to false
                    if (table.getType() == PTableType.TABLE
                            && Boolean.FALSE.equals(multiTenant)
                            && MetaDataUtil.hasViewIndexTable(connection, table.getPhysicalName())) {
                        connection.setAutoCommit(true);
                        MetaDataUtil.deleteViewIndexSequences(connection, table.getPhysicalName());
                        // If we're not dropping metadata, then make sure no rows are left in
                        // our view index physical table.
                        // TODO: remove this, as the DROP INDEX commands run when the DROP VIEW
                        // commands are run would remove all rows already.
                        if (!connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA)) {
                            Long scn = connection.getSCN();
                            long ts = (scn == null ? result.getMutationTime() : scn);
                            String viewIndexSchemaName = MetaDataUtil.getViewIndexSchemaName(schemaName);
                            String viewIndexTableName = MetaDataUtil.getViewIndexTableName(tableName);
                            PTable viewIndexTable = new PTableImpl(null, viewIndexSchemaName, viewIndexTableName, ts,
                                    table.getColumnFamilies());
                            List<TableRef> tableRefs = Collections.singletonList(new TableRef(null, viewIndexTable, ts, false));
                            MutationPlan plan = new PostDDLCompiler(connection).compile(tableRefs, null, null,
                                    Collections.<PColumn> emptyList(), ts);
                            connection.getQueryServices().updateData(plan);
                        }
                    }
                    if (emptyCF != null) {
                        Long scn = connection.getSCN();
                        connection.setAutoCommit(true);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(new TableRef(null, table, ts, false)), emptyCF, projectCF, null, ts);
                        return connection.getQueryServices().updateData(plan);
                    }
                    return new MutationState(0,connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(LogUtil.addCustomAnnotations("Caught ConcurrentTableMutationException for table " + SchemaUtil.getTableName(schemaName, tableName) + ". Will try again...", connection));
                    }
                    retried = true;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private String dropColumnMutations(PTable table, List<PColumn> columnsToDrop, List<Mutation> tableMetaData) throws SQLException {
        String tenantId = connection.getTenantId() == null ? "" : connection.getTenantId().getString();
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        String familyName = null;
        /*
         * Generate a fully qualified RVC with an IN clause, since that's what our optimizer can
         * handle currently. If/when the optimizer handles (A and ((B AND C) OR (D AND E))) we
         * can factor out the tenant ID, schema name, and table name columns
         */
        StringBuilder buf = new StringBuilder("DELETE FROM " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\" WHERE ");
        buf.append("(" +
                TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME + "," +
                COLUMN_NAME + ", " + COLUMN_FAMILY + ") IN (");
        for(PColumn columnToDrop : columnsToDrop) {
            buf.append("('" + tenantId + "'");
            buf.append(",'" + schemaName + "'");
            buf.append(",'" + tableName + "'");
            buf.append(",'" + columnToDrop.getName().getString() + "'");
            buf.append(",'" + (columnToDrop.getFamilyName() == null ? "" : columnToDrop.getFamilyName().getString()) + "'),");
        }
        buf.setCharAt(buf.length()-1, ')');

        connection.createStatement().execute(buf.toString());

        Collections.sort(columnsToDrop,new Comparator<PColumn> () {
            @Override
            public int compare(PColumn left, PColumn right) {
               return Ints.compare(left.getPosition(), right.getPosition());
            }
        });

        boolean isSalted = table.getBucketNum() != null;
        int columnsToDropIndex = 0;
        PreparedStatement colUpdate = connection.prepareStatement(UPDATE_COLUMN_POSITION);
        colUpdate.setString(1, tenantId);
        colUpdate.setString(2, schemaName);
        colUpdate.setString(3, tableName);
        for (int i = columnsToDrop.get(columnsToDropIndex).getPosition() + 1; i < table.getColumns().size(); i++) {
            PColumn column = table.getColumns().get(i);
            if(columnsToDrop.contains(column)) {
                columnsToDropIndex++;
                continue;
            }
            colUpdate.setString(4, column.getName().getString());
            colUpdate.setString(5, column.getFamilyName() == null ? null : column.getFamilyName().getString());
            // Adjust position to not include the salt column
            colUpdate.setInt(6, column.getPosition() - columnsToDropIndex - (isSalted ? 1 : 0));
            colUpdate.execute();
        }
       return familyName;
    }

    /**
     * Calculate what the new column family will be after the column is dropped, returning null
     * if unchanged.
     * @param table table containing column to drop
     * @param columnToDrop column being dropped
     * @return the new column family or null if unchanged.
     */
    private static byte[] getNewEmptyColumnFamilyOrNull (PTable table, PColumn columnToDrop) {
        if (table.getType() != PTableType.VIEW && !SchemaUtil.isPKColumn(columnToDrop) && table.getColumnFamilies().get(0).getName().equals(columnToDrop.getFamilyName()) && table.getColumnFamilies().get(0).getColumns().size() == 1) {
            return SchemaUtil.getEmptyColumnFamily(table.getDefaultFamilyName(), table.getColumnFamilies().subList(1, table.getColumnFamilies().size()));
        }
        // If unchanged, return null
        return null;
    }

    public MutationState dropColumn(DropColumnStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            PName tenantId = connection.getTenantId();
            TableName tableNameNode = statement.getTable().getName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            boolean retried = false;
            while (true) {
                final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                PTable table = resolver.getTables().get(0).getTable();
                List<ColumnName> columnRefs = statement.getColumnRefs();
                if(columnRefs == null) {
                    columnRefs = Lists.newArrayListWithCapacity(0);
                }
                TableRef tableRef = null;
                List<ColumnRef> columnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size() + table.getIndexes().size());
                List<TableRef> indexesToDrop = Lists.newArrayListWithExpectedSize(table.getIndexes().size());
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize((table.getIndexes().size() + 1) * (1 + table.getColumns().size() - columnRefs.size()));
                List<PColumn>  tableColumnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size());

                for(ColumnName column : columnRefs) {
                    ColumnRef columnRef = null;
                    try {
                        columnRef = resolver.resolveColumn(null, column.getFamilyName(), column.getColumnName());
                    } catch (ColumnNotFoundException e) {
                        if (statement.ifExists()) {
                            return new MutationState(0,connection);
                        }
                        throw e;
                    }
                    tableRef = columnRef.getTableRef();
                    PColumn columnToDrop = columnRef.getColumn();
                    tableColumnsToDrop.add(columnToDrop);
                    if (SchemaUtil.isPKColumn(columnToDrop)) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_DROP_PK)
                            .setColumnName(columnToDrop.getName().getString()).build().buildException();
                    }
                    columnsToDrop.add(new ColumnRef(tableRef, columnToDrop.getPosition()));
                }

                dropColumnMutations(table, tableColumnsToDrop, tableMetaData);
                for (PTable index : table.getIndexes()) {
                    List<PColumn> indexColumnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size());
                    for(PColumn columnToDrop : tableColumnsToDrop) {
                        String indexColumnName = IndexUtil.getIndexColumnName(columnToDrop);
                        try {
                            PColumn indexColumn = index.getColumn(indexColumnName);
                            if (SchemaUtil.isPKColumn(indexColumn)) {
                                indexesToDrop.add(new TableRef(index));
                            } else {
                                indexColumnsToDrop.add(indexColumn);
                                columnsToDrop.add(new ColumnRef(tableRef, columnToDrop.getPosition()));
                            }
                        } catch (ColumnNotFoundException e) {
                        }
                    }
                    if(!indexColumnsToDrop.isEmpty()) {
                        incrementTableSeqNum(index, index.getType(), -1);
                        dropColumnMutations(index, indexColumnsToDrop, tableMetaData);
                    }

                }
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();

                long seqNum = incrementTableSeqNum(table, statement.getTableType(), -1);
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
                // Force table header to be first in list
                Collections.reverse(tableMetaData);

                /*
                 * Ensure our "empty column family to be" exists. Somewhat of an edge case, but can occur if we drop the last column
                 * in a column family that was the empty column family. In that case, we have to pick another one. If there are no other
                 * ones, then we need to create our default empty column family. Note that this may no longer be necessary once we
                 * support declaring what the empty column family is on a table, as:
                 * - If you declare it, we'd just ensure it's created at DDL time and never switch what it is unless you change it
                 * - If you don't declare it, we can just continue to use the old empty column family in this case, dynamically updating
                 *    the empty column family name on the PTable.
                 */
                for (ColumnRef columnRefToDrop : columnsToDrop) {
                    PTable tableContainingColumnToDrop = columnRefToDrop.getTable();
                    byte[] emptyCF = getNewEmptyColumnFamilyOrNull(tableContainingColumnToDrop, columnRefToDrop.getColumn());
                    if (emptyCF != null) {
                        try {
                            tableContainingColumnToDrop.getColumnFamily(emptyCF);
                        } catch (ColumnFamilyNotFoundException e) {
                            // Only if it's not already a column family do we need to ensure it's created
                            Map<String, List<Pair<String,Object>>> family = new HashMap<>(1);
                            family.put(Bytes.toString(emptyCF), Collections.<Pair<String, Object>>emptyList());
                            // Just use a Put without any key values as the Mutation, as addColumn will treat this specially
                            // TODO: pass through schema name and table name instead to these methods as it's cleaner
                            byte[] tenantIdBytes = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
                            if (tenantIdBytes == null) tenantIdBytes = ByteUtil.EMPTY_BYTE_ARRAY;
                            connection.getQueryServices().addColumn(
                                    Collections.<Mutation>singletonList(new Put(SchemaUtil.getTableKey
                                            (tenantIdBytes, tableContainingColumnToDrop.getSchemaName().getBytes(),
                                            tableContainingColumnToDrop.getTableName().getBytes()))),
                                            tableContainingColumnToDrop, family, Sets.newHashSet(Bytes.toString(emptyCF)));

                        }
                    }
                }
                MetaDataMutationResult result = connection.getQueryServices().dropColumn(tableMetaData, statement.getTableType());
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_NOT_FOUND) {
                        addTableToCache(result);
                        if (!statement.ifExists()) {
                            throw new ColumnNotFoundException(schemaName, tableName, Bytes.toString(result.getFamilyName()), Bytes.toString(result.getColumnName()));
                        }
                        return new MutationState(0, connection);
                    }
                    // If we've done any index metadata updates, don't bother trying to update
                    // client-side cache as it would be too painful. Just let it pull it over from
                    // the server when needed.
                    if (tableColumnsToDrop.size() > 0 && indexesToDrop.isEmpty()) {
                        connection.removeColumn(tenantId, SchemaUtil.getTableName(schemaName, tableName) , tableColumnsToDrop, result.getMutationTime(), seqNum);
                    }
                    // If we have a VIEW, then only delete the metadata, and leave the table data alone
                    if (table.getType() != PTableType.VIEW) {
                        MutationState state = null;
                        connection.setAutoCommit(true);
                        Long scn = connection.getSCN();
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        PostDDLCompiler compiler = new PostDDLCompiler(connection);
                        boolean dropMetaData = connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                        if(!dropMetaData){
                            // Drop any index tables that had the dropped column in the PK
                            connection.getQueryServices().updateData(compiler.compile(indexesToDrop, null, null, Collections.<PColumn>emptyList(), ts));
                        }
                        // Update empty key value column if necessary
                        for (ColumnRef droppedColumnRef : columnsToDrop) {
                            // Painful, but we need a TableRef with a pre-set timestamp to prevent attempts
                            // to get any updates from the region server.
                            // TODO: move this into PostDDLCompiler
                            // TODO: consider filtering mutable indexes here, but then the issue is that
                            // we'd need to force an update of the data row empty key value if a mutable
                            // secondary index is changing its empty key value family.
                            droppedColumnRef = droppedColumnRef.cloneAtTimestamp(ts);
                            TableRef droppedColumnTableRef = droppedColumnRef.getTableRef();
                            PColumn droppedColumn = droppedColumnRef.getColumn();
                            MutationPlan plan = compiler.compile(
                                    Collections.singletonList(droppedColumnTableRef),
                                    getNewEmptyColumnFamilyOrNull(droppedColumnTableRef.getTable(), droppedColumn),
                                    null,
                                    Collections.singletonList(droppedColumn),
                                    ts);
                            state = connection.getQueryServices().updateData(plan);
                        }
                        // Return the last MutationState
                        return state;
                    }
                    return new MutationState(0, connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    table = connection.getMetaDataCache().getTable(new PTableKey(tenantId, fullTableName));
                    retried = true;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    public MutationState alterIndex(AlterIndexStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            String dataTableName = statement.getTableName();
            String schemaName = statement.getTable().getName().getSchemaName();
            String indexName = statement.getTable().getName().getTableName();
            PIndexState newIndexState = statement.getIndexState();
            if (newIndexState == PIndexState.REBUILD) {
                newIndexState = PIndexState.BUILDING;
            }
            connection.setAutoCommit(false);
            // Confirm index table is valid and up-to-date
            TableRef indexRef = FromCompiler.getResolver(statement, connection).getTables().get(0);
            PreparedStatement tableUpsert = null;
            try {
                if(newIndexState == PIndexState.ACTIVE){
                    tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE_TO_ACTIVE);
                } else {
                    tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE);
                }
                tableUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                tableUpsert.setString(2, schemaName);
                tableUpsert.setString(3, indexName);
                tableUpsert.setString(4, newIndexState.getSerializedValue());
                if(newIndexState == PIndexState.ACTIVE){
                    tableUpsert.setLong(5, 0);
                }
                tableUpsert.execute();
            } finally {
                if(tableUpsert != null) {
                    tableUpsert.close();
                }
            }
            List<Mutation> tableMetadata = connection.getMutationState().toMutations().next().getSecond();
            connection.rollback();

            MetaDataMutationResult result = connection.getQueryServices().updateIndexState(tableMetadata, dataTableName);
            MutationCode code = result.getMutationCode();
            if (code == MutationCode.TABLE_NOT_FOUND) {
                throw new TableNotFoundException(schemaName,indexName);
            }
            if (code == MutationCode.UNALLOWED_TABLE_MUTATION) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION)
                .setMessage(" currentState=" + indexRef.getTable().getIndexState() + ". requestedState=" + newIndexState )
                .setSchemaName(schemaName).setTableName(indexName).build().buildException();
            }
            if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                if (result.getTable() != null) { // To accommodate connection-less update of index state
                    addTableToCache(result);
                }
            }
            if (newIndexState == PIndexState.BUILDING) {
                PTable index = indexRef.getTable();
                // First delete any existing rows of the index
                Long scn = connection.getSCN();
                long ts = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(indexRef), null, null, Collections.<PColumn>emptyList(), ts);
                connection.getQueryServices().updateData(plan);
                NamedTableNode dataTableNode = NamedTableNode.create(null, TableName.create(schemaName, dataTableName), Collections.<ColumnDef>emptyList());
                // Next rebuild the index
                connection.setAutoCommit(true);
                if (connection.getSCN() != null) {
                    return buildIndexAtTimeStamp(index, dataTableNode);
                }
                TableRef dataTableRef = FromCompiler.getResolver(dataTableNode, connection).getTables().get(0);
                return buildIndex(index, dataTableRef);
            }
            return new MutationState(1, connection);
        } catch (TableNotFoundException e) {
            if (!statement.ifExists()) {
                throw e;
            }
            return new MutationState(0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private PTable addTableToCache(MetaDataMutationResult result) throws SQLException {
        addIndexesFromPhysicalTable(result);
        PTable table = result.getTable();
        connection.addTable(table);
        return table;
    }
    
    private void throwIfAlteringViewPK(ColumnDef col, PTable table) throws SQLException {
        if (col != null && col.isPK() && table.getType() == PTableType.VIEW) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MODIFY_VIEW_PK)
            .setSchemaName(table.getSchemaName().getString())
            .setTableName(table.getTableName().getString())
            .setColumnName(col.getColumnDefName().getColumnName())
            .build().buildException();
        }
    }

    public PTableStats getTableStats(PTable table) throws SQLException {
        /*
         *  The shared view index case is tricky, because we don't have
         *  table meta data for it, only an HBase table. We do have stats,
         *  though, so we'll query them directly here and cache them so
         *  we don't keep querying for them.
         */
        boolean isSharedIndex = table.getViewIndexId() != null;
        if (isSharedIndex) {
            return connection.getQueryServices().getTableStats(table.getPhysicalName().getBytes(), getClientTimeStamp());
        }
        boolean isView = table.getType() == PTableType.VIEW;
        String physicalName = table.getPhysicalName().getString();
        if (isView && table.getViewType() != ViewType.MAPPED) {
            try {
                return connection.getMetaDataCache().getTable(new PTableKey(null, physicalName)).getTableStats();
            } catch (TableNotFoundException e) {
                // Possible when the table timestamp == current timestamp - 1.
                // This would be most likely during the initial index build of a view index
                // where we're doing an upsert select from the tenant specific table.
                // TODO: would we want to always load the physical table in updateCache in
                // this case too, as we might not update the view with all of it's indexes?
                String physicalSchemaName = SchemaUtil.getSchemaNameFromFullName(physicalName);
                String physicalTableName = SchemaUtil.getTableNameFromFullName(physicalName);
                MetaDataMutationResult result = updateCache(null, physicalSchemaName, physicalTableName, false);
                if (result.getTable() == null) {
                    throw new TableNotFoundException(physicalSchemaName, physicalTableName);
                }
                return result.getTable().getTableStats();
            }
        }
        return table.getTableStats();
    }

}
