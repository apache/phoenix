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
import static org.apache.phoenix.exception.SQLExceptionCode.INSUFFICIENT_MULTI_TENANT_COLUMNS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
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
import static org.apache.phoenix.schema.PDataType.VARCHAR;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.PostDDLCompiler;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
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
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
            VIEW_INDEX_ID +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String CREATE_LINK =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_FAMILY + "," +
            LINK_TYPE +
            ") VALUES (?, ?, ?, ?, ?)";
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
    private static final String MUTATE_MULTI_TENANT =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " + 
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            MULTI_TENANT + 
            ") VALUES (?, ?, ?, ?)";
    private static final String MUTATE_DISABLE_WAL =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " + 
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            DISABLE_WAL + 
            ") VALUES (?, ?, ?, ?)";
    private static final String MUTATE_IMMUTABLE_ROWS =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " + 
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            IMMUTABLE_ROWS + 
            ") VALUES (?, ?, ?, ?)";
    private static final String UPDATE_INDEX_STATE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " + 
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            INDEX_STATE +
            ") VALUES (?, ?, ?, ?)";
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
        KEY_SEQ +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
    
    private MetaDataMutationResult updateCache(String schemaName, String tableName, boolean alwaysHitServer) throws SQLException { // TODO: pass byte[] here
        Long scn = connection.getSCN();
        boolean systemTable = SYSTEM_CATALOG_SCHEMA.equals(schemaName);
        // System tables must always have a null tenantId
        PName tenantId = systemTable ? null : connection.getTenantId();
        long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        PTable table = null;
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        long tableTimestamp = HConstants.LATEST_TIMESTAMP;
        try {
            table = connection.getMetaDataCache().getTable(new PTableKey(tenantId, fullTableName));
            tableTimestamp = table.getTimeStamp();
        } catch (TableNotFoundException e) {
            // TODO: Try again on services cache, as we may be looking for
            // a global multi-tenant table
        }
        // Don't bother with server call: we can't possibly find a newer table
        if (table != null && !alwaysHitServer && (systemTable || tableTimestamp == clientTimeStamp - 1)) {
            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,QueryConstants.UNSET_TIMESTAMP,table);
        }
        
        int maxTryCount = tenantId == null ? 1 : 2;
        int tryCount = 0;
        MetaDataMutationResult result;
        
        do {
            final byte[] schemaBytes = PDataType.VARCHAR.toBytes(schemaName);
            final byte[] tableBytes = PDataType.VARCHAR.toBytes(tableName);
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
                connection.addTable(resultTable);
                return result;
            } else {
                // if (result.getMutationCode() == MutationCode.NEWER_TABLE_FOUND) {
                // TODO: No table exists at the clientTimestamp, but a newer one exists.
                // Since we disallow creation or modification of a table earlier than the latest
                // timestamp, we can handle this such that we don't ask the
                // server again.
                // If table was not found at the current time stamp and we have one cached, remove it.
                // Otherwise, we're up to date, so there's nothing to do.
                if (table != null) {
                    result.setTable(table);
                    if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                        return result;
                    }
                    if (code == MutationCode.TABLE_NOT_FOUND && tryCount + 1 == maxTryCount) {
                        connection.removeTable(tenantId, fullTableName);
                    }
                }
            }
            tenantId = null; // Try again with global tenantId
        } while (++tryCount < maxTryCount);
        
        return result;
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
                    def.getMaxLength(), def.getScale(), isNull, position, sortOrder, def.getArraySize(), null, false);
            return column;
        } catch (IllegalArgumentException e) { // Based on precondition check in constructor
            throw new SQLException(e);
        }
    }

    public MutationState createTable(CreateTableStatement statement, byte[][] splits, PTable parent, String viewStatement, ViewType viewType, byte[][] viewColumnConstants, BitSet isViewColumnReferenced) throws SQLException {
        PTable table = createTableInternal(statement, splits, parent, viewStatement, viewType, viewColumnConstants, isViewColumnReferenced, null);
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
        boolean wasAutoCommit = connection.getAutoCommit();
        connection.rollback();
        try {
            connection.setAutoCommit(true);
            PostIndexDDLCompiler compiler = new PostIndexDDLCompiler(connection, dataTableRef);
            MutationPlan plan = compiler.compile(index);
            MutationState state = connection.getQueryServices().updateData(plan);
            AlterIndexStatement indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null, 
                    TableName.create(index.getSchemaName().getString(), index.getTableName().getString())),
                    dataTableRef.getTable().getTableName().getString(), false, PIndexState.ACTIVE);
            alterIndex(indexStatement);
            return state;
        } finally {
            connection.setAutoCommit(wasAutoCommit);
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
        PrimaryKeyConstraint pk = statement.getIndexConstraint();
        TableName indexTableName = statement.getIndexTableName();
        
        List<Pair<ColumnName, SortOrder>> indexedPkColumns = pk.getColumnNames();
        List<ColumnName> includedColumns = statement.getIncludeColumns();
        TableRef tableRef = null;
        PTable table = null;
        boolean retry = true;
        Short viewIndexId = null;
        boolean allocateViewIndexId = false;
        while (true) {
            try {
                ColumnResolver resolver = FromCompiler.getResolverForMutation(statement, connection);
                tableRef = resolver.getTables().get(0);
                PTable dataTable = tableRef.getTable();
                boolean isTenantConnection = connection.getTenantId() != null;
                if (isTenantConnection) {
                    if (dataTable.getType() != PTableType.VIEW) {
                        throw new SQLFeatureNotSupportedException("An index may only be created for a VIEW through a tenant-specific connection");
                    }
                }
                int hbaseVersion = connection.getQueryServices().getLowestClusterHBaseVersion();
                if (!dataTable.isImmutableRows()) {
                    if (hbaseVersion < PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
                    }
                    if (connection.getQueryServices().hasInvalidIndexConfiguration()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG).setTableName(indexTableName.getTableName()).build().buildException();
                    }
                }
                int posOffset = 0;
                Set<PColumn> unusedPkColumns;
                if (dataTable.getBucketNum() != null) { // Ignore SALT column
                    unusedPkColumns = new LinkedHashSet<PColumn>(dataTable.getPKColumns().subList(1, dataTable.getPKColumns().size()));
                    posOffset++;
                } else {
                    unusedPkColumns = new LinkedHashSet<PColumn>(dataTable.getPKColumns());
                }
                List<Pair<ColumnName, SortOrder>> allPkColumns = Lists.newArrayListWithExpectedSize(unusedPkColumns.size());
                List<ColumnDef> columnDefs = Lists.newArrayListWithExpectedSize(includedColumns.size() + indexedPkColumns.size());
                
                if (dataTable.isMultiTenant()) {
                    // Add tenant ID column as first column in index
                    PColumn col = dataTable.getPKColumns().get(posOffset);
                    unusedPkColumns.remove(col);
                    PDataType dataType = IndexUtil.getIndexColumnDataType(col);
                    ColumnName colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                    allPkColumns.add(new Pair<ColumnName, SortOrder>(colName, col.getSortOrder()));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, SortOrder.getDefault()));
                }
                if (dataTable.getType() == PTableType.VIEW && dataTable.getViewType() != ViewType.MAPPED) {
                    allocateViewIndexId = true;
                    // Next add index ID column
                    PDataType dataType = MetaDataUtil.getViewIndexIdDataType();
                    ColumnName colName = ColumnName.caseSensitiveColumnName(MetaDataUtil.getViewIndexIdColumnName());
                    allPkColumns.add(new Pair<ColumnName, SortOrder>(colName, SortOrder.getDefault()));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), false, null, null, false, SortOrder.getDefault()));
                }
                // First columns are the indexed ones
                for (Pair<ColumnName, SortOrder> pair : indexedPkColumns) {
                    ColumnName colName = pair.getFirst();
                    PColumn col = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName()).getColumn();
                    unusedPkColumns.remove(col);
                    // Ignore view constants for updatable views as we don't need these in the index
                    if (col.getViewConstant() == null) {
                        PDataType dataType = IndexUtil.getIndexColumnDataType(col);
                        colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                        allPkColumns.add(new Pair<ColumnName, SortOrder>(colName, pair.getSecond()));
                        columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, SortOrder.getDefault()));
                    }
                }
                
                // Next all the PK columns from the data table that aren't indexed
                if (!unusedPkColumns.isEmpty()) {
                    for (PColumn col : unusedPkColumns) {
                        // Don't add columns with constant values from updatable views, as
                        // we don't need these in the index
                        if (col.getViewConstant() == null) {
                            ColumnName colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                            allPkColumns.add(new Pair<ColumnName, SortOrder>(colName, col.getSortOrder()));
                            PDataType dataType = IndexUtil.getIndexColumnDataType(col);
                            columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, col.getSortOrder()));
                        }
                    }
                }
                pk = FACTORY.primaryKey(null, allPkColumns);
                
                // Last all the included columns (minus any PK columns)
                for (ColumnName colName : includedColumns) {
                    PColumn col = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName()).getColumn();
                    if (SchemaUtil.isPKColumn(col)) {
                        if (!unusedPkColumns.contains(col)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_EXIST_IN_DEF).build().buildException();
                        }
                    } else {
                        colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                        // Check for duplicates between indexed and included columns
                        if (pk.contains(colName)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_EXIST_IN_DEF).build().buildException();
                        }
                        if (!SchemaUtil.isPKColumn(col) && col.getViewConstant() == null) {
                            // Need to re-create ColumnName, since the above one won't have the column family name
                            colName = ColumnName.caseSensitiveColumnName(col.getFamilyName().getString(), IndexUtil.getIndexColumnName(col));
                            columnDefs.add(FACTORY.columnDef(colName, col.getDataType().getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, col.getSortOrder()));
                        }
                    }
                }
                
                // Don't re-allocate viewIndexId on ConcurrentTableMutationException,
                // as there's no need to burn another sequence value.
                if (allocateViewIndexId && viewIndexId == null) { 
                    Long scn = connection.getSCN();
                    long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                    PName tenantId = connection.getTenantId();
                    String tenantIdStr = tenantId == null ? null : connection.getTenantId().getString();
                    PName physicalName = dataTable.getPhysicalName();
                    SequenceKey key = MetaDataUtil.getViewIndexSequenceKey(tenantIdStr, physicalName);
                    // Create at parent timestamp as we know that will be earlier than now
                    // and earlier than any SCN if one is set.
                    createSequence(key.getTenantId(), key.getSchemaName(), key.getSequenceName(), true, Short.MIN_VALUE, 1, 1, dataTable.getTimeStamp());
                    long[] seqValues = new long[1];
                    SQLException[] sqlExceptions = new SQLException[1];
                    connection.getQueryServices().incrementSequences(Collections.singletonList(key), timestamp, seqValues, sqlExceptions);
                    if (sqlExceptions[0] != null) {
                        throw sqlExceptions[0];
                    }
                    long seqValue = seqValues[0];
                    if (seqValue > Short.MAX_VALUE) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.TOO_MANY_VIEW_INDEXES)
                        .setSchemaName(SchemaUtil.getSchemaNameFromFullName(physicalName.getString())).setTableName(SchemaUtil.getTableNameFromFullName(physicalName.getString())).build().buildException();
                    }
                    viewIndexId = (short) seqValue;
                }
                // Set DEFAULT_COLUMN_FAMILY_NAME of index to match data table
                // We need this in the props so that the correct column family is created
                if (dataTable.getDefaultFamilyName() != null && dataTable.getType() != PTableType.VIEW) {
                    statement.getProps().put("", new Pair<String,Object>(DEFAULT_COLUMN_FAMILY_NAME,dataTable.getDefaultFamilyName().getString()));
                }
                CreateTableStatement tableStatement = FACTORY.createTable(indexTableName, statement.getProps(), columnDefs, pk, statement.getSplitNodes(), PTableType.INDEX, statement.ifNotExists(), null, null, statement.getBindCount());
                table = createTableInternal(tableStatement, splits, dataTable, null, null, null, null, viewIndexId);
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
    
    public MutationState createSequence(CreateSequenceStatement statement, long startWith, long incrementBy, long cacheSize) throws SQLException {
        Long scn = connection.getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
        return createSequence(tenantId, statement.getSequenceName().getSchemaName(), statement.getSequenceName().getTableName(), statement.ifNotExists(), startWith, incrementBy, cacheSize, timestamp);
    }
    
    private MutationState createSequence(String tenantId, String schemaName, String sequenceName, boolean ifNotExists, long startWith, long incrementBy, long cacheSize, long timestamp) throws SQLException {
        try {
            connection.getQueryServices().createSequence(tenantId, schemaName, sequenceName, startWith, incrementBy, cacheSize, timestamp);
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
    
    private PTable createTableInternal(CreateTableStatement statement, byte[][] splits, final PTable parent, String viewStatement, ViewType viewType, final byte[][] viewColumnConstants, final BitSet isViewColumnReferenced, Short viewIndexId) throws SQLException {
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
            boolean isParentImmutableRows = false;
            boolean multiTenant = false;
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
                if (parent.getType() == PTableType.VIEW && parent.getViewType() != ViewType.MAPPED) {
                    PName physicalName = parent.getPhysicalName();
                    saltBucketNum = parent.getBucketNum();
                    addSaltColumn = (saltBucketNum != null);
                    defaultFamilyName = parent.getDefaultFamilyName() == null ? null : parent.getDefaultFamilyName().getString();
                    // Set physical name of view index table
                    physicalNames = Collections.singletonList(PNameFactory.newName(MetaDataUtil.getViewIndexPhysicalName(physicalName.getBytes())));
                }
                
                multiTenant = parent.isMultiTenant();
                isParentImmutableRows = parent.isImmutableRows();
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
            if (tableType != PTableType.VIEW && viewIndexId == null) {
                saltBucketNum = (Integer) tableProps.remove(PhoenixDatabaseMetaData.SALT_BUCKETS);
                if (saltBucketNum != null && (saltBucketNum < 0 || saltBucketNum > SaltingUtil.MAX_BUCKET_NUM)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_BUCKET_NUM).build().buildException();
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
            // Can't set MULTI_TENANT or DEFAULT_COLUMN_FAMILY_NAME on an index
            if (tableType != PTableType.INDEX && (tableType != PTableType.VIEW || viewType == ViewType.MAPPED)) {
                Boolean multiTenantProp = (Boolean) tableProps.remove(PhoenixDatabaseMetaData.MULTI_TENANT);
                multiTenant = Boolean.TRUE.equals(multiTenantProp);
                // Remove, but add back after our check below
                defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);  
                removedProp = (defaultFamilyName != null);
            }

            boolean disableWAL = false;
            Boolean disableWALProp = (Boolean) tableProps.remove(PhoenixDatabaseMetaData.DISABLE_WAL);
            if (disableWALProp == null) {
                disableWAL = isParentImmutableRows; // By default, disable WAL for immutable indexes
            } else {
                disableWAL = disableWALProp;
            }
            // Delay this check as it is supported to have IMMUTABLE_ROWS and SALT_BUCKETS defined on views
            if ((statement.getTableType() == PTableType.VIEW || viewIndexId != null) && !tableProps.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build().buildException();
            }
            if (removedProp) {
                tableProps.put(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME, defaultFamilyName);  
            }
            
            List<ColumnDef> colDefs = statement.getColumnDefs();
            List<PColumn> columns;
            LinkedHashSet<PColumn> pkColumns;    
            
            if (tenantId != null && (tableType != PTableType.VIEW && viewIndexId == null)) {
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
                if ((colDef.getDataType() == PDataType.VARBINARY || colDef.getDataType().isArrayType())  
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
                        combinedFamilyProps.put(prop.getFirst(), prop.getSecond());
                    }
                    familyPropList.add(new Pair<byte[],Map<String,Object>>(familyName.getBytes(),combinedFamilyProps));
                }
            }
            
            
            // Bootstrapping for our SYSTEM.TABLE that creates itself before it exists 
            if (SchemaUtil.isMetaTable(schemaName,tableName)) {
                PTable table = PTableImpl.makePTable(tenantId,PNameFactory.newName(schemaName), PNameFactory.newName(tableName), tableType,
                        null, MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM,
                        PNameFactory.newName(QueryConstants.SYSTEM_TABLE_PK_NAME), null, columns, null, Collections.<PTable>emptyList(), 
                        isImmutableRows, Collections.<PName>emptyList(),
                        defaultFamilyName == null ? null : PNameFactory.newName(defaultFamilyName), null, Boolean.TRUE.equals(disableWAL), false, null, viewIndexId);
                connection.addTable(table);
            } else if (tableType == PTableType.INDEX && viewIndexId == null) {
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
            if (viewIndexId == null) {
                tableUpsert.setNull(17, Types.SMALLINT);
            } else {
                tableUpsert.setShort(17, viewIndexId);
            }
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
            
            splits = SchemaUtil.processSplits(splits, pkColumns, saltBucketNum, connection.getQueryServices().getProps().getBoolean(
                    QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, QueryServicesOptions.DEFAULT_ROW_KEY_ORDER_SALTED_TABLE));
            MetaDataMutationResult result = connection.getQueryServices().createTable(
                    tableMetaData, 
                    viewType == ViewType.MAPPED || viewIndexId != null ? physicalNames.get(0).getBytes() : null,
                    tableType, tableProps, familyPropList, splits);
            MutationCode code = result.getMutationCode();
            switch(code) {
            case TABLE_ALREADY_EXISTS:
                connection.addTable(result.getTable());
                if (!statement.ifNotExists()) {
                    throw new TableAlreadyExistsException(schemaName, tableName);
                }
                return null;
            case PARENT_TABLE_NOT_FOUND:
                throw new TableNotFoundException(schemaName, parent.getName().getString());
            case NEWER_TABLE_FOUND:
                throw new NewerTableAlreadyExistsException(schemaName, tableName);
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            case CONCURRENT_TABLE_MUTATION:
                connection.addTable(result.getTable());
                throw new ConcurrentTableMutationException(schemaName, tableName);
            default:
                PTable table =  PTableImpl.makePTable(
                        tenantId, PNameFactory.newName(schemaName), PNameFactory.newName(tableName), tableType, indexState, result.getMutationTime(), 
                        PTable.INITIAL_SEQ_NUM, pkName == null ? null : PNameFactory.newName(pkName), saltBucketNum, columns, 
                        dataTableName == null ? null : PNameFactory.newName(dataTableName), Collections.<PTable>emptyList(), isImmutableRows, physicalNames,
                        defaultFamilyName == null ? null : PNameFactory.newName(defaultFamilyName), viewStatement, Boolean.TRUE.equals(disableWAL), multiTenant, viewType, viewIndexId);
                connection.addTable(table);
                return table;
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
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
        if (!tenantIdCol.getDataType().isCoercibleTo(VARCHAR) || tenantIdCol.isNullable()) {
            throw new SQLExceptionInfo.Builder(INSUFFICIENT_MULTI_TENANT_COLUMNS).setSchemaName(schemaName).setTableName(tableName).build().buildException();
        }
    }

    public MutationState dropTable(DropTableStatement statement) throws SQLException {
        String schemaName = statement.getTableName().getSchemaName();
        String tableName = statement.getTableName().getTableName();
        return dropTable(schemaName, tableName, null, statement.getTableType(), statement.ifExists());
    }

    public MutationState dropIndex(DropIndexStatement statement) throws SQLException {
        String schemaName = statement.getTableName().getSchemaName();
        String tableName = statement.getIndexName().getName();
        String parentTableName = statement.getTableName().getTableName();
        return dropTable(schemaName, tableName, parentTableName, PTableType.INDEX, statement.ifExists());
    }

    private MutationState dropTable(String schemaName, String tableName, String parentTableName, PTableType tableType, boolean ifExists) throws SQLException {
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
            if (parentTableName != null) {
                byte[] linkKey = MetaDataUtil.getParentLinkKey(tenantIdStr, schemaName, parentTableName, tableName);
                Delete linkDelete = new Delete(linkKey, clientTimeStamp);
                tableMetaData.add(linkDelete);
            }

            MetaDataMutationResult result = connection.getQueryServices().dropTable(tableMetaData, tableType);
            MutationCode code = result.getMutationCode();
            switch(code) {
                case TABLE_NOT_FOUND:
                    if (!ifExists) {
                        throw new TableNotFoundException(schemaName, tableName);
                    }
                    break;
                case NEWER_TABLE_FOUND:
                    throw new NewerTableAlreadyExistsException(schemaName, tableName);
                case UNALLOWED_TABLE_MUTATION:
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                default:
                    try {
                        // TODO: should we update the parent table by removing the index?
                        connection.removeTable(tenantId, tableName);
                    } catch (TableNotFoundException ignore) { } // Ignore - just means wasn't cached
                    
                    // TODO: we need to drop the index data when a view is dropped
                    boolean dropMetaData = connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                    if (result.getTable() != null && tableType != PTableType.VIEW) {
                        connection.setAutoCommit(true);
                        PTable table = result.getTable();
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        // Create empty table and schema - they're only used to get the name from
                        // PName name, PTableType type, long timeStamp, long sequenceNumber, List<PColumn> columns
                        List<TableRef> tableRefs = Lists.newArrayListWithExpectedSize(2 + table.getIndexes().size());
                        // All multi-tenant tables have a view index table, so no need to check in that case
                        if (tableType == PTableType.TABLE && (table.isMultiTenant() || MetaDataUtil.hasViewIndexTable(connection, table.getPhysicalName()))) {
                            MetaDataUtil.deleteViewIndexSequences(connection, table.getPhysicalName());
                            // TODO: consider removing this, as the DROP INDEX done for each DROP VIEW command
                            // would have deleted all the rows already
                            if (!dropMetaData) {
                                String viewIndexSchemaName = MetaDataUtil.getViewIndexSchemaName(schemaName);
                                String viewIndexTableName = MetaDataUtil.getViewIndexTableName(tableName);
                                PTable viewIndexTable = new PTableImpl(null, viewIndexSchemaName, viewIndexTableName, ts, table.getColumnFamilies());
                                tableRefs.add(new TableRef(null, viewIndexTable, ts, false));
                            }
                        }
                        if (!dropMetaData) {
                            // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                            tableRefs.add(new TableRef(null, table, ts, false));
                            // TODO: Let the standard mutable secondary index maintenance handle this?
                            for (PTable index: table.getIndexes()) {
                                tableRefs.add(new TableRef(null, index, ts, false));
                            }
                            MutationPlan plan = new PostDDLCompiler(connection).compile(tableRefs, null, null, Collections.<PColumn>emptyList(), ts);
                            return connection.getQueryServices().updateData(plan);
                        }
                    }
                    break;
                }
                 return new MutationState(0,connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private MutationCode processMutationResult(String schemaName, String tableName, MetaDataMutationResult result) throws SQLException {
        final MutationCode mutationCode = result.getMutationCode();
        PName tenantId = connection.getTenantId();
        switch (mutationCode) {
        case TABLE_NOT_FOUND:
            connection.removeTable(tenantId, tableName);
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
        case COLUMN_ALREADY_EXISTS:
        case COLUMN_NOT_FOUND:
            break;
        case CONCURRENT_TABLE_MUTATION:
            connection.addTable(result.getTable());
            if (logger.isDebugEnabled()) {
                logger.debug("CONCURRENT_TABLE_MUTATION for table " + SchemaUtil.getTableName(schemaName, tableName));
            }
            throw new ConcurrentTableMutationException(schemaName, tableName);
        case NEWER_TABLE_FOUND:
            if (result.getTable() != null) {
                connection.addTable(result.getTable());
            }
            throw new NewerTableAlreadyExistsException(schemaName, tableName);
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
        return incrementTableSeqNum(table, expectedType, columnCountDelta, null, null, null);
    }
    
    private long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta, Boolean isImmutableRows, Boolean disableWAL, Boolean isMultiTenant) throws SQLException {
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
            PreparedStatement tableBoolUpsert = connection.prepareStatement(MUTATE_IMMUTABLE_ROWS);
            tableBoolUpsert.setString(1, tenantId);
            tableBoolUpsert.setString(2, schemaName);
            tableBoolUpsert.setString(3, tableName);
            tableBoolUpsert.setBoolean(4, isImmutableRows);
            tableBoolUpsert.execute();
        }
        if (disableWAL != null) {
            PreparedStatement tableBoolUpsert = connection.prepareStatement(MUTATE_DISABLE_WAL);
            tableBoolUpsert.setString(1, tenantId);
            tableBoolUpsert.setString(2, schemaName);
            tableBoolUpsert.setString(3, tableName);
            tableBoolUpsert.setBoolean(4, disableWAL);
            tableBoolUpsert.execute();
        }
        if (isMultiTenant != null) {
            PreparedStatement tableBoolUpsert = connection.prepareStatement(MUTATE_MULTI_TENANT);
            tableBoolUpsert.setString(1, tenantId);
            tableBoolUpsert.setString(2, schemaName);
            tableBoolUpsert.setString(3, tableName);
            tableBoolUpsert.setBoolean(4, isMultiTenant);
            tableBoolUpsert.execute();
        }
        return seqNum;
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
            // Outside of retry loop, as we're removing the property and wouldn't find it the second time
            Boolean isImmutableRowsProp = (Boolean)statement.getProps().remove(PTable.IS_IMMUTABLE_ROWS_PROP_NAME);
            Boolean multiTenantProp = (Boolean)statement.getProps().remove(PhoenixDatabaseMetaData.MULTI_TENANT);
            Boolean disableWALProp = (Boolean)statement.getProps().remove(DISABLE_WAL);
            
            boolean retried = false;
            while (true) {
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
                ColumnResolver resolver = FromCompiler.getResolverForMutation(statement, connection);
                PTable table = resolver.getTables().get(0).getTable();
                if (logger.isDebugEnabled()) {
                    logger.debug("Resolved table to " + table.getName().getString() + " with seqNum " + table.getSequenceNumber() + " at timestamp " + table.getTimeStamp() + " with " + table.getColumns().size() + " columns: " + table.getColumns());
                }
                
                int position = table.getColumns().size();
                
                List<PColumn> currentPKs = table.getPKColumns();
                PColumn lastPK = currentPKs.get(currentPKs.size()-1);
                // Disallow adding columns if the last column is VARBIANRY.
                if (lastPK.getDataType() == PDataType.VARBINARY || lastPK.getDataType().isArrayType()) {
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
                    }
                }
                Boolean multiTenant = null;
                if (multiTenantProp != null) {
                    if (multiTenantProp.booleanValue() != table.isMultiTenant()) {
                        multiTenant = multiTenantProp;
                    }
                }
                Boolean disableWAL = null;
                if (disableWALProp != null) {
                    if (disableWALProp.booleanValue() != table.isWALDisabled()) {
                        disableWAL = disableWALProp;
                    }
                }
                
                if (statement.getProps().get(PhoenixDatabaseMetaData.SALT_BUCKETS) != null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SALT_ONLY_ON_CREATE_TABLE)
                    .setTableName(table.getName().getString()).build().buildException();
                }
                if (statement.getProps().get(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME) != null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE)
                    .setTableName(table.getName().getString()).build().buildException();
                }
                
                boolean isAddingPKColumn = false;
                PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN);
                
                List<ColumnDef> columnDefs = statement.getColumnDefs();
                if (columnDefs == null) {                    
                    columnDefs = Lists.newArrayListWithExpectedSize(1);
                }

                List<Pair<byte[],Map<String,Object>>> families = Lists.newArrayList();                
                List<PColumn> columns = Lists.newArrayListWithExpectedSize(columnDefs.size());

                if ( columnDefs.size() > 0 ) {
                    // Disallow setting these props for now on an add column, as that
                    // would a bit of a strange thing to do. We can revisit later if
                    // need be. We'd need to do the error checking we do in the else
                    // if we want to support this.
                    if (isImmutableRowsProp != null || multiTenantProp != null) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.SET_UNSUPPORTED_PROP_ON_ALTER_TABLE)
                        .setTableName(table.getName().getString()).build().buildException();
                    }
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
                            isAddingPKColumn = true;
                            pkName = table.getPKName() == null ? null : table.getPKName().getString();
                            keySeq = ++nextKeySeq;
                        } else {
                            families.add(new Pair<byte[],Map<String,Object>>(column.getFamilyName().getBytes(),statement.getProps()));
                        }
                        addColumnMutation(schemaName, tableName, column, colUpsert, null, pkName, keySeq, table.getBucketNum() != null);
                    }
                    // Add any new PK columns to end of index PK
                    if (isAddingPKColumn) {
                        for (PTable index : table.getIndexes()) {
                            short nextIndexKeySeq = SchemaUtil.getMaxKeySeq(index);
                            int indexPosition = index.getColumns().size();
                            for (ColumnDef colDef : columnDefs) {
                                if (colDef.isPK()) {
                                    PDataType indexColDataType = IndexUtil.getIndexColumnDataType(colDef.isNull(), colDef.getDataType());
                                    ColumnName indexColName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(null, colDef.getColumnDefName().getColumnName()));
                                    ColumnDef indexColDef = FACTORY.columnDef(indexColName, indexColDataType.getSqlTypeName(), colDef.isNull(), colDef.getMaxLength(), colDef.getScale(), true, colDef.getSortOrder());
                                    PColumn indexColumn = newColumn(indexPosition++, indexColDef, PrimaryKeyConstraint.EMPTY, null, true);
                                    addColumnMutation(schemaName, index.getTableName().getString(), indexColumn, colUpsert, index.getParentTableName().getString(), index.getPKName() == null ? null : index.getPKName().getString(), ++nextIndexKeySeq, index.getBucketNum() != null);
                                }
                            }
                        }
                    }

                    tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                    connection.rollback();
                } else {
                    // Only support setting IMMUTABLE_ROWS=true and DISABLE_WAL=true on ALTER TABLE SET command
                    // TODO: support setting HBase table properties too
                    if (!statement.getProps().isEmpty()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.SET_UNSUPPORTED_PROP_ON_ALTER_TABLE)
                        .setTableName(table.getName().getString()).build().buildException();
                    }
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
                
                if (isAddingPKColumn && !table.getIndexes().isEmpty()) {
                    for (PTable index : table.getIndexes()) {
                        incrementTableSeqNum(index, index.getType(), 1);
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                    connection.rollback();
                }
                long seqNum = incrementTableSeqNum(table, statement.getTableType(), 1, isImmutableRows, disableWAL, multiTenant);
                
                tableMetaData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
                // Force the table header row to be first
                Collections.reverse(tableMetaData);
                
                Pair<byte[],Map<String,Object>> family = families.size() > 0 ? families.get(0) : null;
                
                // Figure out if the empty column family is changing as a result of adding the new column
                byte[] emptyCF = null;
                byte[] projectCF = null;
                if (table.getType() != PTableType.VIEW && family != null) {
                    if (table.getColumnFamilies().isEmpty()) {
                        emptyCF = family.getFirst();
                    } else {
                        try {
                            table.getColumnFamily(family.getFirst());
                        } catch (ColumnFamilyNotFoundException e) {
                            projectCF = family.getFirst();
                            emptyCF = SchemaUtil.getEmptyColumnFamily(table);
                        }
                    }
                }
                MetaDataMutationResult result = connection.getQueryServices().addColumn(tableMetaData, families, table);
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_ALREADY_EXISTS) {
                        connection.addTable(result.getTable());
                        if (!statement.ifNotExists()) {
                            throw new ColumnAlreadyExistsException(schemaName, tableName, SchemaUtil.findExistingColumn(result.getTable(), columns));
                        }
                        return new MutationState(0,connection);
                    }
                    // Only update client side cache if we aren't adding a PK column to a table with indexes.
                    // We could update the cache manually then too, it'd just be a pain.
                    if (!isAddingPKColumn || table.getIndexes().isEmpty()) {
                        connection.addColumn(tenantId, SchemaUtil.getTableName(schemaName, tableName), columns, result.getMutationTime(), seqNum, isImmutableRows == null ? table.isImmutableRows() : isImmutableRows);
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
                        logger.debug("Caught ConcurrentTableMutationException for table " + SchemaUtil.getTableName(schemaName, tableName) + ". Will try again...");
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
                final ColumnResolver resolver = FromCompiler.getResolverForMutation(statement, connection);
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
                            List<Pair<byte[],Map<String,Object>>> family = Lists.newArrayListWithExpectedSize(1);
                            family.add(new Pair<byte[],Map<String,Object>>(emptyCF,Collections.<String,Object>emptyMap()));
                            // Just use a Put without any key values as the Mutation, as addColumn will treat this specially
                            // TODO: pass through schema name and table name instead to these methods as it's cleaner
                            byte[] tenantIdBytes = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
                            if (tenantIdBytes == null) tenantIdBytes = ByteUtil.EMPTY_BYTE_ARRAY;
                            connection.getQueryServices().addColumn(
                                    Collections.<Mutation>singletonList(new Put(SchemaUtil.getTableKey
                                            (tenantIdBytes, tableContainingColumnToDrop.getSchemaName().getBytes(),
                                            tableContainingColumnToDrop.getTableName().getBytes()))),
                                    family,tableContainingColumnToDrop);
                        }
                    }
                }
                MetaDataMutationResult result = connection.getQueryServices().dropColumn(tableMetaData, statement.getTableType());
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_NOT_FOUND) {
                        connection.addTable(result.getTable());
                        if (!statement.ifExists()) {
                            throw new ColumnNotFoundException(schemaName, tableName, Bytes.toString(result.getFamilyName()), Bytes.toString(result.getColumnName()));
                        }
                        return new MutationState(0, connection);
                    }
                    // If we've done any index metadata updates, don't bother trying to update
                    // client-side cache as it would be too painful. Just let it pull it over from
                    // the server when needed.
                    if (columnsToDrop.size() > 0 && indexesToDrop.isEmpty()) {
                        for(PColumn columnToDrop : tableColumnsToDrop) {
                            connection.removeColumn(tenantId, SchemaUtil.getTableName(schemaName, tableName) , columnToDrop.getFamilyName().getString(), columnToDrop.getName().getString(), result.getMutationTime(), seqNum);
                        }
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
                            droppedColumnRef = new ColumnRef(droppedColumnRef, ts);
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
            TableRef indexRef = FromCompiler.getResolverForMutation(statement, connection).getTables().get(0);
            PreparedStatement tableUpsert = null;
            try {
                tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE);
                tableUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                tableUpsert.setString(2, schemaName);
                tableUpsert.setString(3, indexName);
                tableUpsert.setString(4, newIndexState.getSerializedValue());
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
                    connection.addTable(result.getTable());
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
    
    private void throwIfAlteringViewPK(ColumnDef col, PTable table) throws SQLException {
        if (col != null && col.isPK() && table.getType() == PTableType.VIEW) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MODIFY_VIEW_PK)
            .setSchemaName(table.getSchemaName().getString())
            .setTableName(table.getTableName().getString())
            .setColumnName(col.getColumnDefName().getColumnName())
            .build().buildException();
        }
    }
}
