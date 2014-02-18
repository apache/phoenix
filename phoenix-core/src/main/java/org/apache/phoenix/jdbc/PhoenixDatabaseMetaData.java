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
package org.apache.phoenix.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.IndexStateNameFunction;
import org.apache.phoenix.expression.function.SQLTableTypeFunction;
import org.apache.phoenix.expression.function.SQLViewTypeFunction;
import org.apache.phoenix.expression.function.SqlTypeNameFunction;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.MaterializedResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;


/**
 * 
 * JDBC DatabaseMetaData implementation of Phoenix reflecting read-only nature of driver.
 * Supported metadata methods include:
 * {@link #getTables(String, String, String, String[])}
 * {@link #getColumns(String, String, String, String)}
 * {@link #getTableTypes()}
 * {@link #getPrimaryKeys(String, String, String)}
 * {@link #getIndexInfo(String, String, String, boolean, boolean)}
 * {@link #getSchemas()}
 * {@link #getSchemas(String, String)}
 * {@link #getDatabaseMajorVersion()}
 * {@link #getDatabaseMinorVersion()}
 * {@link #getClientInfoProperties()}
 * {@link #getConnection()}
 * {@link #getDatabaseProductName()}
 * {@link #getDatabaseProductVersion()}
 * {@link #getDefaultTransactionIsolation()}
 * {@link #getDriverName()}
 * {@link #getDriverVersion()}
 * Other ResultSet methods return an empty result set.
 * 
 * 
 * @since 0.1
 */
public class PhoenixDatabaseMetaData implements DatabaseMetaData, org.apache.phoenix.jdbc.Jdbc7Shim.DatabaseMetaData {
    public static final int INDEX_NAME_INDEX = 4; // Shared with FAMILY_NAME_INDEX
    public static final int FAMILY_NAME_INDEX = 4;
    public static final int COLUMN_NAME_INDEX = 3;
    public static final int TABLE_NAME_INDEX = 2;
    public static final int SCHEMA_NAME_INDEX = 1;
    public static final int TENANT_ID_INDEX = 0;

    public static final String TYPE_SCHEMA = "SYSTEM";
    public static final String TYPE_TABLE = "CATALOG";
    public static final String TYPE_SCHEMA_AND_TABLE = TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"";
    public static final byte[] TYPE_TABLE_BYTES = TYPE_TABLE.getBytes();
    public static final byte[] TYPE_SCHEMA_BYTES = TYPE_SCHEMA.getBytes();
    public static final String TYPE_TABLE_NAME = SchemaUtil.getTableName(TYPE_SCHEMA, TYPE_TABLE);
    public static final byte[] TYPE_TABLE_NAME_BYTES = SchemaUtil.getTableNameAsBytes(TYPE_SCHEMA_BYTES, TYPE_TABLE_BYTES);
    
    public static final String TYPE_SCHEMA_ALIAS = "SYSTEM";
    public static final String TYPE_TABLE_ALIAS = "TABLE";
    public static final String TYPE_SCHEMA_AND_TABLE_ALIAS = "\"" + TYPE_SCHEMA_ALIAS + "." + TYPE_TABLE_ALIAS + "\"";

    public static final String TABLE_NAME_NAME = "TABLE_NAME";
    public static final String TABLE_TYPE_NAME = "TABLE_TYPE";
    public static final byte[] TABLE_TYPE_BYTES = Bytes.toBytes(TABLE_TYPE_NAME);
    
    public static final String TABLE_CAT_NAME = "TABLE_CAT";
    public static final String TABLE_CATALOG_NAME = "TABLE_CATALOG";
    public static final String TABLE_SCHEM_NAME = "TABLE_SCHEM";
    public static final String REMARKS_NAME = "REMARKS";
    public static final String TYPE_CAT_NAME = "TYPE_CAT";
    public static final String TYPE_SCHEM_NAME = "TYPE_SCHEM";
    public static final String TYPE_NAME_NAME = "TYPE_NAME";
    public static final String SELF_REFERENCING_COL_NAME_NAME = "SELF_REFERENCING_COL_NAME";
    public static final String REF_GENERATION_NAME = "REF_GENERATION";
    public static final String PK_NAME = "PK_NAME";
    public static final byte[] PK_NAME_BYTES = Bytes.toBytes(PK_NAME);
    public static final String TABLE_SEQ_NUM = "TABLE_SEQ_NUM";
    public static final byte[] TABLE_SEQ_NUM_BYTES = Bytes.toBytes(TABLE_SEQ_NUM);
    public static final String COLUMN_COUNT = "COLUMN_COUNT";
    public static final byte[] COLUMN_COUNT_BYTES = Bytes.toBytes(COLUMN_COUNT);
    public static final String SALT_BUCKETS = "SALT_BUCKETS";
    public static final byte[] SALT_BUCKETS_BYTES = Bytes.toBytes(SALT_BUCKETS);
 
    public static final String DATA_TABLE_NAME = "DATA_TABLE_NAME";
    public static final byte[] DATA_TABLE_NAME_BYTES = Bytes.toBytes(DATA_TABLE_NAME);
    public static final String INDEX_STATE = "INDEX_STATE";
    public static final byte[] INDEX_STATE_BYTES = Bytes.toBytes(INDEX_STATE);

    public static final String TENANT_ID = "TENANT_ID";
    public static final byte[] TENANT_ID_BYTES = Bytes.toBytes(TENANT_ID);
    
    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String DATA_TYPE = "DATA_TYPE";
    public static final byte[] DATA_TYPE_BYTES = Bytes.toBytes(DATA_TYPE);
    public static final String TYPE_NAME = "TYPE_NAME";
    public static final String COLUMN_SIZE = "COLUMN_SIZE";
    public static final byte[] COLUMN_SIZE_BYTES = Bytes.toBytes(COLUMN_SIZE);
    public static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    public static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    public static final byte[] DECIMAL_DIGITS_BYTES = Bytes.toBytes(DECIMAL_DIGITS);
    public static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
    public static final String NULLABLE = "NULLABLE";
    public static final byte[] NULLABLE_BYTES = Bytes.toBytes(NULLABLE);
    public static final String COLUMN_DEF = "COLUMN_DEF";
    public static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
    public static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
    public static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
    public static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    public static final byte[] ORDINAL_POSITION_BYTES = Bytes.toBytes(ORDINAL_POSITION);
    public static final String IS_NULLABLE = "IS_NULLABLE";
    public static final String SCOPE_CATALOG = "SCOPE_CATALOG";
    public static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
    public static final String SCOPE_TABLE = "SCOPE_TABLE";
    public static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
    public static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
    public static final String SORT_ORDER = "SORT_ORDER";
    public static final byte[] SORT_ORDER_BYTES = Bytes.toBytes(SORT_ORDER);
    public static final String IMMUTABLE_ROWS = "IMMUTABLE_ROWS";
    public static final byte[] IMMUTABLE_ROWS_BYTES = Bytes.toBytes(IMMUTABLE_ROWS);
    public static final String DEFAULT_COLUMN_FAMILY_NAME = "DEFAULT_COLUMN_FAMILY";
    public static final byte[] DEFAULT_COLUMN_FAMILY_NAME_BYTES = Bytes.toBytes(DEFAULT_COLUMN_FAMILY_NAME);
    public static final String VIEW_STATEMENT = "VIEW_STATEMENT";
    public static final byte[] VIEW_STATEMENT_BYTES = Bytes.toBytes(VIEW_STATEMENT);
    public static final String DISABLE_WAL = "DISABLE_WAL";
    public static final byte[] DISABLE_WAL_BYTES = Bytes.toBytes(DISABLE_WAL);
    public static final String MULTI_TENANT = "MULTI_TENANT";
    public static final byte[] MULTI_TENANT_BYTES = Bytes.toBytes(MULTI_TENANT);
    public static final String VIEW_TYPE = "VIEW_TYPE";
    public static final byte[] VIEW_TYPE_BYTES = Bytes.toBytes(VIEW_TYPE);
    public static final String LINK_TYPE = "LINK_TYPE";
    public static final byte[] LINK_TYPE_BYTES = Bytes.toBytes(LINK_TYPE);
    public static final String ARRAY_SIZE = "ARRAY_SIZE";
    public static final byte[] ARRAY_SIZE_BYTES = Bytes.toBytes(ARRAY_SIZE);
    public static final String VIEW_CONSTANT = "VIEW_CONSTANT";
    public static final byte[] VIEW_CONSTANT_BYTES = Bytes.toBytes(VIEW_CONSTANT);
    public static final String VIEW_INDEX_ID = "VIEW_INDEX_ID";
    public static final byte[] VIEW_INDEX_ID_BYTES = Bytes.toBytes(VIEW_INDEX_ID);

    public static final String TABLE_FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY;
    public static final byte[] TABLE_FAMILY_BYTES = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
    
    public static final String TYPE_SEQUENCE = "SEQUENCE";
    public static final byte[] SEQUENCE_FAMILY_BYTES = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
    public static final String SEQUENCE_TABLE_NAME = TYPE_SCHEMA + ".\"" + TYPE_SEQUENCE + "\"";
    public static final byte[] SEQUENCE_TABLE_NAME_BYTES = SchemaUtil.getTableNameAsBytes(TYPE_SCHEMA, TYPE_SEQUENCE);
    public static final String SEQUENCE_SCHEMA = "SEQUENCE_SCHEMA";
    public static final String SEQUENCE_NAME = "SEQUENCE_NAME";
    public static final String CURRENT_VALUE = "CURRENT_VALUE";
    public static final byte[] CURRENT_VALUE_BYTES = Bytes.toBytes(CURRENT_VALUE);
    public static final String START_WITH = "START_WITH";
    public static final byte[] START_WITH_BYTES = Bytes.toBytes(START_WITH);
    public static final String INCREMENT_BY = "INCREMENT_BY";
    public static final byte[] INCREMENT_BY_BYTES = Bytes.toBytes(INCREMENT_BY);
    public static final String CACHE_SIZE = "CACHE_SIZE";
    public static final byte[] CACHE_SIZE_BYTES = Bytes.toBytes(CACHE_SIZE);
    
    private final PhoenixConnection connection;
    private final ResultSet emptyResultSet;

    // Version below which we should turn off essential column family.
    public static final int ESSENTIAL_FAMILY_VERSION_THRESHOLD = MetaDataUtil.encodeVersion("0", "94", "7");
    // Version below which we should disallow usage of mutable secondary indexing.
    public static final int MUTABLE_SI_VERSION_THRESHOLD = MetaDataUtil.encodeVersion("0", "94", "10");
    /** Version below which we fall back on the generic KeyValueBuilder */
    public static final int CLIENT_KEY_VALUE_BUILDER_THRESHOLD = MetaDataUtil.encodeVersion("0", "94", "14");

    PhoenixDatabaseMetaData(PhoenixConnection connection) throws SQLException {
        this.emptyResultSet = new PhoenixResultSet(ResultIterator.EMPTY_ITERATOR, RowProjector.EMPTY_PROJECTOR, new PhoenixStatement(connection));
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "Catalog";
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return emptyResultSet;
    }
    
    private String getTenantIdWhereClause() {
        PName tenantId = connection.getTenantId();
        return "(" + TENANT_ID + " IS NULL " + 
                (tenantId == null
                   ? ") "
                   : " OR " + TENANT_ID + " = '" + StringEscapeUtils.escapeSql(tenantId.getString()) + "') ");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        StringBuilder buf = new StringBuilder("select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/" +
                TABLE_CAT_NAME + "," + // use this column for column family name
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                COLUMN_NAME + "," +
                DATA_TYPE + "," +
                SqlTypeNameFunction.NAME + "(" + DATA_TYPE + ") AS " + TYPE_NAME + "," +
                COLUMN_SIZE + "," +
                BUFFER_LENGTH + "," +
                DECIMAL_DIGITS + "," +
                NUM_PREC_RADIX + "," +
                NULLABLE + "," +
                COLUMN_DEF + "," +
                SQL_DATA_TYPE + "," +
                SQL_DATETIME_SUB + "," +
                CHAR_OCTET_LENGTH + "," +
                ORDINAL_POSITION + "," +
                "CASE " + NULLABLE + " WHEN " + DatabaseMetaData.attributeNoNulls +  " THEN '" + Boolean.FALSE.toString() + "' WHEN " + DatabaseMetaData.attributeNullable + " THEN '" + Boolean.TRUE.toString() + "' END AS " + IS_NULLABLE + "," +
                SCOPE_CATALOG + "," +
                SCOPE_SCHEMA + "," +
                SCOPE_TABLE + "," +
                SOURCE_DATA_TYPE + "," +
                IS_AUTOINCREMENT + "," + 
                ARRAY_SIZE +
                " from " + TYPE_SCHEMA_AND_TABLE + " " + TYPE_SCHEMA_AND_TABLE_ALIAS +
                " where ");
        buf.append(getTenantIdWhereClause());
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM_NAME + (schemaPattern.length() == 0 ? " is null" : " like '" + SchemaUtil.normalizeIdentifier(schemaPattern) + "'" ));
        }
        if (tableNamePattern != null && tableNamePattern.length() > 0) {
            buf.append(" and " + TABLE_NAME_NAME + " like '" + SchemaUtil.normalizeIdentifier(tableNamePattern) + "'" );
        }
        if (catalog != null && catalog.length() > 0) { // if null or empty, will pick up all columns
            // Will pick up only KV columns
            // We supported only getting the PK columns by using catalog="", but some clients pass this through
            // instead of null (namely SQLLine), so better to just treat these the same. If only PK columns are
            // wanted, you can just stop the scan when you get to a non null TABLE_CAT_NAME
            buf.append(" and " + TABLE_CAT_NAME + " like '" + SchemaUtil.normalizeIdentifier(catalog) + "'" );
        }
        if (columnNamePattern != null && columnNamePattern.length() > 0) {
            buf.append(" and " + COLUMN_NAME + " like '" + SchemaUtil.normalizeIdentifier(columnNamePattern) + "'" );
        } else {
            buf.append(" and " + COLUMN_NAME + " is not null" );
        }
        buf.append(" order by " + TABLE_SCHEM_NAME + "," + TABLE_NAME_NAME + "," + ORDINAL_POSITION);
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return MetaDataProtocol.PHOENIX_MAJOR_VERSION;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return MetaDataProtocol.PHOENIX_MINOR_VERSION;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Phoenix";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return Integer.toString(getDatabaseMajorVersion()) + "." + Integer.toString(getDatabaseMinorVersion());
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    @Override
    public int getDriverMajorVersion() {
        return Integer.parseInt(connection.getClientInfo(PhoenixEmbeddedDriver.MAJOR_VERSION_PROP));
    }

    @Override
    public int getDriverMinorVersion() {
        return Integer.parseInt(connection.getClientInfo(PhoenixEmbeddedDriver.MINOR_VERSION_PROP));
    }

    @Override
    public String getDriverName() throws SQLException {
        return connection.getClientInfo(PhoenixEmbeddedDriver.DRIVER_NAME_PROP);
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return connection.getClientInfo(PhoenixEmbeddedDriver.MAJOR_VERSION_PROP) + "." + connection.getClientInfo(PhoenixEmbeddedDriver.MINOR_VERSION_PROP);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        // Catalogs are not supported for schemas
        if (catalog != null && catalog.length() > 0) {
            return emptyResultSet;
        }
        if (unique) { // No unique indexes
            return emptyResultSet;
        }
        StringBuilder buf = new StringBuilder("select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/\n" +
                "null " + TABLE_CAT_NAME + ",\n" + // use this column for column family name
                TABLE_SCHEM_NAME + ",\n" +
                DATA_TABLE_NAME + " " + TABLE_NAME_NAME + ",\n" +
                "true NON_UNIQUE,\n" +
                "null INDEX_QUALIFIER,\n" +
                TABLE_NAME_NAME + " INDEX_NAME,\n" +
                DatabaseMetaData.tableIndexOther + " TYPE,\n" + 
                ORDINAL_POSITION + ",\n" +
                COLUMN_NAME + ",\n" +
                "CASE WHEN " + TABLE_CAT_NAME + " IS NOT NULL THEN null WHEN " + SORT_ORDER + " = " + (SortOrder.DESC.getSystemValue()) + " THEN 'D' ELSE 'A' END ASC_OR_DESC,\n" +
                "null CARDINALITY,\n" +
                "null PAGES,\n" +
                "null FILTER_CONDITION,\n" +
                DATA_TYPE + ",\n" + // Include data type info, though not in spec
                SqlTypeNameFunction.NAME + "(" + DATA_TYPE + ") AS " + TYPE_NAME + 
                "\nfrom " + TYPE_SCHEMA_AND_TABLE + 
                "\nwhere ");
        buf.append(getTenantIdWhereClause());
        buf.append("\nand " + TABLE_SCHEM_NAME + (schema == null || schema.length() == 0 ? " is null" : " = '" + SchemaUtil.normalizeIdentifier(schema) + "'" ));
        buf.append("\nand " + DATA_TABLE_NAME + " = '" + SchemaUtil.normalizeIdentifier(table) + "'" );
        buf.append("\nand " + COLUMN_NAME + " is not null" );
        buf.append("\norder by INDEX_NAME," + ORDINAL_POSITION);
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }


    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 4000;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 200;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // Catalogs are not supported for schemas
        if (catalog != null && catalog.length() > 0) {
            return emptyResultSet;
        }
        if (table == null || table.length() == 0) {
            return emptyResultSet;
        }
        final int keySeqPosition = 4;
        final int pkNamePosition = 5;
        StringBuilder buf = new StringBuilder("select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/" +
                TABLE_CAT_NAME + "," + // use this column for column family name
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                COLUMN_NAME + "," +
                "null as KEY_SEQ," +
                "PK_NAME" + "," +
                "CASE WHEN " + SORT_ORDER + " = " + (SortOrder.DESC.getSystemValue()) + " THEN 'D' ELSE 'A' END ASC_OR_DESC," +
                DATA_TYPE + "," + // include type info, though not in spec
                SqlTypeNameFunction.NAME + "(" + DATA_TYPE + ") AS " + TYPE_NAME +
                " from " + TYPE_SCHEMA_AND_TABLE + " " + TYPE_SCHEMA_AND_TABLE_ALIAS +
                " where ");
        buf.append(getTenantIdWhereClause());
        buf.append(" and " + TABLE_SCHEM_NAME + (schema == null || schema.length() == 0 ? " is null" : " = '" + SchemaUtil.normalizeIdentifier(schema) + "'" ));
        buf.append(" and " + TABLE_NAME_NAME + " = '" + SchemaUtil.normalizeIdentifier(table) + "'" );
        buf.append(" and " + TABLE_CAT_NAME + " is null" );
        buf.append(" order by " + ORDINAL_POSITION);
        // Dynamically replaces the KEY_SEQ with an expression that gets incremented after each next call.
        Statement stmt = connection.createStatement(new PhoenixStatementFactory() {

            @Override
            public PhoenixStatement newStatement(PhoenixConnection connection) {
                final byte[] unsetValue = new byte[0];
                final ImmutableBytesWritable pkNamePtr = new ImmutableBytesWritable(unsetValue);
                final byte[] rowNumberHolder = new byte[PDataType.INTEGER.getByteSize()];
                return new PhoenixStatement(connection) {
                    @Override
                    protected PhoenixResultSet newResultSet(ResultIterator iterator, RowProjector projector) throws SQLException {
                        List<ColumnProjector> columns = new ArrayList<ColumnProjector>(projector.getColumnProjectors());
                        ColumnProjector column = columns.get(keySeqPosition);
                        
                        columns.set(keySeqPosition, new ExpressionProjector(column.getName(), column.getTableName(), 
                                new BaseTerminalExpression() {
                                    @Override
                                    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
                                        ptr.set(rowNumberHolder);
                                        return true;
                                    }

                                    @Override
                                    public PDataType getDataType() {
                                        return PDataType.INTEGER;
                                    }
                                },
                                column.isCaseSensitive())
                        );
                        column = columns.get(pkNamePosition);
                        columns.set(pkNamePosition, new ExpressionProjector(column.getName(), column.getTableName(), 
                                new BaseTerminalExpression() {
                                    @Override
                                    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
                                        if (pkNamePtr.get() == unsetValue) {
                                            boolean b = tuple.getValue(TABLE_FAMILY_BYTES, PK_NAME_BYTES, pkNamePtr);
                                            if (!b) {
                                                pkNamePtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                                            }
                                        }
                                        ptr.set(pkNamePtr.get(),pkNamePtr.getOffset(),pkNamePtr.getLength());
                                        return true;
                                    }

                                    @Override
                                    public PDataType getDataType() {
                                        return PDataType.VARCHAR;
                                    }
                                },
                                column.isCaseSensitive())
                        );
                        final RowProjector newProjector = new RowProjector(columns, projector.getEstimatedRowByteSize(), projector.isProjectEmptyKeyValue());
                        ResultIterator delegate = new DelegateResultIterator(iterator) {
                            private int rowCount = 0;

                            @Override
                            public Tuple next() throws SQLException {
                                // Ignore first row, since it's the table row
                                PDataType.INTEGER.toBytes(rowCount++, rowNumberHolder, 0);
                                return super.next();
                            }
                        };
                        return new PhoenixResultSet(delegate, newProjector, this);
                    }
                    
                };
            }
            
        });
        ResultSet rs = stmt.executeQuery(buf.toString());
        if (rs.next()) { // Skip table row - we just use that to get the PK_NAME
            rs.getString(pkNamePosition+1); // Hack to cause the statement to cache this value
        }
        return rs;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return emptyResultSet;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL99;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        // Catalogs are not supported for schemas
        if (catalog != null && catalog.length() > 0) {
            return emptyResultSet;
        }
        StringBuilder buf = new StringBuilder("select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/ distinct " +
                "null " + TABLE_CATALOG_NAME + "," + // no catalog for tables
                TABLE_SCHEM_NAME +
                " from " + TYPE_SCHEMA_AND_TABLE + " " + TYPE_SCHEMA_AND_TABLE_ALIAS +
                " where " + COLUMN_NAME + " is null");
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM_NAME + " like '" + SchemaUtil.normalizeIdentifier(schemaPattern) + "'");
        }
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    private ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern, String typeNamePattern) throws SQLException {
        // Catalogs are not supported for schemas
        // Tenant specific connections have no derived tables
        if (catalog != null && catalog.length() > 0 || (connection.getTenantId() != null)) {
            return emptyResultSet;
        }
        StringBuilder buf = new StringBuilder("select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/" +
                TABLE_CAT_NAME + "," + // no catalog for tables
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                SQLTableTypeFunction.NAME + "(" + TABLE_TYPE_NAME + ") AS " + TABLE_TYPE_NAME + "," +
                REMARKS_NAME + " ," +
                TYPE_NAME + "," +
                SELF_REFERENCING_COL_NAME_NAME + "," +
                REF_GENERATION_NAME + "," +
                IndexStateNameFunction.NAME + "(" + INDEX_STATE + ") AS " + INDEX_STATE + "," +
                IMMUTABLE_ROWS + "," +
                SALT_BUCKETS + "," +
                TENANT_ID + "," + 
                VIEW_STATEMENT + "," +
                SQLViewTypeFunction.NAME + "(" + VIEW_TYPE + ") AS " + VIEW_TYPE +
                " from " + TYPE_SCHEMA_AND_TABLE + " " + TYPE_SCHEMA_AND_TABLE_ALIAS +
                " where " + COLUMN_NAME + " is null" +
                " and " + TABLE_CAT_NAME + " is null");
        buf.append(" and " + TENANT_ID + " IS NOT NULL ");
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM_NAME + (schemaPattern.length() == 0 ? " is null" : " like '" + SchemaUtil.normalizeIdentifier(schemaPattern) + "'" ));
        }
        if (tableNamePattern != null) {
            buf.append(" and " + TABLE_NAME_NAME + " like '" + SchemaUtil.normalizeIdentifier(tableNamePattern) + "'" );
        }
        if (typeNamePattern != null) {
            buf.append(" and " + SQLTableTypeFunction.NAME + "(" + TABLE_TYPE_NAME + ") like '" + SchemaUtil.normalizeIdentifier(typeNamePattern) + "'" );
        }
        buf.append(" order by " + TENANT_ID + "," + TYPE_SCHEMA_AND_TABLE + "." +TABLE_TYPE_NAME + "," + TABLE_SCHEM_NAME + "," + TABLE_NAME_NAME);
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }
    
    /**
     * Use/abuse this to get the derived tables ordered by TENANT_ID, TABLE_TYPE, SCHEMA_NAME, TABLE_NAME
     */
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return getSuperTables(catalog, schemaPattern, tableNamePattern, null);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return getSuperTables(catalog, schemaPattern, null, typeNamePattern);
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return emptyResultSet;
    }

    private static final PDatum TABLE_TYPE_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }
        @Override
        public PDataType getDataType() {
            return PDataType.VARCHAR;
        }
        @Override
        public Integer getByteSize() {
            return null;
        }
        @Override
        public Integer getMaxLength() {
            return null;
        }
        @Override
        public Integer getScale() {
            return null;
        }
		@Override
		public SortOrder getSortOrder() {
			return SortOrder.getDefault();
		}
    };
    private static final RowProjector TABLE_TYPE_ROW_PROJECTOR = new RowProjector(Arrays.<ColumnProjector>asList(
            new ExpressionProjector(TABLE_TYPE_NAME, TYPE_SCHEMA_AND_TABLE, 
                    new RowKeyColumnExpression(TABLE_TYPE_DATUM,
                            new RowKeyValueAccessor(Collections.<PDatum>singletonList(TABLE_TYPE_DATUM), 0)), false)
            ), 0, true);
    private static final Collection<Tuple> TABLE_TYPE_TUPLES = Lists.newArrayListWithExpectedSize(PTableType.values().length);
    static {
        for (PTableType tableType : PTableType.values()) {
            TABLE_TYPE_TUPLES.add(new SingleKeyValueTuple(KeyValueUtil.newKeyValue(tableType.getValue().getBytes(), TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES, MetaDataProtocol.MIN_TABLE_TIMESTAMP, ByteUtil.EMPTY_BYTE_ARRAY)));
        }
    }
    @Override
    public ResultSet getTableTypes() throws SQLException {
        return new PhoenixResultSet(new MaterializedResultIterator(TABLE_TYPE_TUPLES), TABLE_TYPE_ROW_PROJECTOR, new PhoenixStatement(connection));
    }

    /**
     * We support either:
     * 1) A non null tableNamePattern to find an exactly match with a table name, in which case either a single
     *    row would be returned in the ResultSet (if found) or no rows would be returned (if not
     *    found).
     * 2) A null tableNamePattern, in which case the ResultSet returned would have one row per
     *    table.
     * Note that catalog and schemaPattern must be null or an empty string and types must be null
     * or "TABLE".  Otherwise, no rows will be returned.
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        // Catalogs are not supported for schemas
        if (catalog != null && catalog.length() > 0) {
            return emptyResultSet;
        }
        StringBuilder buf = new StringBuilder("select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/" +
                TABLE_CAT_NAME + "," + // no catalog for tables
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                SQLTableTypeFunction.NAME + "(" + TABLE_TYPE_NAME + ") AS " + TABLE_TYPE_NAME + "," +
                REMARKS_NAME + " ," +
                TYPE_NAME + "," +
                SELF_REFERENCING_COL_NAME_NAME + "," +
                REF_GENERATION_NAME + "," +
                IndexStateNameFunction.NAME + "(" + INDEX_STATE + ") AS " + INDEX_STATE + "," +
                IMMUTABLE_ROWS + "," +
                SALT_BUCKETS + "," +
                MULTI_TENANT + "," +
                VIEW_STATEMENT + "," +
                SQLViewTypeFunction.NAME + "(" + VIEW_TYPE + ") AS " + VIEW_TYPE +
                " from " + TYPE_SCHEMA_AND_TABLE + " " + TYPE_SCHEMA_AND_TABLE_ALIAS +
                " where " + COLUMN_NAME + " is null" +
                " and " + TABLE_CAT_NAME + " is null");
        buf.append(" and " + getTenantIdWhereClause());
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM_NAME + (schemaPattern.length() == 0 ? " is null" : " like '" + SchemaUtil.normalizeIdentifier(schemaPattern) + "'" ));
        }
        if (tableNamePattern != null) {
            buf.append(" and " + TABLE_NAME_NAME + " like '" + SchemaUtil.normalizeIdentifier(tableNamePattern) + "'" );
        }
        if (types != null && types.length > 0) {
            buf.append(" and " + TABLE_TYPE_NAME + " IN (");
            // For b/w compat as table types changed in 2.2.0 TODO remove in 3.0
            if (types[0].length() == 1) {
                for (String type : types) {
                    buf.append('\'');
                    buf.append(type);
                    buf.append('\'');
                    buf.append(',');
                }
            } else {
                for (String type : types) {
                    buf.append('\'');
                    buf.append(PTableType.fromValue(type).getSerializedValue());
                    buf.append('\'');
                    buf.append(',');
                }
            }
            buf.setCharAt(buf.length()-1, ')');
        }
        buf.append(" order by " + TYPE_SCHEMA_AND_TABLE + "." +TABLE_TYPE_NAME + "," + TABLE_SCHEM_NAME + "," + TABLE_NAME_NAME);
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return emptyResultSet;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return ""; // FIXME: what should we return here?
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        // TODO: review
        return type ==  ResultSet.TYPE_FORWARD_ONLY && concurrency == Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        // TODO
        return holdability == connection.getHoldability();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == connection.getTransactionIsolation();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return this.emptyResultSet;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
