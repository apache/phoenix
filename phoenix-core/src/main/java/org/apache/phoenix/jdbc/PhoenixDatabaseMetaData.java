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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.ExternalSqlTypeIdFunction;
import org.apache.phoenix.expression.function.IndexStateNameFunction;
import org.apache.phoenix.expression.function.SQLIndexTypeFunction;
import org.apache.phoenix.expression.function.SQLTableTypeFunction;
import org.apache.phoenix.expression.function.SQLViewTypeFunction;
import org.apache.phoenix.expression.function.SqlTypeNameFunction;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.MaterializedResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;

import com.google.common.collect.ImmutableList;
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
 * {@link #getSuperTables(String, String, String)}
 * {@link #getCatalogs()}
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

    public static final String SYSTEM_CATALOG_SCHEMA = QueryConstants.SYSTEM_SCHEMA_NAME;
    public static final byte[] SYSTEM_CATALOG_SCHEMA_BYTES = QueryConstants.SYSTEM_SCHEMA_NAME_BYTES;
    public static final String SYSTEM_CATALOG_TABLE = "CATALOG";
    public static final byte[] SYSTEM_CATALOG_TABLE_BYTES = Bytes.toBytes(SYSTEM_CATALOG_TABLE);
    public static final String SYSTEM_CATALOG = SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"";
    public static final String SYSTEM_CATALOG_NAME = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_CATALOG_TABLE);
    public static final byte[] SYSTEM_CATALOG_NAME_BYTES = Bytes.toBytes(SYSTEM_CATALOG_NAME);
    public static final String SYSTEM_STATS_TABLE = "STATS";
    public static final String SYSTEM_STATS_NAME = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_STATS_TABLE);
    public static final byte[] SYSTEM_STATS_NAME_BYTES = Bytes.toBytes(SYSTEM_STATS_NAME);

    public static final String SYSTEM_CATALOG_ALIAS = "\"SYSTEM.TABLE\"";

    public static final String TABLE_NAME = "TABLE_NAME";
    public static final byte[] TABLE_NAME_BYTES = Bytes.toBytes(TABLE_NAME);
    public static final String TABLE_TYPE = "TABLE_TYPE";
    public static final byte[] TABLE_TYPE_BYTES = Bytes.toBytes(TABLE_TYPE);
    public static final String PHYSICAL_NAME = "PHYSICAL_NAME";
    public static final byte[] PHYSICAL_NAME_BYTES = Bytes.toBytes(PHYSICAL_NAME);

    public static final String COLUMN_FAMILY = "COLUMN_FAMILY";
    public static final String TABLE_CAT = "TABLE_CAT";
    public static final String TABLE_CATALOG = "TABLE_CATALOG";
    public static final String TABLE_SCHEM = "TABLE_SCHEM";
    public static final String REMARKS = "REMARKS";
    public static final String TYPE_SCHEM = "TYPE_SCHEM";
    public static final String SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME";
    public static final String REF_GENERATION = "REF_GENERATION";
    public static final String PK_NAME = "PK_NAME";
    public static final byte[] PK_NAME_BYTES = Bytes.toBytes(PK_NAME);
    public static final String TABLE_SEQ_NUM = "TABLE_SEQ_NUM";
    public static final byte[] TABLE_SEQ_NUM_BYTES = Bytes.toBytes(TABLE_SEQ_NUM);
    public static final String COLUMN_COUNT = "COLUMN_COUNT";
    public static final byte[] COLUMN_COUNT_BYTES = Bytes.toBytes(COLUMN_COUNT);
    public static final String SALT_BUCKETS = "SALT_BUCKETS";
    public static final byte[] SALT_BUCKETS_BYTES = Bytes.toBytes(SALT_BUCKETS);
    public static final String STORE_NULLS = "STORE_NULLS";
    public static final byte[] STORE_NULLS_BYTES = Bytes.toBytes(STORE_NULLS);

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
    public static final byte[] COLUMN_DEF_BYTES = Bytes.toBytes(COLUMN_DEF);
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
    public static final String INDEX_TYPE = "INDEX_TYPE";
    public static final byte[] INDEX_TYPE_BYTES = Bytes.toBytes(INDEX_TYPE);
    public static final String LINK_TYPE = "LINK_TYPE";
    public static final byte[] LINK_TYPE_BYTES = Bytes.toBytes(LINK_TYPE);
    public static final String ARRAY_SIZE = "ARRAY_SIZE";
    public static final byte[] ARRAY_SIZE_BYTES = Bytes.toBytes(ARRAY_SIZE);
    public static final String VIEW_CONSTANT = "VIEW_CONSTANT";
    public static final byte[] VIEW_CONSTANT_BYTES = Bytes.toBytes(VIEW_CONSTANT);
    public static final String IS_VIEW_REFERENCED = "IS_VIEW_REFERENCED";
    public static final byte[] IS_VIEW_REFERENCED_BYTES = Bytes.toBytes(IS_VIEW_REFERENCED);
    public static final String VIEW_INDEX_ID = "VIEW_INDEX_ID";
    public static final byte[] VIEW_INDEX_ID_BYTES = Bytes.toBytes(VIEW_INDEX_ID);

    public static final String TABLE_FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY;
    public static final byte[] TABLE_FAMILY_BYTES = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;

    public static final String TYPE_SEQUENCE = "SEQUENCE";
    public static final byte[] SEQUENCE_FAMILY_BYTES = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
    public static final String SEQUENCE_SCHEMA_NAME = SYSTEM_CATALOG_SCHEMA;
    public static final byte[] SEQUENCE_SCHEMA_NAME_BYTES = Bytes.toBytes(SEQUENCE_SCHEMA_NAME);
    public static final String SEQUENCE_TABLE_NAME = TYPE_SEQUENCE;
    public static final byte[] SEQUENCE_TABLE_NAME_BYTES = Bytes.toBytes(SEQUENCE_TABLE_NAME);
    public static final String SEQUENCE_FULLNAME_ESCAPED = SYSTEM_CATALOG_SCHEMA + ".\"" + TYPE_SEQUENCE + "\"";
    public static final String SEQUENCE_FULLNAME = SchemaUtil.getTableName(SEQUENCE_SCHEMA_NAME, SEQUENCE_TABLE_NAME);
    public static final byte[] SEQUENCE_FULLNAME_BYTES = Bytes.toBytes(SEQUENCE_FULLNAME);
    public static final String SEQUENCE_SCHEMA = "SEQUENCE_SCHEMA";
    public static final String SEQUENCE_NAME = "SEQUENCE_NAME";
    public static final String CURRENT_VALUE = "CURRENT_VALUE";
    public static final byte[] CURRENT_VALUE_BYTES = Bytes.toBytes(CURRENT_VALUE);
    public static final String START_WITH = "START_WITH";
    public static final byte[] START_WITH_BYTES = Bytes.toBytes(START_WITH);
    // MIN_VALUE, MAX_VALUE, CYCLE_FLAG and LIMIT_FLAG were added in 3.1/4.1
    public static final String MIN_VALUE = "MIN_VALUE";
    public static final byte[] MIN_VALUE_BYTES = Bytes.toBytes(MIN_VALUE);
    public static final String MAX_VALUE = "MAX_VALUE";
    public static final byte[] MAX_VALUE_BYTES = Bytes.toBytes(MAX_VALUE);
    public static final String INCREMENT_BY = "INCREMENT_BY";
    public static final byte[] INCREMENT_BY_BYTES = Bytes.toBytes(INCREMENT_BY);
    public static final String CACHE_SIZE = "CACHE_SIZE";
    public static final byte[] CACHE_SIZE_BYTES = Bytes.toBytes(CACHE_SIZE);
    public static final String CYCLE_FLAG = "CYCLE_FLAG";
    public static final byte[] CYCLE_FLAG_BYTES = Bytes.toBytes(CYCLE_FLAG);
    public static final String LIMIT_REACHED_FLAG = "LIMIT_REACHED_FLAG";
    public static final byte[] LIMIT_REACHED_FLAG_BYTES = Bytes.toBytes(LIMIT_REACHED_FLAG);
    public static final String KEY_SEQ = "KEY_SEQ";
    public static final byte[] KEY_SEQ_BYTES = Bytes.toBytes(KEY_SEQ);
    public static final String SUPERTABLE_NAME = "SUPERTABLE_NAME";

    public static final String TYPE_ID = "TYPE_ID";
    public static final String INDEX_DISABLE_TIMESTAMP = "INDEX_DISABLE_TIMESTAMP";
    public static final byte[] INDEX_DISABLE_TIMESTAMP_BYTES = Bytes.toBytes(INDEX_DISABLE_TIMESTAMP);

    public static final String REGION_NAME = "REGION_NAME";
    public static final byte[] REGION_NAME_BYTES = Bytes.toBytes(REGION_NAME);
    public static final String GUIDE_POSTS = "GUIDE_POSTS";
    public static final byte[] GUIDE_POSTS_BYTES = Bytes.toBytes(GUIDE_POSTS);
    public static final String GUIDE_POSTS_COUNT = "GUIDE_POSTS_COUNT";
    public static final byte[] GUIDE_POSTS_COUNT_BYTES = Bytes.toBytes(GUIDE_POSTS_COUNT);
    public static final String GUIDE_POSTS_WIDTH = "GUIDE_POSTS_WIDTH";
    public static final byte[] GUIDE_POSTS_WIDTH_BYTES = Bytes.toBytes(GUIDE_POSTS_WIDTH);
    public static final String GUIDE_POSTS_ROW_COUNT = "GUIDE_POSTS_ROW_COUNT";
    public static final byte[] GUIDE_POSTS_ROW_COUNT_BYTES = Bytes.toBytes(GUIDE_POSTS_ROW_COUNT);
    public static final String MIN_KEY = "MIN_KEY";
    public static final byte[] MIN_KEY_BYTES = Bytes.toBytes(MIN_KEY);
    public static final String MAX_KEY = "MAX_KEY";
    public static final byte[] MAX_KEY_BYTES = Bytes.toBytes(MAX_KEY);
    public static final String LAST_STATS_UPDATE_TIME = "LAST_STATS_UPDATE_TIME";
    public static final byte[] LAST_STATS_UPDATE_TIME_BYTES = Bytes.toBytes(LAST_STATS_UPDATE_TIME);

    public static final String PARENT_TENANT_ID = "PARENT_TENANT_ID";
    public static final byte[] PARENT_TENANT_ID_BYTES = Bytes.toBytes(PARENT_TENANT_ID);

    private static final String TENANT_POS_SHIFT = "TENANT_POS_SHIFT";
    private static final byte[] TENANT_POS_SHIFT_BYTES = Bytes.toBytes(TENANT_POS_SHIFT);

    private final PhoenixConnection connection;
    private final ResultSet emptyResultSet;
    public static final int MAX_LOCAL_SI_VERSION_DISALLOW = VersionUtil.encodeVersion("0", "98", "8");
    public static final int MIN_LOCAL_SI_VERSION_DISALLOW = VersionUtil.encodeVersion("0", "98", "6");

    // Version below which we should turn off essential column family.
    public static final int ESSENTIAL_FAMILY_VERSION_THRESHOLD = VersionUtil.encodeVersion("0", "94", "7");
    // Version below which we should disallow usage of mutable secondary indexing.
    public static final int MUTABLE_SI_VERSION_THRESHOLD = VersionUtil.encodeVersion("0", "94", "10");
    /** Version below which we fall back on the generic KeyValueBuilder */
    public static final int CLIENT_KEY_VALUE_BUILDER_THRESHOLD = VersionUtil.encodeVersion("0", "94", "14");
    
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
        return "Tenant";
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        StringBuilder buf = new StringBuilder("select \n" +
                " DISTINCT " + TENANT_ID + " " + TABLE_CAT +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
                " where " + COLUMN_NAME + " is null" +
                " and " + COLUMN_FAMILY + " is null" +
                " and " + TENANT_ID + " is not null");
        addTenantIdFilter(buf, null);
        buf.append(" order by " + TENANT_ID);
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
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

    public static final String GLOBAL_TENANANTS_ONLY = "null";

    private void addTenantIdFilter(StringBuilder buf, String tenantIdPattern) {
        PName tenantId = connection.getTenantId();
        if (tenantIdPattern == null) {
            if (tenantId != null) {
                appendConjunction(buf);
                buf.append(" (" + TENANT_ID + " IS NULL " +
                        " OR " + TENANT_ID + " = '" + StringUtil.escapeStringConstant(tenantId.getString()) + "') ");
            }
        } else if (tenantIdPattern.length() == 0) {
                appendConjunction(buf);
                buf.append(TENANT_ID + " IS NULL ");
        } else {
            appendConjunction(buf);
            buf.append(" TENANT_ID LIKE '" + StringUtil.escapeStringConstant(tenantIdPattern) + "' ");
            if (tenantId != null) {
                buf.append(" and TENANT_ID + = '" + StringUtil.escapeStringConstant(tenantId.getString()) + "' ");
            }
        }
    }

    private static void appendConjunction(StringBuilder buf) {
        buf.append(buf.length() == 0 ? "" : " and ");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        StringBuilder buf = new StringBuilder("select \n " +
                TENANT_ID + " " + TABLE_CAT + "," + // use this for tenant id
                TABLE_SCHEM + "," +
                TABLE_NAME + " ," +
                COLUMN_NAME + "," +
                ExternalSqlTypeIdFunction.NAME + "(" + DATA_TYPE + ") AS " + DATA_TYPE + "," +
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
                "CASE WHEN " + TENANT_POS_SHIFT + " THEN " + ORDINAL_POSITION + "-1 ELSE " + ORDINAL_POSITION + " END AS " + ORDINAL_POSITION + "," +
                "CASE " + NULLABLE + " WHEN " + DatabaseMetaData.attributeNoNulls +  " THEN '" + Boolean.FALSE.toString() + "' WHEN " + DatabaseMetaData.attributeNullable + " THEN '" + Boolean.TRUE.toString() + "' END AS " + IS_NULLABLE + "," +
                SCOPE_CATALOG + "," +
                SCOPE_SCHEMA + "," +
                SCOPE_TABLE + "," +
                SOURCE_DATA_TYPE + "," +
                IS_AUTOINCREMENT + "," +
                ARRAY_SIZE + "," +
                COLUMN_FAMILY + "," +
                DATA_TYPE + " " + TYPE_ID + "," +// raw type id for potential internal consumption
                VIEW_CONSTANT + "," +
                MULTI_TENANT + "," +
                "CASE WHEN " + TENANT_POS_SHIFT + " THEN " + KEY_SEQ + "-1 ELSE " + KEY_SEQ + " END AS " + KEY_SEQ +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS + "(" + TENANT_POS_SHIFT + " BOOLEAN)");
        StringBuilder where = new StringBuilder();
        addTenantIdFilter(where, catalog);
        if (schemaPattern != null) {
            appendConjunction(where);
            where.append(TABLE_SCHEM + (schemaPattern.length() == 0 ? " is null" : " like '" + StringUtil.escapeStringConstant(schemaPattern) + "'" ));
        }
        if (tableNamePattern != null && tableNamePattern.length() > 0) {
            appendConjunction(where);
            where.append(TABLE_NAME + " like '" + StringUtil.escapeStringConstant(tableNamePattern) + "'" );
        }
        // Allow a "." in columnNamePattern for column family match
        String colPattern = null;
        if (columnNamePattern != null && columnNamePattern.length() > 0) {
            String cfPattern = null;
            int index = columnNamePattern.indexOf('.');
            if (index <= 0) {
                colPattern = columnNamePattern;
            } else {
                cfPattern = columnNamePattern.substring(0, index);
                if (columnNamePattern.length() > index+1) {
                    colPattern = columnNamePattern.substring(index+1);
                }
            }
            if (cfPattern != null && cfPattern.length() > 0) { // if null or empty, will pick up all columns
                // Will pick up only KV columns
                appendConjunction(where);
                where.append(COLUMN_FAMILY + " like '" + StringUtil.escapeStringConstant(cfPattern) + "'" );
            }
            if (colPattern != null && colPattern.length() > 0) {
                appendConjunction(where);
                where.append(COLUMN_NAME + " like '" + StringUtil.escapeStringConstant(colPattern) + "'" );
            }
        }
        if (colPattern == null) {
            appendConjunction(where);
            where.append(COLUMN_NAME + " is not null" );
        }
        boolean isTenantSpecificConnection = connection.getTenantId() != null;
        if (isTenantSpecificConnection) {
            buf.append(" where (" + where + ") OR ("
                    + COLUMN_FAMILY + " is null AND " +  COLUMN_NAME + " is null)");
        } else {
            buf.append(" where " + where);
        }
        buf.append(" order by " + TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME + "," + SYSTEM_CATALOG_ALIAS + "." + ORDINAL_POSITION);

        Statement stmt;
        if (isTenantSpecificConnection) {
            stmt = connection.createStatement(new PhoenixStatementFactory() {
                @Override
                public PhoenixStatement newStatement(PhoenixConnection connection) {
                    return new PhoenixStatement(connection) {
                        @Override
                        protected PhoenixResultSet newResultSet(ResultIterator iterator, RowProjector projector)
                                throws SQLException {
                            return new PhoenixResultSet(
                                    new TenantColumnFilteringIterator(iterator, projector),
                                    projector, this);
                        }
                    };
                }
            });
        } else {
            stmt = connection.createStatement();
        }
        return stmt.executeQuery(buf.toString());
    }

    /**
     * Filters the tenant id column out of a column metadata result set (thus, where each row is a column definition).
     * The tenant id is by definition the first column of the primary key, but the primary key does not necessarily
     * start at the first column. Assumes columns are sorted on ordinal position.
     */
    private static class TenantColumnFilteringIterator extends DelegateResultIterator {
        private final RowProjector rowProjector;
        private final int columnFamilyIndex;
        private final int columnNameIndex;
        private final int multiTenantIndex;
        private final int keySeqIndex;
        private boolean inMultiTenantTable;
        private boolean tenantColumnSkipped;

        private TenantColumnFilteringIterator(ResultIterator delegate, RowProjector rowProjector) throws SQLException {
            super(delegate);
            this.rowProjector = rowProjector;
            this.columnFamilyIndex = rowProjector.getColumnIndex(COLUMN_FAMILY);
            this.columnNameIndex = rowProjector.getColumnIndex(COLUMN_NAME);
            this.multiTenantIndex = rowProjector.getColumnIndex(MULTI_TENANT);
            this.keySeqIndex = rowProjector.getColumnIndex(KEY_SEQ);
        }

        @Override
        public Tuple next() throws SQLException {
            Tuple tuple = super.next();

            while (tuple != null
                    && getColumn(tuple, columnFamilyIndex) == null && getColumn(tuple, columnNameIndex) == null) {
                // new table, check if it is multitenant
                inMultiTenantTable = getColumn(tuple, multiTenantIndex) == Boolean.TRUE;
                tenantColumnSkipped = false;
                // skip row representing table
                tuple = super.next();
            }

            if (tuple != null && inMultiTenantTable && !tenantColumnSkipped) {
                Object value = getColumn(tuple, keySeqIndex);
                if (value != null && ((Number)value).longValue() == 1L) {
                    tenantColumnSkipped = true;
                    // skip tenant id primary key column
                    return next();
                }
            }

            if (tuple != null && tenantColumnSkipped) {
                ResultTuple resultTuple = (ResultTuple)tuple;
                List<Cell> cells = resultTuple.getResult().listCells();
                KeyValue kv = new KeyValue(resultTuple.getResult().getRow(), TABLE_FAMILY_BYTES,
                        TENANT_POS_SHIFT_BYTES, PDataType.TRUE_BYTES);
                List<Cell> newCells = Lists.newArrayListWithCapacity(cells.size() + 1);
                newCells.addAll(cells);
                newCells.add(kv);
                Collections.sort(newCells, KeyValue.COMPARATOR);
                resultTuple.setResult(Result.create(newCells));
            }

            return tuple;
        }

        private Object getColumn(Tuple tuple, int index) throws SQLException {
            ColumnProjector projector = this.rowProjector.getColumnProjector(index);
            PDataType type = projector.getExpression().getDataType();
            return projector.getValue(tuple, type, new ImmutableBytesPtr());
        }
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
        return SchemaUtil.ESCAPE_CHARACTER;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        if (unique) { // No unique indexes
            return emptyResultSet;
        }
        StringBuilder buf = new StringBuilder("select \n" +
                TENANT_ID + " " + TABLE_CAT + ",\n" + // use this column for column family name
                TABLE_SCHEM + ",\n" +
                DATA_TABLE_NAME + " " + TABLE_NAME + ",\n" +
                "true NON_UNIQUE,\n" +
                "null INDEX_QUALIFIER,\n" +
                TABLE_NAME + " INDEX_NAME,\n" +
                DatabaseMetaData.tableIndexOther + " TYPE,\n" +
                ORDINAL_POSITION + ",\n" +
                COLUMN_NAME + ",\n" +
                "CASE WHEN " + COLUMN_FAMILY + " IS NOT NULL THEN null WHEN " + SORT_ORDER + " = " + (SortOrder.DESC.getSystemValue()) + " THEN 'D' ELSE 'A' END ASC_OR_DESC,\n" +
                "null CARDINALITY,\n" +
                "null PAGES,\n" +
                "null FILTER_CONDITION,\n" +
                // Include data type info, though not in spec
                ExternalSqlTypeIdFunction.NAME + "(" + DATA_TYPE + ") AS " + DATA_TYPE + ",\n" +
                SqlTypeNameFunction.NAME + "(" + DATA_TYPE + ") AS " + TYPE_NAME + ",\n" +
                DATA_TYPE + " " + TYPE_ID + ",\n" +
                COLUMN_FAMILY + ",\n" +
                COLUMN_SIZE + ",\n" +
                ARRAY_SIZE +
                "\nfrom " + SYSTEM_CATALOG +
                "\nwhere ");
        buf.append(TABLE_SCHEM + (schema == null || schema.length() == 0 ? " is null" : " = '" + StringUtil.escapeStringConstant(schema) + "'" ));
        buf.append("\nand " + DATA_TABLE_NAME + " = '" + StringUtil.escapeStringConstant(table) + "'" );
        buf.append("\nand " + COLUMN_NAME + " is not null" );
        addTenantIdFilter(buf, catalog);
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
        if (table == null || table.length() == 0) {
            return emptyResultSet;
        }
        StringBuilder buf = new StringBuilder("select \n" +
                TENANT_ID + " " + TABLE_CAT + "," + // use catalog for tenant_id
                TABLE_SCHEM + "," +
                TABLE_NAME + " ," +
                COLUMN_NAME + "," +
                KEY_SEQ + "," +
                PK_NAME + "," +
                "CASE WHEN " + SORT_ORDER + " = " + (SortOrder.DESC.getSystemValue()) + " THEN 'D' ELSE 'A' END ASC_OR_DESC," +
                ExternalSqlTypeIdFunction.NAME + "(" + DATA_TYPE + ") AS " + DATA_TYPE + "," +
                SqlTypeNameFunction.NAME + "(" + DATA_TYPE + ") AS " + TYPE_NAME + "," +
                COLUMN_SIZE + "," +
                DATA_TYPE + " " + TYPE_ID + "," + // raw type id
                VIEW_CONSTANT +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
                " where ");
        buf.append(TABLE_SCHEM + (schema == null || schema.length() == 0 ? " is null" : " = '" + StringUtil.escapeStringConstant(schema) + "'" ));
        buf.append(" and " + TABLE_NAME + " = '" + StringUtil.escapeStringConstant(table) + "'" );
        buf.append(" and " + COLUMN_NAME + " is not null");
        buf.append(" and " + COLUMN_FAMILY + " is null");
        addTenantIdFilter(buf, catalog);
        buf.append(" order by " + TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME + " ," + COLUMN_NAME);
        ResultSet rs = connection.createStatement().executeQuery(buf.toString());
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
        StringBuilder buf = new StringBuilder("select distinct \n" +
                TABLE_SCHEM + "," +
                TENANT_ID + " " + TABLE_CATALOG +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
                " where " + COLUMN_NAME + " is null");
        this.addTenantIdFilter(buf, catalog);
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM + " like '" + StringUtil.escapeStringConstant(schemaPattern) + "'");
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

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        StringBuilder buf = new StringBuilder("select \n" +
                TENANT_ID + " " + TABLE_CAT + "," + // Use tenantId for catalog
                TABLE_SCHEM + "," +
                TABLE_NAME + "," +
                COLUMN_FAMILY + " " + SUPERTABLE_NAME +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
                " where " + COLUMN_NAME + " is null" +
                " and " + LINK_TYPE + " = " + LinkType.PHYSICAL_TABLE.getSerializedValue());
        addTenantIdFilter(buf, catalog);
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM + (schemaPattern.length() == 0 ? " is null" : " like '" + StringUtil.escapeStringConstant(schemaPattern) + "'" ));
        }
        if (tableNamePattern != null) {
            buf.append(" and " + TABLE_NAME + " like '" + StringUtil.escapeStringConstant(tableNamePattern) + "'" );
        }
        buf.append(" order by " + TENANT_ID + "," + TABLE_SCHEM + "," +TABLE_NAME + "," + SUPERTABLE_NAME);
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(buf.toString());
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return emptyResultSet;
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
            return PVarchar.INSTANCE;
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
            new ExpressionProjector(TABLE_TYPE, SYSTEM_CATALOG,
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
        StringBuilder buf = new StringBuilder("select \n" +
                TENANT_ID + " " + TABLE_CAT + "," + // tenant_id is the catalog
                TABLE_SCHEM + "," +
                TABLE_NAME + " ," +
                SQLTableTypeFunction.NAME + "(" + TABLE_TYPE + ") AS " + TABLE_TYPE + "," +
                REMARKS + " ," +
                TYPE_NAME + "," +
                SELF_REFERENCING_COL_NAME + "," +
                REF_GENERATION + "," +
                IndexStateNameFunction.NAME + "(" + INDEX_STATE + ") AS " + INDEX_STATE + "," +
                IMMUTABLE_ROWS + "," +
                SALT_BUCKETS + "," +
                MULTI_TENANT + "," +
                VIEW_STATEMENT + "," +
                SQLViewTypeFunction.NAME + "(" + VIEW_TYPE + ") AS " + VIEW_TYPE + "," +
                SQLIndexTypeFunction.NAME + "(" + INDEX_TYPE + ") AS " + INDEX_TYPE +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
                " where " + COLUMN_NAME + " is null" +
                " and " + COLUMN_FAMILY + " is null");
        addTenantIdFilter(buf, catalog);
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM + (schemaPattern.length() == 0 ? " is null" : " like '" + StringUtil.escapeStringConstant(schemaPattern) + "'" ));
        }
        if (tableNamePattern != null) {
            buf.append(" and " + TABLE_NAME + " like '" + StringUtil.escapeStringConstant(tableNamePattern) + "'" );
        }
        if (types != null && types.length > 0) {
            buf.append(" and " + TABLE_TYPE + " IN (");
            for (String type : types) {
                buf.append('\'');
                buf.append(PTableType.fromValue(type).getSerializedValue());
                buf.append('\'');
                buf.append(',');
            }
            buf.setCharAt(buf.length()-1, ')');
        }
        buf.append(" order by " + SYSTEM_CATALOG_ALIAS + "." + TABLE_TYPE + "," +TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME);
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
        String userName = connection.getQueryServices().getUserName();
        return userName == null ? StringUtil.EMPTY_STRING : userName;
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
