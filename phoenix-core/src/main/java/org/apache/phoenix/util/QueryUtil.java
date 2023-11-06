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

package org.apache.phoenix.util;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REF_GENERATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REMARKS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SELF_REFERENCING_COL_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SUPERTABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_ALIAS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SEQUENCE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CAT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTIONAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTION_PROVIDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;
import static org.apache.phoenix.util.SchemaUtil.getEscapedFullColumnName;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.phoenix.expression.function.ExternalSqlTypeIdFunction;
import org.apache.phoenix.expression.function.IndexStateNameFunction;
import org.apache.phoenix.expression.function.SQLIndexTypeFunction;
import org.apache.phoenix.expression.function.SQLTableTypeFunction;
import org.apache.phoenix.expression.function.SQLViewTypeFunction;
import org.apache.phoenix.expression.function.SqlTypeNameFunction;
import org.apache.phoenix.expression.function.TransactionProviderNameFunction;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tool.SchemaExtractionProcessor;
import org.apache.phoenix.schema.tool.SchemaProcessor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class QueryUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryUtil.class);

    /**
     *  Column family name index within ResultSet resulting from {@link DatabaseMetaData#getColumns(String, String, String, String)}
     */
    public static final int COLUMN_FAMILY_POSITION = 25;

    /**
     *  Column name index within ResultSet resulting from {@link DatabaseMetaData#getColumns(String, String, String, String)}
     */
    public static final int COLUMN_NAME_POSITION = 4;

    /**
     * Data type index within ResultSet resulting from {@link DatabaseMetaData#getColumns(String, String, String, String)}
     */
    public static final int DATA_TYPE_POSITION = 5;

    /**
     * Index of the column containing the datatype name  within ResultSet resulting from {@link
     * DatabaseMetaData#getColumns(String, String, String, String)}.
     */
    public static final int DATA_TYPE_NAME_POSITION = 6;

    public static final String IS_SERVER_CONNECTION = "IS_SERVER_CONNECTION";
    private static final String SELECT = "SELECT";
    private static final String FROM = "FROM";
    private static final String WHERE = "WHERE";
    private static final String AND = "AND";
    private static final String[] CompareOpString = new String[CompareOperator.values().length];

    static {
        CompareOpString[CompareOperator.EQUAL.ordinal()] = "=";
        CompareOpString[CompareOperator.NOT_EQUAL.ordinal()] = "!=";
        CompareOpString[CompareOperator.GREATER.ordinal()] = ">";
        CompareOpString[CompareOperator.LESS.ordinal()] = "<";
        CompareOpString[CompareOperator.GREATER_OR_EQUAL.ordinal()] = ">=";
        CompareOpString[CompareOperator.LESS_OR_EQUAL.ordinal()] = "<=";
    }

    public static String toSQL(CompareOperator op) {
        return CompareOpString[op.ordinal()];
    }
    
    /**
     * Private constructor
     */
    private QueryUtil() {
    }
    /**
     * Generate an upsert statement based on a list of {@code ColumnInfo}s with parameter markers. The list of
     * {@code ColumnInfo}s must contain at least one element.
     *
     * @param tableName name of the table for which the upsert statement is to be created
     * @param columnInfos list of column to be included in the upsert statement
     * @return the created {@code UPSERT} statement
     */
    public static String constructUpsertStatement(String tableName, List<ColumnInfo> columnInfos) {

        if (columnInfos.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be provided for upserts");
        }

        final List<String> columnNames = Lists.transform(columnInfos, new Function<ColumnInfo,String>() {
            @Override
            public String apply(ColumnInfo columnInfo) {
                return columnInfo.getColumnName();
            }
        });
        return constructUpsertStatement(tableName, columnNames, null);

    }
    
    /**
     * Generate an upsert statement based on a list of {@code ColumnInfo}s with parameter markers. The list of
     * {@code ColumnInfo}s must contain at least one element.
     *
     * @param tableName name of the table for which the upsert statement is to be created
     * @param columns list of columns to be included in the upsert statement
     * @param hint hint to be added to the UPSERT statement.
     * @return the created {@code UPSERT} statement
     */
    public static String constructUpsertStatement(String tableName, List<String> columns, Hint hint) {

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be provided for upserts");
        }
        
        String hintStr = "";
        if(hint != null) {
           final HintNode node = new HintNode(hint.name());
           hintStr = node.toString();
        }
        
        List<String> parameterList = Lists.newArrayList();
        for (int i = 0; i < columns.size(); i++) {
            parameterList.add("?");
        }
        return String.format(
                "UPSERT %s INTO %s (%s) VALUES (%s)",
                hintStr,
                tableName,
                Joiner.on(", ").join(
                        Iterables.transform(
                               columns,
                                new Function<String, String>() {
                                    @Nullable
                                    @Override
                                    public String apply(String columnName) {
                                        return getEscapedFullColumnName(columnName);
                                    }
                                })),
                Joiner.on(", ").join(parameterList));

    }

    /**
     * Generate a generic upsert statement based on a number of columns. The created upsert statement will not include
     * any named columns, but will include parameter markers for the given number of columns. The number of columns
     * must be greater than zero.
     *
     * @param tableName name of the table for which the upsert statement is to be created
     * @param numColumns number of columns to be included in the upsert statement
     * @return the created {@code UPSERT} statement
     */
    public static String constructGenericUpsertStatement(String tableName, int numColumns) {


        if (numColumns == 0) {
            throw new IllegalArgumentException("At least one column must be provided for upserts");
        }

        List<String> parameterList = Lists.newArrayListWithCapacity(numColumns);
        for (int i = 0; i < numColumns; i++) {
            parameterList.add("?");
        }
        return String.format("UPSERT INTO %s VALUES (%s)", tableName, Joiner.on(", ").join(parameterList));
    }
    
    /**
     * 
     * @param fullTableName name of the table for which the select statement needs to be created.
     * @param columnInfos  list of columns to be projected in the select statement.
     * @param conditions   The condition clause to be added to the WHERE condition
     * @return Select Query 
     */
    public static String constructSelectStatement(String fullTableName, List<ColumnInfo> columnInfos,final String conditions) {
        List<String> columns = Lists.transform(columnInfos, new Function<ColumnInfo, String>(){
            @Override
            public String apply(ColumnInfo input) {
                return input.getColumnName();
            }});
        return constructSelectStatement(fullTableName, columns , conditions, null, false);
    }

    /**
     *
     * @param fullTableName name of the table for which the select statement needs to be created.
     * @param columns list of columns to be projected in the select statement.
     * @param whereClause The condition clause to be added to the WHERE condition
     * @param hint hint to use
     * @param escapeCols whether to escape the projected columns
     * @return Select Query
     */
    public static String constructSelectStatement(String fullTableName, List<String> columns,
            final String whereClause, Hint hint, boolean escapeCols) {
        return new QueryBuilder().setFullTableName(fullTableName).setSelectColumns(columns)
                .setWhereClause(whereClause).setHint(hint).setEscapeCols(escapeCols).build();
    }

    /**
     * Constructs parameterized filter for an IN clause e.g. passing in numWhereCols=2, numBatches=3
     * results in ((?,?),(?,?),(?,?))
     * @param numWhereCols number of WHERE columns
     * @param numBatches number of column batches
     * @return paramterized IN filter
     */
    public static String constructParameterizedInClause(int numWhereCols, int numBatches) {
        Preconditions.checkArgument(numWhereCols > 0);
        Preconditions.checkArgument(numBatches > 0);
        String batch = "(" + StringUtils.repeat("?", ",", numWhereCols) + ")";
        return "(" + StringUtils.repeat(batch, ",", numBatches) + ")";
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    @Deprecated
    public static String getUrl(String zkQuorum) {
        return getUrlInternal(zkQuorum, null, null, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    @Deprecated
    public static String getUrl(String zkQuorum, int clientPort) {
        return getUrlInternal(zkQuorum, clientPort, null, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    @Deprecated
    public static String getUrl(String zkQuorum, String znodeParent) {
        return getUrlInternal(zkQuorum, null, znodeParent, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    @Deprecated
    public static String getUrl(String zkQuorum, int port, String znodeParent, String principal) {
        return getUrlInternal(zkQuorum, port, znodeParent, principal);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    @Deprecated
    public static String getUrl(String zkQuorum, int port, String znodeParent) {
        return getUrlInternal(zkQuorum, port, znodeParent, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    @Deprecated
    public static String getUrl(String zkQuorum, Integer port, String znodeParent) {
        return getUrlInternal(zkQuorum, port, znodeParent, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    @Deprecated
    public static String getUrl(String zkQuorum, Integer port, String znodeParent, String principal) {
        return getUrlInternal(zkQuorum, port, znodeParent, principal);
    }

    @Deprecated
    private static String getUrlInternal(String zkQuorum, Integer port, String znodeParent, String principal) {
        return String.join(":", PhoenixRuntime.JDBC_PROTOCOL, zkQuorum, port == null ? "" : port.toString(), znodeParent == null ? "" : znodeParent, principal == null ? "" : principal)
                + Character.toString(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR);
    }

    public static String getExplainPlan(ResultSet rs) throws SQLException {
        StringBuilder buf = new StringBuilder();
        while (rs.next()) {
            buf.append(rs.getString(1));
            buf.append('\n');
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }

    public static String getExplainPlan(ResultIterator iterator) throws SQLException {
        List<String> steps = Lists.newArrayList();
        iterator.explain(steps);
        StringBuilder buf = new StringBuilder();
        for (String step : steps) {
            buf.append(step);
            buf.append('\n');
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }

    /**
     * @return {@link PhoenixConnection} with {@value UpgradeUtil#DO_NOT_UPGRADE} set so that we
     * don't initiate metadata upgrade
     */
    public static Connection getConnectionOnServer(Configuration conf) throws SQLException {
        return getConnectionOnServer(new Properties(), conf);
    }
    
    public static void setServerConnection(Properties props){
        UpgradeUtil.doNotUpgradeOnFirstConnection(props);
        props.setProperty(IS_SERVER_CONNECTION, Boolean.TRUE.toString());
    }
    
    public static boolean isServerConnection(ReadOnlyProps props) {
        return props.getBoolean(IS_SERVER_CONNECTION, false);
    }

    /**
     * @return {@link PhoenixConnection} with {@value UpgradeUtil#DO_NOT_UPGRADE} set
     * and with the upgrade-required flag cleared so that we don't initiate metadata upgrade.
     */
    public static Connection getConnectionOnServer(Properties props, Configuration conf)
            throws SQLException {
        setServerConnection(props);
        Connection conn = getConnection(props, conf);
        conn.unwrap(PhoenixConnection.class).getQueryServices().clearUpgradeRequired();
        return conn;
    }

    public static Connection getConnectionOnServerWithCustomUrl(Properties props, String principal)
            throws SQLException {
        setServerConnection(props);
        String url = getConnectionUrl(props, null, principal);
        LOGGER.info("Creating connection with the jdbc url: " + url);
        return DriverManager.getConnection(url, props);
    }

    public static Connection getConnection(Configuration conf) throws SQLException {
        return getConnection(new Properties(), conf);
    }

    private static Connection getConnection(Properties props, Configuration conf)
            throws SQLException {
        String url = getConnectionUrl(props, conf);
        LOGGER.info(String.format("Creating connection with the jdbc url: %s, isServerSide = %s",
                url, props.getProperty(IS_SERVER_CONNECTION)));
        props = PropertiesUtil.combineProperties(props, conf);
        return DriverManager.getConnection(url, props);
    }

    public static String getConnectionUrl(Properties props, Configuration conf)
            throws SQLException {
        return getConnectionUrl(props, conf, null);
    }
    /**
     * @return connection url using the various properties set in props and conf.
     */
    public static String getConnectionUrl(Properties props, Configuration conf, String principal)
            throws SQLException {
        ReadOnlyProps propsWithPrincipal;
        if (principal != null) {
            Map<String, String> principalProp = new HashMap<>();
            principalProp.put(QueryServices.HBASE_CLIENT_PRINCIPAL, principal);
            propsWithPrincipal = new ReadOnlyProps(principalProp.entrySet().iterator());
        } else {
            propsWithPrincipal = ReadOnlyProps.EMPTY_PROPS;
        }
        ConnectionInfo info =
                ConnectionInfo.createNoLogin(PhoenixRuntime.JDBC_PROTOCOL, conf, propsWithPrincipal,
                    props);
        String url = info.toUrl();
        if (url.endsWith(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + "")) {
            url = url.substring(0, url.length() - 1);
        }
        // Mainly for testing to tack on the test=true part to ensure driver is found on server
        String defaultExtraArgs =
                conf != null
                        ? conf.get(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
                            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS)
                        : QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS;
        // If props doesn't have a default for extra args then use the extra args in conf as default
        String extraArgs =
                props.getProperty(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, defaultExtraArgs);
        if (extraArgs.length() > 0) {
            url +=
                    PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + extraArgs
                            + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
        } else {
            url += PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
        }
        return url;
    }

    private static int getInt(String key, int defaultValue, Properties props, Configuration conf) {
        if (conf == null) {
            Preconditions.checkNotNull(props);
            return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
        }
        return conf.getInt(key, defaultValue);
    }

    private static String getString(String key, String defaultValue, Properties props, Configuration conf) {
        if (conf == null) {
            Preconditions.checkNotNull(props);
            return props.getProperty(key, defaultValue);
        }
        return conf.get(key, defaultValue);
    }

    public static String getViewStatement(String schemaName, String tableName, String where) {
        // Only form we currently support for VIEWs: SELECT * FROM t WHERE ...
        return SELECT + " " + WildcardParseNode.NAME + " " + FROM + " " +
                (schemaName == null || schemaName.length() == 0 ? "" : ("\"" + schemaName + "\".")) +
                ("\"" + tableName + "\" ") +
                (WHERE + " " + where);
    }

    public static Integer getOffsetLimit(Integer limit, Integer offset) {
        if (limit == null) {
            return null;
        } else if (offset == null) {
            return limit;
        } else {
            return limit + offset;
        }

    }

    public static Integer getRemainingOffset(Tuple offsetTuple) {
        if (offsetTuple != null) {
            Cell cell = offsetTuple.getValue(QueryConstants.OFFSET_FAMILY,
                    QueryConstants.OFFSET_COLUMN);
            if (cell != null) {
                return PInteger.INSTANCE.toObject(
                        cell.getValueArray(),
                        cell.getValueOffset(),
                        cell.getValueLength(),
                        PInteger.INSTANCE,
                        SortOrder.ASC,
                        null,
                        null);
            }
        }
        return null;
    }
    
    public static String getViewPartitionClause(String partitionColumnName, long autoPartitionNum) {
        return partitionColumnName  + " " + toSQL(CompareOperator.EQUAL) + " " + autoPartitionNum;
    }

    public static Connection getConnectionForQueryLog(Configuration config) throws SQLException {
        //we don't need this connection to upgrade anything or start dispatcher
        return getConnectionOnServer(config);
    }

    public static PreparedStatement getCatalogsStmt(PhoenixConnection connection) throws SQLException {
        List<String> parameterValues = new ArrayList<String>(4);
        StringBuilder buf = new StringBuilder("select \n" +
            " DISTINCT " + TENANT_ID + " " + TABLE_CAT +
            " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
            " where " + COLUMN_NAME + " is null" +
            " and " + COLUMN_FAMILY + " is null" +
            " and " + TENANT_ID + " is not null");
        addTenantIdFilter(connection, buf, null, parameterValues);
        buf.append(" order by " + TENANT_ID);
        PreparedStatement stmt = connection.prepareStatement(buf.toString());
        for(int i = 0; i < parameterValues.size(); i++) {
            stmt.setString(i+1, parameterValues.get(i));
        }
        return stmt;
    }

    /**
     * Util that generates a PreparedStatement against syscat to fetch schema listings.
     */
    public static PreparedStatement getSchemasStmt(
        PhoenixConnection connection, String catalog, String schemaPattern) throws SQLException {
        List<String> parameterValues = new ArrayList<String>(4);
        StringBuilder buf = new StringBuilder("select distinct \n" +
                TABLE_SCHEM + "," +
                TENANT_ID + " " + TABLE_CATALOG +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
                " where " + COLUMN_NAME + " is null");
        addTenantIdFilter(connection, buf, catalog, parameterValues);
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM + (schemaPattern.length() == 0 ? " is null" : " like ?"));
            if(schemaPattern.length() > 0) {
                parameterValues.add(schemaPattern);
            }
        }
        if (SchemaUtil.isNamespaceMappingEnabled(null, connection.getQueryServices().getProps())) {
            buf.append(" and " + TABLE_NAME + " = '" + MetaDataClient.EMPTY_TABLE + "'");
        }

        // TODO: we should union this with SYSTEM.SEQUENCE too, but we only have support for
        // UNION ALL and we really need UNION so that it dedups.

        PreparedStatement stmt = connection.prepareStatement(buf.toString());
        for(int i = 0; i < parameterValues.size(); i++) {
            stmt.setString(i+1, parameterValues.get(i));
        }
        return stmt;
    }

    public static PreparedStatement getSuperTablesStmt(PhoenixConnection connection,
        String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        List<String> parameterValues = new ArrayList<String>(4);
        StringBuilder buf = new StringBuilder("select \n" +
                TENANT_ID + " " + TABLE_CAT + "," + // Use tenantId for catalog
                TABLE_SCHEM + "," +
                TABLE_NAME + "," +
                COLUMN_FAMILY + " " + SUPERTABLE_NAME +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
                " where " + COLUMN_NAME + " is null" +
                " and " + LINK_TYPE + " = " + PTable.LinkType.PHYSICAL_TABLE.getSerializedValue());
        addTenantIdFilter(connection, buf, catalog, parameterValues);
        if (schemaPattern != null) {
            buf.append(" and " + TABLE_SCHEM + (schemaPattern.length() == 0 ? " is null" : " like ?" ));
            if(schemaPattern.length() > 0) {
                parameterValues.add(schemaPattern);
            }
        }
        if (tableNamePattern != null) {
            buf.append(" and " + TABLE_NAME + " like ?" );
            parameterValues.add(tableNamePattern);
        }
        buf.append(" order by " + TENANT_ID + "," + TABLE_SCHEM + "," +TABLE_NAME + "," + SUPERTABLE_NAME);
        PreparedStatement stmt = connection.prepareStatement(buf.toString());
        for(int i = 0; i < parameterValues.size(); i++) {
            stmt.setString(i+1, parameterValues.get(i));
        }
        return stmt;
    }

    public static PreparedStatement getIndexInfoStmt(PhoenixConnection connection,
            String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        if (unique) { // No unique indexes
            return null;
        }
        List<String> parameterValues = new ArrayList<String>(4);
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
        buf.append(TABLE_SCHEM + (schema == null || schema.length() == 0 ? " is null" : " = ?" ));
        if(schema != null && schema.length() > 0) {
            parameterValues.add(schema);
        }
        buf.append("\nand " + DATA_TABLE_NAME + " = ?" );
        parameterValues.add(table);
        buf.append("\nand " + COLUMN_NAME + " is not null" );
        addTenantIdFilter(connection, buf, catalog, parameterValues);
        buf.append("\norder by INDEX_NAME," + ORDINAL_POSITION);
        PreparedStatement stmt = connection.prepareStatement(buf.toString());
        for(int i = 0; i < parameterValues.size(); i++) {
            stmt.setString(i+1, parameterValues.get(i));
        }
        return stmt;
    }

    /**
     * Util that generates a PreparedStatement against syscat to get the table listing in a given schema.
     */
    public static PreparedStatement getTablesStmt(PhoenixConnection connection, String catalog, String schemaPattern,
        String tableNamePattern, String[] types) throws SQLException {
        boolean isSequence = false;
        boolean hasTableTypes = types != null && types.length > 0;
        StringBuilder typeClauseBuf = new StringBuilder();
        List<String> parameterValues = new ArrayList<String>(4);
        if (hasTableTypes) {
            List<String> tableTypes = Lists.newArrayList(types);
            isSequence = tableTypes.remove(SEQUENCE_TABLE_TYPE);
            StringBuilder typeBuf = new StringBuilder();
            for (String type : tableTypes) {
                try {
                    PTableType tableType = PTableType.fromValue(type);
                    typeBuf.append('\'');
                    typeBuf.append(tableType.getSerializedValue());
                    typeBuf.append('\'');
                    typeBuf.append(',');
                } catch (IllegalArgumentException e) {
                    // Ignore and continue
                }
            }
            if (typeBuf.length() > 0) {
                typeClauseBuf.append(" and " + TABLE_TYPE + " IN (");
                typeClauseBuf.append(typeBuf);
                typeClauseBuf.setCharAt(typeClauseBuf.length()-1, ')');
            }
        }
        StringBuilder buf = new StringBuilder("select \n");
        // If there were table types specified and they were all filtered out
        // and we're not querying for sequences, return an empty result set.
        if (hasTableTypes && typeClauseBuf.length() == 0 && !isSequence) {
            return null;
        }
        if (typeClauseBuf.length() > 0 || !isSequence) {
            buf.append(
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
                SQLIndexTypeFunction.NAME + "(" + INDEX_TYPE + ") AS " + INDEX_TYPE + "," +
                TRANSACTION_PROVIDER + " IS NOT NULL AS " + TRANSACTIONAL + "," +
                IS_NAMESPACE_MAPPED + "," +
                GUIDE_POSTS_WIDTH + "," +
                TransactionProviderNameFunction.NAME + "(" + TRANSACTION_PROVIDER + ") AS TRANSACTION_PROVIDER" +
                " from " + SYSTEM_CATALOG + " " + SYSTEM_CATALOG_ALIAS +
                " where " + COLUMN_NAME + " is null" +
                " and " + COLUMN_FAMILY + " is null" +
                " and " + TABLE_NAME + " != '" + MetaDataClient.EMPTY_TABLE + "'");
            addTenantIdFilter(connection, buf, catalog, parameterValues);
            if (schemaPattern != null) {
                buf.append(" and " + TABLE_SCHEM + (schemaPattern.length() == 0 ? " is null" : " like ?" ));
                if (schemaPattern.length() > 0) {
                    parameterValues.add(schemaPattern);
                }
            }
            if (tableNamePattern != null) {
                buf.append(" and " + TABLE_NAME + " like ?" );
                parameterValues.add(tableNamePattern);
            }
            if (typeClauseBuf.length() > 0) {
                buf.append(typeClauseBuf);
            }
        }
        if (isSequence) {
            // Union the SYSTEM.CATALOG entries with the SYSTEM.SEQUENCE entries
            if (typeClauseBuf.length() > 0) {
                buf.append(" UNION ALL\n");
                buf.append(" select\n");
            }
            buf.append(
                TENANT_ID + " " + TABLE_CAT + "," + // tenant_id is the catalog
                SEQUENCE_SCHEMA + " " + TABLE_SCHEM + "," +
                SEQUENCE_NAME + " " + TABLE_NAME + " ," +
                "'" + SEQUENCE_TABLE_TYPE + "' " + TABLE_TYPE + "," +
                "'' " + REMARKS + " ," +
                "'' " + TYPE_NAME + "," +
                "'' " + SELF_REFERENCING_COL_NAME + "," +
                "'' " + REF_GENERATION + "," +
                "CAST(null AS CHAR(1)) " + INDEX_STATE + "," +
                "CAST(null AS BOOLEAN) " + IMMUTABLE_ROWS + "," +
                "CAST(null AS INTEGER) " + SALT_BUCKETS + "," +
                "CAST(null AS BOOLEAN) " + MULTI_TENANT + "," +
                "'' " + VIEW_STATEMENT + "," +
                "'' " + VIEW_TYPE + "," +
                "'' " + INDEX_TYPE + "," +
                "CAST(null AS BOOLEAN) " + TRANSACTIONAL + "," +
                "CAST(null AS BOOLEAN) " + IS_NAMESPACE_MAPPED + "," +
                "CAST(null AS BIGINT) " + GUIDE_POSTS_WIDTH + "," +
                "CAST(null AS VARCHAR) " + TRANSACTION_PROVIDER + "\n");
            buf.append(" from " + SYSTEM_SEQUENCE + "\n");
            StringBuilder whereClause = new StringBuilder();
            addTenantIdFilter(connection, whereClause, catalog, parameterValues);
            if (schemaPattern != null) {
                appendConjunction(whereClause);
                whereClause.append(SEQUENCE_SCHEMA + (schemaPattern.length() == 0 ? " is null" : " like ?\n" ));
                if (schemaPattern.length() > 0) {
                    parameterValues.add(schemaPattern);
                }
            }
            if (tableNamePattern != null) {
                appendConjunction(whereClause);
                whereClause.append(SEQUENCE_NAME + " like ?\n" );
                parameterValues.add(tableNamePattern);
            }
            if (whereClause.length() > 0) {
                buf.append(" where\n");
                buf.append(whereClause);
            }
        }
        buf.append(" order by 4, 1, 2, 3\n");
        PreparedStatement stmt = connection.prepareStatement(buf.toString());
        for (int i = 0; i < parameterValues.size(); i++) {
            stmt.setString(i+1, parameterValues.get(i));
        }
        return stmt;
    }

    /**
     * Util that generates a PreparedStatement against syscat to get the table listing in a given schema.
     */
    public static PreparedStatement getShowCreateTableStmt(PhoenixConnection connection, String catalog, TableName tn) throws SQLException {

        String output;
        SchemaProcessor processor = new SchemaExtractionProcessor(null,
                connection.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration(),
                tn.getSchemaName() == null ? null : "\"" + tn.getSchemaName()+ "\"",
                "\"" + tn.getTableName() + "\"");
        try {
            output = processor.process();
        } catch (Exception e) {
            LOGGER.error(e.getStackTrace().toString());
            throw new SQLException(e.getMessage());
        }

        StringBuilder buf = new StringBuilder("select \n" +
                " ? as \"CREATE STATEMENT\"");
        PreparedStatement stmt = connection.prepareStatement(buf.toString());

        stmt.setString(1, output);

        return stmt;
    }

    public static void addTenantIdFilter(PhoenixConnection connection, StringBuilder buf, String tenantIdPattern,
                                         List<String> parameterValues) {
        PName tenantId = connection.getTenantId();
        if (tenantIdPattern == null) {
            if (tenantId != null) {
                appendConjunction(buf);
                buf.append(" (" + TENANT_ID + " IS NULL " +
                        " OR " + TENANT_ID + " = ?) ");
                parameterValues.add(tenantId.getString());
            }
        } else if (tenantIdPattern.length() == 0) {
            appendConjunction(buf);
            buf.append(TENANT_ID + " IS NULL ");
        } else {
            appendConjunction(buf);
            buf.append(" TENANT_ID LIKE ? ");
            parameterValues.add(tenantIdPattern);
            if (tenantId != null) {
                buf.append(" and TENANT_ID = ? ");
                parameterValues.add(tenantId.getString());
            }
        }
    }

    private static void appendConjunction(StringBuilder buf) {
        buf.append(buf.length() == 0 ? "" : " and ");
    }

    public static String generateInListParams(int nParams) {
        List<String> paramList = Lists.newArrayList();
        for (int i = 0; i < nParams; i++) {
            paramList.add("?");
        }
        return Joiner.on(", ").join(paramList);
    }

    public static void setQuoteInListElements(PreparedStatement ps, List<String> unQuotedString,
        int index) throws SQLException {
        for (int i = 0; i < unQuotedString.size(); i++) {
            ps.setString(++index, "'" + unQuotedString + "'");
        }
    }
}