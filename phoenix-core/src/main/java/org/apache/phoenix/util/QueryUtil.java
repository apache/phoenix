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

import static org.apache.phoenix.util.SchemaUtil.getEscapedFullColumnName;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public final class QueryUtil {

    private static final Log LOG = LogFactory.getLog(QueryUtil.class);

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
    private static final String[] CompareOpString = new String[CompareOp.values().length];

    static {
        CompareOpString[CompareOp.EQUAL.ordinal()] = "=";
        CompareOpString[CompareOp.NOT_EQUAL.ordinal()] = "!=";
        CompareOpString[CompareOp.GREATER.ordinal()] = ">";
        CompareOpString[CompareOp.LESS.ordinal()] = "<";
        CompareOpString[CompareOp.GREATER_OR_EQUAL.ordinal()] = ">=";
        CompareOpString[CompareOp.LESS_OR_EQUAL.ordinal()] = "<=";
    }

    public static String toSQL(CompareOp op) {
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
                                    public String apply(@Nullable String columnName) {
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
     * @param conditions The condition clause to be added to the WHERE condition
     * @param hint hint to use
     * @param escapeCols whether to escape the projected columns
     * @return Select Query
     */
    public static String constructSelectStatement(String fullTableName, List<String> columns,
            final String conditions, Hint hint, boolean escapeCols) {
        Preconditions.checkNotNull(fullTableName, "Table name cannot be null");
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be provided");
        }
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");

        String hintStr = "";
        if (hint != null) {
            final HintNode node = new HintNode(hint.name());
            hintStr = node.toString();
        }
        query.append(hintStr);

        for (String col : columns) {
            if (col != null) {
                String fullColumnName = col;
                if (escapeCols) {
                    fullColumnName = getEscapedFullColumnName(col);
                }
                query.append(fullColumnName);
                query.append(",");
            }
        }
        // Remove the trailing comma
        query.setLength(query.length() - 1);
        query.append(" FROM ");
        query.append(fullTableName);
        if (conditions != null && conditions.length() > 0) {
            query.append(" WHERE (").append(conditions).append(")");
        }
        return query.toString();
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
    public static String getUrl(String zkQuorum) {
        return getUrlInternal(zkQuorum, null, null, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    public static String getUrl(String zkQuorum, int clientPort) {
        return getUrlInternal(zkQuorum, clientPort, null, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    public static String getUrl(String zkQuorum, String znodeParent) {
        return getUrlInternal(zkQuorum, null, znodeParent, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    public static String getUrl(String zkQuorum, int port, String znodeParent, String principal) {
        return getUrlInternal(zkQuorum, port, znodeParent, principal);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    public static String getUrl(String zkQuorum, int port, String znodeParent) {
        return getUrlInternal(zkQuorum, port, znodeParent, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    public static String getUrl(String zkQuorum, Integer port, String znodeParent) {
        return getUrlInternal(zkQuorum, port, znodeParent, null);
    }

    /**
     * Create the Phoenix JDBC connection URL from the provided cluster connection details.
     */
    public static String getUrl(String zkQuorum, Integer port, String znodeParent, String principal) {
        return getUrlInternal(zkQuorum, port, znodeParent, principal);
    }

    private static String getUrlInternal(String zkQuorum, Integer port, String znodeParent, String principal) {
        return new PhoenixEmbeddedDriver.ConnectionInfo(zkQuorum, port, znodeParent, principal, null).toUrl()
                + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
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
     * @return {@link PhoenixConnection} with {@value UpgradeUtil#RUN_UPGRADE} set so that we don't initiate server upgrade
     */
    public static Connection getConnectionOnServer(Configuration conf) throws ClassNotFoundException,
            SQLException {
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
     * @return {@link PhoenixConnection} with {@value UpgradeUtil#DO_NOT_UPGRADE} set so that we don't initiate metadata upgrade.
     */
    public static Connection getConnectionOnServer(Properties props, Configuration conf)
            throws ClassNotFoundException,
            SQLException {
        setServerConnection(props);
        return getConnection(props, conf);
    }

    public static Connection getConnectionOnServerWithCustomUrl(Properties props, String principal)
            throws SQLException, ClassNotFoundException {
        setServerConnection(props);
        String url = getConnectionUrl(props, null, principal);
        LOG.info("Creating connection with the jdbc url: " + url);
        return DriverManager.getConnection(url, props);
    }

    public static Connection getConnection(Configuration conf) throws ClassNotFoundException,
            SQLException {
        return getConnection(new Properties(), conf);
    }

    private static Connection getConnection(Properties props, Configuration conf)
            throws ClassNotFoundException, SQLException {
        String url = getConnectionUrl(props, conf);
        LOG.info("Creating connection with the jdbc url: " + url);
        props = PropertiesUtil.combineProperties(props, conf);
        return DriverManager.getConnection(url, props);
    }

    public static String getConnectionUrl(Properties props, Configuration conf)
            throws ClassNotFoundException, SQLException {
        return getConnectionUrl(props, conf, null);
    }
    /**
     * @return connection url using the various properties set in props and conf.
     */
    public static String getConnectionUrl(Properties props, Configuration conf, String principal)
            throws ClassNotFoundException, SQLException {
        // read the hbase properties from the configuration
        int port = getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT, props, conf);
        // Build the ZK quorum server string with "server:clientport" list, separated by ','
        final String server = getString(HConstants.ZOOKEEPER_QUORUM, HConstants.LOCALHOST, props, conf);
        String znodeParent = getString(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT, props, conf);
        String url = getUrl(server, port, znodeParent, principal);
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
            ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr();
            offsetTuple.getKey(rowKeyPtr);
            if (QueryConstants.OFFSET_ROW_KEY_PTR.compareTo(rowKeyPtr) == 0) {
                Cell cell = offsetTuple.getValue(QueryConstants.OFFSET_FAMILY, QueryConstants.OFFSET_COLUMN);
                return PInteger.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), PInteger.INSTANCE, SortOrder.ASC, null, null);
            }
        }
        return null;
    }
    
    public static String getViewPartitionClause(String partitionColumnName, long autoPartitionNum) {
        return partitionColumnName  + " " + toSQL(CompareOp.EQUAL) + " " + autoPartitionNum;
    }
    
}