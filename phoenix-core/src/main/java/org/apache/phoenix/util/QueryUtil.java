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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.query.QueryServices;

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
    public static final int COLUMN_FAMILY_POSITION = 24;

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

    private static final String SELECT = "SELECT";
    private static final String FROM = "FROM";
    private static final String WHERE = "WHERE";
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
     * @param Hint hint to be added to the UPSERT statement.
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
        Preconditions.checkNotNull(fullTableName,"Table name cannot be null");
        if(columnInfos == null || columnInfos.isEmpty()) {
             throw new IllegalArgumentException("At least one column must be provided");
        }
        // escape the table name to ensure it is case sensitive.
        final String escapedFullTableName = SchemaUtil.getEscapedFullTableName(fullTableName);
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        for (ColumnInfo cinfo : columnInfos) {
            if (cinfo != null) {
                String fullColumnName = getEscapedFullColumnName(cinfo.getColumnName());
                query.append(fullColumnName);
                query.append(",");
             }
         }
        // Remove the trailing comma
        query.setLength(query.length() - 1);
        query.append(" FROM ");
        query.append(escapedFullTableName);
        if(conditions != null && conditions.length() > 0) {
            query.append(" WHERE (").append(conditions).append(")");
        }
        return query.toString();
    }

    public static String getUrl(String server) {
        return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + server;
    }

    public static String getUrl(String server, long port) {
        String serverUrl = getUrl(server);
        return serverUrl + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + port
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

    public static Connection getConnection(Configuration conf) throws ClassNotFoundException,
            SQLException {
        return getConnection(new Properties(), conf);
    }

    public static Connection getConnection(Properties props, Configuration conf)
            throws ClassNotFoundException,
            SQLException {
        String url = getConnectionUrl(props, conf);
        LOG.info("Creating connection with the jdbc url:" + url);
        return DriverManager.getConnection(url, props);
    }

    public static String getConnectionUrl(Properties props, Configuration conf)
            throws ClassNotFoundException, SQLException {
        // make sure we load the phoenix driver
        Class.forName(PhoenixDriver.class.getName());

        // read the hbase properties from the configuration
        String server = ZKConfig.getZKQuorumServersString(conf);
        // could be a comma-separated list
        String[] rawServers = server.split(",");
        List<String> servers = new ArrayList<String>(rawServers.length);
        boolean first = true;
        int port = -1;
        for (String serverPort : rawServers) {
            try {
                server = Addressing.parseHostname(serverPort);
                int specifiedPort = Addressing.parsePort(serverPort);
                // there was a previously specified port and it doesn't match this server
                if (port > 0 && specifiedPort != port) {
                    throw new IllegalStateException("Phoenix/HBase only supports connecting to a " +
                            "single zookeeper client port. Specify servers only as host names in " +
                            "HBase configuration");
                }
                // set the port to the specified port
                port = specifiedPort;
                servers.add(server);
            } catch (IllegalArgumentException e) {
            }
        }
        // port wasn't set, shouldn't ever happen from HBase, but just in case
        if (port == -1) {
            port = conf.getInt(QueryServices.ZOOKEEPER_PORT_ATTRIB, -1);
            if (port == -1) {
                throw new RuntimeException("Client zk port was not set!");
            }
        }
        server = Joiner.on(',').join(servers);

        return getUrl(server, port);
    }
    
    public static String getViewStatement(String schemaName, String tableName, String where) {
        // Only form we currently support for VIEWs: SELECT * FROM t WHERE ...
        return SELECT + " " + WildcardParseNode.NAME + " " + FROM + " " +
                (schemaName == null || schemaName.length() == 0 ? "" : ("\"" + schemaName + "\".")) +
                ("\"" + tableName + "\" ") +
                (WHERE + " " + where);
    }
}