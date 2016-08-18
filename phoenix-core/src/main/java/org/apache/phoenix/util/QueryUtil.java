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

import static org.apache.phoenix.util.SchemaUtil.ESCAPE_CHARACTER;
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
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
                                        columnName = getUpsertDynamicColumnName(columnName);
                                        return getEscapedFullColumnName(columnName);
                                    }
                                })),
                Joiner.on(", ").join(parameterList));

    }

    private static String getUpsertDynamicColumnName(String columnName) {
        String[] tokens = columnName.replaceAll(ESCAPE_CHARACTER,"").split(QueryConstants.NAMESPACE_SEPARATOR);
        if(tokens.length == 2){
            return ESCAPE_CHARACTER + tokens[0].trim() + ESCAPE_CHARACTER +
                    " " + ESCAPE_CHARACTER + tokens[1].trim() + ESCAPE_CHARACTER;
        }
        return columnName;
    }

    private static String getSelectDynamicColumnName(String columnName,ArrayList<String> dynamicColumns) {
        String[] tokens = columnName.replaceAll(ESCAPE_CHARACTER,"").split(QueryConstants.NAMESPACE_SEPARATOR);
        if(tokens.length == 2){
            dynamicColumns.add(ESCAPE_CHARACTER + tokens[0].trim() + ESCAPE_CHARACTER +
                    " " + ESCAPE_CHARACTER + tokens[1].trim() + ESCAPE_CHARACTER);
            //only return the column name, the full part will be used later in select query
            //dynamic part after TABLE phase
            return tokens[0];

        }
        return columnName;
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
        StringBuilder query = new StringBuilder();
        ArrayList<String> dynamicColumns =new ArrayList<String>();
        query.append("SELECT ");
        for (ColumnInfo cinfo : columnInfos) {
            if (cinfo != null) {
                String fullColumnName = getEscapedFullColumnName(
                        getSelectDynamicColumnName(cinfo.getColumnName(),dynamicColumns));
                query.append(fullColumnName);
                query.append(",");
             }
         }
        // Remove the trailing comma
        query.setLength(query.length() - 1);
        query.append(" FROM ");
        query.append(fullTableName);
        query.append(getSelectDynamicColumnDefinition(dynamicColumns));
        if(conditions != null && conditions.length() > 0) {
            query.append(" WHERE (").append(conditions).append(")");
        }
        return query.toString();
    }

    private static String getSelectDynamicColumnDefinition(ArrayList<String> dynamicColumns) {
        if(dynamicColumns.size() <= 0) return "";
        return "(" + Joiner.on(",").join(dynamicColumns) + ")";
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
     * @return {@link PhoenixConnection} with NO_UPGRADE_ATTRIB set so that we don't initiate server upgrade
     */
    public static Connection getConnectionOnServer(Configuration conf) throws ClassNotFoundException,
            SQLException {
        return getConnectionOnServer(new Properties(), conf);
    }

    /**
     * @return {@link PhoenixConnection} with NO_UPGRADE_ATTRIB set so that we don't initiate server upgrade
     */
    public static Connection getConnectionOnServer(Properties props, Configuration conf)
            throws ClassNotFoundException,
            SQLException {
        props.setProperty(PhoenixRuntime.NO_UPGRADE_ATTRIB, Boolean.TRUE.toString());
        return getConnection(props, conf);
    }

    public static Connection getConnection(Configuration conf) throws ClassNotFoundException,
            SQLException {
        return getConnection(new Properties(), conf);
    }
    
    private static Connection getConnection(Properties props, Configuration conf)
            throws ClassNotFoundException, SQLException {
        String url = getConnectionUrl(props, conf);
        LOG.info("Creating connection with the jdbc url: " + url);
        PropertiesUtil.extractProperties(props, conf);
        return DriverManager.getConnection(url, props);
    }

    public static String getConnectionUrl(Properties props, Configuration conf)
            throws ClassNotFoundException, SQLException {
        return getConnectionUrl(props, conf, null);
    }
    public static String getConnectionUrl(Properties props, Configuration conf, String principal)
            throws ClassNotFoundException, SQLException {
        // TODO: props is ignored!
        // read the hbase properties from the configuration
        String server = ZKConfig.getZKQuorumServersString(conf);
        // could be a comma-separated list
        String[] rawServers = server.split(",");
        List<String> servers = new ArrayList<String>(rawServers.length);
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
                // TODO: fall back to the default in HConstants#DEFAULT_ZOOKEPER_CLIENT_PORT
                throw new RuntimeException("Client zk port was not set!");
            }
        }
        server = Joiner.on(',').join(servers);
        String znodeParent = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
                HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        String url = getUrl(server, port, znodeParent, principal);
        // Mainly for testing to tack on the test=true part to ensure driver is found on server
        String extraArgs = conf.get(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        if (extraArgs.length() > 0) {
            url += extraArgs + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
        }
        return url;
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