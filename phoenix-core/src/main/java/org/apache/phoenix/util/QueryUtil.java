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

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.phoenix.jdbc.PhoenixDriver;

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

        List<String> parameterList = Lists.newArrayList();
        for (int i = 0; i < columnInfos.size(); i++) {
            parameterList.add("?");
        }
        return String.format(
                "UPSERT INTO %s (%s) VALUES (%s)",
                tableName,
                Joiner.on(", ").join(
                        Iterables.transform(
                                columnInfos,
                                new Function<ColumnInfo, String>() {
                                    @Nullable
                                    @Override
                                    public String apply(@Nullable ColumnInfo columnInfo) {
                                        return getEscapedFullColumnName(columnInfo.getColumnName());
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
     * @return Select Query 
     */
    public static String constructSelectStatement(String fullTableName, List<ColumnInfo> columnInfos) {
        Preconditions.checkNotNull(fullTableName,"Table name cannot be null");
        if(columnInfos == null || columnInfos.isEmpty()) {
             throw new IllegalArgumentException("At least one column must be provided");
        }
        // escape the table name to ensure it is case sensitive.
        final String escapedFullTableName = SchemaUtil.getEscapedFullTableName(fullTableName);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (ColumnInfo cinfo : columnInfos) {
            if (cinfo != null) {
                String fullColumnName = getEscapedFullColumnName(cinfo.getColumnName());
                sb.append(fullColumnName);
                sb.append(",");
             }
         }
        // Remove the trailing comma
        sb.setLength(sb.length() - 1);
        sb.append(" FROM ");
        sb.append(escapedFullTableName);
        return sb.toString();
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
    public static Connection getConnection(Configuration conf) throws ClassNotFoundException,
          SQLException {
        // read the hbase properties from the configuration
        String server = ZKConfig.getZKQuorumServersString(conf);
        // make sure we load the phoenix driver
        Class.forName(PhoenixDriver.class.getName());
        int port =
            conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
        String jdbcUrl = getUrl(server, port);
        LOG.info("Creating connection with the jdbc url:" + jdbcUrl);
        return DriverManager.getConnection(jdbcUrl);
      }
}
