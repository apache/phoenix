/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.util.PhoenixConnectionUtil;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.hive.util.ColumnMappingUtils.getColumnMappingMap;

/**
 * Implementation for notification methods which are invoked as part of transactions against the
 * hive metastore,allowing Phoenix metadata to be kept in sync with Hive'smetastore.
 */
public class PhoenixMetaHook implements HiveMetaHook {

    private static final Log LOG = LogFactory.getLog(PhoenixMetaHook.class);

    @Override
    public void preCreateTable(Table table) throws MetaException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Precreate  table : " + table.getTableName());
        }

        try (Connection conn = PhoenixConnectionUtil.getConnection(table)) {
            String tableType = table.getTableType();
            String tableName = PhoenixStorageHandlerUtil.getTargetTableName(table);

            if (TableType.EXTERNAL_TABLE.name().equals(tableType)) {
                // Check whether phoenix table exists.
                if (!PhoenixUtil.existTable(conn, tableName)) {
                    // Error if phoenix table not exist.
                    throw new MetaException("Phoenix table " + tableName + " doesn't exist");
                }
            } else if (TableType.MANAGED_TABLE.name().equals(tableType)) {
                // Check whether phoenix table exists.
                if (PhoenixUtil.existTable(conn, tableName)) {
                    // Error if phoenix table already exist.
                    throw new MetaException("Phoenix table " + tableName + " already exist.");
                }

                PhoenixUtil.createTable(conn, createTableStatement(table));
            } else {
                throw new MetaException("Unsupported table Type: " + table.getTableType());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Phoenix table " + tableName + " was created");
            }
        } catch (SQLException e) {
            throw new MetaException(e.getMessage());
        }
    }

    private String createTableStatement(Table table) throws MetaException {
        Map<String, String> tableParameterMap = table.getParameters();

        String tableName = PhoenixStorageHandlerUtil.getTargetTableName(table);
        StringBuilder ddl = new StringBuilder("create table ").append(tableName).append(" (\n");

        String phoenixRowKeys = tableParameterMap.get(PhoenixStorageHandlerConstants
                .PHOENIX_ROWKEYS);
        StringBuilder realRowKeys = new StringBuilder();
        List<String> phoenixRowKeyList = Lists.newArrayList(Splitter.on
                (PhoenixStorageHandlerConstants.COMMA).trimResults().split(phoenixRowKeys));
        Map<String, String> columnMappingMap = getColumnMappingMap(tableParameterMap.get
                (PhoenixStorageHandlerConstants.PHOENIX_COLUMN_MAPPING));

        List<FieldSchema> fieldSchemaList = table.getSd().getCols();
        for (int i = 0, limit = fieldSchemaList.size(); i < limit; i++) {
            FieldSchema fieldSchema = fieldSchemaList.get(i);
            String fieldName = fieldSchema.getName();
            String fieldType = fieldSchema.getType();
            String columnType = PhoenixUtil.getPhoenixType(fieldType);

            String rowKeyName = getRowKeyMapping(fieldName, phoenixRowKeyList);
            if (rowKeyName != null) {
                String columnName = columnMappingMap.get(fieldName);
                if(columnName != null) {
                    rowKeyName = columnName;
                }
                // In case of RowKey
                if ("binary".equals(columnType)) {
                    // Phoenix must define max length of binary when type definition. Obtaining
                    // information from the column mapping. ex) phoenix.rowkeys = "r1, r2(100), ..."
                    List<String> tokenList = Lists.newArrayList(Splitter.on(CharMatcher.is('(')
                            .or(CharMatcher.is(')'))).trimResults().split(rowKeyName));
                    columnType = columnType + "(" + tokenList.get(1) + ")";
                    rowKeyName = tokenList.get(0);
                }

                ddl.append("  ").append("\"").append(rowKeyName).append("\"").append(" ").append(columnType).append(" not " +
                        "null,\n");
                realRowKeys.append("\"").append(rowKeyName).append("\",");
            } else {
                // In case of Column
                String columnName = columnMappingMap.get(fieldName);

                if (columnName == null) {
                    // Use field definition.
                    columnName = fieldName;
                }

                if ("binary".equals(columnType)) {
                    // Phoenix must define max length of binary when type definition. Obtaining
                    // information from the column mapping. ex) phoenix.column.mapping=c1:c1(100)
                    List<String> tokenList = Lists.newArrayList(Splitter.on(CharMatcher.is('(')
                            .or(CharMatcher.is(')'))).trimResults().split(columnName));
                    columnType = columnType + "(" + tokenList.get(1) + ")";
                    columnName = tokenList.get(0);
                }

                ddl.append("  ").append("\"").append(columnName).append("\"").append(" ").append(columnType).append(",\n");
            }
        }
        ddl.append("  ").append("constraint pk_").append(PhoenixUtil.getTableSchema(tableName.toUpperCase())[1]).append(" primary key(")
                .append(realRowKeys.deleteCharAt(realRowKeys.length() - 1)).append(")\n)\n");

        String tableOptions = tableParameterMap.get(PhoenixStorageHandlerConstants
                .PHOENIX_TABLE_OPTIONS);
        if (tableOptions != null) {
            ddl.append(tableOptions);
        }

        String statement = ddl.toString();

        if (LOG.isDebugEnabled()) {
            LOG.debug("DDL : " + statement);
        }

        return statement;
    }

    private String getRowKeyMapping(String rowKeyName, List<String> phoenixRowKeyList) {
        String rowKeyMapping = null;

        for (String phoenixRowKey : phoenixRowKeyList) {
            if (phoenixRowKey.equals(rowKeyName)) {
                rowKeyMapping = phoenixRowKey;
                break;
            } else if (phoenixRowKey.startsWith(rowKeyName + "(") && phoenixRowKey.endsWith(")")) {
                rowKeyMapping = phoenixRowKey;
                break;
            }
        }

        return rowKeyMapping;
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Rollback for table : " + table.getTableName());
        }

        dropTableIfExist(table);
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {

    }

    @Override
    public void preDropTable(Table table) throws MetaException {
    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {
    }

    @Override
    public void commitDropTable(Table table, boolean deleteData) throws MetaException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Commit drop table : " + table.getTableName());
        }

        dropTableIfExist(table);
    }

    private void dropTableIfExist(Table table) throws MetaException {
        try (Connection conn = PhoenixConnectionUtil.getConnection(table)) {
            String tableType = table.getTableType();
            String tableName = PhoenixStorageHandlerUtil.getTargetTableName(table);

            if (TableType.MANAGED_TABLE.name().equals(tableType)) {
                // Drop if phoenix table exist.
                if (PhoenixUtil.existTable(conn, tableName)) {
                    PhoenixUtil.dropTable(conn, tableName);
                }
            }
        } catch (SQLException e) {
            throw new MetaException(e.getMessage());
        }
    }
}
