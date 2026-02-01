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
package org.apache.phoenix.mapreduce.index;

import static org.apache.phoenix.query.QueryConstants.SYSTEM_SCHEMA_NAME;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Utility class to create index tables and/or migrate them.
 */
public class IndexToolTableUtil extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexToolTableUtil.class);

  public final static String OUTPUT_TABLE_NAME = "PHOENIX_INDEX_TOOL";
  public static String OUTPUT_TABLE_FULL_NAME =
    SchemaUtil.getTableName(SYSTEM_SCHEMA_NAME, OUTPUT_TABLE_NAME);

  public final static String RESULT_TABLE_NAME = "PHOENIX_INDEX_TOOL_RESULT";
  public static String RESULT_TABLE_FULL_NAME =
    SchemaUtil.getTableName(SYSTEM_SCHEMA_NAME, RESULT_TABLE_NAME);

  public static void setIndexToolTableName(Connection connection) throws Exception {
    ConnectionQueryServices queryServices =
      connection.unwrap(PhoenixConnection.class).getQueryServices();
    if (SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, queryServices.getConfiguration())) {
      OUTPUT_TABLE_FULL_NAME = SchemaUtil.getTableName(SYSTEM_SCHEMA_NAME, OUTPUT_TABLE_NAME)
        .replace(QueryConstants.NAME_SEPARATOR, QueryConstants.NAMESPACE_SEPARATOR);
      RESULT_TABLE_FULL_NAME = SchemaUtil.getTableName(SYSTEM_SCHEMA_NAME, RESULT_TABLE_NAME)
        .replace(QueryConstants.NAME_SEPARATOR, QueryConstants.NAMESPACE_SEPARATOR);
    } else {
      OUTPUT_TABLE_FULL_NAME = SchemaUtil.getTableName(SYSTEM_SCHEMA_NAME, OUTPUT_TABLE_NAME);
      RESULT_TABLE_FULL_NAME = SchemaUtil.getTableName(SYSTEM_SCHEMA_NAME, RESULT_TABLE_NAME);
    }
  }

  public static Table createResultTable(Connection connection) throws IOException, SQLException {
    ConnectionQueryServices queryServices =
      connection.unwrap(PhoenixConnection.class).getQueryServices();
    try (Admin admin = queryServices.getAdmin()) {
      TableName resultTableName = TableName.valueOf(RESULT_TABLE_FULL_NAME);
      if (RESULT_TABLE_FULL_NAME.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
        createSystemNamespaceTable(connection);
      }
      return createTable(admin, resultTableName);
    }
  }

  public static Table createOutputTable(Connection connection) throws IOException, SQLException {
    ConnectionQueryServices queryServices =
      connection.unwrap(PhoenixConnection.class).getQueryServices();
    try (Admin admin = queryServices.getAdmin()) {
      TableName outputTableName = TableName.valueOf(OUTPUT_TABLE_FULL_NAME);
      if (OUTPUT_TABLE_FULL_NAME.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
        createSystemNamespaceTable(connection);
      }
      return createTable(admin, outputTableName);
    }
  }

  public static void createSystemNamespaceTable(Connection connection)
    throws IOException, SQLException {
    ConnectionQueryServices queryServices =
      connection.unwrap(PhoenixConnection.class).getQueryServices();
    if (SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, queryServices.getConfiguration())) {
      try (Admin admin = queryServices.getAdmin()) {
        if (!ClientUtil.isHBaseNamespaceAvailable(admin, SYSTEM_SCHEMA_NAME)) {
          NamespaceDescriptor namespaceDescriptor =
            NamespaceDescriptor.create(SYSTEM_SCHEMA_NAME).build();
          admin.createNamespace(namespaceDescriptor);
        }
      }
    }
  }

  @VisibleForTesting
  private static Table createTable(Admin admin, TableName tableName) throws IOException {
    if (!admin.tableExists(tableName)) {
      ColumnFamilyDescriptor columnDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES)
          .setTimeToLive(MetaDataProtocol.DEFAULT_LOG_TTL).build();
      TableDescriptor tableDescriptor =
        TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(columnDescriptor).build();
      try {
        admin.createTable(tableDescriptor);
      } catch (TableExistsException e) {
        LOGGER.warn("Table exists, ignoring", e);
      }
    }
    return admin.getConnection().getTable(tableName);
  }

  public static void createNewIndexToolTables(Connection connection) throws Exception {
    setIndexToolTableName(connection);

    migrateTable(connection, OUTPUT_TABLE_NAME);
    migrateTable(connection, RESULT_TABLE_NAME);
  }

  private static void migrateTable(Connection connection, String tableName) throws Exception {
    if (!tableName.equals(OUTPUT_TABLE_NAME) && !tableName.equals(RESULT_TABLE_NAME)) {
      LOGGER.info("Only migrating PHOENIX_INDEX_TOOL tables!");
    } else {
      ConnectionQueryServices queryServices =
        connection.unwrap(PhoenixConnection.class).getQueryServices();
      try (Admin admin = queryServices.getAdmin()) {
        TableName oldTableName = TableName.valueOf(tableName);
        String newTableNameString =
          tableName.equals(OUTPUT_TABLE_NAME) ? OUTPUT_TABLE_FULL_NAME : RESULT_TABLE_FULL_NAME;

        TableName newTableName = TableName.valueOf(newTableNameString);

        if (admin.tableExists(oldTableName)) {
          String snapshotName = tableName + "_" + UUID.randomUUID();
          admin.disableTable(oldTableName);
          admin.snapshot(snapshotName, oldTableName);
          admin.cloneSnapshot(snapshotName, newTableName);
          admin.deleteSnapshot(snapshotName);
          admin.deleteTable(oldTableName);
        } else {
          createTable(admin, newTableName);
        }
      }
    }
  }
}
