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
package org.apache.phoenix.end2end;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.junit.Assert;

final class VarBinaryEncodedUpsertSelectTestUtil {

  static final String SINGLE_PK_COLUMNS = "PK1, COL1";

  static final String MULTI_PK_COLUMNS = "PK1, PK2, COL1";

  private VarBinaryEncodedUpsertSelectTestUtil() {
  }

  static void createTable(Connection conn, String tableName, String tableOptions)
    throws SQLException {
    createTable(conn, tableName, tableOptions, false);
  }

  static void createTable(Connection conn, String tableName, String tableOptions,
    boolean descendingPk) throws SQLException {
    conn.createStatement()
      .execute("CREATE TABLE " + tableName + " (PK1 VARBINARY_ENCODED NOT NULL,"
        + " COL1 VARBINARY_ENCODED, COL2 VARCHAR CONSTRAINT pk PRIMARY KEY(PK1"
        + (descendingPk ? " DESC" : "") + ")) " + tableOptions);
  }

  static void createMultiColumnPkTable(Connection conn, String tableName, String tableOptions)
    throws SQLException {
    conn.createStatement()
      .execute("CREATE TABLE " + tableName + " (PK1 VARBINARY_ENCODED NOT NULL,"
        + " PK2 VARBINARY_ENCODED NOT NULL, COL1 VARBINARY_ENCODED"
        + " CONSTRAINT pk PRIMARY KEY(PK1, PK2 DESC)) " + tableOptions);
  }

  static void upsertEncodedRow(Connection conn, String tableName, byte[] pk, byte[] col1)
    throws SQLException {
    upsertEncodedRow(conn, tableName, pk, col1, null, true);
  }

  static void upsertEncodedRow(Connection conn, String tableName, byte[] pk, byte[] col1,
    boolean bindParameters) throws SQLException {
    upsertEncodedRow(conn, tableName, pk, col1, null, bindParameters);
  }

  static void upsertEncodedRow(Connection conn, String tableName, byte[] pk, byte[] col1,
    String col2, boolean bindParameters) throws SQLException {
    if (!bindParameters) {
      conn.createStatement()
        .executeUpdate("UPSERT INTO " + tableName + " (PK1, COL1, COL2) VALUES (" + toLiteral(pk)
          + ", " + toLiteral(col1) + ", " + (col2 == null ? "NULL" : "'" + col2 + "'") + ")");
      return;
    }
    try (PreparedStatement preparedStatement =
      conn.prepareStatement("UPSERT INTO " + tableName + " (PK1, COL1, COL2) VALUES (?, ?, ?)")) {
      preparedStatement.setBytes(1, pk);
      preparedStatement.setBytes(2, col1);
      preparedStatement.setString(3, col2);
      preparedStatement.executeUpdate();
    }
  }

  static void upsertEncodedMultiPkRow(Connection conn, String tableName, byte[] pk1, byte[] pk2,
    byte[] col1, boolean bindParameters) throws SQLException {
    if (!bindParameters) {
      conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " (PK1, PK2, COL1) VALUES ("
        + toLiteral(pk1) + ", " + toLiteral(pk2) + ", " + toLiteral(col1) + ")");
      return;
    }
    try (PreparedStatement preparedStatement =
      conn.prepareStatement("UPSERT INTO " + tableName + " (PK1, PK2, COL1) VALUES (?, ?, ?)")) {
      preparedStatement.setBytes(1, pk1);
      preparedStatement.setBytes(2, pk2);
      preparedStatement.setBytes(3, col1);
      preparedStatement.executeUpdate();
    }
  }

  private static String toLiteral(byte[] value) {
    return value == null ? "NULL" : PVarbinary.INSTANCE.toStringLiteral(value);
  }

  static void assertRows(Connection conn, String tableName, String columnList,
    byte[][]... expectedRows) throws SQLException {
    try (ResultSet resultSet =
      conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + tableName)) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(expectedRows.length, resultSet.getInt(1));
    }

    try (ResultSet resultSet = conn.createStatement()
      .executeQuery("SELECT /*+ NO_INDEX */ " + columnList + " FROM " + tableName)) {
      for (byte[][] expectedRow : expectedRows) {
        Assert.assertTrue(resultSet.next());
        for (int i = 0; i < expectedRow.length; i++) {
          Assert.assertArrayEquals(expectedRow[i], resultSet.getBytes(i + 1));
        }
      }
      Assert.assertFalse(resultSet.next());
    }
  }

  static void assertRowsAndRowKeys(Connection conn, String tableName, byte[][]... expectedRows)
    throws Exception {
    assertRows(conn, tableName, SINGLE_PK_COLUMNS, expectedRows);

    List<byte[]> actualRowKeys = new ArrayList<>();
    try (
      Table hTable =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName));
      ResultScanner scanner = hTable.getScanner(new Scan())) {
      for (Result result : scanner) {
        actualRowKeys.add(result.getRow());
      }
    }

    byte[][] expectedRowKeys = new byte[expectedRows.length][];
    for (int i = 0; i < expectedRows.length; i++) {
      expectedRowKeys[i] = PVarbinaryEncoded.INSTANCE.toBytes(expectedRows[i][0]);
    }
    Assert.assertEquals(
      Arrays.deepToString(expectedRowKeys) + " vs " + Arrays.deepToString(actualRowKeys.toArray()),
      expectedRowKeys.length, actualRowKeys.size());
    for (int i = 0; i < expectedRowKeys.length; i++) {
      Assert.assertArrayEquals(expectedRowKeys[i], actualRowKeys.get(i));
    }
  }

  static int countHBaseRows(Connection conn, String tableName) throws Exception {
    int rowCount = 0;
    try (
      Table hTable =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName));
      ResultScanner scanner = hTable.getScanner(new Scan())) {
      while (scanner.next() != null) {
        rowCount++;
      }
    }
    return rowCount;
  }

}
