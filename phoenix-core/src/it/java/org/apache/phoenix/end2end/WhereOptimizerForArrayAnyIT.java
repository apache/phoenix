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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.bson.RawBsonDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class WhereOptimizerForArrayAnyIT extends BaseTest {
  @BeforeClass
  public static void setup() throws Exception {
    setUpTestDriver(new ReadOnlyProps(new HashMap<String, String>()));
  }

  @Test
  public void testArrayAnyComparisonForNonPkColumn() throws Exception {
    String tableName = generateUniqueName();
    createTableASCPkColumns(tableName);
    insertData(tableName, 1, "x", "a");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE col1 = ANY(?)";
      Array arr = conn.createArrayOf("VARCHAR", new String[] { "a", "b" });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertEquals("x", rs.getString(2));
          assertEquals("a", rs.getString(3));
        }
        assertPointLookupsAreNotGenerated(stmt);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithInequalityOperator() throws Exception {
    String tableName = generateUniqueName();
    createTableASCPkColumns(tableName);
    insertData(tableName, 2, "x", "a");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 > ANY(?)";
      Array arr = conn.createArrayOf("INTEGER", new Integer[] { 1, 2, 3 });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
          assertEquals("x", rs.getString(2));
          assertEquals("a", rs.getString(3));
        }
        assertPointLookupsAreNotGenerated(stmt);
      }
    }
  }

  @Test
  public void testArrayAnyComparsionWithBindVariable() throws Exception {
    String tableName = generateUniqueName();
    createTableASCPkColumns(tableName);
    insertData(tableName, 1, "x", "a");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = 'x'";
      Array arr = conn.createArrayOf("INTEGER", new Integer[] { 1, 2 });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertEquals("x", rs.getString(2));
          assertEquals("a", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, 2);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithLiteralArray() throws Exception {
    String tableName = generateUniqueName();
    createTableASCPkColumns(tableName);
    insertData(tableName, 1, "x", "a");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql =
        "SELECT * FROM " + tableName + " WHERE pk1 = 1 AND pk2 = ANY(ARRAY['x', 'y'])";
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(selectSql)) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertEquals("x", rs.getString(2));
          assertEquals("a", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, selectSql, 2);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithDoubleToFloatConversion() throws Exception {
    String tableName = generateUniqueName();
    String ddl =
      "CREATE TABLE " + tableName + " (" + "pk1 FLOAT NOT NULL, " + "pk2 VARCHAR(3) NOT NULL, "
        + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1 DESC, pk2)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
        stmt.execute("UPSERT INTO " + tableName + " VALUES (2.2, 'y', 'b')");
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = 'y'";
      Array arr = conn.createArrayOf("DOUBLE", new Double[] { 4.4d, 2.2d, 0d });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
          assertEquals("y", rs.getString(2));
          assertEquals("b", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithLongToIntegerConversion() throws Exception {
    String tableName = generateUniqueName();
    String ddl =
      "CREATE TABLE " + tableName + " (" + "pk1 INTEGER NOT NULL, " + "pk2 VARCHAR(3) NOT NULL, "
        + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1 DESC, pk2)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    insertData(tableName, 2, "y", "b");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = 'y'";
      Array arr = conn.createArrayOf("BIGINT", new Long[] { 4L, 2L, 0L });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
          assertEquals("y", rs.getString(2));
          assertEquals("b", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithNullInArray() throws Exception {
    String tableName = generateUniqueName();
    createTableASCPkColumns(tableName);
    insertData(tableName, 1, null, "a");
    insertData(tableName, 2, "y", "b");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = 1 AND pk2 = ANY(?)";
      Array arr = conn.createArrayOf("VARCHAR", new String[] { "y", "z", null });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertFalse(rs.next());
        }
        // 2 point lookups are generated instead of 3 as the null is not considered as a value
        assertPointLookupsAreGenerated(stmt, 2);
      }

      selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = 2 AND pk2 = ANY(?)";
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
          assertEquals("y", rs.getString(2));
          assertEquals("b", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, 2);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithDescPKAndNullInArray() throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 INTEGER NOT NULL, " + "pk2 VARCHAR(3), "
      + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2 DESC)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    insertData(tableName, 1, null, "a");
    insertData(tableName, 2, "y", "b");
    insertData(tableName, 3, "z", null);
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = 1 AND pk2 = ANY(?)";
      Array arr = conn.createArrayOf("VARCHAR", new String[] { "y", "z", null });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertFalse(rs.next());
        }
        // 2 point lookups are generated instead of 3 as the null is not considered as a value
        assertPointLookupsAreGenerated(stmt, 2);
      }

      selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = 2 AND pk2 = ANY(?)";
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
          assertEquals("y", rs.getString(2));
          assertEquals("b", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, 2);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonForDescCharPKWithPadding() throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 CHAR(3) NOT NULL, " + "pk2 VARCHAR(3), "
      + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1 DESC, pk2)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        stmt.setString(1, "a");
        stmt.setString(2, "b");
        stmt.setString(3, "c");
        stmt.executeUpdate();
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = 'b'";
      Array arr = conn.createArrayOf("VARCHAR", new String[] { "a", "c", null });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("a", rs.getString(1));
          assertEquals("b", rs.getString(2));
          assertEquals("c", rs.getString(3));
        }
        // 3 point lookups are generated though one of the array values is null as CHAR type pads it
        // and the value is a string consisting only of pad characrters
        assertPointLookupsAreGenerated(stmt, 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithDecimalArray() throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 DECIMAL(10, 2) NOT NULL, "
      + "pk2 VARCHAR(3), " + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        stmt.setBigDecimal(1, new BigDecimal("1.23"));
        stmt.setString(2, "x");
        stmt.setString(3, "a");
        stmt.executeUpdate();
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = 'x'";
      Array arr = conn.createArrayOf("DECIMAL", new BigDecimal[] { new BigDecimal("1.230"),
        new BigDecimal("2.340"), new BigDecimal("3.450") });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(new BigDecimal("1.23"), rs.getBigDecimal(1));
          assertEquals("x", rs.getString(2));
          assertEquals("a", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithDataTypeAndSortOrderCoercionForDecimalColumn()
    throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 DECIMAL(10, 2) NOT NULL, "
      + "pk2 VARCHAR(3), " + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1 DESC, pk2)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        stmt.setBigDecimal(1, new BigDecimal("1.23"));
        stmt.setString(2, "x");
        stmt.setString(3, "a");
        stmt.executeUpdate();
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = 'x'";
      Array arr = conn.createArrayOf("DOUBLE", new Double[] { 1.230d, 2.340d, 3.450d });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(new BigDecimal("1.23"), rs.getBigDecimal(1));
          assertEquals("x", rs.getString(2));
          assertEquals("a", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonForDateTimeColumn() throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 TIMESTAMP NOT NULL, "
      + "pk2 TIME NOT NULL, " + "pk3 DATE NOT NULL, " + "col1 VARCHAR, "
      + "CONSTRAINT pk PRIMARY KEY (pk1, pk2, pk3)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    String pk1Value = "2025-07-18 10:00:00";
    String pk2Value = "2025-07-18 11:00:00";
    String pk3Value = "2025-07-18 12:00:00";
    String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        stmt.setTimestamp(1, DateUtil.parseTimestamp(pk1Value));
        stmt.setTime(2, DateUtil.parseTime(pk2Value));
        stmt.setDate(3, DateUtil.parseDate(pk3Value));
        stmt.setString(4, "a");
        stmt.executeUpdate();
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {

      // Use arrays as bind variables to test the point lookup optimization.
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) " + "AND pk2 = ANY(?) "
        + "AND pk3 = ANY(?)";
      Array timestampArr = conn.createArrayOf("TIMESTAMP",
        new Timestamp[] { DateUtil.parseTimestamp(pk1Value),
          DateUtil.parseTimestamp("2025-07-19 10:00:00"),
          DateUtil.parseTimestamp("2025-07-17 10:00:00"), });
      Array timeArr = conn.createArrayOf("TIME", new Time[] { DateUtil.parseTime(pk2Value),
        DateUtil.parseTime("2025-07-19 11:00:00"), DateUtil.parseTime("2025-07-17 11:00:00"), });
      Array dateArr = conn.createArrayOf("DATE", new Date[] { DateUtil.parseDate(pk3Value),
        DateUtil.parseDate("2025-07-19 12:00:00"), DateUtil.parseDate("2025-07-17 12:00:00"), });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, timestampArr);
        stmt.setArray(2, timeArr);
        stmt.setArray(3, dateArr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(DateUtil.parseTimestamp(pk1Value), rs.getTimestamp(1));
          assertEquals(DateUtil.parseTime(pk2Value), rs.getTime(2));
          assertEquals(DateUtil.parseDate(pk3Value), rs.getDate(3));
        }
        assertPointLookupsAreGenerated(stmt, 3 * 3 * 3);
      }

      // Use literal arrays to test the point lookup optimization.
      String timestampLiteralArr = "ARRAY[" + "TO_TIMESTAMP('" + pk1Value + "'), "
        + "TO_TIMESTAMP('" + pk2Value + "'), " + "TO_TIMESTAMP('" + pk3Value + "')]";
      String timeLiteralArr = "ARRAY[" + "TO_TIME('" + pk1Value + "'), " + "TO_TIME('" + pk2Value
        + "'), " + "TO_TIME('" + pk3Value + "')]";
      String dateLiteralArr = "ARRAY[" + "TO_DATE('" + pk1Value + "'), " + "TO_DATE('" + pk2Value
        + "'), " + "TO_DATE('" + pk3Value + "')]";
      selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(" + timestampLiteralArr + ") "
        + "AND pk2 = ANY(" + timeLiteralArr + ")" + "AND pk3 = ANY(" + dateLiteralArr + ")";
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(selectSql)) {
          assertTrue(rs.next());
          assertEquals(DateUtil.parseTimestamp(pk1Value), rs.getTimestamp(1));
          assertEquals(DateUtil.parseTime(pk2Value), rs.getTime(2));
          assertEquals(DateUtil.parseDate(pk3Value), rs.getDate(3));
        }
        assertPointLookupsAreGenerated(stmt, selectSql, 3 * 3 * 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonWithTimestampToDateCoercion() throws Exception {
    String tableName = generateUniqueName();
    String ddl =
      "CREATE TABLE " + tableName + " (" + "pk1 TIMESTAMP NOT NULL, " + "pk2 DATE NOT NULL, "
        + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2 DESC)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    String pk1Value = "2025-07-18 10:00:00";
    String pk2Value = "2025-07-18 11:00:00";
    String pk3Value = "2025-07-18 12:00:00";
    String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        stmt.setTimestamp(1, DateUtil.parseTimestamp(pk1Value));
        stmt.setDate(2, DateUtil.parseDate(pk2Value));
        stmt.setString(3, "a");
        stmt.executeUpdate();
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Use literal arrays to test the point lookup optimization.
      String timestampLiteralArr = "ARRAY[" + "TO_TIMESTAMP('" + pk1Value + "'), "
        + "TO_TIMESTAMP('" + pk2Value + "'), " + "TO_TIMESTAMP('" + pk3Value + "')]";
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(" + timestampLiteralArr
        + ") " + "AND pk2 = ANY(" + timestampLiteralArr + ")";
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(selectSql)) {
          assertTrue(rs.next());
          assertEquals(DateUtil.parseTimestamp(pk1Value), rs.getTimestamp(1));
          assertEquals(DateUtil.parseDate(pk2Value), rs.getDate(2));
          assertEquals("a", rs.getString(3));
        }
        assertPointLookupsAreGenerated(stmt, selectSql, 3 * 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonForBinaryColumn() throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 BINARY(3) NOT NULL, "
      + "pk2 VARBINARY_ENCODED(3) NOT NULL, " + "pk3 VARBINARY(3) NOT NULL, " + "col1 VARCHAR, "
      + "CONSTRAINT pk PRIMARY KEY (pk1, pk2, pk3)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    String pk1Value = "a";
    String pk2Value = "b";
    String pk3Value = "c";
    String col1Value = "d";
    String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        stmt.setBytes(1, pk1Value.getBytes());
        stmt.setBytes(2, pk2Value.getBytes());
        stmt.setBytes(3, pk3Value.getBytes());
        stmt.setString(4, col1Value);
        stmt.executeUpdate();
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      TestUtil.dumpTable(conn, TableName.valueOf(tableName));
      String selectSql =
        "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = ANY(?) AND pk3 = ANY(?)";
      byte[][] nativeByteArr =
        new byte[][] { pk1Value.getBytes(), pk2Value.getBytes(), pk3Value.getBytes() };
      Array binaryArr = conn.createArrayOf("BINARY", nativeByteArr);
      Array varbinaryArr = conn.createArrayOf("VARBINARY", nativeByteArr);
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, binaryArr);
        stmt.setArray(2, varbinaryArr);
        stmt.setArray(3, varbinaryArr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertBinaryValue(pk1Value.getBytes(), rs.getBytes(1));
          assertBinaryValue(pk2Value.getBytes(), rs.getBytes(2));
          assertBinaryValue(pk3Value.getBytes(), rs.getBytes(3));
          assertEquals(col1Value, rs.getString(4));
        }
        assertPointLookupsAreGenerated(stmt, 3 * 3 * 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonForBinaryColumnWithCoercion() throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 BINARY(3) NOT NULL, "
      + "pk2 VARBINARY_ENCODED(3) NOT NULL, " + "pk3 VARBINARY(3) NOT NULL, " + "col1 VARCHAR, "
      + "CONSTRAINT pk PRIMARY KEY (pk1 DESC, pk2 DESC, pk3)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    String pk1Value = "a";
    String pk2Value = "b";
    String pk3Value = "c";
    String col1Value = "d";
    String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        stmt.setBytes(1, pk1Value.getBytes());
        stmt.setBytes(2, pk2Value.getBytes());
        stmt.setBytes(3, pk3Value.getBytes());
        stmt.setString(4, col1Value);
        stmt.executeUpdate();
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      TestUtil.dumpTable(conn, TableName.valueOf(tableName));
      String selectSql =
        "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = ANY(?) AND pk3 = ANY(?)";
      byte[][] nativeByteArr =
        new byte[][] { pk1Value.getBytes(), pk2Value.getBytes(), pk3Value.getBytes() };
      Array binaryArr = conn.createArrayOf("BINARY", nativeByteArr);
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, binaryArr);
        stmt.setArray(2, binaryArr);
        stmt.setArray(3, binaryArr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertBinaryValue(pk1Value.getBytes(), rs.getBytes(1));
          assertBinaryValue(pk2Value.getBytes(), rs.getBytes(2));
          assertBinaryValue(pk3Value.getBytes(), rs.getBytes(3));
          assertEquals(col1Value, rs.getString(4));
        }
        assertPointLookupsAreGenerated(stmt, 3 * 3 * 3);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonForBsonColumn() throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 BSON NOT NULL, " + "col1 VARCHAR, "
      + "CONSTRAINT pk PRIMARY KEY (pk1 DESC)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    RawBsonDocument pk1Value = getBsonDocument1();
    String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        stmt.setObject(1, pk1Value);
        stmt.setString(2, "a");
        stmt.executeUpdate();
        conn.commit();
      }
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      TestUtil.dumpTable(conn, TableName.valueOf(tableName));
      String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?)";
      Array bsonArr = conn.createArrayOf("VARBINARY",
        new byte[][] { ByteUtil.toBytes(pk1Value.getByteBuffer().asNIO()),
          ByteUtil.toBytes(getBsonDocument2().getByteBuffer().asNIO()), });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, bsonArr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          // ResultSet#getBytes() takes care of inverting the bytes as sort order of
          // column is DESC. Is this a gap in PBson#toObject()?
          byte[] pk1ValueBytes = rs.getBytes(1);
          RawBsonDocument actualPk1Value =
            new RawBsonDocument(pk1ValueBytes, 0, pk1ValueBytes.length);
          assertEquals(pk1Value, actualPk1Value);
          assertEquals("a", rs.getString(2));
        }
        assertPointLookupsAreGenerated(stmt, 2);
      }
    }
  }

  @Test
  public void testArrayAnyComparisonInGroupedAggregateQuery() throws Exception {
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 INTEGER NOT NULL, "
      + "pk2 VARCHAR NOT NULL, " + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
    insertData(tableName, 1, "a11", "b11");
    insertData(tableName, 1, "a12", "b12");
    insertData(tableName, 2, "a21", "b21");
    insertData(tableName, 2, "a22", "b22");
    insertData(tableName, 2, "a23", "b23");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql =
        "SELECT pk1, COUNT(*) FROM " + tableName + " WHERE pk1 = 2 AND pk2 = ANY(?) GROUP BY pk1";
      Array arr = conn.createArrayOf("VARCHAR", new String[] { "a11", "a21", "a23" });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setArray(1, arr);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
          assertEquals(2, rs.getInt(2));
        }
        assertPointLookupsAreGenerated(stmt, 3);
      }
    }
  }

  private void assertBinaryValue(byte[] expected, byte[] actual) {
    int expectedLength = expected.length;
    for (int i = 0; i < expectedLength; i++) {
      assertEquals(expected[i], actual[i]);
    }
  }

  private void assertPointLookupsAreNotGenerated(PreparedStatement stmt) throws SQLException {
    ExplainPlan explain =
      stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
    ExplainPlanAttributes planAttributes = explain.getPlanStepsAsAttributes();
    assertEquals("FULL SCAN ", planAttributes.getExplainScanType());
  }

  private void assertPointLookupsAreGenerated(PreparedStatement stmt, int noOfPointLookups)
    throws SQLException {
    QueryPlan queryPlan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
    assertPointLookupsAreGenerated(queryPlan, noOfPointLookups);
  }

  private void assertPointLookupsAreGenerated(Statement stmt, String selectSql,
    int noOfPointLookups) throws SQLException {
    QueryPlan queryPlan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(selectSql);
    assertPointLookupsAreGenerated(queryPlan, noOfPointLookups);
  }

  private void assertPointLookupsAreGenerated(QueryPlan queryPlan, int noOfPointLookups)
    throws SQLException {
    ExplainPlan explain = queryPlan.getExplainPlan();
    ExplainPlanAttributes planAttributes = explain.getPlanStepsAsAttributes();
    String expectedScanType =
      "POINT LOOKUP ON " + noOfPointLookups + " KEY" + (noOfPointLookups > 1 ? "S " : " ");
    assertEquals(expectedScanType, planAttributes.getExplainScanType());
  }

  private void createTableASCPkColumns(String tableName) throws SQLException {
    String ddl = "CREATE TABLE " + tableName + " (" + "pk1 INTEGER NOT NULL, " + "pk2 VARCHAR(3), "
      + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2)" + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }
  }

  private void insertData(String tableName, int pk1, String pk2, String col1) throws SQLException {
    String ddl = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(ddl)) {
        stmt.setInt(1, pk1);
        if (pk2 != null) {
          stmt.setString(2, pk2);
        } else {
          stmt.setNull(2, Types.VARCHAR);
        }
        if (col1 != null) {
          stmt.setString(3, col1);
        } else {
          stmt.setNull(3, Types.VARCHAR);
        }
        stmt.executeUpdate();
        conn.commit();
      }
    }
  }

  private static RawBsonDocument getBsonDocument1() {
    String json = "{\n" + "  \"attr_9\" : {\n" + "    \"$set\" : [ {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"YWJjZA==\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    }, {\n" + "      \"$binary\" : {\n" + "        \"base64\" : \"c3RyaW5nXzAyMDM=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    } ]\n" + "  },\n"
      + "  \"attr_8\" : {\n" + "    \"$set\" : [ 3802.34, -40.667, -4839, 7593 ]\n" + "  },\n"
      + "  \"attr_7\" : {\n" + "    \"$set\" : [ \"str_set002\", \"strset003\", \"strset001\" ]\n"
      + "  },\n" + "  \"attr_6\" : {\n" + "    \"n_attr_0\" : \"str_val_0\",\n"
      + "    \"n_attr_1\" : 1295.03,\n" + "    \"n_attr_2\" : {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    },\n" + "    \"n_attr_3\" : true,\n"
      + "    \"n_attr_4\" : null\n" + "  },\n" + "  \"attr_5\" : [ 1234, \"str001\", {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"AAECAwQF\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  } ],\n" + "  \"attr_4\" : null,\n"
      + "  \"attr_3\" : true,\n" + "  \"attr_2\" : {\n" + "    \"$binary\" : {\n"
      + "      \"base64\" : \"cmFuZG9tZTkzaDVvbmVmaHUxbmtyXzE5MzBga2p2LSwhJCVeaWVpZmhiajAzNA==\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  },\n" + "  \"attr_1\" : 1295.03,\n"
      + "  \"attr_0\" : \"str_val_0\"\n" + "}";
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getBsonDocument2() {
    String json = "{\n" + "  \"InPublication\" : false,\n" + "  \"ISBN\" : \"111-1111111111\",\n"
      + "  \"NestedList1\" : [ -485.34, \"1234abcd\", [ \"xyz0123\", {\n"
      + "    \"InPublication\" : false,\n" + "    \"ISBN\" : \"111-1111111111\",\n"
      + "    \"Title\" : \"Book 101 Title\",\n" + "    \"Id\" : 101.01\n" + "  } ] ],\n"
      + "  \"NestedMap1\" : {\n" + "    \"InPublication\" : false,\n"
      + "    \"ISBN\" : \"111-1111111111\",\n" + "    \"Title\" : \"Book 101 Title\",\n"
      + "    \"Id\" : 101.01,\n" + "    \"NList1\" : [ \"NListVal01\", -23.4 ]\n" + "  },\n"
      + "  \"Id2\" : 101.01,\n" + "  \"attr_6\" : {\n" + "    \"n_attr_0\" : \"str_val_0\",\n"
      + "    \"n_attr_1\" : 1295.03,\n" + "    \"n_attr_2\" : {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    },\n" + "    \"n_attr_3\" : true,\n"
      + "    \"n_attr_4\" : null\n" + "  },\n" + "  \"attr_5\" : [ 1234, \"str001\", {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"AAECAwQF\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  } ],\n" + "  \"IdS\" : \"101.01\",\n"
      + "  \"Title\" : \"Book 101 Title\",\n" + "  \"Id\" : 101.01,\n" + "  \"attr_1\" : 1295.03,\n"
      + "  \"attr_0\" : \"str_val_0\"\n" + "}";
    return RawBsonDocument.parse(json);
  }

}
