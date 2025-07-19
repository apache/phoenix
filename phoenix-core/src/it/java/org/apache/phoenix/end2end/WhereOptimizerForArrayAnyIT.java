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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
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
            Array arr = conn.createArrayOf("VARCHAR", new String[] { "a" });
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setArray(1, arr);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                    assertEquals("x", rs.getString(2));
                    assertEquals("a", rs.getString(3));
                }
            }
            assertPointLookupsAreNotGenerated(selectSql, conn, arr);
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
            }
            assertPointLookupsAreNotGenerated(selectSql, conn, arr);
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
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);
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
            }
            assertPointLookupsAreGenerated(selectSql, conn, null);
        }
    }

    @Test
    public void testArrayAnyComparisonWithDoubleToFloatConversion() throws Exception {
        String tableName = generateUniqueName();
        String ddl ="CREATE TABLE " + tableName + " (" 
            + "pk1 FLOAT NOT NULL, " 
            + "pk2 VARCHAR(3) NOT NULL, "
            + "col1 VARCHAR, " 
            + "CONSTRAINT pk PRIMARY KEY (pk1, pk2)"
        + ")";
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
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);
        }
    }

    @Test
    public void testArrayAnyComparisonWithLongToIntegerConversion() throws Exception {
        String tableName = generateUniqueName();
        createTableASCPkColumns(tableName);
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
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);
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
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);

            selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = 2 AND pk2 = ANY(?)";
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setArray(1, arr);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertEquals("y", rs.getString(2));
                    assertEquals("b", rs.getString(3));
                }
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);
        }
    }

    @Test
    public void testArrayAnyComparisonWithDescPKAndNullInArray() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (" 
            + "pk1 INTEGER NOT NULL, " 
            + "pk2 VARCHAR(3), " 
            + "col1 VARCHAR, " 
            + "CONSTRAINT pk PRIMARY KEY (pk1, pk2 DESC)"
        + ")";
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
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);

            selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = 2 AND pk2 = ANY(?)";
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setArray(1, arr);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertEquals("y", rs.getString(2));
                    assertEquals("b", rs.getString(3));
                }
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);
        }
    }

    @Test
    public void testArrayAnyComparisonForDescCharPKWithPadding() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (" 
            + "pk1 CHAR(3) NOT NULL, "
            + "pk2 VARCHAR(3), "
            + "col1 VARCHAR, "
            + "CONSTRAINT pk PRIMARY KEY (pk1 DESC, pk2)"
        + ")";
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
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);
        }
    }

    @Test
    public void testArrayAnyComparisonWithDecimalArray() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (" 
            + "pk1 DECIMAL(10, 2) NOT NULL, "
            + "pk2 VARCHAR(3), "
            + "col1 VARCHAR, "
            + "CONSTRAINT pk PRIMARY KEY (pk1, pk2)"
        + ")";
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
            Array arr = conn.createArrayOf("DECIMAL", new BigDecimal[] {
                new BigDecimal("1.230"),
                new BigDecimal("2.340"), 
                new BigDecimal("3.450") });
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setArray(1, arr);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(new BigDecimal("1.23"), rs.getBigDecimal(1));
                    assertEquals("x", rs.getString(2));
                    assertEquals("a", rs.getString(3));
                }
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);
        }
    }

    @Test
    public void testArrayAnyComparisonWithDataTypeAndSortOrderCoercionForDecimalColumn()
        throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " ("
            + "pk1 DECIMAL(10, 2) NOT NULL, "
            + "pk2 VARCHAR(3), "
            + "col1 VARCHAR, "
            + "CONSTRAINT pk PRIMARY KEY (pk1 DESC, pk2)"
        + ")";
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
            }
            assertPointLookupsAreGenerated(selectSql, conn, arr);
        }
    }

    @Test
    public void testArrayAnyComparisonForDateTimeColumn() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " ("
            + "pk1 TIMESTAMP NOT NULL, "
            + "pk2 TIME NOT NULL, "
            + "pk3 DATE NOT NULL, "
            + "col1 VARCHAR, "
            + "CONSTRAINT pk PRIMARY KEY (pk1, pk2, pk3)" 
        + ")";
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
            String selectSql = "SELECT * FROM " + tableName
                + " WHERE pk1 = ANY(?) " 
                + "AND pk2 = ANY(?) " 
                + "AND pk3 = ANY(?)";
            Array timestampArr = conn.createArrayOf("TIMESTAMP", new Timestamp[] {
                DateUtil.parseTimestamp(pk1Value),
                DateUtil.parseTimestamp("2025-07-19 10:00:00"),
                DateUtil.parseTimestamp("2025-07-17 10:00:00"),
            });
            Array timeArr = conn.createArrayOf("TIME", new Time[] {
                DateUtil.parseTime(pk2Value),
                DateUtil.parseTime("2025-07-19 11:00:00"),
                DateUtil.parseTime("2025-07-17 11:00:00"),
            });
            Array dateArr = conn.createArrayOf("DATE", new Date[] {
                DateUtil.parseDate(pk3Value),
                DateUtil.parseDate("2025-07-19 12:00:00"),
                DateUtil.parseDate("2025-07-17 12:00:00"),
            });
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
            }
            try (PreparedStatement stmt = conn.prepareStatement("EXPLAIN " + selectSql)) {
                stmt.setArray(1, timestampArr);
                stmt.setArray(2, timeArr);
                stmt.setArray(3, dateArr);
                try (ResultSet rs = stmt.executeQuery()) {
                    String explainPlan = QueryUtil.getExplainPlan(rs);
                    assertTrue(explainPlan.contains("POINT LOOKUP ON"));
                }
            }

            // Use literal arrays to test the point lookup optimization.
            String timestampLiteralArr = "ARRAY["
                + "TO_TIMESTAMP('" + pk1Value + "'), "
                + "TO_TIMESTAMP('" + pk2Value + "'), "
                + "TO_TIMESTAMP('" + pk3Value + "')]";
            String timeLiteralArr = "ARRAY["
                + "TO_TIME('" + pk2Value + "'), "
                + "TO_TIME('" + pk2Value + "'), "
                + "TO_TIME('" + pk2Value + "')]";
            String dateLiteralArr = "ARRAY["
                + "TO_DATE('" + pk3Value + "'), "
                + "TO_DATE('" + pk3Value + "'), "
                + "TO_DATE('" + pk3Value + "')]";
            selectSql = "SELECT * FROM " + tableName
                + " WHERE pk1 = ANY(" + timestampLiteralArr + ") " 
                + "AND pk2 = ANY(" + timeLiteralArr + ")" 
                + "AND pk3 = ANY(" + dateLiteralArr + ")";
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(selectSql)) {
                    assertTrue(rs.next());
                    assertEquals(DateUtil.parseTimestamp(pk1Value), rs.getTimestamp(1));
                    assertEquals(DateUtil.parseTime(pk2Value), rs.getTime(2));
                    assertEquals(DateUtil.parseDate(pk3Value), rs.getDate(3));
                }
            }
            assertPointLookupsAreGenerated(selectSql, conn, null);
        }
    }
    
    @Test
    public void testArrayAnyComparisonWithTimestampToDatTimeCoercion() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
            "CREATE TABLE " + tableName + " ("
                + "pk1 TIMESTAMP NOT NULL, "
                + "pk2 DATE NOT NULL, "
                + "col1 VARCHAR, " 
                + "CONSTRAINT pk PRIMARY KEY (pk1, pk2 DESC)" 
            + ")";
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
            TestUtil.dumpTable(conn, TableName.valueOf(tableName));
            // Use literal arrays to test the point lookup optimization.
            String timestampLiteralArr = "ARRAY["
                + "TO_TIMESTAMP('" + pk1Value + "'), "
                + "TO_TIMESTAMP('" + pk2Value + "'), "
                + "TO_TIMESTAMP('" + pk3Value + "')]";
            String selectSql = "SELECT * FROM " + tableName
                + " WHERE pk1 = ANY(" + timestampLiteralArr + ") "
                + "AND pk2 = ANY(" + timestampLiteralArr + ")";
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(selectSql)) {
                    assertTrue(rs.next());
                    assertEquals(DateUtil.parseTimestamp(pk1Value), rs.getTimestamp(1));
                    assertEquals(DateUtil.parseDate(pk2Value), rs.getDate(2));
                    assertEquals("a", rs.getString(3));
                }
            }
            assertPointLookupsAreGenerated(selectSql, conn, null);
        }
    }
    
    @Test
    public void testArrayAnyComparisonForBinaryColumn() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (pk1 BINARY NOT NULL, " + "pk2 VARCHAR(3), "
            + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2))";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
                conn.commit();
            }
        }
        String pk1Value = "1234567890";
        String pk2Value = "x";
        String upsertStmt = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
                stmt.setBytes(1, pk1Value.getBytes());
                stmt.setString(2, pk2Value);
                stmt.setString(3, "a");
                stmt.executeUpdate();
                conn.commit();
            }
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            TestUtil.dumpTable(conn, TableName.valueOf(tableName));
            String selectSql = "SELECT * FROM " + tableName + " WHERE pk1 = ANY(?) AND pk2 = ANY(?)";
            Array binaryArr = conn.createArrayOf("BINARY", new byte[][] { pk1Value.getBytes(), pk1Value.getBytes(), pk1Value.getBytes() });
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setArray(1, binaryArr);
                stmt.setString(2, pk2Value);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(pk1Value, new String(rs.getBytes(1)));
                    assertEquals(pk2Value, rs.getString(2));
                    assertEquals("a", rs.getString(3));
                }
            }
            assertPointLookupsAreGenerated(selectSql, conn, binaryArr);
        }
    }

    private void assertPointLookupsAreNotGenerated(String selectSql, Connection conn, Array arr)
        throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement("EXPLAIN " + selectSql)) {
            if (arr != null) {
                stmt.setArray(1, arr);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                String explainPlan = QueryUtil.getExplainPlan(rs);
                assertTrue(explainPlan.contains("FULL SCAN") || explainPlan.contains("RANGE SCAN"));
                assertFalse(explainPlan.contains("POINT LOOKUP ON"));
            }
        }
    }

    private void assertPointLookupsAreGenerated(String selectSql, Connection conn, Array arr)
        throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement("EXPLAIN " + selectSql)) {
            if (arr != null) {
                stmt.setArray(1, arr);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                String explainPlan = QueryUtil.getExplainPlan(rs);
                assertTrue(explainPlan.contains("POINT LOOKUP ON"));
            }
        }
    }

    private void createTableASCPkColumns(String tableName) throws SQLException {
        String ddl = "CREATE TABLE " + tableName + " (pk1 INTEGER NOT NULL, " + "pk2 VARCHAR(3), "
            + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2))";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
                conn.commit();
            }
        }
    }

    private void insertData(String tableName, int pk1, String pk2, String col1)
        throws SQLException {
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
}
