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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.function.SqrtFunction;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End to end tests for {@link SqrtFunction}
 */
@Category(ParallelStatsDisabledTest.class)
public class GetSetObjectIT extends ParallelStatsDisabledIT {

    // Temporals are tested in DateTimeIT
    // Arrays are tested in Array1IT

    @Test
    public void testNonNumeric() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        String tableName = generateUniqueName();

        byte[] bytes = { 1, 2 };
        String characters = "11";

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
                PreparedStatement insertStmt =
                        conn.prepareStatement("upsert into " + tableName
                                + "(id, binary_col, varbinary_col, boolean_col, char_col, varchar_col) values (?,?,?,?,?,?)")) {
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INTEGER PRIMARY KEY, BINARY_COL BINARY(2), VARBINARY_COL VARBINARY, BOOLEAN_COL BOOLEAN, CHAR_COL CHAR(2), VARCHAR_COL VARCHAR)");
            insertStmt.setInt(1, 1);
            // The type specific setters also delegate to these
            insertStmt.setObject(2, bytes);
            insertStmt.setObject(3, bytes);
            insertStmt.setObject(4, Boolean.TRUE);
            insertStmt.setObject(5, characters);
            insertStmt.setObject(6, characters);
            insertStmt.executeUpdate();
            conn.commit();

            ResultSet rs = stmt.executeQuery("select * from " + tableName);
            while (rs.next()) {
                assertArrayEquals(bytes, rs.getBytes(2));
                assertArrayEquals(bytes, (byte[]) rs.getObject(2));
                assertArrayEquals(bytes, rs.getObject(2, byte[].class));
                try {
                    rs.getObject(2, Long.class);
                    fail("We only implement byte[]");
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
                }
                assertArrayEquals(bytes, rs.getBytes(3));
                assertArrayEquals(bytes, (byte[]) rs.getObject(3));
                assertArrayEquals(bytes, rs.getObject(3, byte[].class));
                try {
                    rs.getObject(3, Long.class);
                    fail("We only implement byte[]");
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
                }
                assertEquals(Boolean.TRUE, rs.getBoolean(4));
                assertEquals(Boolean.TRUE, rs.getObject(4));
                assertEquals(Boolean.TRUE, rs.getObject(4, Boolean.class));
                try {
                    assertEquals(Boolean.TRUE, rs.getObject(3, Long.class));
                    fail("We only implement Boolean");
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
                }
                assertEquals(characters, rs.getString(5));
                assertEquals(characters, rs.getObject(5));
                assertEquals(characters, rs.getObject(5, String.class));
                try {
                    assertEquals(Boolean.TRUE, rs.getObject(5, Boolean.class));
                    fail("We only implement String");
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
                }
                assertEquals(characters, rs.getString(6));
                assertEquals(characters, rs.getObject(6));
                assertEquals(characters, rs.getObject(6, String.class));
                try {
                    assertEquals(Boolean.TRUE, rs.getObject(6, Boolean.class));
                    fail("We only implement String");
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
                }
            }
        }
    }

    @Test
    public void testNumeric() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        String tableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
                PreparedStatement insertStmt =
                        conn.prepareStatement("upsert into " + tableName
                                + "(id, TINY_COL, SMALL_COL, INT_COL, BIG_COL, FLOAT_COL, DOUBLE_COL, DECIMAL_COL) values (?,?,?,?,?,?,?,?)")) {
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INTEGER PRIMARY KEY, TINY_COL TINYINT, SMALL_COL SMALLINT, INT_COL INTEGER, BIG_COL BIGINT, FLOAT_COL FLOAT, DOUBLE_COL DOUBLE, DECIMAL_COL DECIMAL)");
            insertStmt.setInt(1, 1);
            // The type specific setters also delegate to these
            insertStmt.setObject(2, Byte.valueOf("1"));
            insertStmt.setObject(3, Short.valueOf("1"));
            insertStmt.setObject(4, Integer.valueOf("1"));
            insertStmt.setObject(5, Long.valueOf("1"));
            insertStmt.setObject(6, Float.valueOf("1.0"));
            insertStmt.setObject(7, Double.valueOf("1.0"));
            insertStmt.setObject(8, BigDecimal.ONE);
            insertStmt.executeUpdate();
            conn.commit();

            ResultSet rs = stmt.executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            assertEquals(Byte.valueOf("1"), rs.getObject(2));
            assertEquals(Short.valueOf("1"), rs.getObject(3));
            assertEquals(Integer.valueOf("1"), rs.getObject(4));
            assertEquals(Long.valueOf("1"), rs.getObject(5));
            assertEquals(Float.valueOf("1.0"), rs.getObject(6));
            assertEquals(Double.valueOf("1.0"), rs.getObject(7));
            assertEquals(BigDecimal.ONE, rs.getObject(8));
            for (int i = 2; i <= 8; i++) {
                assertEquals(Byte.valueOf("1"), Byte.valueOf(rs.getByte(i)));
                assertEquals(Byte.valueOf("1"), rs.getObject(i, Byte.class));
                assertEquals(Short.valueOf("1"), Short.valueOf(rs.getShort(i)));
                assertEquals(Short.valueOf("1"), rs.getObject(i, Short.class));
                assertEquals(Integer.valueOf("1"), Integer.valueOf(rs.getInt(i)));
                assertEquals(Integer.valueOf("1"), rs.getObject(i, Integer.class));
                assertEquals(Long.valueOf("1"), Long.valueOf(rs.getLong(i)));
                assertEquals(Long.valueOf("1"), rs.getObject(i, Long.class));
                assertEquals(Float.valueOf("1.0"), Float.valueOf(rs.getFloat(i)));
                assertEquals(Float.valueOf("1.0"), rs.getObject(i, Float.class));
                assertEquals(Double.valueOf("1.0"), Double.valueOf(rs.getDouble(i)));
                assertEquals(Double.valueOf("1.0"), rs.getObject(i, Double.class));
                assertTrue(BigDecimal.ONE.abs().compareTo(rs.getBigDecimal(i)) == 0);
                assertTrue(BigDecimal.ONE.abs().compareTo(rs.getObject(i, BigDecimal.class)) == 0);
                try {
                    rs.getObject(i, Boolean.class);
                    fail("Non-numeric types are not supported");
                } catch (SQLException e) {
                    // Expected
                }
            }
        }
    }
}
