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
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
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
    public void testArrayAnyComparisonWithNonPkColumn() throws Exception {
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
    public void testArrayAnyComparsionGeneratesPointLookup() throws Exception {
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
    public void testArrayAnyComparisonForLiteralArrayInWhereClause() throws Exception {
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
        String ddl = "CREATE TABLE " + tableName + " (pk1 FLOAT NOT NULL, "
            + "pk2 VARCHAR(3) NOT NULL, " + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2))";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
                conn.commit();
                stmt.execute("UPSERT INTO " + tableName + " VALUES (1.1, 'x', 'a')");
                conn.commit();
                stmt.execute("UPSERT INTO " + tableName + " VALUES (2.2, 'y', 'b')");
                conn.commit();
                stmt.execute("UPSERT INTO " + tableName + " VALUES (3.3, 'z', 'c')");
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
        String ddl = "CREATE TABLE " + tableName + " (pk1 INTEGER NOT NULL, "
            + "pk2 VARCHAR(3) NOT NULL, " + "col1 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (pk1, pk2))";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
                conn.commit();
            }
        }
    }
    
    private void insertData(String tableName, int pk1, String pk2, String col1) throws SQLException {
        String ddl =
            "UPSERT INTO " + tableName + " VALUES (" + pk1 + ", '" + pk2 + "', '" + col1 + "')";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
                conn.commit();
            }
        }
    }
}
