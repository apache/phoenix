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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;


public class MultiCfQueryExecIT extends ParallelStatsEnabledIT {
    private String fullTableName;

    @Before
    public void generateTableNames() throws SQLException {
        String schemaName = TestUtil.DEFAULT_SCHEMA_NAME;
        String tableName = "T_" + generateUniqueName();
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);
    }

    private void createTable(Connection conn) throws SQLException {
        conn.createStatement().execute(
                "create table " + fullTableName + "   (id char(15) not null primary key,\n"
                        + "    a.unique_user_count integer,\n" + "    b.unique_org_count integer,\n"
                        + "    c.db_cpu_utilization decimal(31,10),\n" + "    d.transaction_count bigint,\n"
                        + "    e.cpu_utilization decimal(31,10),\n" + "    f.response_time bigint,\n"
                        + "    g.response_time bigint)");
    }

    private void initTableValues(Connection conn) throws Exception {
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
"upsert into " + fullTableName + "(" + "    ID, "
                + "    TRANSACTION_COUNT, " + "    CPU_UTILIZATION, " + "    DB_CPU_UTILIZATION,"
                + "    UNIQUE_USER_COUNT," + "    F.RESPONSE_TIME," + "    G.RESPONSE_TIME)"
                +
                "VALUES (?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "000000000000001");
        stmt.setInt(2, 100);
        stmt.setBigDecimal(3, BigDecimal.valueOf(0.5));
        stmt.setBigDecimal(4, BigDecimal.valueOf(0.2));
        stmt.setInt(5, 1000);
        stmt.setLong(6, 11111);
        stmt.setLong(7, 11112);
        stmt.execute();
        stmt.setString(1, "000000000000002");
        stmt.setInt(2, 200);
        stmt.setBigDecimal(3, BigDecimal.valueOf(2.5));
        stmt.setBigDecimal(4, BigDecimal.valueOf(2.2));
        stmt.setInt(5, 2000);
        stmt.setLong(6, 2222);
        stmt.setLong(7, 22222);
        stmt.execute();
        conn.commit();
    }

    @Test
    public void testConstantCount() throws Exception {
        String query = "SELECT count(1) from " + fullTableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            createTable(conn);
            initTableValues(conn);
            analyzeTable(conn, fullTableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCFToDisambiguateInSelectOnly1() throws Exception {
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from " + fullTableName + " where ID = '000000000000002'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            createTable(conn);
            initTableValues(conn);
            analyzeTable(conn, fullTableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCFToDisambiguateInSelectOnly2() throws Exception {
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from " + fullTableName + " where TRANSACTION_COUNT = 200";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            createTable(conn);
            initTableValues(conn);
            analyzeTable(conn, fullTableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGuidePostsForMultiCFs() throws Exception {
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from " + fullTableName + " where F.RESPONSE_TIME = 2222";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            createTable(conn);
            initTableValues(conn);
            analyzeTable(conn, fullTableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
            // Use E column family. Since the column family with the empty key value (the first one, A)
            // is always added to the scan, we never really use other guideposts (but this may change).
            List<KeyRange> splits = getAllSplits(conn, fullTableName, "e.cpu_utilization IS NOT NULL", "COUNT(*)");
            // Since the E column family is not populated, it won't have as many splits
            assertEquals(3, splits.size());
            // Same as above for G column family.
            splits = getAllSplits(conn, fullTableName, "g.response_time IS NOT NULL", "COUNT(*)");
            assertEquals(3, splits.size());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGuidePostsForMultiCFsOverUnevenDistrib() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName + " (K1 CHAR(1) NOT NULL, "
 + "K2 VARCHAR NOT NULL, " + "CF1.A INTEGER, "
                        + "CF2.B INTEGER, " + "CF3.C INTEGER, " + "CF4.D INTEGER, " + "CF5.E INTEGER, "
                        + "CF6.F INTEGER " + "CONSTRAINT PK PRIMARY KEY (K1,K2)) SPLIT ON ('B','C','D')");

        for (int i = 0; i < 100; i++) {
            String upsert = "UPSERT INTO " + fullTableName + "(K1,K2,A) VALUES('" + Character.toString((char)('A' + i % 10))
                    + "','" + (i * 10) + "'," + i + ")";
            conn.createStatement().execute(upsert);
            if (i % 10 == 0) {
                conn.createStatement().execute(
                        "UPSERT INTO " + fullTableName + "(K1,K2,F) VALUES('" + Character.toString((char)('A' + i % 10))
                                + "','" + (i * 10) + "'," + (i * 10) + ")");
            }
        }
        conn.commit();
        try {
            analyzeTable(conn, fullTableName);
            PreparedStatement statement = conn.prepareStatement("select count(*) from " + fullTableName + " where f < 400");
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
            assertFalse(rs.next());
            List<KeyRange> splits = getAllSplits(conn, fullTableName, "f < 400", "COUNT(*)");
            // Uses less populated column f
            assertEquals(14, splits.size());
            // Uses more populated column a
            splits = getAllSplits(conn, fullTableName, "a < 80", "COUNT(*)");
            assertEquals(104, splits.size());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testGuidePostsRetrievedForMultiCF() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName + " (  k INTEGER PRIMARY KEY, A.V1 VARCHAR, B.V2 VARCHAR, C.V3 VARCHAR)");

        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?,?)");
        stmt.setInt(1, 1);
        stmt.setString(2, "A");
        stmt.setString(3, "B");
        stmt.setString(4, "C");
        stmt.execute();
        conn.commit();

        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?,?)");
        stmt.setInt(1, 2);
        stmt.setString(2, "D");
        stmt.setString(3, "E");
        stmt.setString(4, "F");
        stmt.execute();
        conn.commit();

        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k, A.V1, C.V3) VALUES(?,?,?)");
        stmt.setInt(1, 3);
        stmt.setString(2, "E");
        stmt.setString(3, "X");
        stmt.execute();
        conn.commit();

        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k, A.V1, C.V3) VALUES(?,?,?)");
        stmt.setInt(1, 4);
        stmt.setString(2, "F");
        stmt.setString(3, "F");
        stmt.execute();
        conn.commit();

        analyzeTable(conn, fullTableName);

        rs = conn.createStatement().executeQuery("SELECT B.V2 FROM " + fullTableName + " WHERE B.V2 = 'B'");
        assertTrue(rs.next());
        assertEquals("B", rs.getString(1));
        List<KeyRange> splits = getAllSplits(conn, fullTableName, "C.V3 = 'X'", "A.V1");
        assertEquals(5, splits.size());
        splits = getAllSplits(conn, fullTableName, "B.V2 = 'B'", "B.V2");
        assertEquals(3, splits.size());
        conn.close();
    }

    @Test
    public void testCFToDisambiguate2() throws Exception {
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from " + fullTableName
                + " where G.RESPONSE_TIME-1 = F.RESPONSE_TIME";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            createTable(conn);
            initTableValues(conn);
            analyzeTable(conn, fullTableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(11111, rs.getLong(1));
            assertEquals(11112, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDefaultCFToDisambiguate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        createTable(conn);
        initTableValues(conn);

        String ddl = "ALTER TABLE " + fullTableName + " ADD response_time BIGINT";
        conn.createStatement().execute(ddl);

        String dml = "upsert into " + fullTableName + "(" + "    ID, " + "    RESPONSE_TIME)"
                + "VALUES ('000000000000003', 333)";
        conn.createStatement().execute(dml);
        conn.commit();

        analyzeTable(conn, fullTableName);

        String query = "SELECT ID,RESPONSE_TIME from " + fullTableName + " where RESPONSE_TIME = 333";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("000000000000003", rs.getString(1));
            assertEquals(333, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testEssentialColumnFamilyForRowKeyFilter() throws Exception {
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from " + fullTableName + " where SUBSTR(ID, 15) = '2'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            createTable(conn);
            initTableValues(conn);
            analyzeTable(conn, fullTableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2222, rs.getLong(1));
            assertEquals(22222, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
