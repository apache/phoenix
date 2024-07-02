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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class OnDuplicateKeyIT extends ParallelStatsDisabledIT {
    private final String indexDDL;

    public OnDuplicateKeyIT(String indexDDL) {
        this.indexDDL = indexDDL;
    }

    @Parameters
    public static synchronized Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[] {
                "",
        });
        testCases.add(new String[] {
                "create local index %s_IDX on %s(counter1) include (counter2)",
        });
        testCases.add(new String[] {
                "create local index %s_IDX on %s(counter1, counter2)",
        });
        testCases.add(new String[] {
                "create index %s_IDX on %s(counter1) include (counter2)",
        });
        testCases.add(new String[] {
                "create index %s_IDX on %s(counter1, counter2)",
        });
        testCases.add(new String[] {
                "create uncovered index %s_IDX on %s(counter1)",
        });
        testCases.add(new String[] {
                "create uncovered index %s_IDX on %s(counter1, counter2)",
        });
        return testCases;
    }
    
    private void createIndex(Connection conn, String tableName) throws SQLException {
        if (indexDDL == null || indexDDL.length() == 0) {
            return;
        }
        String ddl = String.format(indexDDL, tableName, tableName);
        conn.createStatement().execute(ddl);
    }
    
    @Test
    public void testNewAndUpdateOnSingleNumericColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 smallint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        String dml = "UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(0,rs.getLong(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute(dml);
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(1,rs.getLong(2));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testNewAndUpdateOnSingleNumericColumnWithOtherColumns() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(k1 varchar, k2 varchar, counter1 varchar, counter2 date, other1 char(3), other2 varchar default 'f', constraint pk primary key (k1,k2))";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        String dml = "UPSERT INTO " + tableName + " VALUES('a','b','c',null,'eee') " + 
                     "ON DUPLICATE KEY UPDATE counter1 = counter1 || CASE WHEN LENGTH(counter1) < 10 THEN 'SMALL' ELSE 'LARGE' END || k2 || other2 || other1 ";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("b",rs.getString(2));
        assertEquals("c",rs.getString(3));
        assertEquals(null,rs.getDate(4));
        assertEquals("eee",rs.getString(5));
        assertEquals("f",rs.getString(6));
        assertFalse(rs.next());
        
        conn.createStatement().execute(dml);
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("b",rs.getString(2));
        assertEquals("cSMALLbfeee",rs.getString(3));
        assertEquals(null,rs.getDate(4));
        assertEquals("eee",rs.getString(5));
        assertEquals("f",rs.getString(6));
        assertFalse(rs.next());
        
        conn.createStatement().execute(dml);
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("b",rs.getString(2));
        assertEquals("cSMALLbfeeeLARGEbfeee",rs.getString(3));
        assertEquals(null,rs.getDate(4));
        assertEquals("eee",rs.getString(5));
        assertEquals("f",rs.getString(6));
        assertFalse(rs.next());
        
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a','b','c',null,'eee') " + 
                "ON DUPLICATE KEY UPDATE counter1 = to_char(rand()), counter2 = current_date() + 1");
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("b",rs.getString(2));
        double d = Double.parseDouble(rs.getString(3));
        assertTrue(d >= 0.0 && d <= 1.0);
        Date date = rs.getDate(4);
        assertTrue(date.after(new Date(System.currentTimeMillis())));
        assertEquals("eee",rs.getString(5));
        assertEquals("f",rs.getString(6));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testNewAndUpdateOnSingleVarcharColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 varchar, counter2 smallint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        String dml = "UPSERT INTO " + tableName + " VALUES('a','b') ON DUPLICATE KEY UPDATE counter1 = counter1 || 'b'";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE substr(counter1,1,1) = 'b'");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("b",rs.getString(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute(dml);
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE substr(counter1,1,1) = 'b'");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("bb",rs.getString(2));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testDeleteOnSingleVarcharColumnAutoCommit() throws Exception {
        testDeleteOnSingleVarcharColumn(true);
    }
    
    @Test
    public void testDeleteOnSingleVarcharColumnNoAutoCommit() throws Exception {
        testDeleteOnSingleVarcharColumn(false);
    }
    
    private void testDeleteOnSingleVarcharColumn(boolean autoCommit) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(autoCommit);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 varchar, counter2 smallint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        String dml = "UPSERT INTO " + tableName + " VALUES('a','b') ON DUPLICATE KEY UPDATE counter1 = null";
        conn.createStatement().execute(dml);
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(null,rs.getString(2));
        assertFalse(rs.next());
        
        dml = "UPSERT INTO " + tableName + " VALUES('a','b',0)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES('a','b', 0) ON DUPLICATE KEY UPDATE counter1 = null, counter2 = counter2 + 1";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES('a','b', 0) ON DUPLICATE KEY UPDATE counter1 = 'c', counter2 = counter2 + 1";
        conn.createStatement().execute(dml);
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("c",rs.getString(2));
        assertEquals(2,rs.getInt(3));
        assertFalse(rs.next());

        conn.close();
    }
    
    @Test
    public void testIgnoreOnSingleColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 bigint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',10)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(10,rs.getLong(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY IGNORE");
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(10,rs.getLong(2));
        assertFalse(rs.next()); 
        
        conn.close();
    }
    
    @Test
    public void testInitialIgnoreWithUpdateOnSingleColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 bigint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        // Test ignore combined with update in same commit batch for new record
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',10) ON DUPLICATE KEY IGNORE");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(11,rs.getLong(2));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testOverrideOnDupKeyUpdateWithUpsert() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 bigint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        // Test upsert overriding ON DUPLICATE KEY entries
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',1) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',2) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',10)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(10,rs.getLong(2));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testNewAndMultiUpdateOnSingleColumnAutoCommit() throws Exception {
        testNewAndMultiUpdateOnSingleColumn(true);
    }
    
    @Test
    public void testNewAndMultiUpdateOnSingleColumnNoAutoCommit() throws Exception {
        testNewAndMultiUpdateOnSingleColumn(false);
    }
    
    private void testNewAndMultiUpdateOnSingleColumn(boolean autoCommit) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(autoCommit);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 integer)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',5) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1"); // VALUES ignored
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY IGNORE"); // no impact
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',10) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1"); // VALUES ignored
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(2,rs.getLong(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 2");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 2");
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(9,rs.getLong(2));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testNewAndMultiDifferentUpdateOnSingleColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 decimal)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        String dml = "UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 2";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(3,rs.getLong(2));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testNewAndMultiDifferentUpdateOnMultipleColumnsAutoCommit() throws Exception {
        testNewAndMultiDifferentUpdateOnMultipleColumns(true);
    }
    
    @Test
    public void testNewAndMultiDifferentUpdateOnMultipleColumnsNoAutoCommit() throws Exception {
        testNewAndMultiDifferentUpdateOnMultipleColumns(false);
    }
    
    private void testNewAndMultiDifferentUpdateOnMultipleColumns(boolean autoCommit) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(autoCommit);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 tinyint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        String dml = "UPSERT INTO " + tableName + " VALUES('a',0,0) ON DUPLICATE KEY UPDATE counter1 = counter2 + 1, counter2 = counter1 + 2";
        conn.createStatement().execute(dml);
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(1,rs.getLong(2));
        assertEquals(2,rs.getLong(3));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(1,rs.getLong(2));
        assertEquals(2,rs.getLong(3));
        assertFalse(rs.next());

        conn.createStatement().execute(dml);
        conn.commit();
        
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(3,rs.getLong(2));
        assertEquals(3,rs.getLong(3));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(3,rs.getLong(2));
        assertEquals(3,rs.getLong(3));
        assertFalse(rs.next());

        conn.close();
    }
    
    @Test
    public void testAtomicUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        final String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 integer, counter2 integer)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        int nThreads = 10;
        final int[] resultHolder = new int[1];
        final int nCommits = 100;
        final int nIncrementsPerCommit = 2;
        ExecutorService exec = Executors.newFixedThreadPool(nThreads);
        List<Future> futures = Lists.newArrayListWithExpectedSize(nThreads);
        Connection[] connections = new Connection[nThreads];
        for (int i = 0; i < nThreads; i++) {
            connections[i] = DriverManager.getConnection(getUrl(), props);
        }
        for (int i = 0; i < nThreads; i++) {
            final Connection myConn = connections[i];
            futures.add(exec.submit(new Runnable() {
                @Override
                public void run() {
                    String dml = "UPSERT INTO " + tableName + " VALUES('a',1) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1";
                    try {
                        for (int j = 0; j < nCommits; j++) {
                            for (int k = 0; k < nIncrementsPerCommit; k++) {
                                myConn.createStatement().execute(dml);
                                resultHolder[0]++;
                            }
                            myConn.commit();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }));
        }
        Collections.shuffle(futures);
        for (Future future : futures) {
            future.get();
        }
        exec.shutdownNow();

        int finalResult = nThreads * nCommits * nIncrementsPerCommit;
        boolean isIndexCreated = this.indexDDL != null && this.indexDDL.length() > 0;

        ResultSet rs;
        String selectSql = "SELECT * FROM " + tableName + " WHERE counter1 >= 0";
        if (isIndexCreated) {
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            IndexToolIT.assertExplainPlan(this.indexDDL.contains("local"), actualExplainPlan,
                tableName, tableName + "_IDX");
        }
        rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(finalResult,rs.getInt(2));
        assertFalse(rs.next());

        if (isIndexCreated) {
            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ * FROM " + tableName + " WHERE counter1 >= 0");
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(finalResult, rs.getInt(2));
            assertFalse(rs.next());
        }
        
        conn.close();
    }
    
    @Test
    public void testDeleteOnSingleLowerCaseVarcharColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, \"counter1\" varchar, \"counter2\" smallint)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " VALUES('a','b') ON DUPLICATE KEY UPDATE \"counter1\" = null";
        conn.createStatement().execute(dml);
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(null,rs.getString(2));
        assertFalse(rs.next());
        
        dml = "UPSERT INTO " + tableName + " VALUES('a','b',0)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES('a','b', 0) ON DUPLICATE KEY UPDATE \"counter1\" = null, \"counter2\" = \"counter2\" + 1";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + tableName + " VALUES('a','b', 0) ON DUPLICATE KEY UPDATE \"counter1\" = 'c', \"counter2\" = \"counter2\" + 1";
        conn.createStatement().execute(dml);
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("c",rs.getString(2));
        assertEquals(2,rs.getInt(3));
        assertFalse(rs.next());

        conn.close();
    }
    @Test
    public void testDuplicateUpdateWithSaltedTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        final Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        try {
            String ddl = "create table " + tableName + " (id varchar not null,id1 varchar not null, counter1 bigint, counter2 bigint CONSTRAINT pk PRIMARY KEY (id,id1)) SALT_BUCKETS=6";
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);
            String dml = "UPSERT INTO " + tableName + " (id,id1, counter1, counter2) VALUES ('abc','123', 0, 0) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1, counter2 = counter2 + 1";
            conn.createStatement().execute(dml);
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            assertEquals("0",rs.getString(3));
            assertEquals("0",rs.getString(4));
            conn.createStatement().execute(dml);
            conn.commit();
            rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            assertEquals("1",rs.getString(3));
            assertEquals("1",rs.getString(4));

        } catch (Exception e) {
            fail();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRowsCreatedViaUpsertOnDuplicateKeyShouldNotBeReturnedInQueryIfNotMatched() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl = "create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 smallint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        // The data has to be specifically starting with null for the first counter to fail the test. If you reverse the values, the test passes.
        String dml1 = "UPSERT INTO " + tableName + " VALUES('a',NULL,2) ON DUPLICATE KEY UPDATE " +
                "counter1 = CASE WHEN (counter1 IS NULL) THEN NULL ELSE counter1 END, " +
                "counter2 = CASE WHEN (counter1 IS NULL) THEN 2 ELSE counter2 END";
        conn.createStatement().execute(dml1);
        conn.commit();

        String dml2 = "UPSERT INTO " + tableName + " VALUES('b',1,2) ON DUPLICATE KEY UPDATE " +
                "counter1 = CASE WHEN (counter1 IS NULL) THEN 1 ELSE counter1 END, " +
                "counter2 = CASE WHEN (counter1 IS NULL) THEN 2 ELSE counter2 END";
        conn.createStatement().execute(dml2);
        conn.commit();

        // Using this statement causes the test to pass
        //ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter2 = 2 AND counter1 = 1");
        // This statement should be equivalent to the one above, but it selects both rows.
        ResultSet rs = conn.createStatement().executeQuery("SELECT pk, counter1, counter2 FROM " + tableName + " WHERE counter2 = 2 AND (counter1 = 1 OR counter1 = 1)");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals(1,rs.getLong(2));
        assertEquals(2,rs.getLong(3));
        assertFalse(rs.next());

        conn.close();
    }

    @Test
    public void testOnDupAndUpsertInSameCommitBatch() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 varchar)";
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);

            // row doesn't exist
            conn.createStatement().execute(String.format("UPSERT INTO %s VALUES('a',0,'abc')", tableName));
            conn.createStatement().execute(String.format(
                "UPSERT INTO %s VALUES('a',1,'zzz') ON DUPLICATE KEY UPDATE counter1 = counter1 + 2", tableName));
            conn.commit();
            assertRow(conn, tableName, "a", 2, "abc");

            // row exists
            conn.createStatement().execute(String.format("UPSERT INTO %s VALUES('a', 7, 'fff')", tableName));
            conn.createStatement().execute(String.format(
                "UPSERT INTO %s VALUES('a',1, 'bazz') ON DUPLICATE KEY UPDATE counter1 = counter1 + 2," +
                    "counter2 = counter2 || 'ddd'", tableName));
            conn.commit();
            assertRow(conn, tableName, "a", 9, "fffddd");

            // partial update
            conn.createStatement().execute(String.format(
                "UPSERT INTO %s (pk, counter2) VALUES('a', 'gggg') ON DUPLICATE KEY UPDATE counter1 = counter1 + 2", tableName));
            conn.createStatement().execute(String.format(
                "UPSERT INTO %s (pk, counter2) VALUES ('a', 'bar')", tableName));
            conn.commit();
            assertRow(conn, tableName, "a", 11, "bar");
        }
    }

    @Test
    public void testMultiplePartialUpdatesInSameBatch() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 bigint)";
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);
            String dml;
            ResultSet rs;
            // first commit
            dml = String.format("UPSERT INTO %s VALUES('a',0,0)", tableName);
            conn.createStatement().execute(dml);
            conn.commit();
            // batch multiple conditional updates (partial) in a single batch
            dml = String.format(
                "UPSERT INTO %s VALUES('a',2,3) ON DUPLICATE KEY UPDATE counter1 = counter1 + 1", tableName);
            conn.createStatement().execute(dml);
            dml = String.format(
                "UPSERT INTO %s VALUES('a',2,3) ON DUPLICATE KEY UPDATE counter2 = counter2 + 2", tableName);
            conn.createStatement().execute(dml);
            dml = String.format(
                "UPSERT INTO %s VALUES('a',2,3) ON DUPLICATE KEY UPDATE counter1 = counter1 + 100", tableName);
            conn.createStatement().execute(dml);
            dml = String.format(
                "UPSERT INTO %s VALUES('a',2,3) ON DUPLICATE KEY UPDATE counter2 = counter2 + 200", tableName);
            conn.createStatement().execute(dml);
            conn.commit();
            String dql = String.format("SELECT counter1, counter2 FROM %s WHERE counter1 > 0", tableName);
            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals(101, rs.getInt(1));
            assertEquals(202, rs.getInt(2));
        }
    }

    @Test
    public void testComplexDuplicateKeyExpression() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName +
                "(pk varchar primary key, counter1 bigint, counter2 bigint, approval varchar)";
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);
            String dml;
            dml = String.format("UPSERT INTO %s VALUES('abc', 0, 100, 'NONE')", tableName);
            conn.createStatement().execute(dml);
            conn.commit();
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                "ON DUPLICATE KEY UPDATE " +
                "counter1 = counter1 + counter2," +
                "approval = CASE WHEN counter1 < 100 THEN 'NONE' " +
                "WHEN counter1 < 1000 THEN 'MANAGER_APPROVAL' " +
                "ELSE 'VP_APPROVAL' END", tableName);
            conn.createStatement().execute(dml);
            conn.commit();
            String dql = "SELECT * from " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(100, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertEquals("NONE", rs.getString("approval"));

            conn.createStatement().execute(dml);
            conn.commit();
            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(200, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertEquals("MANAGER_APPROVAL", rs.getString("approval"));
        }
    }

    @Test
    public void testRowStampCol() throws Exception {
        // ROW_TIMESTAMP is not supported for tables with indexes
        if (indexDDL.length() > 0) {
            return;
        }
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName +
                "(\n" +
                "ORGANIZATION_ID CHAR(15) NOT NULL,\n" +
                "USER_ID CHAR(15) NOT NULL,\n" +
                "TIME_STAMP DATE NOT NULL,\n" +
                "STATUS VARCHAR,\n" +
                "CONSTRAINT PK PRIMARY KEY \n" +
                "    (\n" +
                "        ORGANIZATION_ID, \n" +
                "        USER_ID,\n" +
                "        TIME_STAMP ROW_TIMESTAMP\n" + // ROW_TIMESTAMP col
                "    ) \n" +
                ")\n";

            conn.createStatement().execute(ddl);
            String orgid = "ORG1";
            String userid = "USER1";
            String original = "ORIGINAL";
            String updated = "UPDATED";
            String duplicate = "DUPLICATE";
            long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() - 10;
            String dml = "UPSERT INTO  " + tableName +
                "(ORGANIZATION_ID, USER_ID, TIME_STAMP, STATUS) VALUES (?, ?, ?, ?)";
            String ignoreDml = dml + "ON DUPLICATE KEY IGNORE";
            String updateDml = dml + "ON DUPLICATE KEY UPDATE status='" + duplicate + "'";
            String nullDml = dml + "ON DUPLICATE KEY UPDATE status = null";
            String dql = "SELECT count(*) from " + tableName + " WHERE STATUS = ?";

            // row doesn't exist
            upsertRecord(conn, ignoreDml, orgid, userid, rowTimestamp, original);
            assertNumRecords(1, conn, dql, original);
            assertHBaseRowTimestamp(tableName, rowTimestamp);

            // on duplicate key ignore
            upsertRecord(conn, ignoreDml, orgid, userid, rowTimestamp, updated);
            assertNumRecords(1, conn, dql, original);
            assertNumRecords(0, conn, dql, updated);
            assertHBaseRowTimestamp(tableName, rowTimestamp);

            // regular upsert override
            upsertRecord(conn, dml, orgid, userid, rowTimestamp, updated);
            assertNumRecords(0, conn, dql, original);
            assertNumRecords(1, conn, dql, updated);
            assertHBaseRowTimestamp(tableName, rowTimestamp);

            // on duplicate key update generates extra mutations on the server but those mutations
            // don't honor ROW_TIMESTAMP
            upsertRecord(conn, updateDml, orgid, userid, rowTimestamp, "");
            assertNumRecords(0, conn, dql, updated);
            assertNumRecords(1, conn, dql, duplicate);

            // set null, new mutations generated on the server
            upsertRecord(conn, nullDml, orgid, userid, rowTimestamp, "");
            assertNumRecords(0, conn, dql, duplicate);
            dql = "SELECT count(*) from " + tableName + " WHERE STATUS is null";
            assertNumRecords(1, conn, dql);
        }
    }

    private void assertRow(Connection conn, String tableName, String expectedPK, int expectedCol1, String expectedCol2) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedPK,rs.getString(1));
        assertEquals(expectedCol1,rs.getInt(2));
        assertEquals(expectedCol2,rs.getString(3));
        assertFalse(rs.next());
    }

    private void upsertRecord(Connection conn, String dml, String orgid, String userid, long ts, String status) throws SQLException {
        try(PreparedStatement stmt = conn.prepareStatement(dml)) { // regular upsert
            stmt.setString(1, orgid);
            stmt.setString(2, userid);
            stmt.setDate(3, new Date(ts));
            stmt.setString(4, status); // status should change now
            stmt.executeUpdate();
            conn.commit();
        }
    }

    private void assertNumRecords(int count, Connection conn, String dql, String... params)
        throws Exception {
        PreparedStatement stmt = conn.prepareStatement(dql);
        int counter = 1;
        for (String param : params) {
            stmt.setString(counter++, param);
        }
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
    }

    private void assertHBaseRowTimestamp(String tableName, long expectedTimestamp) throws Exception {
        long actualTimestamp = getEmptyKVLatestCellTimestamp(tableName);
        assertEquals(expectedTimestamp, actualTimestamp);
    }

    private long getEmptyKVLatestCellTimestamp(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        PTable pTable = PhoenixRuntime.getTable(conn, tableName);
        byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(pTable).getFirst();
        return getColumnLatestCellTimestamp(tableName, emptyCQ);
    }

    private long getColumnLatestCellTimestamp(String tableName, byte[] cq) throws Exception {
        Scan scan = new Scan();
        try (org.apache.hadoop.hbase.client.Connection hconn =
                     ConnectionFactory.createConnection(config)) {
            Table table = hconn.getTable(TableName.valueOf(tableName));
            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();
            return result.getColumnLatestCell(
                    QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, cq).getTimestamp();
        }
    }
}
