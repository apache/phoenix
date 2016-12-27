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

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class OnDuplicateKeyIT extends ParallelStatsDisabledIT {
    private final String indexDDL;
    
    public OnDuplicateKeyIT(String indexDDL) {
        this.indexDDL = indexDDL;
    }
    
    @Parameters
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[] {
                "",
        });
        testCases.add(new String[] {
                "create index %s_IDX on %s(counter1) include (counter2)",
        });
        testCases.add(new String[] {
                "create index %s_IDX on %s(counter1, counter2)",
        });
        testCases.add(new String[] {
                "create local index %s_IDX on %s(counter1) include (counter2)",
        });
        testCases.add(new String[] {
                "create local index %s_IDX on %s(counter1, counter2)",
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
        //assertEquals(finalResult,resultHolder[0]);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(finalResult,rs.getInt(2));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ * FROM " + tableName + " WHERE counter1 >= 0");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(finalResult,rs.getInt(2));
        assertFalse(rs.next());
        
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
    
}
    
