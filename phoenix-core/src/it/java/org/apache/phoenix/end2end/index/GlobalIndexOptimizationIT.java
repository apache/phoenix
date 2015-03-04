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
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class GlobalIndexOptimizationIT extends BaseHBaseManagedTimeIT {

    @BeforeClass 
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Drop the HBase table metadata for this test
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    private void createBaseTable(String tableName, Integer saltBuckets, String splits, boolean multiTenant) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "k3 INTEGER,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n" +
                (multiTenant ? "MULTI_TENANT = true\n" : "")
                        + (saltBuckets != null && splits == null ? (",salt_buckets=" + saltBuckets) : "" 
                        + (saltBuckets == null && splits != null ? (" split on " + splits) : ""));
        conn.createStatement().execute(ddl);
        conn.close();
    }

    private void createIndex(String indexName, String tableName, String columns) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE INDEX " + indexName + " ON " + tableName + " (" + columns + ")";
        conn.createStatement().execute(ddl);
        conn.close();
    }
    
    @Test
    public void testGlobalIndexOptimization() throws Exception {
        testOptimization(null);
    }
    
    @Test
    public void testGlobalIndexOptimizationWithSalting() throws Exception {
        testOptimization(4);
    }
    
    @Test
    public void testGlobalIndexOptimizationTenantSpecific() throws Exception {
        testOptimizationTenantSpecific(null);
    }
    
    @Test
    public void testGlobalIndexOptimizationWithSaltingTenantSpecific() throws Exception {
        testOptimizationTenantSpecific(4);
    }

    private void testOptimization(Integer saltBuckets) throws Exception {
        createBaseTable(TestUtil.DEFAULT_DATA_TABLE_NAME, saltBuckets, "('e','i','o')", false);
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            createIndex(TestUtil.DEFAULT_INDEX_TABLE_NAME, TestUtil.DEFAULT_DATA_TABLE_NAME, "v1");
            
            String query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ * FROM " + TestUtil.DEFAULT_DATA_TABLE_NAME +" where v1='a'";
            ResultSet rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            String expected = 
                    "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_DATA_TABLE_NAME + "\n" +
                    "    SKIP-SCAN-JOIN TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " \\['a'\\]\n" +
                    "            SERVER FILTER BY FIRST KEY ONLY\n" +
                    "    DYNAMIC SERVER FILTER BY \\(\"T.T_ID\", \"T.K1\", \"T.K2\"\\) IN \\(\\(\\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+\\)\\)";
            String actual = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected:\n" + expected + "\nbut got\n" + actual, Pattern.matches(expected, actual));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertFalse(rs.next());
            
            query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ * FROM " + TestUtil.DEFAULT_DATA_TABLE_NAME +" where v1='a'";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            expected = 
                    "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_DATA_TABLE_NAME + "\n" +
                    "    SKIP-SCAN-JOIN TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " \\['a'\\]\n" +
                    "            SERVER FILTER BY FIRST KEY ONLY\n" +
                    "    DYNAMIC SERVER FILTER BY \\(\"T.T_ID\", \"T.K1\", \"T.K2\"\\) IN \\(\\(\\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+\\)\\)";
            actual = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected:\n" + expected + "\nbut got\n" + actual, Pattern.matches(expected, actual));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertEquals("a", rs.getString("v1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("v1"));
            assertFalse(rs.next());
            
            query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ t_id, k1, k2, k3, V1 from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "  where v1<='z' and k3 > 1 order by V1,t_id";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            expected = 
                    "CLIENT PARALLEL \\d-WAY FULL SCAN OVER " + TestUtil.DEFAULT_DATA_TABLE_NAME + "\n" +
                    "    SERVER FILTER BY K3 > 1\n" +
                    "    SERVER SORTED BY \\[T.V1, T.T_ID\\]\n" +
                    "CLIENT MERGE SORT\n" +
                    "    SKIP-SCAN-JOIN TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " \\[\\*\\] - \\['z'\\]\n" +
                    "            SERVER FILTER BY FIRST KEY ONLY\n" +
                    "    DYNAMIC SERVER FILTER BY \\(\"T.T_ID\", \"T.K1\", \"T.K2\"\\) IN \\(\\(\\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+\\)\\)";
            actual = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected:\n" + expected + "\nbut got\n" + actual, Pattern.matches(expected, actual));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(4, rs.getInt("k3"));
            assertEquals("z", rs.getString("V1"));
            assertFalse(rs.next());
            
            query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ t_id, V1, k3 from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "  where v1 <='z' group by v1,t_id, k3";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            expected = 
                    "CLIENT PARALLEL \\d-WAY FULL SCAN OVER " + TestUtil.DEFAULT_DATA_TABLE_NAME + "\n" +
                            "    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[T.T_ID, T.V1, T.K3\\]\n" +
                            "CLIENT MERGE SORT\n" +
                            "    SKIP-SCAN-JOIN TABLE 0\n" +
                            "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " \\[\\*\\] - \\['z'\\]\n" +
                            "            SERVER FILTER BY FIRST KEY ONLY\n" +
                            "    DYNAMIC SERVER FILTER BY \\(\"T.T_ID\", \"T.K1\", \"T.K2\"\\) IN \\(\\(\\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+\\)\\)";
            actual = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected:\n" + expected + "\nbut got\n" + actual, Pattern.matches(expected, actual));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b", rs.getString("t_id"));;
            assertEquals(4, rs.getInt("k3"));
            assertEquals("z", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(3, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("q", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k3"));
            assertEquals("c", rs.getString("V1"));
            assertFalse(rs.next());
            
            query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ v1,sum(k3) from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " where v1 <='z'  group by v1 order by v1";
            
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            expected = 
                    "CLIENT PARALLEL \\d-WAY FULL SCAN OVER T\n" +
                            "    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[T.V1\\]\n" +
                            "CLIENT MERGE SORT\n" +
                            "CLIENT SORTED BY \\[T.V1\\]\n" +
                            "    SKIP-SCAN-JOIN TABLE 0\n" +
                            "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER I \\[\\*\\] - \\['z'\\]\n" +
                            "            SERVER FILTER BY FIRST KEY ONLY\n" +
                            "    DYNAMIC SERVER FILTER BY \\(\"T.T_ID\", \"T.K1\", \"T.K2\"\\) IN \\(\\(\\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+\\)\\)";
            actual = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected:\n" + expected + "\nbut got\n" + actual, Pattern.matches(expected, actual));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(5, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("z", rs.getString(1));
            assertEquals(4, rs.getInt(2));
        } finally {
            conn1.close();
        }
    }

    private void testOptimizationTenantSpecific(Integer saltBuckets) throws Exception {
        createBaseTable(TestUtil.DEFAULT_DATA_TABLE_NAME, saltBuckets, "('e','i','o')", true);
        Connection conn1 = DriverManager.getConnection(getUrl() + ';' + PhoenixRuntime.TENANT_ID_ATTRIB + "=tid1");
        try{
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values(1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values(1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values(2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values(3,1,1,'c')");
            conn1.commit();
            createIndex(TestUtil.DEFAULT_INDEX_TABLE_NAME, TestUtil.DEFAULT_DATA_TABLE_NAME, "v1");
            
            String query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ k1,k2,k3,v1 FROM " + TestUtil.DEFAULT_DATA_TABLE_NAME +" where v1='a'";
            ResultSet rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            String actual = QueryUtil.getExplainPlan(rs);
            String expected = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T \\['tid1'\\]\n" +
                            "    SKIP-SCAN-JOIN TABLE 0\n" +
                            "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER I \\['tid1','a'\\]\n" +
                            "            SERVER FILTER BY FIRST KEY ONLY\n" +
                            "    DYNAMIC SERVER FILTER BY \\(\"T.K1\", \"T.K2\"\\) IN \\(\\(\\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+\\)\\)";
            assertTrue("Expected:\n" + expected + "\ndid not match\n" + actual, Pattern.matches(expected, actual));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertEquals("a", rs.getString("v1"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("v1"));
            assertFalse(rs.next());
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testGlobalIndexOptimizationOnSharedIndex() throws Exception {
        createBaseTable(TestUtil.DEFAULT_DATA_TABLE_NAME, null, "('e','i','o')", false);
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("CREATE INDEX i1 ON " + TestUtil.DEFAULT_DATA_TABLE_NAME + "(k2,k1) INCLUDE (v1)");
            conn1.createStatement().execute("CREATE VIEW v AS SELECT * FROM t WHERE v1 = 'a'");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM v");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            conn1.createStatement().execute("CREATE INDEX vi1 ON v(k1)");
            
            String query = "SELECT /*+ INDEX(v vi1)*/ t_id,k1,k2,k3,v1 FROM v where k1 IN (1,2) and k2 IN (3,4)";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            String actual = QueryUtil.getExplainPlan(rs);
            String expected = 
                    "CLIENT PARALLEL 1-WAY FULL SCAN OVER T\n" +
                    "    SERVER FILTER BY V1 = 'a'\n" +
                    "    SKIP-SCAN-JOIN TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER _IDX_T \\[-32768,1\\] - \\[-32768,2\\]\n" +
                    "            SERVER FILTER BY FIRST KEY ONLY AND \"K2\" IN \\(3,4\\)\n" +
                    "    DYNAMIC SERVER FILTER BY \\(\"V.T_ID\", \"V.K1\", \"V.K2\"\\) IN \\(\\(\\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+, \\$\\d+.\\$\\d+\\)\\)";
            assertTrue("Expected:\n" + expected + "\ndid not match\n" + actual, Pattern.matches(expected,actual));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("v1"));
            assertFalse(rs.next());
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testNoGlobalIndexOptimization() throws Exception {
        createBaseTable(TestUtil.DEFAULT_DATA_TABLE_NAME, null, "('e','i','o')", false);
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_NAME + "(v1)");
            
            // All columns available in index
            String query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ t_id, k1, k2, V1 FROM " + TestUtil.DEFAULT_DATA_TABLE_NAME +" where v1='a'";
            ResultSet rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                        "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ['a']\n"
                                + "    SERVER FILTER BY FIRST KEY ONLY",
                        QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertFalse(rs.next());

            // No INDEX hint specified
            query = "SELECT t_id, k1, k2, k3, V1 FROM " + TestUtil.DEFAULT_DATA_TABLE_NAME +" where v1='a'";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                        "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_DATA_TABLE_NAME + "\n" +
                        "    SERVER FILTER BY V1 = 'a'",
                        QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertFalse(rs.next());
            
            // No where clause
            query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ t_id, k1, k2, k3, V1 from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " order by V1,t_id";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            assertEquals(
                        "CLIENT PARALLEL 4-WAY FULL SCAN OVER " + TestUtil.DEFAULT_DATA_TABLE_NAME + "\n" +
                        "    SERVER SORTED BY [V1, T_ID]\n" +
                        "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("q", rs.getString("t_id"));
            assertEquals(3, rs.getInt("k1"));
            assertEquals(1, rs.getInt("k2"));
            assertEquals(1, rs.getInt("k3"));
            assertEquals("c", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(4, rs.getInt("k3"));
            assertEquals("z", rs.getString("V1"));
            assertFalse(rs.next());
            
            // No where clause in index scan
            query = "SELECT /*+ INDEX(" + TestUtil.DEFAULT_DATA_TABLE_NAME + " " + TestUtil.DEFAULT_INDEX_TABLE_NAME + ")*/ t_id, k1, k2, k3, V1 from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "  where k3 > 1 order by V1,t_id";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            assertEquals(
                        "CLIENT PARALLEL 4-WAY FULL SCAN OVER " + TestUtil.DEFAULT_DATA_TABLE_NAME + "\n" +
                        "    SERVER FILTER BY K3 > 1\n" +
                        "    SERVER SORTED BY [V1, T_ID]\n" +
                        "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(4, rs.getInt("k3"));
            assertEquals("z", rs.getString("V1"));
            assertFalse(rs.next());
        } finally {
            conn1.close();
        }
    }

}
