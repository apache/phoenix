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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class CostBasedDecisionIT extends BaseUniqueNamesOwnClusterIT {

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        props.put(QueryServices.COST_BASED_OPTIMIZER_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testCostOverridesStaticPlanOrdering1() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            String tableName = BaseTest.generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "rowkey VARCHAR PRIMARY KEY,\n" +
                    "c1 VARCHAR,\n" +
                    "c2 VARCHAR)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx ON " + tableName + " (c1)");

            String query = "SELECT rowkey, c1, c2 FROM " + tableName + " where c1 LIKE 'X0%' ORDER BY rowkey";
            // Use the data table plan that opts out order-by when stats are not available.
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected 'FULL SCAN' in the plan:\n" + plan + ".",
                    plan.contains("FULL SCAN"));

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (rowkey, c1, c2) VALUES (?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                int c1 = i % 16;
                stmt.setString(1, "k" + i);
                stmt.setString(2, "X" + Integer.toHexString(c1) + c1);
                stmt.setString(3, "c");
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS " + tableName);

            // Use the index table plan that has a lower cost when stats become available.
            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected 'RANGE SCAN' in the plan:\n" + plan + ".",
                    plan.contains("RANGE SCAN"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCostOverridesStaticPlanOrdering2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            String tableName = BaseTest.generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "rowkey VARCHAR PRIMARY KEY,\n" +
                    "c1 VARCHAR,\n" +
                    "c2 VARCHAR)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx ON " + tableName + " (c1)");

            String query = "SELECT rowkey, max(c1), max(c2) FROM " + tableName + " where c1 LIKE 'X%' GROUP BY rowkey";
            // Use the index table plan that opts out order-by when stats are not available.
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected 'RANGE SCAN' in the plan:\n" + plan + ".",
                    plan.contains("RANGE SCAN"));

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (rowkey, c1, c2) VALUES (?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                int c1 = i % 16;
                stmt.setString(1, "k" + i);
                stmt.setString(2, "X" + Integer.toHexString(c1) + c1);
                stmt.setString(3, "c");
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS " + tableName);

            // Given that the range on C1 is meaningless and group-by becomes
            // order-preserving if using the data table, the data table plan should
            // come out as the best plan based on the costs.
            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected 'FULL SCAN' in the plan:\n" + plan + ".",
                    plan.contains("FULL SCAN"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCostOverridesStaticPlanOrdering3() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            String tableName = BaseTest.generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "rowkey VARCHAR PRIMARY KEY,\n" +
                    "c1 INTEGER,\n" +
                    "c2 INTEGER,\n" +
                    "c3 INTEGER)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx1 ON " + tableName + " (c1) INCLUDE (c2, c3)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx2 ON " + tableName + " (c2, c3) INCLUDE (c1)");

            String query = "SELECT * FROM " + tableName + " where c1 BETWEEN 10 AND 20 AND c2 < 9000 AND C3 < 5000";
            // Use the idx2 plan with a wider PK slot span when stats are not available.
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            String indexPlan =
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [2,*] - [2,9,000]\n" +
                    "    SERVER FILTER BY ((\"C1\" >= 10 AND \"C1\" <= 20) AND TO_INTEGER(\"C3\") < 5000)\n" +
                    "CLIENT MERGE SORT";
            assertTrue("Expected '" + indexPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(indexPlan));

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (rowkey, c1, c2, c3) VALUES (?, ?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                stmt.setString(1, "k" + i);
                stmt.setInt(2, i);
                stmt.setInt(3, i);
                stmt.setInt(4, i);
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS " + tableName);

            // Use the idx2 plan that scans less data when stats become available.
            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            String dataPlan =
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1,10] - [1,20]\n" +
                    "    SERVER FILTER BY (\"C2\" < 9000 AND \"C3\" < 5000)\n" +
                    "CLIENT MERGE SORT";
            assertTrue("Expected '" + dataPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(dataPlan));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCostOverridesStaticPlanOrderingInUpsertQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            String tableName = BaseTest.generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "rowkey VARCHAR PRIMARY KEY,\n" +
                    "c1 INTEGER,\n" +
                    "c2 INTEGER,\n" +
                    "c3 INTEGER)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx1 ON " + tableName + " (c1) INCLUDE (c2, c3)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx2 ON " + tableName + " (c2, c3) INCLUDE (c1)");

            String query = "UPSERT INTO " + tableName + " SELECT * FROM " + tableName + " where c1 BETWEEN 10 AND 20 AND c2 < 9000 AND C3 < 5000";
            // Use the idx2 plan with a wider PK slot span when stats are not available.
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            String indexPlan =
                    "UPSERT SELECT\n" +
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [2,*] - [2,9,000]\n" +
                            "    SERVER FILTER BY ((\"C1\" >= 10 AND \"C1\" <= 20) AND TO_INTEGER(\"C3\") < 5000)\n" +
                            "CLIENT MERGE SORT";
            assertTrue("Expected '" + indexPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(indexPlan));

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (rowkey, c1, c2, c3) VALUES (?, ?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                stmt.setString(1, "k" + i);
                stmt.setInt(2, i);
                stmt.setInt(3, i);
                stmt.setInt(4, i);
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS " + tableName);

            // Use the idx2 plan that scans less data when stats become available.
            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            String dataPlan =
                    "UPSERT SELECT\n" +
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1,10] - [1,20]\n" +
                            "    SERVER FILTER BY (\"C2\" < 9000 AND \"C3\" < 5000)\n" +
                            "CLIENT MERGE SORT";
            assertTrue("Expected '" + dataPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(dataPlan));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCostOverridesStaticPlanOrderingInDeleteQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            String tableName = BaseTest.generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "rowkey VARCHAR PRIMARY KEY,\n" +
                    "c1 INTEGER,\n" +
                    "c2 INTEGER,\n" +
                    "c3 INTEGER)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx1 ON " + tableName + " (c1) INCLUDE (c2, c3)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx2 ON " + tableName + " (c2, c3) INCLUDE (c1)");

            String query = "DELETE FROM " + tableName + " where c1 BETWEEN 10 AND 20 AND c2 < 9000 AND C3 < 5000";
            // Use the idx2 plan with a wider PK slot span when stats are not available.
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            String indexPlan =
                    "DELETE ROWS\n" +
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [2,*] - [2,9,000]\n" +
                            "    SERVER FILTER BY ((\"C1\" >= 10 AND \"C1\" <= 20) AND TO_INTEGER(\"C3\") < 5000)\n" +
                            "CLIENT MERGE SORT";
            assertTrue("Expected '" + indexPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(indexPlan));

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (rowkey, c1, c2, c3) VALUES (?, ?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                stmt.setString(1, "k" + i);
                stmt.setInt(2, i);
                stmt.setInt(3, i);
                stmt.setInt(4, i);
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS " + tableName);

            // Use the idx2 plan that scans less data when stats become available.
            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            String dataPlan =
                    "DELETE ROWS\n" +
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1,10] - [1,20]\n" +
                            "    SERVER FILTER BY (\"C2\" < 9000 AND \"C3\" < 5000)\n" +
                            "CLIENT MERGE SORT";
            assertTrue("Expected '" + dataPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(dataPlan));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCostOverridesStaticPlanOrderingInUnionQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            String tableName = BaseTest.generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "rowkey VARCHAR PRIMARY KEY,\n" +
                    "c1 VARCHAR,\n" +
                    "c2 VARCHAR)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx ON " + tableName + " (c1)");

            String query = "SELECT c1, max(rowkey), max(c2) FROM " + tableName + " where rowkey LIKE 'k%' GROUP BY c1 "
                    + "UNION ALL SELECT rowkey, max(c1), max(c2) FROM " + tableName + " where c1 LIKE 'X%' GROUP BY rowkey";
            // Use the default plan when stats are not available.
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            String defaultPlan =
                    "UNION ALL OVER 2 QUERIES\n" +
                    "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " ['k'] - ['l']\n" +
                    "        SERVER AGGREGATE INTO DISTINCT ROWS BY [C1]\n" +
                    "    CLIENT MERGE SORT\n" +
                    "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1,'X'] - [1,'Y']\n" +
                    "        SERVER FILTER BY FIRST KEY ONLY\n" +
                    "        SERVER AGGREGATE INTO DISTINCT ROWS BY [\"ROWKEY\"]\n" +
                    "    CLIENT MERGE SORT";
            assertTrue("Expected '" + defaultPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(defaultPlan));

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (rowkey, c1, c2) VALUES (?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                int c1 = i % 16;
                stmt.setString(1, "k" + i);
                stmt.setString(2, "X" + Integer.toHexString(c1) + c1);
                stmt.setString(3, "c");
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS " + tableName);

            // Use the optimal plan based on cost when stats become available.
            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            String optimizedPlan =
                    "UNION ALL OVER 2 QUERIES\n" +
                    "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1]\n" +
                    "        SERVER FILTER BY FIRST KEY ONLY AND \"ROWKEY\" LIKE 'k%'\n" +
                    "        SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"C1\"]\n" +
                    "    CLIENT MERGE SORT\n" +
                    "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName + "\n" +
                    "        SERVER FILTER BY C1 LIKE 'X%'\n" +
                    "        SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [ROWKEY]";
            assertTrue("Expected '" + optimizedPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(optimizedPlan));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCostOverridesStaticPlanOrderingInJoinQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            String tableName = BaseTest.generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "rowkey VARCHAR PRIMARY KEY,\n" +
                    "c1 VARCHAR,\n" +
                    "c2 VARCHAR)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx ON " + tableName + " (c1)");

            String query = "SELECT t1.rowkey, t1.c1, t1.c2, mc1, mc2 FROM " + tableName + " t1 "
                    + "JOIN (SELECT rowkey, max(c1) mc1, max(c2) mc2 FROM " + tableName + " where c1 LIKE 'X%' GROUP BY rowkey) t2 "
                    + "ON t1.rowkey = t2.rowkey WHERE t1.c1 LIKE 'X0%' ORDER BY t1.rowkey";
            // Use the default plan when stats are not available.
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            String defaultPlan =
                    "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName + "\n" +
                    "    SERVER FILTER BY C1 LIKE 'X0%'\n" +
                    "    PARALLEL INNER-JOIN TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1,'X'] - [1,'Y']\n" +
                    "            SERVER FILTER BY FIRST KEY ONLY\n" +
                    "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"ROWKEY\"]\n" +
                    "        CLIENT MERGE SORT\n" +
                    "    DYNAMIC SERVER FILTER BY T1.ROWKEY IN (T2.ROWKEY)";
            assertTrue("Expected '" + defaultPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(defaultPlan));

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (rowkey, c1, c2) VALUES (?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                int c1 = i % 16;
                stmt.setString(1, "k" + i);
                stmt.setString(2, "X" + Integer.toHexString(c1) + c1);
                stmt.setString(3, "c");
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS " + tableName);

            // Use the optimal plan based on cost when stats become available.
            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            String optimizedPlan =
                    "CLIENT PARALLEL 626-WAY RANGE SCAN OVER " + tableName + " [1,'X0'] - [1,'X1']\n" +
                    "    SERVER FILTER BY FIRST KEY ONLY\n" +
                    "    SERVER SORTED BY [\"T1.:ROWKEY\"]\n" +
                    "CLIENT MERGE SORT\n" +
                    "    PARALLEL INNER-JOIN TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName + "\n" +
                    "            SERVER FILTER BY C1 LIKE 'X%'\n" +
                    "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [ROWKEY]\n" +
                    "    DYNAMIC SERVER FILTER BY \"T1.:ROWKEY\" IN (T2.ROWKEY)";
            assertTrue("Expected '" + optimizedPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(optimizedPlan));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testHintOverridesCost() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            String tableName = BaseTest.generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "rowkey INTEGER PRIMARY KEY,\n" +
                    "c1 VARCHAR,\n" +
                    "c2 VARCHAR)");
            conn.createStatement().execute("CREATE LOCAL INDEX " + tableName + "_idx ON " + tableName + " (c1)");

            String query = "SELECT rowkey, c1, c2 FROM " + tableName + " where rowkey between 1 and 10 ORDER BY c1";
            String hintedQuery = query.replaceFirst("SELECT",
                    "SELECT  /*+ INDEX(" + tableName + " " + tableName + "_idx) */");
            String dataPlan = "SERVER SORTED BY [C1]";
            String indexPlan = "SERVER FILTER BY FIRST KEY ONLY AND (\"ROWKEY\" >= 1 AND \"ROWKEY\" <= 10)";

            // Use the index table plan that opts out order-by when stats are not available.
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected '" + indexPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(indexPlan));

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " (rowkey, c1, c2) VALUES (?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                int c1 = i % 16;
                stmt.setInt(1, i);
                stmt.setString(2, "X" + Integer.toHexString(c1) + c1);
                stmt.setString(3, "c");
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS " + tableName);

            // Use the data table plan that has a lower cost when stats are available.
            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected '" + dataPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(dataPlan));

            // Use the index table plan as has been hinted.
            rs = conn.createStatement().executeQuery("explain " + hintedQuery);
            plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected '" + indexPlan + "' in the plan:\n" + plan + ".",
                    plan.contains(indexPlan));
        } finally {
            conn.close();
        }
    }
}
