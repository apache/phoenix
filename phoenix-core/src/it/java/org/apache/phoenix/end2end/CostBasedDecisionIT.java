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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class CostBasedDecisionIT extends BaseTest {
    private final String testTable500;
    private final String testTable990;
    private final String testTable1000;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        props.put(QueryServices.COST_BASED_OPTIMIZER_ENABLED, Boolean.toString(true));
        props.put(QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB, Long.toString(150000));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    public CostBasedDecisionIT() throws Exception {
        testTable500 = initTestTableValues(500);
        testTable990 = initTestTableValues(990);
        testTable1000 = initTestTableValues(1000);
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
            verifyQueryPlan(query, "FULL SCAN");

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
            verifyQueryPlan(query, "RANGE SCAN");
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

            String query = "SELECT c1, max(rowkey), max(c2) FROM " + tableName + " where rowkey <= 'z' GROUP BY c1";
            // Use the index table plan that opts out order-by when stats are not available.
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(tableName, explainPlanAttributes.getTableName());
            assertEquals(" [*] - ['z']", explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER AGGREGATE INTO DISTINCT ROWS BY [C1]",
                explainPlanAttributes.getServerAggregate());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());

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
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(tableName, explainPlanAttributes.getTableName());
            assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY AND \"ROWKEY\" <= 'z'",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"C1\"]",
                explainPlanAttributes.getServerAggregate());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());
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
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(tableName, explainPlanAttributes.getTableName());
            assertEquals(" [2,*] - [2,9,000]",
                explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER FILTER BY ((\"C1\" >= 10 AND \"C1\" <= 20) AND TO_INTEGER(\"C3\") < 5000)",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());

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
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(tableName, explainPlanAttributes.getTableName());
            assertEquals(" [1,10] - [1,20]",
                explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER FILTER BY (\"C2\" < 9000 AND \"C3\" < 5000)",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());
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
            verifyQueryPlan(query,
                    "UPSERT SELECT\n" +
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [2,*] - [2,9,000]\n" +
                            "    SERVER FILTER BY ((\"C1\" >= 10 AND \"C1\" <= 20) AND TO_INTEGER(\"C3\") < 5000)\n" +
                            "CLIENT MERGE SORT");

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
            verifyQueryPlan(query,
                    "UPSERT SELECT\n" +
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1,10] - [1,20]\n" +
                            "    SERVER FILTER BY (\"C2\" < 9000 AND \"C3\" < 5000)\n" +
                            "CLIENT MERGE SORT");
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
            verifyQueryPlan(query,
                    "DELETE ROWS CLIENT SELECT\n" +
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [2,*] - [2,9,000]\n" +
                            "    SERVER FILTER BY ((\"C1\" >= 10 AND \"C1\" <= 20) AND TO_INTEGER(\"C3\") < 5000)\n" +
                            "CLIENT MERGE SORT");

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
            verifyQueryPlan(query,
                    "DELETE ROWS CLIENT SELECT\n" +
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1,10] - [1,20]\n" +
                            "    SERVER FILTER BY (\"C2\" < 9000 AND \"C3\" < 5000)\n" +
                            "CLIENT MERGE SORT");
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

            String query = "SELECT c1, max(rowkey), max(c2) FROM " + tableName + " where rowkey <= 'z' GROUP BY c1 "
                    + "UNION ALL SELECT c1, max(rowkey), max(c2) FROM " + tableName + " where rowkey >= 'a' GROUP BY c1";
            // Use the default plan when stats are not available.
            verifyQueryPlan(query,
                    "UNION ALL OVER 2 QUERIES\n" +
                    "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [*] - ['z']\n" +
                    "        SERVER AGGREGATE INTO DISTINCT ROWS BY [C1]\n" +
                    "    CLIENT MERGE SORT\n" +
                    "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " ['a'] - [*]\n" +
                    "        SERVER AGGREGATE INTO DISTINCT ROWS BY [C1]\n" +
                    "    CLIENT MERGE SORT");

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
            verifyQueryPlan(query,
                    "UNION ALL OVER 2 QUERIES\n" +
                    "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1]\n" +
                    "        SERVER MERGE [0.C2]\n" +
                    "        SERVER FILTER BY FIRST KEY ONLY AND \"ROWKEY\" <= 'z'\n" +
                    "        SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"C1\"]\n" +
                    "    CLIENT MERGE SORT\n" +
                    "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1]\n" +
                    "        SERVER MERGE [0.C2]\n" +
                    "        SERVER FILTER BY FIRST KEY ONLY AND \"ROWKEY\" >= 'a'\n" +
                    "        SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"C1\"]\n" +
                    "    CLIENT MERGE SORT");
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

            String query = "SELECT t1.rowkey, t1.c1, t1.c2, t2.c1, mc2 FROM " + tableName + " t1 "
                    + "JOIN (SELECT c1, max(rowkey) mrk, max(c2) mc2 FROM " + tableName + " where rowkey <= 'z' GROUP BY c1) t2 "
                    + "ON t1.rowkey = t2.mrk WHERE t1.c1 LIKE 'X0%' ORDER BY t1.rowkey";
            // Use the default plan when stats are not available.
            verifyQueryPlan(query,
                    "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + tableName + "\n" +
                    "    SERVER FILTER BY C1 LIKE 'X0%'\n" +
                    "    PARALLEL INNER-JOIN TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [*] - ['z']\n" +
                    "            SERVER AGGREGATE INTO DISTINCT ROWS BY [C1]\n" +
                    "        CLIENT MERGE SORT\n" +
                    "    DYNAMIC SERVER FILTER BY T1.ROWKEY IN (T2.MRK)");

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
            verifyQueryPlan(query,
                    "CLIENT PARALLEL 626-WAY RANGE SCAN OVER " + tableName + " [1,'X0'] - [1,'X1']\n" +
                    "    SERVER MERGE [0.C2]\n" +
                    "    SERVER FILTER BY FIRST KEY ONLY\n" +
                    "    SERVER SORTED BY [\"T1.:ROWKEY\"]\n" +
                    "CLIENT MERGE SORT\n" +
                    "    PARALLEL INNER-JOIN TABLE 0\n" +
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1]\n" +
                    "            SERVER MERGE [0.C2]\n" +
                    "            SERVER FILTER BY FIRST KEY ONLY AND \"ROWKEY\" <= 'z'\n" +
                    "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"C1\"]\n" +
                    "        CLIENT MERGE SORT\n" +
                    "    DYNAMIC SERVER FILTER BY \"T1.:ROWKEY\" IN (T2.MRK)");
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
            String dataPlan = "[C1]";
            String indexPlan = "SERVER FILTER BY FIRST KEY ONLY AND (\"ROWKEY\" >= 1 AND \"ROWKEY\" <= 10)";

            // Use the index table plan that opts out order-by when stats are not available.
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals(indexPlan,
                explainPlanAttributes.getServerWhereFilter());

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
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals(dataPlan,
                explainPlanAttributes.getServerSortedBy());

            // Use the index table plan as has been hinted.
            plan = conn.prepareStatement(hintedQuery)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals(indexPlan,
                explainPlanAttributes.getServerWhereFilter());
        } finally {
            conn.close();
        }
    }

    /** Sort-merge-join w/ both children ordered wins over hash-join. */
    @Test
    public void testJoinStrategy() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable500 + " t1 JOIN " + testTable1000 + " t2\n" +
                "ON t1.ID = t2.ID";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            ExplainPlan plan = conn.prepareStatement(q)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("SORT-MERGE-JOIN (INNER)",
                explainPlanAttributes.getAbstractExplainPlan());
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(testTable500, explainPlanAttributes.getTableName());
            ExplainPlanAttributes rhsTable =
                explainPlanAttributes.getRhsJoinQueryExplainPlan();
            assertEquals("PARALLEL 1-WAY",
                rhsTable.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ", rhsTable.getExplainScanType());
            assertEquals(testTable1000, rhsTable.getTableName());
        }
    }

    /** Sort-merge-join w/ both children ordered wins over hash-join in an un-grouped aggregate query. */
    @Test
    public void testJoinStrategy2() throws Exception {
        String q = "SELECT count(*)\n" +
                "FROM " + testTable500 + " t1 JOIN " + testTable1000 + " t2\n" +
                "ON t1.ID = t2.ID\n" +
                "WHERE t1.COL1 < 200";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            ExplainPlan plan = conn.prepareStatement(q)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("SORT-MERGE-JOIN (INNER)",
                explainPlanAttributes.getAbstractExplainPlan());
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals("SERVER FILTER BY COL1 < 200",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals(testTable500, explainPlanAttributes.getTableName());
            assertEquals("CLIENT AGGREGATE INTO SINGLE ROW",
                explainPlanAttributes.getClientAggregate());
            ExplainPlanAttributes rhsTable =
                explainPlanAttributes.getRhsJoinQueryExplainPlan();
            assertEquals("PARALLEL 1-WAY",
                rhsTable.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ", rhsTable.getExplainScanType());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                rhsTable.getServerWhereFilter());
            assertEquals(testTable1000, rhsTable.getTableName());
        }
    }

    /** Hash-join w/ PK/FK optimization wins over sort-merge-join w/ larger side ordered. */
    @Test
    public void testJoinStrategy3() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable500 + " t1 JOIN " + testTable1000 + " t2\n" +
                "ON t1.COL1 = t2.ID\n" +
                "WHERE t1.ID > 200";
        String expected =
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable500 + " [201] - [*]\n" +
                "    DYNAMIC SERVER FILTER BY T2.ID IN (T1.COL1)";
        verifyQueryPlan(q, expected);
    }

    /** Hash-join w/ PK/FK optimization wins over hash-join w/o PK/FK optimization when two sides are close in size. */
    @Test
    public void testJoinStrategy4() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable990 + " t1 JOIN " + testTable1000 + " t2\n" +
                "ON t1.ID = t2.COL1";
        String expected =
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable990 + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "    DYNAMIC SERVER FILTER BY T1.ID IN (T2.COL1)";
        verifyQueryPlan(q, expected);
    }

    /** Hash-join wins over sort-merge-join w/ smaller side ordered. */
    @Test
    public void testJoinStrategy5() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable500 + " t1 JOIN " + testTable1000 + " t2\n" +
                "ON t1.ID = t2.COL1\n" +
                "WHERE t1.ID > 200";
        String expected =
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable500 + " [201] - [*]";
        verifyQueryPlan(q, expected);
    }

    /** Hash-join wins over sort-merge-join w/o any side ordered. */
    @Test
    public void testJoinStrategy6() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable500 + " t1 JOIN " + testTable1000 + " t2\n" +
                "ON t1.COL1 = t2.COL1\n" +
                "WHERE t1.ID > 200";
        String expected =
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable500 + " [201] - [*]";
        verifyQueryPlan(q, expected);
    }

    /**
     * Hash-join wins over sort-merge-join w/ both sides ordered in an order-by query.
     * This is because order-by can only be done on the client side after sort-merge-join
     * and order-by w/o limit on the client side is very expensive.
     */
    @Test
    public void testJoinStrategy7() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable500 + " t1 JOIN " + testTable1000 + " t2\n" +
                "ON t1.ID = t2.ID\n" +
                "ORDER BY t1.COL1";
        String expected =
                "CLIENT PARALLEL 1001-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "    SERVER SORTED BY [T1.COL1]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable500 + "\n" +
                "    DYNAMIC SERVER FILTER BY T2.ID IN (T1.ID)";
        verifyQueryPlan(q, expected);
    }

    /**
     * Sort-merge-join w/ both sides ordered wins over hash-join in an order-by limit query.
     * This is because order-by can only be done on the client side after sort-merge-join
     * but order-by w/ limit on the client side is less expensive.
     */
    @Test
    public void testJoinStrategy8() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable500 + " t1 JOIN " + testTable1000 + " t2\n" +
                "ON t1.ID = t2.ID\n" +
                "ORDER BY t1.COL1 LIMIT 5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            ExplainPlan plan = conn.prepareStatement(q)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("SORT-MERGE-JOIN (INNER)",
                explainPlanAttributes.getAbstractExplainPlan());
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(testTable500, explainPlanAttributes.getTableName());
            assertEquals("[T1.COL1]",
                explainPlanAttributes.getClientSortedBy());
            assertEquals(new Integer(5),
                explainPlanAttributes.getClientRowLimit());
            ExplainPlanAttributes rhsTable =
                explainPlanAttributes.getRhsJoinQueryExplainPlan();
            assertEquals("PARALLEL 1-WAY",
                rhsTable.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ", rhsTable.getExplainScanType());
            assertEquals(testTable1000, rhsTable.getTableName());
        }
    }

    /**
     * Multi-table join: sort-merge-join chosen since all join keys are PK.
     */
    @Test
    public void testJoinStrategy9() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable1000 + " t1 LEFT JOIN " + testTable500 + " t2\n" +
                "ON t1.ID = t2.ID AND t2.ID > 200\n" +
                "LEFT JOIN " + testTable990 + " t3\n" +
                "ON t1.ID = t3.ID AND t3.ID < 100";
        String expected =
                "SORT-MERGE-JOIN (LEFT) TABLES\n" +
                "    SORT-MERGE-JOIN (LEFT) TABLES\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "    AND\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable500 + " [201] - [*]\n" +
                "AND\n" +
                "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable990 + " [*] - [100]";
        verifyQueryPlan(q, expected);
    }

    /**
     * Multi-table join: a mix of join strategies chosen based on cost.
     */
    @Test
    public void testJoinStrategy10() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable1000 + " t1 JOIN " + testTable500 + " t2\n" +
                "ON t1.ID = t2.COL1 AND t2.ID > 200\n" +
                "JOIN " + testTable990 + " t3\n" +
                "ON t1.ID = t3.ID AND t3.ID < 100";
        String expected =
                "SORT-MERGE-JOIN (INNER) TABLES\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "        PARALLEL INNER-JOIN TABLE 0\n" +
                "            CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable500 + " [201] - [*]\n" +
                "        DYNAMIC SERVER FILTER BY T1.ID IN (T2.COL1)\n" +
                "AND\n" +
                "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable990 + " [*] - [100]";
        verifyQueryPlan(q, expected);
    }

    /**
     * Multi-table join: hash-join two tables in parallel since two RHS tables are both small
     * and can fit in memory at the same time.
     */
    @Test
    public void testJoinStrategy11() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable1000 + " t1 JOIN " + testTable500 + " t2\n" +
                "ON t1.COL2 = t2.COL1 AND t2.ID > 200\n" +
                "JOIN " + testTable990 + " t3\n" +
                "ON t1.COL1 = t3.COL2 AND t3.ID < 100";
        String expected =
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable500 + " [201] - [*]\n" +
                "    PARALLEL INNER-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + testTable990 + " [*] - [100]";
        verifyQueryPlan(q, expected);
    }

    /**
     * Multi-table join: similar to {@link this#testJoinStrategy11()}, but the two RHS
     * tables cannot fit in memory at the same time, and thus a mix of join strategies
     * is chosen based on cost.
     */
    @Test
    public void testJoinStrategy12() throws Exception {
        String q = "SELECT *\n" +
                "FROM " + testTable1000 + " t1 JOIN " + testTable990 + " t2\n" +
                "ON t1.COL2 = t2.COL1\n" +
                "JOIN " + testTable990 + " t3\n" +
                "ON t1.COL1 = t3.COL2";
        String expected =
                "SORT-MERGE-JOIN (INNER) TABLES\n" +
                "    CLIENT PARALLEL 1001-WAY FULL SCAN OVER " + testTable1000 + "\n" +
                "        SERVER SORTED BY [T1.COL1]\n" +
                "    CLIENT MERGE SORT\n" +
                "        PARALLEL INNER-JOIN TABLE 0\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + testTable990 + "\n" +
                "AND\n" +
                "    CLIENT PARALLEL 991-WAY FULL SCAN OVER " + testTable990 + "\n" +
                "        SERVER SORTED BY [T3.COL2]\n" +
                "    CLIENT MERGE SORT";
        verifyQueryPlan(q, expected);
    }

    private static void verifyQueryPlan(String query, String expected) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("explain " + query);
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("Expected '" + expected + "' in the plan:\n" + plan + ".",
                plan.contains(expected));
    }

    private static String initTestTableValues(int rows) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (\n" +
                    "ID INTEGER NOT NULL PRIMARY KEY,\n" +
                    "COL1 INTEGER," +
                    "COL2 INTEGER)");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + tableName + " VALUES(?, ?, ?)");
            for (int i = 0; i < rows; i++) {
                stmt.setInt(1, i + 1);
                stmt.setInt(2, rows - i);
                stmt.setInt(3, rows + i);
                stmt.execute();
            }
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + tableName);
            return tableName;
        }
    }
}
