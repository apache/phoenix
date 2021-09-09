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

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class IndexUsageIT extends ParallelStatsDisabledIT {

    /**
     * Adds a row to the index data table
     * 
     * @param i row number
     */
    private void insertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setString(1, "varchar" + String.valueOf(i));
        stmt.setString(2, "char" + String.valueOf(i));
        stmt.setInt(3, i);
        stmt.setLong(4, i);
        stmt.setBigDecimal(5, new BigDecimal(i*0.5d));
        Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i - 1) * MILLIS_IN_DAY);
        stmt.setDate(6, date);
        stmt.setString(7, "a.varchar" + String.valueOf(i));
        stmt.setString(8, "a.char" + String.valueOf(i));
        stmt.setInt(9, i);
        stmt.setLong(10, i);
        stmt.setBigDecimal(11, new BigDecimal(i*0.5d));
        stmt.setDate(12, date);
        stmt.setString(13, "b.varchar" + String.valueOf(i));
        stmt.setString(14, "b.char" + String.valueOf(i));
        stmt.setInt(15, i);
        stmt.setLong(16, i);
        stmt.setBigDecimal(17, new BigDecimal(i*0.5d));
        stmt.setDate(18, date);
        stmt.executeUpdate();
    }

    private void createDataTable(Connection conn, String dataTableName, String tableProps) throws SQLException {
        String tableDDL = "create table " + dataTableName + TestUtil.TEST_TABLE_SCHEMA + tableProps;
        conn.createStatement().execute(tableDDL);
    }
    
    private void populateDataTable(Connection conn, String dataTable) throws SQLException {
        String upsert = "UPSERT INTO " + dataTable
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement stmt1 = conn.prepareStatement(upsert);
        // insert two rows
        insertRow(stmt1, 1);
        insertRow(stmt1, 2);
        conn.commit();
    }

    @Test
    public void testGroupByCountImmutableIndex() throws Exception {
        helpTestGroupByCount(false, false);
    }

    @Test
    public void testGroupByCountImmutableLocalIndex() throws Exception {
        helpTestGroupByCount(false, true);
    }

    @Test
    public void testGroupByCountMutableIndex() throws Exception {
        helpTestGroupByCount(true, false);
    }

    @Test
    public void testGroupByCountMutableLocalIndex() throws Exception {
        helpTestGroupByCount(true, true);
    }

    protected void helpTestGroupByCount(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, mutable ? ""
                : "IMMUTABLE_ROWS=true");
            populateDataTable(conn, fullDataTableName);
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX "
                + indexName + " ON " + fullDataTableName
                + " (int_col1+int_col2)";
            conn.createStatement().execute(ddl);

            String groupBySql = "SELECT (int_col1+int_col2), COUNT(*) FROM "
                + fullDataTableName + " GROUP BY (int_col1+int_col2)";

            ExplainPlan plan = conn.prepareStatement(groupBySql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY "
                    + "[TO_BIGINT(\"(A.INT_COL1 + B.INT_COL2)\")]",
                explainPlanAttributes.getServerAggregate());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullDataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals("INDEX_TEST." + indexName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }
            ResultSet rs = conn.createStatement().executeQuery(groupBySql);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectDistinctImmutableIndex() throws Exception {
        helpTestSelectDistinct(false, false);
    }

    @Test
    public void testSelectDistinctImmutableIndexLocal() throws Exception {
        helpTestSelectDistinct(false, true);
    }

    @Test
    public void testSelectDistinctMutableIndex() throws Exception {
        helpTestSelectDistinct(true, false);
    }

    @Test
    public void testSelectDistinctMutableLocalIndex() throws Exception {
        helpTestSelectDistinct(true, true);
    }

    protected void helpTestSelectDistinct(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, mutable ? "" : "IMMUTABLE_ROWS=true");
            populateDataTable(conn, fullDataTableName);
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX "
                + indexName + " ON " + fullDataTableName + " (int_col1+1)";
            conn.createStatement().execute(ddl);
            String sql = "SELECT distinct int_col1+1 FROM " + fullDataTableName
                + " where int_col1+1 > 0";

            ExplainPlan plan = conn.prepareStatement(sql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("SERVER DISTINCT PREFIX FILTER OVER [TO_BIGINT(\"(A.INT_COL1 + 1)\")]",
                explainPlanAttributes.getServerDistinctFilter());
            assertEquals("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [TO_BIGINT(\"(A.INT_COL1 + 1)\")]",
                explainPlanAttributes.getServerAggregate());
            if (localIndex) {
                assertEquals(fullDataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1,0] - [1,*]",
                    explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("INDEX_TEST." + indexName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [0] - [*]",
                    explainPlanAttributes.getKeyRanges());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testInClauseWithImmutableIndex() throws Exception {
        helpTestInClauseWithIndex(false, false);
    }

    @Test
    public void testInClauseWithImmutableLocalIndex() throws Exception {
        helpTestInClauseWithIndex(false, true);
    }

    @Test
    public void testInClauseWithMutableIndex() throws Exception {
        helpTestInClauseWithIndex(true, false);
    }

    @Test
    public void testInClauseWithMutableLocalIndex() throws Exception {
        helpTestInClauseWithIndex(true, false);
    }

    protected void helpTestInClauseWithIndex(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, mutable ? "" : "IMMUTABLE_ROWS=true");
            populateDataTable(conn, fullDataTableName);
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullDataTableName
                + " (int_col1+1)";

            conn.createStatement().execute(ddl);
            String sql = "SELECT int_col1+1 FROM " + fullDataTableName + " where int_col1+1 IN (2)";

            ExplainPlan plan = conn.prepareStatement(sql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            if (localIndex) {
                assertEquals(fullDataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1,2]", explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("INDEX_TEST." + indexName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [2]", explainPlanAttributes.getKeyRanges());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testOrderByWithImmutableIndex() throws Exception {
        helpTestSelectAliasAndOrderByWithIndex(false, false);
    }

    @Test
    public void testOrderByWithImmutableLocalIndex() throws Exception {
        helpTestSelectAliasAndOrderByWithIndex(false, true);
    }

    @Test
    public void testOrderByWithMutableIndex() throws Exception {
        helpTestSelectAliasAndOrderByWithIndex(true, false);
    }

    @Test
    public void testOrderByWithMutableLocalIndex() throws Exception {
        helpTestSelectAliasAndOrderByWithIndex(true, false);
    }

    protected void helpTestSelectAliasAndOrderByWithIndex(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, mutable ? "" : "IMMUTABLE_ROWS=true");
            populateDataTable(conn, fullDataTableName);
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX "
                + indexName + " ON " + fullDataTableName
                + " (int_col1+1)";

            conn.createStatement().execute(ddl);
            String sql = "SELECT int_col1+1 AS foo FROM " + fullDataTableName
                + " ORDER BY foo";

            ExplainPlan plan = conn.prepareStatement(sql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullDataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals("INDEX_TEST." + indexName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testImmutableIndexWithCaseSensitiveCols() throws Exception {
        helpTestIndexWithCaseSensitiveCols(false, false);
    }
    
    @Test
    public void testImmutableLocalIndexWithCaseSensitiveCols() throws Exception {
        helpTestIndexWithCaseSensitiveCols(false, true);
    }
    
    @Test
    public void testMutableIndexWithCaseSensitiveCols() throws Exception {
        helpTestIndexWithCaseSensitiveCols(true, false);
    }
    
    @Test
    public void testMutableLocalIndexWithCaseSensitiveCols() throws Exception {
        helpTestIndexWithCaseSensitiveCols(true, true);
    }
    
    protected void helpTestIndexWithCaseSensitiveCols(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableName
                + " (k VARCHAR NOT NULL PRIMARY KEY, \"cf1\".\"V1\" VARCHAR, \"CF2\".\"v2\" VARCHAR) "
                + (mutable ? "IMMUTABLE_ROWS=true" : ""));
            String query = "SELECT * FROM " + dataTableName;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX "
                + indexName + " ON " + dataTableName
                + " (\"cf1\".\"V1\" || '_' || \"CF2\".\"v2\") INCLUDE (\"V1\",\"v2\")";
            conn.createStatement().execute(ddl);
            query = "SELECT * FROM " + indexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setString(2, "y");
            stmt.setString(3, "2");
            stmt.execute();
            conn.commit();

            query = "SELECT (\"V1\" || '_' || \"v2\"), k, \"V1\", \"v2\"  FROM " + dataTableName + " WHERE (\"V1\" || '_' || \"v2\") = 'x_1'";
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            if (localIndex) {
                assertEquals(dataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1,'x_1']",
                    explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals(indexName,
                    explainPlanAttributes.getTableName());
                assertEquals(" ['x_1']", explainPlanAttributes.getKeyRanges());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x_1", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("x", rs.getString(3));
            assertEquals("1", rs.getString(4));
            //TODO figure out why this " " is needed
            assertEquals("x_1", rs.getString("\"('cf1'.'V1' || '_' || 'CF2'.'v2')\""));
            assertEquals("a", rs.getString("k"));
            assertEquals("x", rs.getString("V1"));
            assertEquals("1", rs.getString("v2"));
            assertFalse(rs.next());

            query = "SELECT \"V1\", \"V1\" as foo1, (\"V1\" || '_' || \"v2\") as foo, (\"V1\" || '_' || \"v2\") as \"Foo1\", (\"V1\" || '_' || \"v2\") FROM " + dataTableName + " ORDER BY foo";
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(dataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1]",
                    explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(indexName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("x", rs.getString("V1"));
            assertEquals("x", rs.getString(2));
            assertEquals("x", rs.getString("foo1"));
            assertEquals("x_1", rs.getString(3));
            assertEquals("x_1", rs.getString("Foo"));
            assertEquals("x_1", rs.getString(4));
            assertEquals("x_1", rs.getString("Foo1"));
            assertEquals("x_1", rs.getString(5));
            assertEquals("x_1", rs.getString("\"('cf1'.'V1' || '_' || 'CF2'.'v2')\""));
            assertTrue(rs.next());
            assertEquals("y", rs.getString(1));
            assertEquals("y", rs.getString("V1"));
            assertEquals("y", rs.getString(2));
            assertEquals("y", rs.getString("foo1"));
            assertEquals("y_2", rs.getString(3));
            assertEquals("y_2", rs.getString("Foo"));
            assertEquals("y_2", rs.getString(4));
            assertEquals("y_2", rs.getString("Foo1"));
            assertEquals("y_2", rs.getString(5));
            assertEquals("y_2", rs.getString("\"('cf1'.'V1' || '_' || 'CF2'.'v2')\""));
            assertFalse(rs.next());
        }
    }    
    
    @Test
    public void testSelectColOnlyInDataTableImmutableIndex() throws Exception {
        helpTestSelectColOnlyInDataTable(false, false);
    }

    @Test
    public void testSelectColOnlyInDataTableImmutableLocalIndex() throws Exception {
        helpTestSelectColOnlyInDataTable(false, true);
    }

    @Test
    public void testSelectColOnlyInDataTableMutableIndex() throws Exception {
        helpTestSelectColOnlyInDataTable(true, false);
    }

    @Test
    public void testSelectColOnlyInDataTableMutableLocalIndex() throws Exception {
        helpTestSelectColOnlyInDataTable(true, true);
    }

    protected void helpTestSelectColOnlyInDataTable(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, mutable ? "" : "IMMUTABLE_ROWS=true");
            populateDataTable(conn, fullDataTableName);
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullDataTableName
                    + " (int_col1+1)";

            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(false);
            conn.createStatement().execute(ddl);
            String sql = "SELECT int_col1+1, int_col2 FROM " + fullDataTableName + " WHERE int_col1+1=2";

            ExplainPlan plan = conn.prepareStatement(sql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullDataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1,2]",
                    explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
                assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                    explainPlanAttributes.getServerWhereFilter());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullDataTableName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
                assertEquals("SERVER FILTER BY (A.INT_COL1 + 1) = 2",
                    explainPlanAttributes.getServerWhereFilter());
            }

            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
        
    @Test
    public void testUpdatableViewWithIndex() throws Exception {
        helpTestUpdatableViewIndex(false);
    }
    
    @Test
    public void testUpdatableViewWithLocalIndex() throws Exception {
        helpTestUpdatableViewIndex(true);
    }
       
    private void helpTestUpdatableViewIndex(boolean local) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName1 = generateUniqueName();
            String viewName = generateUniqueName();
            String indexName2 = generateUniqueName();
            String ddl = "CREATE TABLE " + dataTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, k3 DECIMAL, s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2, k3))";
            conn.createStatement().execute(ddl);
            ddl = "CREATE VIEW " + viewName + " AS SELECT * FROM " + dataTableName + " WHERE k1 = 1";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,s1,s2,k3) VALUES(120,'foo0','bar0',50.0)");
            conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,s1,s2,k3) VALUES(121,'foo1','bar1',51.0)");
            conn.commit();

            ResultSet rs;
            conn.createStatement().execute("CREATE " + (local ? "LOCAL" : "") + " INDEX " + indexName1 + " on " + viewName + "(k1+k2+k3) include (s1, s2)");
            conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,s1,s2,k3) VALUES(120,'foo2','bar2',50.0)");
            conn.commit();

            String query = "SELECT k1, k2, k3, s1, s2 FROM " + viewName + " WHERE k1+k2+k3 = 173.0";
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            if (local) {
                assertEquals(dataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1,173]",
                    explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("_IDX_" + dataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [" + Short.MIN_VALUE + ",173]",
                    explainPlanAttributes.getKeyRanges());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(121, rs.getInt(2));
            assertTrue(BigDecimal.valueOf(51.0).compareTo(rs.getBigDecimal(3)) == 0);
            assertEquals("foo1", rs.getString(4));
            assertEquals("bar1", rs.getString(5));
            assertFalse(rs.next());

            conn.createStatement().execute("CREATE " + (local ? "LOCAL" : "") + " INDEX " + indexName2 + " on " + viewName + "(s1||'_'||s2)");

            query = "SELECT k1, k2, s1||'_'||s2 FROM " + viewName + " WHERE (s1||'_'||s2)='foo2_bar2'";
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            if (local) {
                assertEquals(dataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [" + (2) + ",'foo2_bar2']",
                    explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("_IDX_" + dataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [" + (Short.MIN_VALUE + 1) + ",'foo2_bar2']",
                    explainPlanAttributes.getKeyRanges());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(120, rs.getInt(2));
            assertEquals("foo2_bar2", rs.getString(3));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testViewUsesMutableTableIndex() throws Exception {
        helpTestViewUsesTableIndex(false);
    }
    
    @Test
    public void testViewUsesImmutableTableIndex() throws Exception {
        helpTestViewUsesTableIndex(true);
    }
    
    private void helpTestViewUsesTableIndex(boolean immutable) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try 
        {
            String dataTableName = generateUniqueName();
            String indexName1 = generateUniqueName();
            String viewName = generateUniqueName();
            String indexName2 = generateUniqueName();
        	ResultSet rs;
	        String ddl = "CREATE TABLE " + dataTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, s1 VARCHAR, s2 VARCHAR, s3 VARCHAR, s4 VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2)) " + (immutable ? "IMMUTABLE_ROWS = true" : "");
	        conn.createStatement().execute(ddl);
	        conn.createStatement().execute("CREATE INDEX " + indexName1 + " ON " + dataTableName + "(k2, s2, s3, s1)");
	        conn.createStatement().execute("CREATE INDEX " + indexName2 + " ON " + dataTableName + "(k2, s2||'_'||s3, s1, s4)");
	        
	        ddl = "CREATE VIEW " + viewName + " AS SELECT * FROM " + dataTableName + " WHERE s1 = 'foo'";
	        conn.createStatement().execute(ddl);
	        conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES(1,1,'foo','abc','cab')");
	        conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES(2,2,'bar','xyz','zyx')");
	        conn.commit();
	        
	        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + viewName);
	        assertTrue(rs.next());
	        assertEquals(1, rs.getLong(1));
	        assertFalse(rs.next());
	        
	        //i2 should be used since it contains s3||'_'||s4 i
	        String query = "SELECT s2||'_'||s3 FROM " + viewName + " WHERE k2=1 AND (s2||'_'||s3)='abc_cab'";

            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(indexName2,
                explainPlanAttributes.getTableName());
            assertEquals(" [1,'abc_cab','foo']",
                explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());

	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("abc_cab", rs.getString(1));
	        assertFalse(rs.next());
	        
	        conn.createStatement().execute("ALTER VIEW " + viewName + " DROP COLUMN s4");
	        //i2 cannot be used since s4 has been dropped from the view, so i1 will be used 
	        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String queryPlan = QueryUtil.getExplainPlan(rs);
	        assertEquals(
	                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + indexName1 + " [1]\n" +
	                "    SERVER FILTER BY FIRST KEY ONLY AND ((\"S2\" || '_' || \"S3\") = 'abc_cab' AND \"S1\" = 'foo')", queryPlan);
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("abc_cab", rs.getString(1));
	        assertFalse(rs.next());    
        }
        finally {
        	conn.close();
        }
    }
    
	@Test
	public void testExpressionThrowsException() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();
		try {
			String ddl = "CREATE TABLE " + dataTableName + " (k1 INTEGER PRIMARY KEY, k2 INTEGER)";
			conn.createStatement().execute(ddl);
			ddl = "CREATE INDEX " + indexName + " on " + dataTableName + "(k1/k2)";
			conn.createStatement().execute(ddl);
			// upsert should succeed
			conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES(1,1)");
			conn.commit();
			// divide by zero should fail
			conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES(1,0)");
			conn.commit();
			fail();
		} catch (CommitException e) {
		} finally {
			conn.close();
		}
	}
	
	@Test
	public void testImmutableCaseSensitiveFunctionIndex() throws Exception {
		helpTestCaseSensitiveFunctionIndex(false, false);
	}

	@Test
	public void testImmutableLocalCaseSensitiveFunctionIndex() throws Exception {
		helpTestCaseSensitiveFunctionIndex(false, true);
	}

	@Test
	public void testMutableCaseSensitiveFunctionIndex() throws Exception {
		helpTestCaseSensitiveFunctionIndex(true, false);
	}

	@Test
	public void testMutableLocalCaseSensitiveFunctionIndex() throws Exception {
		helpTestCaseSensitiveFunctionIndex(true, true);
	}

	protected void helpTestCaseSensitiveFunctionIndex(boolean mutable,
			boolean localIndex) throws Exception {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            conn.createStatement().execute(
                "CREATE TABLE " + dataTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) "
                    + (!mutable ? "IMMUTABLE_ROWS=true" : ""));
            String query = "SELECT * FROM  " + dataTableName;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "")
                + " INDEX " + indexName + " ON " + dataTableName + " (REGEXP_SUBSTR(v,'id:\\\\w+'))";
            conn.createStatement().execute(ddl);
            query = "SELECT * FROM " + indexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?)");
            stmt.setString(1, "k1");
            stmt.setString(2, "{id:id1}");
            stmt.execute();
            stmt.setString(1, "k2");
            stmt.setString(2, "{id:id2}");
            stmt.execute();
            conn.commit();

            query = "SELECT k FROM " + dataTableName + " WHERE REGEXP_SUBSTR(v,'id:\\\\w+') = 'id:id1'";

            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            if (localIndex) {
                assertEquals(dataTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1,'id:id1']",
                    explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals(indexName,
                    explainPlanAttributes.getTableName());
                assertEquals(" ['id:id1']",
                    explainPlanAttributes.getKeyRanges());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("k1", rs.getString(1));
            assertFalse(rs.next());
        }
	}

	@Test
    public void testImmutableTableGlobalIndexExpressionWithJoin() throws Exception {
        helpTestIndexExpressionWithJoin(false, false);
    }
	
	@Test
    public void testImmutableTableLocalIndexExpressionWithJoin() throws Exception {
        helpTestIndexExpressionWithJoin(false, true);
    }
	
	@Test
    public void testMutableTableGlobalIndexExpressionWithJoin() throws Exception {
        helpTestIndexExpressionWithJoin(true, false);
    }
	
	@Test
    public void testMutableTableLocalIndexExpressionWithJoin() throws Exception {
	    helpTestIndexExpressionWithJoin(true, true);
    }

    public void helpTestIndexExpressionWithJoin(boolean mutable,
            boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String nameSuffix = "T" + (mutable ? "MUTABLE" : "_IMMUTABLE") + (localIndex ? "_LOCAL" : "_GLOBAL");
        String tableName = "T" + nameSuffix;
        String indexName = "IDX" + nameSuffix;
        try {
            conn.createStatement().execute(
                        "CREATE TABLE "
                                + tableName
                                + "( c_customer_sk varchar primary key, c_first_name varchar, c_last_name varchar )"
                                + (!mutable ? "IMMUTABLE_ROWS=true" : ""));
            String query = "SELECT * FROM " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            
            conn.createStatement().execute(
                "CREATE " + (localIndex ? "LOCAL" : "")
                + " INDEX " + indexName + " ON " + tableName + " (c_customer_sk || c_first_name asc)");
            query = "SELECT * FROM " + indexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
            stmt.setString(1, "1");
            stmt.setString(2, "David");
            stmt.setString(3, "Smith");
            stmt.execute();
            conn.commit();
            
            query = "select c.c_customer_sk from  " + tableName + " c "
                    + "left outer join " + tableName + " c2 on c.c_customer_sk = c2.c_customer_sk "
                    + "where c.c_customer_sk || c.c_first_name = '1David'";
            rs = conn.createStatement().executeQuery("EXPLAIN "+query);
            String explainPlan = QueryUtil.getExplainPlan(rs);
            if (localIndex) {
            	assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1,'1David']\n" + 
                        "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                        "CLIENT MERGE SORT\n" +
                        "    PARALLEL LEFT-JOIN TABLE 0 (SKIP MERGE)\n" +
                        "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1]\n" + 
                        "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                        "        CLIENT MERGE SORT", explainPlan);
            }
            else {
            	assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + indexName + " ['1David']\n" + 
                        "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                        "    PARALLEL LEFT-JOIN TABLE 0 (SKIP MERGE)\n" +
                        "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + indexName + "\n" + 
                        "            SERVER FILTER BY FIRST KEY ONLY", explainPlan);
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
	}

    @Test
    public void testIndexNotFoundForWrongIndexNameRebuild() throws Exception{
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String dataTableName = generateUniqueName();
        String wrongIndexName = generateUniqueName();

        try {
            conn.createStatement().execute("CREATE TABLE " + dataTableName +
                            " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR)");

            conn.createStatement().execute(
                    "ALTER INDEX " + wrongIndexName + " ON " + dataTableName + " rebuild");

        }catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.INDEX_UNDEFINED.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexNotFoundForDropWongIndexName() throws Exception{
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String dataTableName = generateUniqueName();
        String wrongIndexName = generateUniqueName();

        try {
            conn.createStatement().execute("CREATE TABLE " + dataTableName +
                    " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR)");
            conn.createStatement().execute("DROP INDEX " + wrongIndexName + " ON " +
                    dataTableName);
        }catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.INDEX_UNDEFINED.getErrorCode());
        } finally {
            conn.close();
        }
    }
}
