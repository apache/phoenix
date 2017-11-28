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
import org.junit.AfterClass;
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
