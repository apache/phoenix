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

public class CostBasedDecisionIT extends BaseTest {

    @BeforeClass
    public static final void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        props.put(QueryServices.COST_BASED_OPTIMIZER_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        BaseTest.tearDownMiniClusterIfBeyondThreshold();
    }

    @Test
    public void testBug4288() throws Exception {
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

            rs = conn.createStatement().executeQuery("explain " + query);
            plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected 'RANGE SCAN' in the plan:\n" + plan + ".",
                    plan.contains("RANGE SCAN"));
        } finally {
            conn.close();
        }        
    }
}
