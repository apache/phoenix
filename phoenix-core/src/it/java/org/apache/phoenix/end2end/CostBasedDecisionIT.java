package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class CostBasedDecisionIT extends ParallelStatsEnabledIT {

    @Test
    public void testBug4288() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().execute("CREATE TABLE test4288 (\n" + 
                    "rowkey VARCHAR PRIMARY KEY,\n" + 
                    "c1 VARCHAR,\n" + 
                    "c2 VARCHAR)");
            conn.createStatement().execute("CREATE LOCAL INDEX test4388_c1_idx ON test4288 (c1)");

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO test4288 (rowkey, c1, c2) VALUES (?, ?, ?)");
            for (int i = 0; i < 10000; i++) {
                int c1 = i % 16;
                stmt.setString(1, "k" + i);
                stmt.setString(2, "X" + Integer.toHexString(c1) + c1);
                stmt.setString(3, "c");
                stmt.execute();
            }

            conn.createStatement().execute("UPDATE STATISTICS test4288");

            String query = "SELECT rowkey, c1, c2 FROM test4288 where c1 LIKE 'X0%' ORDER BY rowkey";
            ResultSet rs = conn.createStatement().executeQuery("explain " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue("Expected 'RANGE SCAN' in the plan:\n" + plan + ".",
                    plan.contains("RANGE SCAN"));
        } finally {
            conn.close();
        }        
    }
}
