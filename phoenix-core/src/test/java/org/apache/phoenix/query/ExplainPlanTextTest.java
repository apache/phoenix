package org.apache.phoenix.query;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.query.QueryServices.AUTO_COMMIT_ATTRIB;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

public class ExplainPlanTextTest extends BaseConnectionlessQueryTest{

    String defaultDeleteStatement = "DELETE FROM " + ATABLE_NAME + " WHERE entity_id='abc'";

    @Test
    public void explainDeleteClientTest() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        List<String> plan = getExplain(defaultDeleteStatement, props);
        assertEquals("DELETE ROWS CLIENT SELECT", plan.get(0));
    }

    @Test
    public void explainDeleteServerTest() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(AUTO_COMMIT_ATTRIB,"true"); //need autocommit for server today
        List<String> plan = getExplain(defaultDeleteStatement, props);
        assertEquals("DELETE ROWS SERVER SELECT", plan.get(0));
    }

    private List<String> getExplain(String query, Properties props) throws SQLException {
        List<String> explainPlan = new ArrayList<>();
        try(Connection conn = DriverManager.getConnection(getUrl(), props);
            PreparedStatement statement = conn.prepareStatement("EXPLAIN " + query);
            ResultSet rs = statement.executeQuery()) {
            while(rs.next()) {
                String plan = rs.getString(1);
                explainPlan.add(plan);
            }
        }
        return explainPlan;
    }
}

