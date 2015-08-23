package org.apache.phoenix.expression;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class NullValueTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testComparisonExpressionWithNullOperands() throws Exception {
        String[] query = {"SELECT 'a' >= ''", 
                          "SELECT '' < 'a'", 
                          "SELECT '' = ''"};
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            for (String q : query) {
                ResultSet rs = conn.createStatement().executeQuery(q);
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertEquals(false, rs.getBoolean(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }       
    }
    
    @Test
    public void testAndOrExpressionWithNullOperands() throws Exception {
        String[] query = {"SELECT 'a' >= '' or '' < 'a'", 
                          "SELECT 'a' >= '' and '' < 'a'"};
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            for (String q : query) {
                ResultSet rs = conn.createStatement().executeQuery(q);
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertEquals(false, rs.getBoolean(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }       
    }

}
