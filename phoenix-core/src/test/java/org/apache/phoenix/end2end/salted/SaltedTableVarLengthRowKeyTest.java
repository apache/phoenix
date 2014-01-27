package org.apache.phoenix.end2end.salted;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;

import org.apache.phoenix.end2end.BaseClientManagedTimeTest;


public class SaltedTableVarLengthRowKeyTest extends BaseClientManagedTimeTest {

    private static void initTableValues() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            createTestTable(getUrl(), "create table testVarcharKey " +
                    " (key_string varchar not null primary key, kv integer) SALT_BUCKETS=4\n");
            String query = "UPSERT INTO testVarcharKey VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "a");
            stmt.setInt(2, 1);
            stmt.execute();
            
            stmt.setString(1, "ab");
            stmt.setInt(2, 2);
            stmt.execute();
            
            stmt.setString(1, "abc");
            stmt.setInt(2, 3);
            stmt.execute();
            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValueWithPointKeyQuery() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            initTableValues();
            String query;
            PreparedStatement stmt;
            ResultSet rs;
            
            query = "SELECT * FROM testVarcharKey where key_string = 'abc'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
