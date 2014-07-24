package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ClientManagedTimeTest.class)
public class InListIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testLeadingPKWithTrailingRVC() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE in_test "
                + "( col1 VARCHAR NOT NULL,"
                + "  col2 VARCHAR NOT NULL, "
                + "  id VARCHAR NOT NULL,"
                + "  CONSTRAINT pk PRIMARY KEY (col1, col2, id))");

        conn.createStatement().execute("upsert into in_test (col1, col2, id) values ('a', 'b', 'c')");
        conn.createStatement().execute("upsert into in_test (col1, col2, id) values ('a', 'b', 'd')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select id from in_test WHERE col1 = 'a' and ((col2, id) IN (('b', 'c'),('b', 'e')))");
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testLeadingPKWithTrailingRVC2() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE in_test ( user VARCHAR, tenant_id VARCHAR(5) NOT NULL,tenant_type_id VARCHAR(3) NOT NULL,  id INTEGER NOT NULL CONSTRAINT pk PRIMARY KEY (tenant_id, tenant_type_id, id))");

        conn.createStatement().execute("upsert into in_test (tenant_id, tenant_type_id, id, user) values ('a', 'a', 1, 'BonA')");
        conn.createStatement().execute("upsert into in_test (tenant_id, tenant_type_id, id, user) values ('a', 'a', 2, 'BonB')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select id from in_test WHERE tenant_id = 'a' and tenant_type_id = 'a' and ((id, user) IN ((1, 'BonA'),(1, 'BonB')))");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
    }
}