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

import static org.apache.phoenix.util.TestUtil.GROUPBYTEST_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class GroupByCaseIT extends BaseClientManagedTimeIT {

    private static String GROUPBY1 = "select " +
            "case when uri LIKE 'Report%' then 'Reports' else 'Other' END category" +
            ", avg(appcpu) from " + GROUPBYTEST_NAME +
            " group by category";

    private static String GROUPBY2 = "select " +
            "case uri when 'Report%' then 'Reports' else 'Other' END category" +
            ", avg(appcpu) from " + GROUPBYTEST_NAME +
            " group by appcpu, category";

    private static String GROUPBY3 = "select " +
            "case uri when 'Report%' then 'Reports' else 'Other' END category" +
            ", avg(appcpu) from " + GROUPBYTEST_NAME +
            " group by avg(appcpu), category";
    
    private int id;

    private long createTable() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(), GROUPBYTEST_NAME, null, ts-2);
        return ts;
    }

    private void loadData(long ts) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        insertRow(conn, "Report1", 10);
        insertRow(conn, "Report2", 10);
        insertRow(conn, "Report3", 30);
        insertRow(conn, "Report4", 30);
        insertRow(conn, "SOQL1", 10);
        insertRow(conn, "SOQL2", 10);
        insertRow(conn, "SOQL3", 30);
        insertRow(conn, "SOQL4", 30);
        conn.commit();
        conn.close();
    }

    private void insertRow(Connection conn, String uri, int appcpu) throws SQLException {
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO " + GROUPBYTEST_NAME + "(id, uri, appcpu) values (?,?,?)");
        statement.setString(1, "id" + id);
        statement.setString(2, uri);
        statement.setInt(3, appcpu);
        statement.executeUpdate();
        id++;
    }

    /*
    @Test
    public void testGroupByCaseWithIndex() throws Exception {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        GroupByCaseIT gbt = new GroupByCaseIT();
        long ts = gbt.createTable();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("ALTER TABLE " + GROUPBYTEST_NAME + " SET IMMUTABLE_ROWS=true");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX idx ON " + GROUPBYTEST_NAME + "(uri)");
        gbt.loadData(ts);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        gbt.executeQuery(conn,GROUPBY1);
        gbt.executeQuery(conn,GROUPBY2);
        // TODO: validate query results
        try {
            gbt.executeQuery(conn,GROUPBY3);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Aggregate expressions may not be used in GROUP BY"));
        }
        conn.close();
    }
    */

    @Test
    public void testScanUri() throws Exception {
        GroupByCaseIT gbt = new GroupByCaseIT();
        long ts = gbt.createTable();
        gbt.loadData(ts);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select uri from " + GROUPBYTEST_NAME);
        assertTrue(rs.next());
        assertEquals("Report1", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report2", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report3", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Report4", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL1", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL2", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL3", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("SOQL4", rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testCount() throws Exception {
        GroupByCaseIT gbt = new GroupByCaseIT();
        long ts = gbt.createTable();
        gbt.loadData(ts);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(1) from " + GROUPBYTEST_NAME);
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="RV_RETURN_VALUE_IGNORED",
            justification="Test code.")
    private void executeQuery(Connection conn, String query) throws SQLException {
        PreparedStatement st = conn.prepareStatement(query);
        st.executeQuery();
    }
    
    @Test
    public void testGroupByCase() throws Exception {
        GroupByCaseIT gbt = new GroupByCaseIT();
        long ts = gbt.createTable();
        gbt.loadData(ts);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        gbt.executeQuery(conn,GROUPBY1);
        gbt.executeQuery(conn,GROUPBY2);
        // TODO: validate query results
        try {
            gbt.executeQuery(conn,GROUPBY3);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Aggregate expressions may not be used in GROUP BY"));
        }
        conn.close();
    }

}
