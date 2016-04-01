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

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;


public class GroupByCaseIT extends BaseHBaseManagedTimeIT {

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
    
    private static int id;

    private static void initData(Connection conn) throws SQLException {
        ensureTableCreated(getUrl(), GROUPBYTEST_NAME);
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

    private static void insertRow(Connection conn, String uri, int appcpu) throws SQLException {
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO " + GROUPBYTEST_NAME + "(id, uri, appcpu) values (?,?,?)");
        statement.setString(1, "id" + id);
        statement.setString(2, uri);
        statement.setInt(3, appcpu);
        statement.executeUpdate();
        id++;
    }

    @Test
    public void testExpressionInGroupBy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = " create table tgb_counter(tgb_id integer NOT NULL,utc_date_epoch integer NOT NULL,tgb_name varchar(40),ack_success_count integer" +
                ",ack_success_one_ack_count integer, CONSTRAINT pk_tgb_counter PRIMARY KEY(tgb_id, utc_date_epoch))";
        String query = "SELECT tgb_id, tgb_name, (utc_date_epoch/10)*10 AS utc_epoch_hour,SUM(ack_success_count + ack_success_one_ack_count) AS ack_tx_sum" +
                " FROM tgb_counter GROUP BY tgb_id, tgb_name, utc_epoch_hour";

        createTestTable(getUrl(), ddl);
        String dml = "UPSERT INTO tgb_counter VALUES(?,?,?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setInt(2, 1000);
        stmt.setString(3, "aaa");
        stmt.setInt(4, 1);
        stmt.setInt(5, 1);
        stmt.execute();
        stmt.setInt(1, 2);
        stmt.setInt(2, 2000);
        stmt.setString(3, "bbb");
        stmt.setInt(4, 2);
        stmt.setInt(5, 2);
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertEquals("aaa",rs.getString(2));
        assertEquals(1000,rs.getInt(3));
        assertEquals(2,rs.getInt(4));
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertEquals("bbb",rs.getString(2));
        assertEquals(2000,rs.getInt(3));
        assertEquals(4,rs.getInt(4));
        assertFalse(rs.next());
        rs.close();
        conn.close();
    }
    
    @Test
    public void testBooleanInGroupBy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = " create table bool_gb(id varchar primary key,v1 boolean, v2 integer, v3 integer)";

        createTestTable(getUrl(), ddl);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO bool_gb(id,v2,v3) VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setInt(2, 1);
        stmt.setInt(3, 1);
        stmt.execute();
        stmt.close();
        stmt = conn.prepareStatement("UPSERT INTO bool_gb VALUES(?,?,?,?)");
        stmt.setString(1, "b");
        stmt.setBoolean(2, false);
        stmt.setInt(3, 2);
        stmt.setInt(4, 2);
        stmt.execute();
        stmt.setString(1, "c");
        stmt.setBoolean(2, true);
        stmt.setInt(3, 3);
        stmt.setInt(4, 3);
        stmt.execute();
        conn.commit();

        String[] gbs = {"v1,v2,v3","v1,v3,v2","v2,v1,v3"};
        for (String gb : gbs) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT v1, v2, v3 from bool_gb group by " + gb);
            assertTrue(rs.next());
            assertEquals(false,rs.getBoolean("v1"));
            assertTrue(rs.wasNull());
            assertEquals(1,rs.getInt("v2"));
            assertEquals(1,rs.getInt("v3"));
            assertTrue(rs.next());
            assertEquals(false,rs.getBoolean("v1"));
            assertFalse(rs.wasNull());
            assertEquals(2,rs.getInt("v2"));
            assertEquals(2,rs.getInt("v3"));
            assertTrue(rs.next());
            assertEquals(true,rs.getBoolean("v1"));
            assertEquals(3,rs.getInt("v2"));
            assertEquals(3,rs.getInt("v3"));
            assertFalse(rs.next());
            rs.close();
        }
        conn.close();
    }
    
    @Test
    public void testScanUri() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        initData(conn);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        initData(conn);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(1) from " + GROUPBYTEST_NAME);
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testGroupByCase() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        initData(conn);
        conn.createStatement().executeQuery(GROUPBY1);
        conn.createStatement().executeQuery(GROUPBY2);
        // TODO: validate query results
        try {
            conn.createStatement().executeQuery(GROUPBY3);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Aggregate expressions may not be used in GROUP BY"));
        }
        conn.close();
    }


    @Test
    public void testGroupByArray() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE test1(\n" + 
                "  a VARCHAR NOT NULL,\n" + 
                "  b VARCHAR,\n" + 
                "  c INTEGER,\n" + 
                "  d VARCHAR,\n" + 
                "  e VARCHAR ARRAY,\n" + 
                "  f BIGINT,\n" + 
                "  g BIGINT,\n" + 
                "  CONSTRAINT pk PRIMARY KEY(a)\n" + 
                ")");
        conn.createStatement().execute("UPSERT INTO test1 VALUES('1', 'val', 100, 'a', ARRAY ['b'], 1, 2)");
        conn.createStatement().execute("UPSERT INTO test1 VALUES('2', 'val', 100, 'a', ARRAY ['b'], 3, 4)");
        conn.createStatement().execute("UPSERT INTO test1 VALUES('3', 'val', 100, 'a', ARRAY ['b','c'], 5, 6)");
        conn.commit();
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT c, SUM(f + g) AS sumone, d, e\n" + 
                "FROM test1\n" + 
                "WHERE b = 'val'\n" + 
                "  AND a IN ('1','2','3')\n" + 
                "GROUP BY c, d, e\n" + 
                "ORDER BY sumone DESC");
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertEquals(11, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertEquals(10, rs.getLong(2));
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testGroupByOrderPreserving() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE T (ORGANIZATION_ID char(15) not null, \n" + 
                "JOURNEY_ID char(15) not null, \n" + 
                "DATASOURCE SMALLINT not null, \n" + 
                "MATCH_STATUS TINYINT not null, \n" + 
                "EXTERNAL_DATASOURCE_KEY varchar(30), \n" + 
                "ENTITY_ID char(15) not null, \n" + 
                "CONSTRAINT PK PRIMARY KEY (\n" + 
                "    ORGANIZATION_ID, \n" + 
                "    JOURNEY_ID, \n" + 
                "    DATASOURCE, \n" + 
                "    MATCH_STATUS,\n" + 
                "    EXTERNAL_DATASOURCE_KEY,\n" + 
                "    ENTITY_ID))");
        conn.createStatement().execute("UPSERT INTO T VALUES('000001111122222', '333334444455555', 0, 0, 'abc', '666667777788888')");
        conn.createStatement().execute("UPSERT INTO T VALUES('000001111122222', '333334444455555', 0, 0, 'abcd', '666667777788889')");
        conn.createStatement().execute("UPSERT INTO T VALUES('000001111122222', '333334444455555', 0, 0, 'abc', '666667777788899')");
        conn.commit();
        String query =
                "SELECT COUNT(1), EXTERNAL_DATASOURCE_KEY As DUP_COUNT\n" + 
                "    FROM T \n" + 
                "   WHERE JOURNEY_ID='333334444455555' AND \n" + 
                "                 DATASOURCE=0 AND MATCH_STATUS <= 1 and \n" + 
                "                 ORGANIZATION_ID='000001111122222' \n" + 
                "    GROUP BY MATCH_STATUS, EXTERNAL_DATASOURCE_KEY \n" + 
                "    HAVING COUNT(1) > 1";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertEquals("abc", rs.getString(2));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['000001111122222','333334444455555',0,*] - ['000001111122222','333334444455555',0,1]\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [MATCH_STATUS, EXTERNAL_DATASOURCE_KEY]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT FILTER BY COUNT(1) > 1",QueryUtil.getExplainPlan(rs));
    }
    
}
