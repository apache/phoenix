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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;


public class RegexpSubstrFunctionIT extends BaseHBaseManagedTimeIT {

    private int id;

    @Before
    public void doBeforeTestSetup() throws Exception {
        ensureTableCreated(getUrl(), GROUPBYTEST_NAME);
        Connection conn = DriverManager.getConnection(getUrl());
        insertRow(conn, "Report1?1", 10);
        insertRow(conn, "Report1?2", 10);
        insertRow(conn, "Report2?1", 30);
        insertRow(conn, "Report3?2", 30);
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

    @Test
    public void testGroupByScanWithRegexpSubstr() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select REGEXP_SUBSTR(uri, '[^\\\\?]+') suburi, sum(appcpu) sumcpu from " + GROUPBYTEST_NAME
            + " group by suburi");
        assertTrue(rs.next());
        assertEquals(rs.getString("suburi"), "Report1");
        assertEquals(rs.getInt("sumcpu"), 20);
        assertTrue(rs.next());
        assertEquals(rs.getString("suburi"), "Report2");
        assertEquals(rs.getInt("sumcpu"), 30);
        assertTrue(rs.next());
        assertEquals(rs.getString("suburi"), "Report3");
        assertEquals(rs.getInt("sumcpu"), 30);
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testFilterWithRegexSubstr() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery(
                "select id from " + GROUPBYTEST_NAME + " where REGEXP_SUBSTR(uri, '[^\\\\?]+') = 'Report1'");
        assertTrue(rs.next());
        assertEquals("id0", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("id1", rs.getString(1));
        assertFalse(rs.next());
    }

}
