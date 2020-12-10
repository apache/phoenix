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

import static org.apache.phoenix.util.TestUtil.createGroupByTestTable;
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


public class RegexpReplaceFunctionIT extends ParallelStatsDisabledIT {

    private int id;
    protected  String tableName;

    @Before
    public void doBeforeTestSetup() throws Exception {
        this.tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        createGroupByTestTable(conn, tableName);
        insertRow(conn, "Report11", 10);
        insertRow(conn, "Report11", 10);
        insertRow(conn, "Report22", 30);
        insertRow(conn, "Report33", 30);
        conn.commit();
        conn.close();
    }

    private void insertRow(Connection conn, String uri, int appcpu) throws SQLException {

        PreparedStatement statement = conn.prepareStatement("UPSERT INTO " + this.tableName + "(id, uri, appcpu) values (?,?,?)");
        statement.setString(1, "id" + id);
        statement.setString(2, uri);
        statement.setInt(3, appcpu);
        statement.executeUpdate();
        id++;
    }

    @Test
    public void testGroupByScanWithRegexpReplace() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select REGEXP_REPLACE(uri, '[1-3]+', '*') suburi, sum(appcpu) sumcpu from " + this.tableName + " group by suburi");
        assertTrue(rs.next());
        assertEquals(rs.getString("suburi"), "Report*");
        assertEquals(rs.getInt("sumcpu"), 80);
        assertFalse(rs.next());

        stmt = conn.createStatement();
        rs = stmt.executeQuery("select REGEXP_REPLACE(uri, '[1-3]+') suburi, sum(appcpu) sumcpu from " + this.tableName + " group by suburi");
        assertTrue(rs.next());
        assertEquals(rs.getString("suburi"), "Report");
        assertEquals(rs.getInt("sumcpu"), 80);
        assertFalse(rs.next());

        conn.close();
    }

    @Test
    public void testFilterWithRegexReplace() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("select id from " + this.tableName + " where REGEXP_REPLACE(uri, '[2-3]+', '*') = 'Report*'");
        assertTrue(rs.next());
        assertEquals("id2", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("id3", rs.getString(1));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("select id from " + this.tableName + " where REGEXP_REPLACE(uri, '[2-3]+') = 'Report'");
        assertTrue(rs.next());
        assertEquals("id2", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("id3", rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }

}
