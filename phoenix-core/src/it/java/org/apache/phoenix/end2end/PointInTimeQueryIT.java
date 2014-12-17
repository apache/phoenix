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

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class PointInTimeQueryIT extends BaseQueryIT {

    public PointInTimeQueryIT(String indexDDL) {
        super(indexDDL);
    }

    @BeforeClass
    @Shadower(classBeingShadowed = BaseQueryIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.DEFAULT_KEEP_DELETED_CELLS_ATTRIB, Boolean.TRUE.toString());
        BaseQueryIT.doSetup(props);
    }
    
    @Test
    public void testPointInTimeScan() throws Exception {
        // Override value that was set at creation time
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 10);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        String upsertStmt =
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_INTEGER) " +
            "VALUES (?, ?, ?)";
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, 5);
        stmt.execute(); // should commit too
        
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 15);
        Connection conn1 = DriverManager.getConnection(url, props);
        analyzeTable(conn1, "ATABLE");
        conn1.close();
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 30);
        upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, 9);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        String query = "SELECT organization_id, a_string AS a FROM atable WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(tenantId, rs.getString(1));
        assertEquals(A_VALUE, rs.getString("a"));
        assertTrue(rs.next());
        assertEquals(tenantId, rs.getString(1));
        assertEquals(B_VALUE, rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testPointInTimeSequence() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn;
        ResultSet rs;

        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE SEQUENCE s");
        
        try {
            conn.createStatement().executeQuery("SELECT next value for s FROM ATABLE LIMIT 1");
            fail();
        } catch (SequenceNotFoundException e) {
            conn.close();
        }
        
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT next value for s FROM ATABLE LIMIT 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        conn.close();
        
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+7));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT next value for s FROM ATABLE LIMIT 1");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        conn.close();
        
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+15));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP SEQUENCE s");
        rs = conn.createStatement().executeQuery("SELECT next value for s FROM ATABLE LIMIT 1");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        conn.close();

        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+20));
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            rs = conn.createStatement().executeQuery("SELECT next value for s FROM ATABLE LIMIT 1");
            fail();
        } catch (SequenceNotFoundException e) {
            conn.close();            
        }
        
        conn.createStatement().execute("CREATE SEQUENCE s");
        conn.close();
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+25));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT next value for s FROM ATABLE LIMIT 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        conn.close();

        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+6));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT next value for s FROM ATABLE LIMIT 1");
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        conn.close();
    }
    
}
