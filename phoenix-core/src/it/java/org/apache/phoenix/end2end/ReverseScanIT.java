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

import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class ReverseScanIT extends ParallelStatsDisabledIT {

    private static byte[][] getSplitsAtRowKeys(String tenantId) {
        return new byte[][] { 
            Bytes.toBytes(tenantId + ROW3),
            Bytes.toBytes(tenantId + ROW7),
            Bytes.toBytes(tenantId + ROW9),
            };
    }
    
    @Test
    public void testReverseRangeScan() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, getSplitsAtRowKeys(tenantId), getUrl());
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT entity_id FROM " + tableName + " WHERE entity_id >= '" + ROW3 + "' ORDER BY organization_id DESC, entity_id DESC";
        try {
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(2);
            ResultSet rs = stmt.executeQuery(query);

            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW4,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                    "CLIENT PARALLEL 1-WAY REVERSE FULL SCAN OVER " + tableName + "\n" +
                    "    SERVER FILTER BY FIRST KEY ONLY AND ENTITY_ID >= '00A323122312312'",
                    QueryUtil.getExplainPlan(rs));
            
            PreparedStatement statement = conn.prepareStatement("SELECT entity_id FROM " + tableName + " WHERE organization_id = ? AND entity_id >= ? ORDER BY organization_id DESC, entity_id DESC");
            statement.setString(1, tenantId);
            statement.setString(2, ROW7);
            rs = statement.executeQuery();

            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testReverseSkipScan() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, getSplitsAtRowKeys(tenantId), getUrl());
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id = ? AND entity_id IN (?,?,?,?,?) ORDER BY organization_id DESC, entity_id DESC";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW3);
            statement.setString(4, ROW7);
            statement.setString(5, ROW9);
            statement.setString(6, "00BOGUSROW00000");
            ResultSet rs = statement.executeQuery();

            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testReverseScanForSpecificRangeInRegion() throws Exception {
        Connection conn;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement()
                .execute("CREATE TABLE " + tableName + " ( k VARCHAR, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k)) ");
        conn.createStatement().execute("upsert into " + tableName + " values ('a',1,3)");
        conn.createStatement().execute("upsert into " + tableName + " values ('b',1,3)");
        conn.createStatement().execute("upsert into " + tableName + " values ('c',1,3)");
        conn.createStatement().execute("upsert into " + tableName + " values ('d',1,3)");
        conn.createStatement().execute("upsert into " + tableName + " values ('e',1,3)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k FROM " + tableName + " where k>'b' and k<'d' order by k desc");
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
        assertTrue(!rs.next());
        conn.close();
    }

    @Test
    public void testReverseScanIndex() throws Exception {
        String indexName = generateUniqueName();
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, getSplitsAtRowKeys(tenantId), getUrl());
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE INDEX " + indexName + " ON " + tableName + " (a_integer DESC) INCLUDE ("
        + "    A_STRING, " + "    B_STRING, " + "    A_DATE)";
        conn.createStatement().execute(ddl);
        
        String query = 
                "SELECT a_integer FROM " + tableName + " where a_integer is not null order by a_integer nulls last limit 1";

        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs=statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(
                "CLIENT SERIAL 1-WAY REVERSE RANGE SCAN OVER " + indexName + " [not null]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER 1 ROW LIMIT\n" + 
                "CLIENT 1 ROW LIMIT",QueryUtil.getExplainPlan(rs));
    }
    
}