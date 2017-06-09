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
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class AggregateQueryIT extends BaseQueryIT {

    public AggregateQueryIT(String indexDDL, boolean mutable, boolean columnEncoded) {
        super(indexDDL, mutable, columnEncoded);
    }

    @Test
    public void testGroupByPlusOne() throws Exception {
        String query = "SELECT a_integer+1 FROM " + tableName + " WHERE organization_id=? and a_integer = 5 GROUP BY a_integer+1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(6, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSplitWithCachedMeta() throws Exception {
        // Tests that you don't get an ambiguous column exception when using the same alias as the column name
        String query = "SELECT a_string, b_string, count(1) FROM " + tableName + " WHERE organization_id=? and entity_id<=? GROUP BY a_string,b_string";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        HBaseAdmin admin = null;
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(B_VALUE, rs.getString(2));
            assertEquals(2, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(C_VALUE, rs.getString(2));
            assertEquals(1, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(E_VALUE, rs.getString(2));
            assertEquals(1, rs.getLong(3));
            assertFalse(rs.next());
            
            byte[] tableNameBytes = Bytes.toBytes(tableName);
            admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            HTable htable = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(tableNameBytes);
            htable.clearRegionCache();
            int nRegions = htable.getRegionLocations().size();
            admin.split(tableNameBytes, ByteUtil.concat(Bytes.toBytes(tenantId), Bytes.toBytes("00A" + Character.valueOf((char) ('3' + nextRunCount())) + ts))); // vary split point with test run
            int retryCount = 0;
            do {
                Thread.sleep(2000);
                retryCount++;
                //htable.clearRegionCache();
            } while (retryCount < 10 && htable.getRegionLocations().size() == nRegions);
            assertNotEquals(nRegions, htable.getRegionLocations().size());
            
            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(B_VALUE, rs.getString(2));
            assertEquals(2, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(C_VALUE, rs.getString(2));
            assertEquals(1, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(E_VALUE, rs.getString(2));
           assertEquals(1, rs.getLong(3));
            assertFalse(rs.next());
        } finally {
            if (admin != null) {
            admin.close();
            }
            conn.close();
        }
    }    
    
    @Test
    public void testCountIsNull() throws Exception {
        String query = "SELECT count(1) FROM " + tableName + " WHERE X_DECIMAL is null";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(6, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    
    @Test
    public void testCountWithNoScanRanges() throws Exception {
        String query = "SELECT count(1) FROM " + tableName + " WHERE organization_id = 'not_existing_organization_id'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			assertEquals(0, rs.getLong(1));
			assertFalse(rs.next());
			query = "SELECT count(1) FROM " + tableName + " WHERE organization_id = 'not_existing_organization_id' having count(*)>0";
			rs = conn.prepareStatement(query).executeQuery();
			assertFalse(rs.next());
			query = "SELECT count(1) FROM " + tableName + " WHERE organization_id = 'not_existing_organization_id' limit 1 offset 1";
			rs = conn.prepareStatement(query).executeQuery();
			assertFalse(rs.next());
			query = "SELECT count(1),123 FROM " + tableName + " WHERE organization_id = 'not_existing_organization_id'";
			rs = conn.prepareStatement(query).executeQuery();
			assertTrue(rs.next());
			assertEquals(0, rs.getLong(1));
			assertEquals("123", rs.getString(2));
			assertFalse(rs.next());
			query = "SELECT count(1),sum(x_decimal) FROM " + tableName + " WHERE organization_id = 'not_existing_organization_id'";
			rs = conn.prepareStatement(query).executeQuery();
			assertTrue(rs.next());
			assertEquals(0, rs.getLong(1));
			assertEquals(null, rs.getBigDecimal(2));
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

    @Test
    public void testCountIsNotNull() throws Exception {
        String query = "SELECT count(1) FROM " + tableName + " WHERE X_DECIMAL is not null";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(3, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test to repro Null Pointer Exception
     */
    @Test
    public void testInFilterOnKey() throws Exception {
        String query = "SELECT count(entity_id) FROM " + tableName + " WHERE organization_id IN (?,?)";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(9, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
}
