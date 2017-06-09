/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;


@RunWith(Parameterized.class)
public class ScanQueryIT extends BaseQueryIT {
    
    @Parameters(name="ScanQueryIT_{index}") // name is used by failsafe as file name in reports
    public static Collection<Object> data() {
        return QueryIT.data();
    }

    public ScanQueryIT(String indexDDL, boolean mutable, boolean columnEncoded) {
        super(indexDDL, mutable, columnEncoded);
    }
    
    @Test
    public void testScan() throws Exception {
        String query = "SELECT a_string, /* comment ok? */ b_string FROM " + tableName + " WHERE ?=organization_id and 5=a_integer";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getString("B_string"), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByByteValue() throws Exception {
        String query = "SELECT a_string, b_string, a_byte FROM " + tableName + " WHERE ?=organization_id and 1=a_byte";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertEquals(rs.getByte(3), 1);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByShortValue() throws Exception {
        String query = "SELECT a_string, b_string, a_short FROM " + tableName + " WHERE ?=organization_id and 128=a_short";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertEquals(rs.getShort("a_short"), 128);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByFloatValue() throws Exception {
        String query = "SELECT a_string, b_string, a_float FROM " + tableName + " WHERE ?=organization_id and ?=a_float";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setFloat(2, 0.01f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByUnsignedFloatValue() throws Exception {
        String query = "SELECT a_string, b_string, a_unsigned_float FROM " + tableName + " WHERE ?=organization_id and ?=a_unsigned_float";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setFloat(2, 0.01f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByDoubleValue() throws Exception {
        String query = "SELECT a_string, b_string, a_double FROM " + tableName + " WHERE ?=organization_id and ?=a_double";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDouble(2, 0.0001);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Doubles.compare(rs.getDouble(3), 0.0001) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByUnsigned_DoubleValue() throws Exception {
        String query = "SELECT a_string, b_string, a_unsigned_double FROM " + tableName + " WHERE ?=organization_id and ?=a_unsigned_double";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDouble(2, 0.0001);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Doubles.compare(rs.getDouble(3), 0.0001) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAllScan() throws Exception {
        String query = "SELECT ALL a_string, b_string FROM " + tableName + " WHERE ?=organization_id and 5=a_integer";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertEquals(rs.getString("B_string"), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDistinctScan() throws Exception {
        String query = "SELECT DISTINCT a_string FROM " + tableName + " WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), B_VALUE);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), C_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinctLimitScan() throws Exception {
        String query = "SELECT DISTINCT a_string FROM " + tableName + " WHERE organization_id=? LIMIT 1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInListSkipScan() throws Exception {
        String query = "SELECT entity_id, b_string FROM " + tableName + " WHERE organization_id=? and entity_id IN (?,?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW4);
            ResultSet rs = statement.executeQuery();
            Set<String> expectedvals = new HashSet<String>();
            expectedvals.add(ROW2+"_"+C_VALUE);
            expectedvals.add(ROW4+"_"+B_VALUE);
            Set<String> vals = new HashSet<String>();
            assertTrue (rs.next());
            vals.add(rs.getString(1) + "_" + rs.getString(2));
            assertTrue (rs.next());
            vals.add(rs.getString(1) + "_" + rs.getString(2));
            assertFalse(rs.next());
            assertEquals(expectedvals, vals);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUnboundRangeScan1() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id <= ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUnboundRangeScan2() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id >= ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpperLowerBoundRangeScan() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? and substr(entity_id,1,3) > '00A' and substr(entity_id,1,3) < '00C'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpperBoundRangeScan() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? and substr(entity_id,1,3) >= '00B' ";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLowerBoundRangeScan() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? and substr(entity_id,1,3) < '00B' ";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW3);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW4);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
