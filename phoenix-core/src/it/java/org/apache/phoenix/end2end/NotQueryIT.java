/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
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
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;


@RunWith(Parameterized.class)
public class NotQueryIT extends BaseQueryIT {

    public NotQueryIT(String indexDDL) {
        super(indexDDL);
    }
    
    @Parameters(name="{0}")
    public static Collection<Object> data() {
        return QueryIT.data();
    }
    
    @Test
    public void testNotInList() throws Exception {
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and entity_id NOT IN (?,?,?,?,?,?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW4);
            statement.setString(4, ROW1);
            statement.setString(5, ROW5);
            statement.setString(6, ROW7);
            statement.setString(7, ROW8);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotInListOfFloat() throws Exception {
        String query = "SELECT a_float FROM aTable WHERE organization_id=? and a_float NOT IN (?,?,?,?,?,?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setFloat(2, 0.01f);
            statement.setFloat(3, 0.02f);
            statement.setFloat(4, 0.03f);
            statement.setFloat(5, 0.04f);
            statement.setFloat(6, 0.05f);
            statement.setFloat(7, 0.06f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.07f)==0);
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.08f)==0);
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.09f)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotInListOfDouble() throws Exception {
        String query = "SELECT a_double FROM aTable WHERE organization_id=? and a_double NOT IN (?,?,?,?,?,?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDouble(2, 0.0001);
            statement.setDouble(3, 0.0002);
            statement.setDouble(4, 0.0003);
            statement.setDouble(5, 0.0004);
            statement.setDouble(6, 0.0005);
            statement.setDouble(7, 0.0006);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0007)==0);
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0008)==0);
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0009)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEquals() throws Exception {
        String query = "SELECT entity_id -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_integer != 1 and a_integer <= 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByTinyInt() throws Exception {
        String query = "SELECT a_byte -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_byte != 1 and a_byte <= 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getByte(1), 2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsBySmallInt() throws Exception {
        String query = "SELECT a_short -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_short != 128 and a_short !=0 and a_short <= 129";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getShort(1), 129);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByFloat() throws Exception {
        String query = "SELECT a_float -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_float != CAST(0.01 AS FLOAT) and a_float <= CAST(0.02 AS FLOAT)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.02f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByUnsignedFloat() throws Exception {
        String query = "SELECT a_unsigned_float -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_unsigned_float != 0.01 and a_unsigned_float <= 0.02";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.02f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByDouble() throws Exception {
        String query = "SELECT a_double -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_double != CAST(0.0001 AS DOUBLE) and a_double <= CAST(0.0002 AS DOUBLE)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0002) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNotEqualsByUnsignedDouble() throws Exception {
        String query = "SELECT a_unsigned_double -- and here comment\n" + 
        "FROM aTable WHERE organization_id=? and a_unsigned_double != 0.0001 and a_unsigned_double <= 0.0002";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.0002) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNotEquals2() throws Exception {
        String query = "SELECT entity_id FROM // one more comment  \n" +
        "aTable WHERE organization_id=? and not a_integer = 1 and a_integer <= 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }


}
