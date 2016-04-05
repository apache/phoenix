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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;


public class PrimitiveTypeIT extends BaseHBaseManagedTimeTableReuseIT {

    private static final String TABLE_NAME = generateRandomString();
    private static final Properties PROPS = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    private static Connection conn;

    @BeforeClass
    public static void initTableValues() throws Exception {
        conn = DriverManager.getConnection(getUrl(), PROPS);
        conn.createStatement().execute(
            "create table " + TABLE_NAME + " (l bigint not null primary key, b boolean)");
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + TABLE_NAME + " VALUES(?)");
        stmt.setLong(1, 2);
        stmt.execute();
        conn.commit();
    }

    @Test
    public void testCompareLongGTDecimal() throws Exception {
        String query = "SELECT l FROM " + TABLE_NAME + " where l > 1.5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCompareLongGTEDecimal() throws Exception {
        String query = "SELECT l FROM " + TABLE_NAME + " where l >= 1.5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            /*
             *  Failing because we're not converting the constant to the type of the RHS
             *  when forming the start/stop key.
             *  For this case, 1.5 -> 1L
             *  if where l < 1.5 then 1.5 -> 1L and then to 2L because it's not inclusive
             *  
             */
            assertTrue (rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCompareLongLTDecimal() throws Exception {
        String query = "SELECT l FROM " + TABLE_NAME + " where l < 1.5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            /*
             *  Failing because we're not converting the constant to the type of the RHS
             *  when forming the start/stop key.
             *  For this case, 1.5 -> 1L
             *  if where l < 1.5 then 1.5 -> 1L and then to 2L because it's not inclusive
             *  
             */
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCompareLongLTEDecimal() throws Exception {
        String query = "SELECT l FROM " + TABLE_NAME + " where l <= 1.5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            /*
             *  Failing because we're not converting the constant to the type of the RHS
             *  when forming the start/stop key.
             *  For this case, 1.5 -> 1L
             *  if where l < 1.5 then 1.5 -> 1L and then to 2L because it's not inclusive
             *  
             */
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testCompareLongGTDecimal2() throws Exception {
        String query = "SELECT l FROM " + TABLE_NAME + " where l > 2.5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            /*
             *  Failing because we're not converting the constant to the type of the RHS
             *  when forming the start/stop key.
             *  For this case, 1.5 -> 1L
             *  if where l < 1.5 then 1.5 -> 1L and then to 2L because it's not inclusive
             *  
             */
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCompareLongGTEDecimal2() throws Exception {
        String query = "SELECT l FROM " + TABLE_NAME + " where l >= 2.5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            /*
             *  Failing because we're not converting the constant to the type of the RHS
             *  when forming the start/stop key.
             *  For this case, 1.5 -> 1L
             *  if where l < 1.5 then 1.5 -> 1L and then to 2L because it's not inclusive
             *  
             */
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCompareLongLTDecimal2() throws Exception {
        String query = "SELECT l FROM " + TABLE_NAME + " where l < 2.5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            /*
             *  Failing because we're not converting the constant to the type of the RHS
             *  when forming the start/stop key.
             *  For this case, 1.5 -> 1L
             *  if where l < 1.5 then 1.5 -> 1L and then to 2L because it's not inclusive
             *  
             */
            assertTrue (rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCompareLongLTEDecimal2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT l FROM " + TABLE_NAME + " where l <= 2.5";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            /*
             *  Failing because we're not converting the constant to the type of the RHS
             *  when forming the start/stop key.
             *  For this case, 1.5 -> 1L
             *  if where l < 1.5 then 1.5 -> 1L and then to 2L because it's not inclusive
             *  
             */
            assertTrue (rs.next());
            assertEquals(2, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testBooleanAsObject() throws Exception {
        String query = "upsert into " + TABLE_NAME + " values (2, ?)";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setObject(1, new Boolean("false"));
            statement.execute();
            conn.commit();
            statement = conn.prepareStatement("SELECT l,b,? FROM " + TABLE_NAME);
            statement.setObject(1, new Boolean("false"));
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertEquals(Boolean.FALSE, rs.getObject(2));
            assertEquals(Boolean.FALSE, rs.getObject(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
