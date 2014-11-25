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

import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;


public class CompareDecimalToLongIT extends BaseClientManagedTimeIT {
    protected static void initTableValues(byte[][] splits, long ts) throws Exception {
        ensureTableCreated(getUrl(),"LongInKeyTest",splits, ts-2);
        
        // Insert all rows at ts
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Connection conn = DriverManager.getConnection(url);
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "LongInKeyTest VALUES(?)");
        stmt.setLong(1, 2);
        stmt.execute();
        conn.close();
    }

    @Test
    public void testCompareLongGTDecimal() throws Exception {
        long ts = nextTimestamp();
        initTableValues(null, ts);
        String query = "SELECT l FROM LongInKeyTest where l > 1.5";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        long ts = nextTimestamp();
        initTableValues(null, ts);
        String query = "SELECT l FROM LongInKeyTest where l >= 1.5";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        long ts = nextTimestamp();
        initTableValues(null, ts);
        String query = "SELECT l FROM LongInKeyTest where l < 1.5";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        long ts = nextTimestamp();
        initTableValues(null, ts);
        String query = "SELECT l FROM LongInKeyTest where l <= 1.5";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        long ts = nextTimestamp();
        initTableValues(null, ts);
        String query = "SELECT l FROM LongInKeyTest where l > 2.5";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        long ts = nextTimestamp();
        initTableValues(null, ts);
        String query = "SELECT l FROM LongInKeyTest where l >= 2.5";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        long ts = nextTimestamp();
        initTableValues(null, ts);
        String query = "SELECT l FROM LongInKeyTest where l < 2.5";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        long ts = nextTimestamp();
        initTableValues(null, ts);
        String query = "SELECT l FROM LongInKeyTest where l <= 2.5";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
}
