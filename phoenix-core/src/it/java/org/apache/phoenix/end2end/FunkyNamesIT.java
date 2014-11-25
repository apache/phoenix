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

import static org.apache.phoenix.util.TestUtil.FUNKY_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class FunkyNamesIT extends BaseClientManagedTimeIT {

    protected static void initTableValues(byte[][] splits, long ts) throws Exception {
        ensureTableCreated(getUrl(),FUNKY_NAME,splits, ts-2);

        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "FUNKY_NAMES(" +
                "    \"foo!\", " +
                "    \"#@$\", " +
                "    \"foo.bar-bas\", " +
                "    \"_blah^\"," +
                "    \"Value\", " +
                "    \"VALUE\", " +
                "    \"value\") " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "a");
        stmt.setString(2, "b");
        stmt.setString(3, "c");
        stmt.setString(4, "d");
        stmt.setInt(5, 1);
        stmt.setInt(6, 2);
        stmt.setInt(7, 3);
        stmt.executeUpdate();
        conn.close();
    }

    @Test
    public void testUnaliasedFunkyNames() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT \"foo!\",\"#@$\",\"foo.bar-bas\",\"_blah^\" FROM FUNKY_NAMES";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertEquals("c", rs.getString(3));
            assertEquals("d", rs.getString(4));
            
            assertEquals("a", rs.getString("foo!"));
            assertEquals("b", rs.getString("#@$"));
            assertEquals("c", rs.getString("foo.bar-bas"));
            assertEquals("d", rs.getString("_blah^"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCaseSensitive() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT \"Value\",\"VALUE\",\"value\" FROM FUNKY_NAMES";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3, rs.getInt(3));
            
            assertEquals(1, rs.getInt("Value"));
            assertEquals(2, rs.getInt("VALUE"));
            assertEquals(3, rs.getInt("value"));
            try {
                rs.getInt("vAlue");
                fail();
            } catch (ColumnNotFoundException e) {
            }
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAliasedFunkyNames() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT \"1-3.4$\".\"foo!\" as \"1-2\",\"#@$\" as \"[3]\",\"foo.bar-bas\" as \"$$$\",\"_blah^\" \"0\" FROM FUNKY_NAMES \"1-3.4$\"";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString("1-2"));
            assertEquals("b", rs.getString("[3]"));
            assertEquals("c", rs.getString("$$$"));
            assertEquals("d", rs.getString("0"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}

