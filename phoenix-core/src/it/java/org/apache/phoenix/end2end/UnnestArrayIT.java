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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public abstract class UnnestArrayIT extends BaseClientManagedTimeIT {

    private static long timestamp;

    public static long nextTimestamp() {
        timestamp += 100;
        return timestamp;
    }

    @Test
    public void testUnnestWithIntArray1() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE a (p INTEGER PRIMARY KEY, k INTEGER[])";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO a VALUES (1, ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("SELECT t.k FROM UNNEST((SELECT k FROM a)) AS t(k)");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 3);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithIntArray2() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE a (p INTEGER PRIMARY KEY, k INTEGER[])";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO a VALUES (1, ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO a VALUES (2, ARRAY[4, 5])");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("SELECT t.k FROM UNNEST((SELECT k FROM a)) AS t(k)");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 3);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 4);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 5);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithVarcharArray1() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE a (p INTEGER PRIMARY KEY, k VARCHAR[])";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO a VALUES (1, ARRAY['a', 'b', 'c'])");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("SELECT t.k FROM UNNEST((SELECT k FROM a)) AS t(k)");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "a");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "b");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "c");
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithDoubleArray1() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE a (p INTEGER PRIMARY KEY, k DOUBLE[])";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO a VALUES (1, ARRAY[2.3, 3.4, 4.5])");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("SELECT t.k FROM UNNEST((SELECT k FROM a)) AS t(k)");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getDouble(1), 2.3);
        assertTrue(rs.next());
        assertEquals(rs.getDouble(1), 3.4);
        assertTrue(rs.next());
        assertEquals(rs.getDouble(1), 4.5);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithBooleanArray1() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE a (p INTEGER PRIMARY KEY, k BOOLEAN[])";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO a VALUES (1, ARRAY[true, true, false])");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("SELECT t.k FROM UNNEST((SELECT k FROM a)) AS t(k)");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getBoolean(1), true);
        assertTrue(rs.next());
        assertEquals(rs.getBoolean(1), true);
        assertTrue(rs.next());
        assertEquals(rs.getBoolean(1), false);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithOrdinality() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("SELECT ar1, ordinality FROM UNNEST(ARRAY['a','b','c']) WITH ORDINALITY AS t1(ar1, ordinality)");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "a");
        assertEquals(rs.getInt(2), 1);
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "b");
        assertEquals(rs.getInt(2), 2);
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "c");
        assertEquals(rs.getInt(2), 3);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithJoins1() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("SELECT ar1, ar2 FROM UNNEST(ARRAY[2,3,4]) WITH ORDINALITY AS t1(ar1, index) FULL OUTER JOIN UNNEST(ARRAY[5,6]) with ORDINALITY AS t2(ar2, index) ON t1.index=t2.index");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getInt(2), 5);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getInt(2), 6);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 4);
        assertEquals(rs.getInt(2), 0);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithJoins2() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("SELECT ar1, ar2 FROM UNNEST(ARRAY[2,3,4]) WITH ORDINALITY AS t1(ar1, index) INNER JOIN UNNEST(ARRAY[5,6]) with ORDINALITY AS t2(ar2, index) ON t1.index=t2.index");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getInt(2), 5);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getInt(2), 6);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithJoins3() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE a (p INTEGER PRIMARY KEY, k VARCHAR[])";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO a VALUES (1, ARRAY['a', 'b', 'c'])");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("SELECT ar1, ar2 FROM UNNEST(ARRAY[2,3,4]) WITH ORDINALITY AS t1(ar1, index) FULL OUTER JOIN UNNEST((SELECT k FROM a)) with ORDINALITY AS t2(ar2, index) ON t1.index=t2.index");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "a");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getString(2), "b");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 4);
        assertEquals(rs.getString(2), "c");
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithJoins4() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("SELECT ar1, ar2 FROM UNNEST(ARRAY[2,3,4]) WITH ORDINALITY AS t1(ar1, index) FULL OUTER JOIN UNNEST(ARRAY['a','b']) with ORDINALITY AS t2(ar2, index) ON t1.index=t2.index");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "a");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 3);
        assertEquals(rs.getString(2), "b");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 4);
        assertEquals(rs.getString(2), null);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestWithJoins5() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("SELECT ar1, ar2 FROM UNNEST(ARRAY[1,2]) AS t1(ar1), UNNEST(ARRAY[2,3]) AS t2(ar2);");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getInt(2), 2);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getInt(2), 3);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getInt(2), 2);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getInt(2), 3);
        assertFalse(rs.next());
    }

    @Test
    public void testUnnestInWhere() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE a (p INTEGER PRIMARY KEY)";
        conn.createStatement().execute(ddl);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO a VALUES (2)");
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("SELECT * FROM a WHERE p IN(SELECT t.a FROM UNNEST(ARRAY[2,3]) AS t(a))");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertFalse(rs.next());
    }
}
