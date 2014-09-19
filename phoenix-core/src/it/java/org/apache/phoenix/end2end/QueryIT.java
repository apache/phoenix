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

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;



/**
 * 
 * Basic tests for Phoenix JDBC implementation
 *
 * 
 * @since 0.1
 */

@Category(ClientManagedTimeTest.class)
public class QueryIT extends BaseQueryIT {
    
    public QueryIT(String indexDDL) {
        super(indexDDL);
    }
    
    @Test
    public void testIntFilter() throws Exception {
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_INTEGER) " +
            "VALUES (?, ?, ?)";
        // Override value that was set at creation time
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        stmt.setInt(3, -10);
        stmt.execute();
        upsertConn.close();
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 6);
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        upsertConn = DriverManager.getConnection(url, props);
        analyzeTable(upsertConn, "ATABLE");
        upsertConn.close();

        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_integer >= ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        statement.setInt(2, 7);
        ResultSet rs = statement.executeQuery();
        assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW7, ROW8, ROW9));

        query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_integer < 2";
        statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW1, ROW4));

        query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_integer <= 2";
        statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW1, ROW2, ROW4));

        query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_integer >=9";
        statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(rs.getString(1), ROW9);
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testEmptyStringValue() throws Exception {
        testNoStringValue("");
    }

    
    @Test
    public void testGroupByPlusOne() throws Exception {
        String query = "SELECT a_integer+1 FROM aTable WHERE organization_id=? and a_integer = 5 GROUP BY a_integer+1";
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
    public void testToDateOnString() throws Exception { // TODO: test more conversion combinations
        String query = "SELECT a_string FROM aTable WHERE organization_id=? and a_integer = 5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            rs.getDate(1);
            fail();
        } catch (ConstraintViolationException e) { // Expected
        } finally {
            conn.close();
        }
    }

    
    @Test
    public void testColumnOnBothSides() throws Exception {
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and a_string = b_string";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private void testNoStringValue(String value) throws Exception {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(
                "upsert into ATABLE VALUES (?, ?, ?)"); // without specifying columns
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        stmt.setString(3, value);
        stmt.execute(); // should commit too
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, "ATABLE");
        conn1.close();
        upsertConn.close();
        
        String query = "SELECT a_string, b_string FROM aTable WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(null, rs.getString(1));
            assertTrue(rs.wasNull());
            assertEquals(C_VALUE, rs.getString("B_string"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNullStringValue() throws Exception {
        testNoStringValue(null);
    }
    
    @Test
    public void testPointInTimeScan() throws Exception {
        // Override value that was set at creation time
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
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
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, "ATABLE");
        conn1.close();
        upsertConn.close();

        // Override value again, but should be ignored since it's past the SCN
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 3); // Run query at timestamp 5
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
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
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
    
    @Test
    public void testDateInList() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE a_date IN (?,?) AND a_integer < 4";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(0));
            statement.setDate(2, date);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testTimestamp() throws Exception {
        String updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_TIMESTAMP) " +
            "VALUES (?, ?, ?)";
        // Override value that was set at creation time
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        Timestamp tsValue1 = new Timestamp(5000);
        byte[] ts1 = PDataType.TIMESTAMP.toBytes(tsValue1);
        stmt.setTimestamp(3, tsValue1);
        stmt.execute();
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, "ATABLE");
        conn1.close();
        updateStmt = 
            "upsert into " +
            "ATABLE(" +
            "    ORGANIZATION_ID, " +
            "    ENTITY_ID, " +
            "    A_TIMESTAMP," +
            "    A_TIME) " +
            "VALUES (?, ?, ?, ?)";
        stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5);
        Timestamp tsValue2 = new Timestamp(5000);
        tsValue2.setNanos(200);
        byte[] ts2 = PDataType.TIMESTAMP.toBytes(tsValue2);
        stmt.setTimestamp(3, tsValue2);
        stmt.setTime(4, new Time(tsValue2.getTime()));
        stmt.execute();
        upsertConn.close();
        conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, "ATABLE");
        conn1.close();
        analyzeTable(upsertConn, "ATABLE");
        assertTrue(compare(CompareOp.GREATER, new ImmutableBytesWritable(ts2), new ImmutableBytesWritable(ts1)));
        assertFalse(compare(CompareOp.GREATER, new ImmutableBytesWritable(ts1), new ImmutableBytesWritable(ts1)));

        String query = "SELECT entity_id, a_timestamp, a_time FROM aTable WHERE organization_id=? and a_timestamp > ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setTimestamp(2, new Timestamp(5000));
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertEquals(rs.getTimestamp("A_TIMESTAMP"), tsValue2);
            assertEquals(rs.getTime("A_TIME"), new Time(tsValue2.getTime()));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSimpleInListStatement() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND a_integer IN (2,4)";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW2, ROW4));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testPartiallyQualifiedRVCInList() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE (a_integer,a_string) IN ((2,'a'),(5,'b'))";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW2, ROW5));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testFullyQualifiedRVCInList() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE (a_integer,a_string, organization_id,entity_id) IN ((2,'a',:1,:2),(5,'b',:1,:3))";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW5);
            ResultSet rs = statement.executeQuery();
            assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW2, ROW5));
        } finally {
            conn.close();
        }
    }
    
    /**
     * Test to repro Null Pointer Exception
     * @throws Exception
     */
    @Test
    public void testInFilterOnKey() throws Exception {
        String query = "SELECT count(entity_id) FROM ATABLE WHERE organization_id IN (?,?)";
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
    
    @Test
    public void testOneInListStatement() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND b_string IN (?)";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, E_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW6, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    
    @Test
    public void testMixedTypeInListStatement() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? AND x_long IN (5, ?)";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            long l = Integer.MAX_VALUE + 1L;
            statement.setLong(2, l);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIsNull() throws Exception {
        String query = "SELECT entity_id FROM aTable WHERE X_DECIMAL is null";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
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
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCountIsNull() throws Exception {
        String query = "SELECT count(1) FROM aTable WHERE X_DECIMAL is null";
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
    public void testCountIsNotNull() throws Exception {
        String query = "SELECT count(1) FROM aTable WHERE X_DECIMAL is not null";
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

    @Test
    public void testIsNotNull() throws Exception {
        String query = "SELECT entity_id FROM aTable WHERE X_DECIMAL is not null";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
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
    public void testValidStringConcatExpression() throws Exception {//test fails with stack overflow wee
        int counter=0;
        String[] answers = new String[]{"00D300000000XHP5bar","a5bar","15bar","5bar","5bar"};
        String[] queries = new String[] { 
        		"SELECT  organization_id || 5 || 'bar' FROM atable limit 1",
        		"SELECT a_string || 5 || 'bar' FROM atable  order by a_string  limit 1",
        		"SELECT a_integer||5||'bar' FROM atable order by a_integer  limit 1",
        		"SELECT x_decimal||5||'bar' FROM atable limit 1",
        		"SELECT x_long||5||'bar' FROM atable limit 1"
        };

        for (String query : queries) {
        	Properties props = new Properties();
        	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        	Connection conn = DriverManager.getConnection(getUrl(), props);
        	try {
        		PreparedStatement statement = conn.prepareStatement(query);
        		ResultSet rs=statement.executeQuery();
        		assertTrue(rs.next());
        		assertEquals(answers[counter++],rs.getString(1));
           		assertFalse(rs.next());
        	}
        	finally {
        		conn.close();
        	}
        }
    }
    
    @Test
    public void testRowKeySingleIn() throws Exception {
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and entity_id IN (?,?,?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW6);
            statement.setString(4, ROW8);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    
    @Test
    public void testRowKeyMultiIn() throws Exception {
        String query = "SELECT entity_id FROM aTable WHERE organization_id=? and entity_id IN (?,?,?) and a_string IN (?,?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW2);
            statement.setString(3, ROW6);
            statement.setString(4, ROW9);
            statement.setString(5, B_VALUE);
            statement.setString(6, C_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW6);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSplitWithCachedMeta() throws Exception {
        // Tests that you don't get an ambiguous column exception when using the same alias as the column name
        String query = "SELECT a_string, b_string, count(1) FROM atable WHERE organization_id=? and entity_id<=? GROUP BY a_string,b_string";
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
            
            byte[] tableName = Bytes.toBytes(ATABLE_NAME);
            admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            HTable htable = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(tableName);
            htable.clearRegionCache();
            int nRegions = htable.getRegionLocations().size();
            admin.split(tableName, ByteUtil.concat(Bytes.toBytes(tenantId), Bytes.toBytes("00A" + Character.valueOf((char) ('3' + nextRunCount())) + ts))); // vary split point with test run
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
    public void testColumnAliasMapping() throws Exception {
        String query = "SELECT a.a_string, aTable.b_string FROM aTable a WHERE ?=organization_id and 5=a_integer ORDER BY a_string, b_string";
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
    public void testSumOverNullIntegerColumn() throws Exception {
        String query = "SELECT sum(a_integer) FROM aTable a";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("UPSERT INTO atable(organization_id,entity_id,a_integer) VALUES('" + getOrganizationId() + "','" + ROW3 + "',NULL)");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 6));
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, "ATABLE");
        conn1.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(42, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 7));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("UPSERT INTO atable(organization_id,entity_id,a_integer) SELECT organization_id, entity_id, null FROM atable");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 6));
        conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, "ATABLE");
        conn1.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 9));
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private void analyzeTable(Connection conn, String tableName) throws IOException, SQLException {
        String query = "ANALYZE " + tableName;
        conn.createStatement().execute(query);
    }
}
