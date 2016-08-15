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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


/**
 * 
 * Basic tests for Phoenix JDBC implementation
 *
 */
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
        } catch (SQLException e) { // Expected
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(),e.getErrorCode());
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
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 10);
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
        upsertConn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn1, "ATABLE");
        conn1.close();
        
        String query = "SELECT a_string, b_string FROM aTable WHERE organization_id=? and a_integer = 5";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
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
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 10);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection upsertConn = DriverManager.getConnection(url, props);
        upsertConn.setAutoCommit(true); // Test auto commit
        PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW4);
        Timestamp tsValue1 = new Timestamp(5000);
        byte[] ts1 = PTimestamp.INSTANCE.toBytes(tsValue1);
        stmt.setTimestamp(3, tsValue1);
        stmt.execute();
        
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 15);       
        Connection conn1 = DriverManager.getConnection(url, props);
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
        byte[] ts2 = PTimestamp.INSTANCE.toBytes(tsValue2);
        stmt.setTimestamp(3, tsValue2);
        stmt.setTime(4, new Time(tsValue2.getTime()));
        stmt.execute();
        upsertConn.close();
        
        assertTrue(compare(CompareOp.GREATER, new ImmutableBytesWritable(ts2), new ImmutableBytesWritable(ts1)));
        assertFalse(compare(CompareOp.GREATER, new ImmutableBytesWritable(ts1), new ImmutableBytesWritable(ts1)));

        String query = "SELECT entity_id, a_timestamp, a_time FROM aTable WHERE organization_id=? and a_timestamp > ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30)); // Execute at timestamp 2
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
}
