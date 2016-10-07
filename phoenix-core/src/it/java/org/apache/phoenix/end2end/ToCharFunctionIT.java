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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.phoenix.expression.function.ToCharFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the TO_CHAR built-in function.
 * 
 * @see ToCharFunction
 * 
 * @since 0.1
 */

public class ToCharFunctionIT extends ParallelStatsDisabledIT {
    
    private String TO_CHAR_TABLE_NAME;
    
    private Date row1Date;
    private Time row1Time;
    private Timestamp row1Timestamp;
    private Integer row1Integer;
    private BigDecimal row1Decimal;
    private Date row2Date;
    private Time row2Time;
    private Timestamp row2Timestamp;
    private Integer row2Integer;
    private BigDecimal row2Decimal;
    
    @Before
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="DMI_BIGDECIMAL_CONSTRUCTED_FROM_DOUBLE", 
            justification="Test code.")
    public void initTable() throws Exception {
        TO_CHAR_TABLE_NAME = generateUniqueName();
        String ddl = "create table " + TO_CHAR_TABLE_NAME +
                "(pk integer not null, \n" + 
                "col_date date, \n" +
                "col_time date, \n" +
                "col_timestamp timestamp, \n" +
                "col_integer integer, \n" + 
                "col_decimal decimal\n" + 
                "CONSTRAINT my_pk PRIMARY KEY (pk))";
        createTestTable(getUrl(), ddl);
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + TO_CHAR_TABLE_NAME +
                "    (pk, " +
                "    col_date," +
                "    col_time," +
                "    col_timestamp," +
                "    col_integer," +
                "    col_decimal)" +
                "VALUES (?, ?, ?, ?, ?, ?)");
        
        row1Date = new Date(System.currentTimeMillis() - 10000);
        row1Time = new Time(System.currentTimeMillis() - 1000);
        row1Timestamp = new Timestamp(System.currentTimeMillis() + 10000);
        row1Integer = 666;
        row1Decimal = new BigDecimal(33.333);
        
        stmt.setInt(1, 1);
        stmt.setDate(2, row1Date);
        stmt.setTime(3, row1Time);
        stmt.setTimestamp(4, row1Timestamp);
        stmt.setInt(5, row1Integer);
        stmt.setBigDecimal(6, row1Decimal);
        stmt.execute();
        
        row2Date = new Date(System.currentTimeMillis() - 1234567);
        row2Time = new Time(System.currentTimeMillis() - 1234);
        row2Timestamp = new Timestamp(System.currentTimeMillis() + 1234567);
        row2Integer = 10011;
        row2Decimal = new BigDecimal(123456789.66);
        
        stmt.setInt(1, 2);
        stmt.setDate(2, row2Date);
        stmt.setTime(3, row2Time);
        stmt.setTimestamp(4, row2Timestamp);
        stmt.setInt(5, row2Integer);
        stmt.setBigDecimal(6, row2Decimal);
        
        stmt.execute();
        conn.commit();
        conn.close();
    }
    
    @Test
    public void testDateProjection() throws Exception {
        String pattern = "yyyy.MM.dd G HH:mm:ss z";
        String query = "select to_char(col_date, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 1";
        String expectedString = getGMTDateFormat(pattern).format(row1Date);
        runOneRowProjectionQuery(query, expectedString);
    }
    
    @Test
    public void testTimeProjection() throws Exception {
        String pattern = "HH:mm:ss z";
        String query = "select to_char(col_time, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 1";
        String expectedString = getGMTDateFormat(pattern).format(row1Time);
        runOneRowProjectionQuery(query, expectedString);
    }

    @Test
    public void testTimestampProjection() throws Exception {
        String pattern = "yyMMddHHmmssZ";
        String query = "select to_char(col_timestamp, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 2";
        String expectedString = getGMTDateFormat(pattern).format(row2Timestamp);
        runOneRowProjectionQuery(query, expectedString);
    }
    
    @Test
    public void testIntegerProjection() throws Exception {
        String pattern = "00";
        String query = "select to_char(col_integer, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 1";
        String expectedString = new DecimalFormat(pattern).format(row1Integer);
        runOneRowProjectionQuery(query, expectedString);
    }
    
    @Test
    public void testDecimalProjection() throws Exception {
        String pattern = "0.###E0";
        String query = "select to_char(col_decimal, '" + pattern + "') from " + TO_CHAR_TABLE_NAME + " WHERE pk = 2";
        String expectedString = new DecimalFormat(pattern).format(row2Decimal);
        runOneRowProjectionQuery(query, expectedString);
    }
    
    @Test 
    public void testDateFilter() throws Exception {
        String pattern = "yyyyMMddHHmmssZ";
        String expectedString = getGMTDateFormat(pattern).format(row1Date);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_date, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 1);
    }
    
    @Test 
    public void testTimeFilter() throws Exception {
        String pattern = "ddHHmmssSSSZ";
        String expectedString = getGMTDateFormat(pattern).format(row1Time);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_time, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 1);
    }
    
    @Test 
    public void testTimestampFilter() throws Exception {
        String pattern = "yy.MM.dd G HH:mm:ss z";
        String expectedString = getGMTDateFormat(pattern).format(row2Timestamp);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_timestamp, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 2);
    }
    
    @Test 
    public void testIntegerFilter() throws Exception {
        String pattern = "000";
        String expectedString = new DecimalFormat(pattern).format(row1Integer);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_integer, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 1);
    }
    
    @Test 
    public void testDecimalFilter() throws Exception {
        String pattern = "00.###E0";
        String expectedString = new DecimalFormat(pattern).format(row2Decimal);
        String query = "select pk from " + TO_CHAR_TABLE_NAME + " WHERE to_char(col_decimal, '" + pattern + "') = '" + expectedString + "'";
        runOneRowFilterQuery(query, 2);
    }
    
    private void runOneRowProjectionQuery(String oneRowQuery, String projectedValue) throws Exception {
    	runOneRowQueryTest(oneRowQuery, null, projectedValue);
    }
    
    private void runOneRowFilterQuery(String oneRowQuery, int pkValue) throws Exception {
    	runOneRowQueryTest(oneRowQuery, pkValue, null);
    }
    
    private void runOneRowQueryTest(String oneRowQuery, Integer pkValue, String projectedValue) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(oneRowQuery);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            if (pkValue != null)
            	assertEquals(pkValue.intValue(), rs.getInt(1));
            else
            	assertEquals(projectedValue, rs.getString(1));
            assertFalse(rs.next());
        }
        finally {
        	conn.close();
        }
    }
    
    private DateFormat getGMTDateFormat(String pattern) {
        DateFormat result = new SimpleDateFormat(pattern);
        result.setTimeZone(TimeZone.getTimeZone("GMT"));
        return result;
    }
    
    @Test
    public void testToCharWithCloneMethod() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
    	String ddl = "create table " + tableName + " (k varchar primary key, v integer[])";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('x',ARRAY[1234])");
        conn.commit();
        
        ResultSet rs = conn.createStatement().executeQuery("select to_char(v[1],'000') from " + tableName);
        assertTrue(rs.next());
        assertEquals("Unexpected value for date ", String.valueOf(1234), rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testIndexedNull() throws SQLException {
        final String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id integer primary key, ts1 timestamp, ts2 timestamp)");
        conn.createStatement().execute("create index t_ts2_idx on " + tableName + " (ts2)");
        conn.createStatement().execute("upsert into " + tableName + " values (1, null, null)");
        conn.commit();
        for (String columnName : new String[]{"ts1", "ts2"}) {
            try (ResultSet rs = conn.createStatement().executeQuery(
                    String.format("select to_char(%s) from %s", columnName, tableName))) {
                assertTrue(rs.next());
                assertEquals(null, rs.getString(1));
            }
        }
        conn.close();
    }
}