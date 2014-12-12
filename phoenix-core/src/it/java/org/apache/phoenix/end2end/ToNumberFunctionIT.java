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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.Properties;

import org.apache.phoenix.expression.function.ToNumberFunction;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the TO_NUMBER built-in function.
 * 
 * @see ToNumberFunction
 * 
 * @since 0.1
 */

public class ToNumberFunctionIT extends BaseClientManagedTimeIT {

    // This test changes to locale to en_US, and saves the previous locale here
    private static Locale saveLocale;

    public static final String TO_NUMBER_TABLE_NAME = "TO_NUMBER_TABLE";
    
    public static final String TO_NUMBER_TABLE_DDL = "create table " + TO_NUMBER_TABLE_NAME +
        "(a_id integer not null, \n" + 
        "a_string char(4) not null, \n" +
        "b_string char(4), \n" + 
        "a_date date, \n" + 
        "a_time date, \n" + 
        "a_timestamp timestamp \n" + 
        "CONSTRAINT my_pk PRIMARY KEY (a_id, a_string))";
    
    private Date row1Date;
    private Date row2Date;
    private Date row3Date;
    private Time row1Time;
    private Time row2Time;
    private Time row3Time;
    private Timestamp row1Timestamp;
    private Timestamp row2Timestamp;
    private Timestamp row3Timestamp;

    @BeforeClass
    public static void setUpBeforeClass() {
        saveLocale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @AfterClass
    public static void tearDownAfterClass() {
        Locale.setDefault(saveLocale);
    }

    @Before
    public void initTable() throws Exception {
        long ts = nextTimestamp();
        createTestTable(getUrl(), TO_NUMBER_TABLE_DDL, null, ts-2);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(false);
        
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + TO_NUMBER_TABLE_NAME +
                "    (a_id, " +
                "    a_string," +
                "    b_string," +
                "    a_date," +
                "    a_time," +
                "    a_timestamp)" +
                "VALUES (?, ?, ?, ?, ?, ?)");
        
        stmt.setInt(1, 1);
        stmt.setString(2, "   1");
        stmt.setString(3, "   1");
        row1Date = new Date(System.currentTimeMillis() - 1000);
        row1Time = new Time(System.currentTimeMillis() - 1000);
        row1Timestamp = new Timestamp(System.currentTimeMillis() + 10000);
        stmt.setDate(4, row1Date);
        stmt.setTime(5, row1Time);
        stmt.setTimestamp(6, row1Timestamp);
        stmt.execute();
        
        stmt.setInt(1, 2);
        stmt.setString(2, " 2.2");
        stmt.setString(3, " 2.2");
        row2Date = new Date(System.currentTimeMillis() - 10000);
        row2Time = new Time(System.currentTimeMillis() - 1234);
        row2Timestamp = new Timestamp(System.currentTimeMillis() + 1234567);
        stmt.setDate(4, row2Date);
        stmt.setTime(5, row2Time);
        stmt.setTimestamp(6, row2Timestamp);
        stmt.execute();
        
        stmt.setInt(1, 3);
        stmt.setString(2, "$3.3");
        stmt.setString(3, "$3.3");
        row3Date = new Date(System.currentTimeMillis() - 100);
        row3Time = new Time(System.currentTimeMillis() - 789);
        row3Timestamp = new Timestamp(System.currentTimeMillis() + 78901);
        stmt.setDate(4, row3Date);
        stmt.setTime(5, row3Time);
        stmt.setTimestamp(6, row3Timestamp);
        stmt.execute();
        
        conn.commit();
        conn.close();
    }

    @Test
    public void testKeyFilterWithIntegerValue() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_string) = 1";
        int expectedId = 1;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testKeyFilterWithDoubleValue() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_string) = 2.2";
        int expectedId = 2;
        runOneRowQueryTest(query, expectedId);
    }

    @Test
    public void testNonKeyFilterWithIntegerValue() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(b_string) = 1";
        int expectedId = 1;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testNonKeyFilterWithDoubleValue() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(b_string) = 2.2";
        int expectedId = 2;
        runOneRowQueryTest(query, expectedId);
    }

    @Test
    public void testKeyProjectionWithIntegerValue() throws Exception {
        String query = "select to_number(a_string) from " + TO_NUMBER_TABLE_NAME + " where a_id = 1";
        int expectedIntValue = 1;
        runOneRowQueryTest(query, expectedIntValue);
    }
    
    @Test
    public void testKeyProjectionWithDecimalValue() throws Exception {
        String query = "select to_number(a_string) from " + TO_NUMBER_TABLE_NAME + " where a_id = 2";
        BigDecimal expectedDecimalValue = (BigDecimal) PDecimal.INSTANCE.toObject("2.2");
        runOneRowQueryTest(query, expectedDecimalValue);
    }
    
    @Test
    public void testNonKeyProjectionWithIntegerValue() throws Exception {
        String query = "select to_number(b_string) from " + TO_NUMBER_TABLE_NAME + " where a_id = 1";
        int expectedIntValue = 1;
        runOneRowQueryTest(query, expectedIntValue);
    }
    
    @Test
    public void testNonKeyProjectionWithDecimalValue() throws Exception {
        String query = "select to_number(b_string) from " + TO_NUMBER_TABLE_NAME + " where a_id = 2";
        BigDecimal expectedDecimalValue = (BigDecimal) PDecimal.INSTANCE.toObject("2.2");
        runOneRowQueryTest(query, expectedDecimalValue);
    }
    
    @Test
    public void testKeyFilterWithPatternParam() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_string, '\u00A4###.####') = 3.3";
        int expectedId = 3;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testNonKeyFilterWithPatternParam() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(b_string, '\u00A4#.#') = 3.3";
        int expectedId = 3;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testDateFilter() throws Exception {
    	String pattern = "yyyyMMddHHmmssZ";
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_date, '" + pattern + "') = " + row1Date.getTime() ;
        int expectedId = 1;
        runOneRowQueryTest(query, expectedId);
    }
    
    
    @Test
    public void testTimeFilter() throws Exception {
    	String pattern = "HH:mm:ss z";
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_time, '" + pattern + "') = " + row1Time.getTime() ;
        int expectedId = 1;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testDateFilterWithoutPattern() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_date) = " + row2Date.getTime() ;
        int expectedId = 2;
        runOneRowQueryTest(query, expectedId);
    }
    
    
    @Test
    public void testTimeFilterWithoutPattern() throws Exception {
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_time) = " + row2Time.getTime() ;
        int expectedId = 2;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testTimeStampFilter() throws Exception {
    	String pattern = "yyMMddHHmmssZ";
        String query = "SELECT a_id FROM " + TO_NUMBER_TABLE_NAME + " WHERE to_number(a_timestamp, '" + pattern + "') = " + row1Timestamp.getTime() ;
        int expectedId = 1;
        runOneRowQueryTest(query, expectedId);
    }
    
    @Test
    public void testDateProjection() throws Exception {
        String query = "select to_number(a_date) from " + TO_NUMBER_TABLE_NAME + " where a_id = 1";
        BigDecimal expectedDecimalValue = new BigDecimal(row1Date.getTime());
        runOneRowQueryTest(query, expectedDecimalValue);
    }
    
    @Test
    public void testTimeProjection() throws Exception {
        String query = "select to_number(a_time) from " + TO_NUMBER_TABLE_NAME + " where a_id = 2";
        BigDecimal expectedDecimalValue = new BigDecimal(row2Time.getTime());
        runOneRowQueryTest(query, expectedDecimalValue);
    }
    
    @Test
    public void testTimeStampProjection() throws Exception {
        String query = "select to_number(a_timestamp) from " + TO_NUMBER_TABLE_NAME + " where a_id = 3";
        BigDecimal expectedDecimalValue = new BigDecimal(row3Timestamp.getTime());
        runOneRowQueryTest(query, expectedDecimalValue);
    }
    
    private void runOneRowQueryTest(String oneRowQuery, BigDecimal expectedDecimalValue) throws Exception {
    	runOneRowQueryTest(oneRowQuery, false, null, expectedDecimalValue);
    }
    
    private void runOneRowQueryTest(String oneRowQuery, int expectedIntValue) throws Exception {
    	runOneRowQueryTest(oneRowQuery, true, expectedIntValue, null);
    }
    
    private void runOneRowQueryTest(String oneRowQuery, boolean isIntegerColumn, Integer expectedIntValue, BigDecimal expectedDecimalValue) throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Connection conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(oneRowQuery);
            ResultSet rs = statement.executeQuery();
            
            assertTrue (rs.next());
            if (isIntegerColumn)
            	assertEquals(expectedIntValue.intValue(), rs.getInt(1));
            else
            	assertTrue(expectedDecimalValue == rs.getBigDecimal(1) || (expectedDecimalValue != null && expectedDecimalValue.compareTo(rs.getBigDecimal(1)) == 0));
            assertFalse(rs.next());
        }
        finally {
        	conn.close();
        }
    }
}