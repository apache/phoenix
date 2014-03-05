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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.CSVLoader;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;

public class CSVLoaderTest extends BaseHBaseManagedTimeTest {

	private static final String DATATYPE_TABLE = "DATATYPE";
	private static final String DATATYPES_CSV_VALUES = "CKEY, CVARCHAR, CINTEGER, CDECIMAL, CUNSIGNED_INT, CBOOLEAN, CBIGINT, CUNSIGNED_LONG, CTIME, CDATE\n" + 
			"KEY1,A,2147483647,1.1,0,TRUE,9223372036854775807,0,1990-12-31 10:59:59,1999-12-31 23:59:59\n" + 
			"KEY2,B,-2147483648,-1.1,2147483647,FALSE,-9223372036854775808,9223372036854775807,2000-01-01 00:00:01,2012-02-29 23:59:59\n";
	private static final String STOCK_TABLE = "STOCK_SYMBOL";
	private static final String STOCK_TABLE_CASESENSITIVE = "STOCK_SYMBOL_CASESENSITIVE";
	private static final String STOCK_CSV_VALUES = 
			"AAPL,APPLE Inc.\n" + 
			"CRM,SALESFORCE\n" + 
			"GOOG,Google\n" + 
			"HOG,Harlet-Davidson Inc.\n" + 
			"HPQ,Hewlett Packard\n" + 
			"INTC,Intel\n" + 
			"MSFT,Microsoft\n" + 
			"WAG,Walgreens\n" + 
			"WMT,Walmart\n";
    private static final String[] STOCK_COLUMNS_WITH_BOGUS = new String[] {"SYMBOL", "BOGUS"};
    private static final String[] STOCK_COLUMNS = new String[] {"SYMBOL", "COMPANY"};
    private static final String[] STOCK_COLUMNS_CASESENSITIVE = new String[] {"SYMBOL", "\"Company\""};
    private static final String STOCK_CSV_VALUES_WITH_HEADER =  STOCK_COLUMNS[0] + "," + STOCK_COLUMNS[1] + "\n" + STOCK_CSV_VALUES;
    private static final String STOCK_CSV_VALUES_WITH_DELIMITER = "APPL" + '\u0001' + '\u0002' + "APPLE\n" +
                                                                  " Inc" + '\u0002' + "\n" +
                                                                  "MSFT" + '\u0001' + "Microsoft\n";

    
    @Test
    public void testCSVUpsert() throws Exception {
    	// Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Collections.<String>emptyList(), true);
		CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES_WITH_HEADER));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES_WITH_HEADER));
        reader.readNext();
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
        	assertTrue (phoenixResultSet.next());
        	for (int i=0; i<csvData.length; i++) {
        		assertEquals(csvData[i], phoenixResultSet.getString(i+1));
        	}
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithCustomDelimiters() throws Exception {
     // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);

        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.<String>asList(STOCK_COLUMNS), true, Arrays.asList("1","2","3"));
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES_WITH_DELIMITER),'\u0001','\u0002','\u0003');
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES_WITH_DELIMITER),'\u0001','\u0002','\u0003');
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            for (int i=0; i<csvData.length; i++) {
                assertEquals(csvData[i], phoenixResultSet.getString(i+1));
            }
        }

        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithColumns() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.<String>asList(STOCK_COLUMNS), true);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            for (int i=0; i<csvData.length; i++) {
                assertEquals(csvData[i], phoenixResultSet.getString(i+1));
            }
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    
    @Test
    public void testCSVUpsertWithNoColumns() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, null, true);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            for (int i=0; i<csvData.length; i++) {
                assertEquals(csvData[i], phoenixResultSet.getString(i+1));
            }
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithBogusColumn() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.asList(STOCK_COLUMNS_WITH_BOGUS), false);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, COMPANY FROM " + STOCK_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            assertEquals(csvData[0], phoenixResultSet.getString(1));
            assertNull(phoenixResultSet.getString(2));
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithAllColumn() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.asList("FOO","BAR"), false);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        try {
            csvUtil.upsert(reader);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 504 (42703): Undefined column. columnName=STOCK_SYMBOL.[FOO, BAR]"));
        }
        conn.close();
    }
    
    @Test
    public void testCSVUpsertWithBogusColumnStrict() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE, Arrays.asList(STOCK_COLUMNS_WITH_BOGUS), true);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        try {
            csvUtil.upsert(reader);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 504 (42703): Undefined column. columnName=STOCK_SYMBOL.BOGUS"));
        }
        conn.close();
    }

    @Test
    public void testAllDatatypes() throws Exception {
    	// Create table
        String statements = "CREATE TABLE IF NOT EXISTS " 
        	    + DATATYPE_TABLE +
        		" (CKEY VARCHAR NOT NULL PRIMARY KEY," +
        		"  CVARCHAR VARCHAR, CINTEGER INTEGER, CDECIMAL DECIMAL(31,10), CUNSIGNED_INT UNSIGNED_INT, CBOOLEAN BOOLEAN, CBIGINT BIGINT, CUNSIGNED_LONG UNSIGNED_LONG, CTIME TIME, CDATE DATE);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, DATATYPE_TABLE, Collections.<String>emptyList(), true); 
		CSVReader reader = new CSVReader(new StringReader(DATATYPES_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
		PreparedStatement statement = conn
				.prepareStatement("SELECT CKEY, CVARCHAR, CINTEGER, CDECIMAL, CUNSIGNED_INT, CBOOLEAN, CBIGINT, CUNSIGNED_LONG, CTIME, CDATE FROM "
						+ DATATYPE_TABLE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(DATATYPES_CSV_VALUES));
        reader.readNext();
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
        	assertTrue (phoenixResultSet.next());
        	for (int i=0; i<csvData.length - 2; i++) {
        		assertEquals(csvData[i], phoenixResultSet.getObject(i+1).toString().toUpperCase());
        	}
        	// special case for matching date, time values
        	assertEquals(DateUtil.parseTime(csvData[8]), phoenixResultSet.getTime("CTIME"));
        	assertEquals(DateUtil.parseDate(csvData[9]), phoenixResultSet.getDate("CDATE"));
        }
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    @Test
    public void testCaseSensitiveCSVUpsertWithColumns() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE_CASESENSITIVE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, \"Company\" VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE_CASESENSITIVE, Arrays.<String>asList(STOCK_COLUMNS_CASESENSITIVE), true);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, \"Company\" FROM " + STOCK_TABLE_CASESENSITIVE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            for (int i=0; i<csvData.length; i++) {
                assertEquals(csvData[i], phoenixResultSet.getString(i+1));
            }
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
    
    
    @Test
    public void testCaseSensitiveCSVUpsertWithNoColumns() throws Exception {
        // Create table
        String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE_CASESENSITIVE + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, \"Company\" VARCHAR);";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);
        
        // Upsert CSV file
        CSVLoader csvUtil = new CSVLoader(conn, STOCK_TABLE_CASESENSITIVE, null, true);
        CSVReader reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        csvUtil.upsert(reader);

        // Compare Phoenix ResultSet with CSV file content
        PreparedStatement statement = conn.prepareStatement("SELECT SYMBOL, \"Company\" FROM " + STOCK_TABLE_CASESENSITIVE);
        ResultSet phoenixResultSet = statement.executeQuery();
        reader = new CSVReader(new StringReader(STOCK_CSV_VALUES));
        String[] csvData;
        while ((csvData = reader.readNext()) != null) {
            assertTrue (phoenixResultSet.next());
            for (int i=0; i<csvData.length; i++) {
                assertEquals(csvData[i], phoenixResultSet.getString(i+1));
            }
        }
        
        assertFalse(phoenixResultSet.next());
        conn.close();
    }
}
