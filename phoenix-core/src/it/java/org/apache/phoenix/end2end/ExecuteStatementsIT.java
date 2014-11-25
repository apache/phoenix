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
import static org.apache.phoenix.util.TestUtil.BTABLE_NAME;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.PTSDB_NAME;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;


public class ExecuteStatementsIT extends BaseHBaseManagedTimeIT {
    
    @Test
    public void testExecuteStatements() throws Exception {
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());
        String statements = 
            "create table if not exists " + ATABLE_NAME + // Shouldn't error out b/c of if not exists clause
            "   (organization_id char(15) not null, \n" + 
            "    entity_id char(15) not null,\n" + 
            "    a_string varchar(100),\n" + 
            "    b_string varchar(100)\n" +
            "    CONSTRAINT pk PRIMARY KEY (organization_id,entity_id));\n" + 
            "create table " + PTSDB_NAME +
            "   (inst varchar null,\n" + 
            "    host varchar null,\n" + 
            "    date date not null,\n" + 
            "    val decimal\n" +
            "    CONSTRAINT pk PRIMARY KEY (inst,host,date))\n" +
            "    split on (?,?,?);\n" +
            "alter table " + PTSDB_NAME + " add if not exists val decimal;\n" +  // Shouldn't error out b/c of if not exists clause
            "alter table " + PTSDB_NAME + " drop column if exists blah;\n" +  // Shouldn't error out b/c of if exists clause
            "drop table if exists FOO.BAR;\n" + // Shouldn't error out b/c of if exists clause
            "UPSERT INTO " + PTSDB_NAME + "(date, val, host) " +
            "    SELECT current_date(), x_integer+2, entity_id FROM ATABLE WHERE a_integer >= ?;" +
            "UPSERT INTO " + PTSDB_NAME + "(date, val, inst)\n" +
            "    SELECT date+1, val*10, host FROM " + PTSDB_NAME + ";";
        
        Date now = new Date(System.currentTimeMillis());
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        List<Object> binds = Arrays.<Object>asList("a","j","s", 6);
        int nStatements = PhoenixRuntime.executeStatements(conn, new StringReader(statements), binds);
        assertEquals(7, nStatements);

        Date then = new Date(System.currentTimeMillis() + QueryConstants.MILLIS_IN_DAY);
        String query = "SELECT host,inst, date,val FROM " + PTSDB_NAME + " where inst is not null";
        PreparedStatement statement = conn.prepareStatement(query);
        
        ResultSet rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW6, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW7, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(70).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW8, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(60).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW9, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(50).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testCharPadding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = "foo";
        String rowKey = "hello"; 
        String testString = "world";
        String query = "create table " + tableName +
                "(a_id integer not null, \n" + 
                "a_string char(10) not null, \n" +
                "b_string char(8)\n" + 
                "CONSTRAINT my_pk PRIMARY KEY (a_id, a_string))";
        
    
        PreparedStatement statement = conn.prepareStatement(query);
        statement.execute();
        statement = conn.prepareStatement(
                "upsert into " + tableName +
                "    (a_id, " +
                "    a_string, " +
                "    b_string)" +
                "VALUES (?, ?, ?)");
        statement.setInt(1, 1);
        statement.setString(2, rowKey);
        statement.setString(3, testString);
        statement.execute();       
        conn.commit();
        
        ensureTableCreated(getUrl(),BTABLE_NAME, null, nextTimestamp()-2);
        statement = conn.prepareStatement(
                "upsert into BTABLE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        statement.setString(1, "abc");
        statement.setString(2, "xyz");
        statement.setString(3, "x");
        statement.setInt(4, 9);
        statement.setString(5, "ab");
        statement.setInt(6, 1);
        statement.setInt(7, 1);
        statement.setString(8, "ab");
        statement.setString(9, "morning1");
        statement.execute();       
        conn.commit();
        try {
            // test rowkey and non-rowkey values in select statement
            query = "select a_string, b_string from " + tableName;
            assertCharacterPadding(conn.prepareStatement(query), rowKey, testString);
            
            // test with rowkey  in where clause
            query = "select a_string, b_string from " + tableName + " where a_id = 1 and a_string = '" + rowKey + "'";
            assertCharacterPadding(conn.prepareStatement(query), rowKey, testString);
            
            // test with non-rowkey  in where clause
            query = "select a_string, b_string from " + tableName + " where b_string = '" + testString + "'";
            assertCharacterPadding(conn.prepareStatement(query), rowKey, testString);
            
            // test with rowkey and id  in where clause
            query = "select a_string, b_string from " + tableName + " where a_id = 1 and a_string = '" + rowKey + "'";
            assertCharacterPadding(conn.prepareStatement(query), rowKey, testString);
            
            // test with rowkey and id  in where clause where rowkey is greater than the length of the char.len
            query = "select a_string, b_string from " + tableName + " where a_id = 1 and a_string  = '" + rowKey + testString + "'";
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
            
            // test with rowkey and id  in where clause where rowkey is lesser than the length of the char.len
            query = "select a_string, b_string from " + tableName + " where a_id = 1 and a_string  = 'he'";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertFalse(rs.next());
            
            String rowKey2 = "good"; 
            String testString2 = "morning";
            String testString8Char = "morning1";
            String testString10Char = "morning123";
            String upsert = "UPSERT INTO " + tableName + " values (2, '" + rowKey2 + "', '" + testString2+ "') ";
            statement = conn.prepareStatement(upsert);
            statement.execute();
            conn.commit();
            
            // test upsert statement with padding
            String tenantId = getOrganizationId();
            initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());
            
            upsert = "UPSERT INTO " + tableName + "(a_id, a_string, b_string) " +
                    "SELECT A_INTEGER, A_STRING, B_STRING FROM ATABLE WHERE a_string = ?";
            
            statement = conn.prepareStatement(upsert);
            statement.setString(1, A_VALUE);
            int rowsInserted = statement.executeUpdate();
            assertEquals(4, rowsInserted);
            conn.commit();
            
            query = "select a_string, b_string from " + tableName + " where a_string  = '" + A_VALUE+"'";
            assertCharacterPadding(conn.prepareStatement(query), A_VALUE, B_VALUE);            
            
            upsert = "UPSERT INTO " + tableName + " values (3, '" + testString2 + "', '" + testString2+ "') ";
            statement = conn.prepareStatement(upsert);
            statement.execute();
            conn.commit();
            query = "select a_string, b_string from " + tableName + "  where a_id = 3 and a_string = b_string";
            assertCharacterPadding(conn.prepareStatement(query), testString2, testString2);
            
            // compare a higher length col with lower length : a_string(10), b_string(8) 
            query = "select a_string, b_string from " + tableName + "  where a_id = 3 and b_string = a_string";
            statement = conn.prepareStatement(query);
            statement.executeQuery();
            assertCharacterPadding(conn.prepareStatement(query), testString2, testString2);
            
            upsert = "UPSERT INTO " + tableName + " values (4, '" + rowKey2 + "', '" + rowKey2 + "') ";
            statement = conn.prepareStatement(upsert);
            statement.execute();
            conn.commit();
            
            // where both the columns have same value with different paddings
            query = "select a_string, b_string from " + tableName + "  where a_id = 4 and b_string = a_string";
            assertCharacterPadding(conn.prepareStatement(query), rowKey2, rowKey2);
            
            upsert = "UPSERT INTO " + tableName + " values (5, '" + testString10Char + "', '" + testString8Char + "') ";
            statement = conn.prepareStatement(upsert);
            statement.execute();
            conn.commit();
            
            // where smaller column is the subset of larger string
            query = "select a_string, b_string from " + tableName + "  where a_id = 5 and b_string = a_string";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertFalse(rs.next());
            
            //where selecting from a CHAR(x) and upserting into a CHAR(y) where x>y
            // upsert rowkey value greater than rowkey limit
            try {
                
                upsert = "UPSERT INTO " + tableName + "(a_id, a_string, b_string) " +
                        "SELECT x_integer, organization_id, b_string FROM ATABLE WHERE a_string = ?";
                
                statement = conn.prepareStatement(upsert);
                statement.setString(1, A_VALUE);
                statement.executeUpdate();
                fail("Should fail when bigger than expected character is inserted");
            } catch (SQLException ex) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(), ex.getErrorCode());
            }
            
            // upsert non-rowkey value greater than its limit
            try {
                
                upsert = "UPSERT INTO " + tableName + "(a_id, a_string, b_string) " +
                        "SELECT y_integer, a_string, entity_id FROM ATABLE WHERE a_string = ?";
                
                statement = conn.prepareStatement(upsert);
                statement.setString(1, A_VALUE);
                statement.executeUpdate();
                fail("Should fail when bigger than expected character is inserted");
            }
            catch (SQLException ex) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(), ex.getErrorCode());
            }
                        
            //where selecting from a CHAR(x) and upserting into a CHAR(y) where x<=y.
            upsert = "UPSERT INTO " + tableName + "(a_id, a_string, b_string) " +
                    "SELECT a_integer, e_string, a_id FROM BTABLE";
            
            statement = conn.prepareStatement(upsert);
            rowsInserted = statement.executeUpdate();
            assertEquals(1, rowsInserted);
            conn.commit();
            
            query = "select a_string, b_string from " + tableName + " where a_string  = 'morning1'";
            assertCharacterPadding(conn.prepareStatement(query), "morning1", "xyz");
        } finally {
            conn.close();
        }
    }
    
    
    private void assertCharacterPadding(PreparedStatement statement, String rowKey, String testString) throws SQLException {
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(rowKey, rs.getString(1));
        assertEquals(testString, rs.getString(2));
    }
    
}
