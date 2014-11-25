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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;


public class ModulusExpressionIT extends BaseHBaseManagedTimeIT {
    
    private static final long SMALL_VALUE = 31L;
    private static final long LARGE_VALUE = 0x5dec6f3847021a9bL;
    
    private static final long[] DIVIDENDS = {Long.MAX_VALUE, LARGE_VALUE, SMALL_VALUE, 0, -SMALL_VALUE, -LARGE_VALUE, Long.MIN_VALUE};
    private static final long[] DIVISORS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 31, 127, 1024};
    
    private void initTable(Connection conn, long value) throws SQLException {
        String ddl = "CREATE TABLE MODULUS_TEST (pk BIGINT NOT NULL PRIMARY KEY, kv BIGINT)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO MODULUS_TEST VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setLong(1, value);
        stmt.execute();
        conn.commit();        
    }
    
    private void testDividend(long dividend) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, dividend);
        
        for(long divisor : DIVISORS) {
            long remainder = dividend % divisor;
            String sql = "SELECT pk % " + divisor + " FROM MODULUS_TEST";
            
            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(remainder, rs.getLong(1));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testSmallPositiveDividend() throws SQLException {
        testDividend(SMALL_VALUE);
    }
    
    @Test
    public void testLargePositiveDividend() throws SQLException {
        testDividend(LARGE_VALUE);
    }
    
    @Test
    public void testLongMaxDividend() throws SQLException {
        testDividend(Long.MAX_VALUE);
    }      
    
    @Test
    public void testSmallNegativeDividend() throws Exception {
        testDividend(-1 * SMALL_VALUE);
    }
    
    @Test
    public void testLargeNegativeDividend() throws SQLException {
        testDividend(-1 * LARGE_VALUE);
    }
    
    @Test
    public void testLongMinDividend() throws SQLException {
        testDividend(Long.MIN_VALUE);
    }   
    
    @Test
    public void testZeroDividend() throws SQLException {
        testDividend(0);
    }
    
    @Test
    public void testZeroDivisor() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, 0);
        
        for(long dividend : DIVIDENDS) {
            try {
                String sql = "SELECT " + dividend + " % pk FROM MODULUS_TEST";

                // workaround for parser not being able to parse Long.MIN_VALUE
                // see: https://issues.apache.org/jira/browse/PHOENIX-1061
                if(dividend == Long.MIN_VALUE) {
                    sql = "SELECT (" + (dividend + 1) + " + -1) % pk FROM MODULUS_TEST";
                }

                ResultSet rs = conn.createStatement().executeQuery(sql);
                rs.next();
                rs.getLong(1);
                fail("modulus by zero: dividend: " + dividend + ". divisor : 0");
            }
            catch (ArithmeticException ex) {
                // success
            }
        }
    }
    
    @Test
    public void testNullDividend() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, SMALL_VALUE);
        
        for(long divisor : DIVISORS) {
            String sql = "SELECT kv % " + divisor + " FROM MODULUS_TEST";
            
            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testNullDivisor() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, SMALL_VALUE);
        
        for(long dividend : DIVIDENDS) {
            String sql = "SELECT " + dividend + " % kv FROM MODULUS_TEST";
            
            // workaround for parser not being able to parse Long.MIN_VALUE
            // see: https://issues.apache.org/jira/browse/PHOENIX-1061
            if(dividend == Long.MIN_VALUE) {
                sql = "SELECT (" + (dividend + 1) + " + -1) % kv FROM MODULUS_TEST";
            }
            
            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testNullEverything() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, SMALL_VALUE);
        
        String sql = "SELECT null % kv FROM MODULUS_TEST";
        
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertNull(rs.getObject(1));
        assertFalse(rs.next());
        
        sql = "SELECT kv % null FROM MODULUS_TEST";
        
        rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertNull(rs.getObject(1));
        assertFalse(rs.next());
    }
    
}
