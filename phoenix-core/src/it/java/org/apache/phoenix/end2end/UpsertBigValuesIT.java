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

import org.junit.Test;


public class UpsertBigValuesIT extends BaseHBaseManagedTimeIT {

    private static final long INTEGER_MIN_MINUS_ONE = (long)Integer.MIN_VALUE - 1;
    private static final long INTEGER_MAX_PLUS_ONE = (long)Integer.MAX_VALUE + 1;

    @Test
    public void testIntegerPK() throws Exception {
        int[] testNumbers = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1,
                -2, -1, 0, 1, 2, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
        ensureTableCreated(getUrl(),"PKIntValueTest");
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO PKIntValueTest VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i = 0; i < testNumbers.length; i++) {
            stmt.setInt(1, testNumbers[i]);
            stmt.execute();
        }
        conn.commit();
        conn.close();
        
        String select = "SELECT COUNT(*) from PKIntValueTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM PKIntValueTest where pk >= " + Integer.MIN_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM PKIntValueTest where pk >= " + Integer.MIN_VALUE + 
                " GROUP BY pk ORDER BY pk ASC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        // NOTE: This case currently fails with an error message:
        // "Overflow trying to get next key for [-1, -1, -1, -1]"
        select = "SELECT count(*) FROM PKIntValueTest where pk <= " + Integer.MAX_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM PKIntValueTest where pk <= " + Integer.MAX_VALUE + 
                " GROUP BY pk ORDER BY pk DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = testNumbers.length - 1; i >= 0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        // NOTE: This case currently fails since it is not retrieving the negative values.
        select = "SELECT count(*) FROM PKIntValueTest where pk >= " + INTEGER_MIN_MINUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM PKIntValueTest where pk >= " + INTEGER_MIN_MINUS_ONE + 
                " GROUP BY pk ORDER BY pk ASC NULLS LAST ";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        // NOTE: This test case fails because it is not retrieving positive values.
        select = "SELECT count(*) FROM PKIntValueTest where pk <= " + INTEGER_MAX_PLUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM PKIntValueTest where pk <= " + INTEGER_MAX_PLUS_ONE + 
                " GROUP BY pk ORDER BY pk DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = testNumbers.length - 1; i >= 0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testBigIntPK() throws Exception {
      // NOTE: Due to how we parse negative long, -9223372036854775808L, the minimum value of 
      // bigint is not recognizable in the current version. As a result, we start with 
      // Long.MIN_VALUE+1 as the smallest value.
        long[] testNumbers = {Long.MIN_VALUE+1 , Long.MIN_VALUE+2 , 
                -2L, -1L, 0L, 1L, 2L, Long.MAX_VALUE-1, Long.MAX_VALUE};
        ensureTableCreated(getUrl(),"PKBigIntValueTest");
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO PKBigIntValueTest VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i=0; i<testNumbers.length; i++) {
            stmt.setLong(1, testNumbers[i]);
            stmt.execute();
        }
        conn.commit();
        conn.close();
        
        String select = "SELECT COUNT(*) from PKBigIntValueTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM PKBigIntValueTest where pk >= " + (Long.MIN_VALUE + 1);
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM PKBigIntValueTest WHERE pk >= " + (Long.MIN_VALUE + 1) +
                " GROUP BY pk ORDER BY pk ASC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getLong(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM PKBigIntValueTest where pk <= " + Long.MAX_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM PKBigIntValueTest WHERE pk <= " + Long.MAX_VALUE + 
                " GROUP BY pk ORDER BY pk DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = testNumbers.length - 1; i >= 0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getLong(1));
        }
        assertFalse(rs.next());
        
        /* NOTE: This section currently fails due to the fact that we cannot parse literal values
           that are bigger than Long.MAX_VALUE and Long.MIN_VALUE. We will need to fix the parse
           before enabling this section of the test.
        select = "SELECT count(*) FROM PKBigIntValueTest where pk >= " + LONG_MIN_MINUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM PKBigIntValueTest WHERE pk >= " + LONG_MIN_MINUS_ONE +
                " GROUP BY pk ORDER BY pk ASC NULLS LAST ";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getLong(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM PKBigIntValueTest where pk <= " + LONG_MAX_PLUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM PKBigIntValueTest WHERE pk <= " + LONG_MAX_PLUS_ONE +
                " GROUP BY pk ORDER BY pk DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = testNumbers.length-1; i >= 0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getLong(1));
        }
        assertFalse(rs.next());
        */
    }

    @Test
    public void testIntegerKV() throws Exception {
        int[] testNumbers = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, 
                -2, -1, 0, 1, 2, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
        ensureTableCreated(getUrl(),"KVIntValueTest");
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO KVIntValueTest VALUES(?, ?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i=0; i<testNumbers.length; i++) {
            stmt.setInt(1, i);
            stmt.setInt(2, testNumbers[i]);
            stmt.execute();
        }
        conn.commit();
        conn.close();
        
        String select = "SELECT COUNT(*) from KVIntValueTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM KVIntValueTest where kv >= " + Integer.MIN_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM KVIntValueTest WHERE kv >= " + Integer.MIN_VALUE +
                " GROUP BY kv ORDER BY kv ASC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i=0; i<testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM KVIntValueTest where kv <= " + Integer.MAX_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM KVIntValueTest WHERE kv <= " + Integer.MAX_VALUE +
                " GROUP BY kv ORDER BY kv DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i=testNumbers.length-1; i>=0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM KVIntValueTest where kv >= " + INTEGER_MIN_MINUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM KVIntValueTest WHERE kv >= " + INTEGER_MIN_MINUS_ONE +
                " GROUP BY kv ORDER BY kv ASC NULLS LAST ";
        rs = conn.createStatement().executeQuery(select);
        for (int i=0; i<testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM KVIntValueTest where kv <= " + INTEGER_MAX_PLUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM KVIntValueTest WHERE kv <= " + INTEGER_MAX_PLUS_ONE +
                " GROUP BY kv ORDER BY kv DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i=testNumbers.length-1; i>=0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testBigIntKV() throws Exception {
        // NOTE: Due to how we parse negative long, -9223372036854775808L, the minimum value of 
        // bigint is not recognizable in the current version. As a result, we start with 
        // Long.MIN_VALUE+1 as the smallest value.
        long[] testNumbers = {Long.MIN_VALUE+1, Long.MIN_VALUE+2, 
                -2L, -1L, 0L, 1L, 2L, Long.MAX_VALUE-1, Long.MAX_VALUE};
        ensureTableCreated(getUrl(),"KVBigIntValueTest");
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO KVBigIntValueTest VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i = 0; i < testNumbers.length; i++) {
            stmt.setLong(1, i);
            stmt.setLong(2, testNumbers[i]);
            stmt.execute();
        }
        conn.commit();
        conn.close();
        
        String select = "SELECT COUNT(*) from KVBigIntValueTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM KVBigIntValueTest where kv >= " + (Long.MIN_VALUE+1);
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM KVBigIntValueTest WHERE kv >= " + (Long.MIN_VALUE+1) + 
                " GROUP BY kv ORDER BY kv ASC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getLong(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM KVBigIntValueTest where kv <= " + Long.MAX_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM KVBigIntValueTest WHERE kv <= " + Long.MAX_VALUE +
                " GROUP BY kv ORDER BY kv DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = testNumbers.length-1; i >= 0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getLong(1));
        }
        assertFalse(rs.next());
        
        /* NOTE: This section currently fails due to the fact that we cannot parse literal values
           that are bigger than Long.MAX_VALUE and Long.MIN_VALUE. We will need to fix the parse
           before enabling this section of the test.
        select = "SELECT count(*) FROM KVBigIntValueTest where kv >= " + LONG_MIN_MINUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM KVBigIntValueTest WHERE kv >= " + LONG_MIN_MINUS_ONE +
                " GROUP BY kv ORDER BY kv ASC NULLS LAST ";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM KVBigIntValueTest where kv <= " + LONG_MAX_PLUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM KVBigIntValueTest WHERE kv <= " + LONG_MAX_PLUS_ONE +
                " GROUP BY kv ORDER BY kv DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = testNumbers.length-1; i >= 0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        */
    }
}
