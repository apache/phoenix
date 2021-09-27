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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(ParallelStatsDisabledTest.class)
public class UpsertBigValuesIT extends ParallelStatsDisabledIT {

    private static final long INTEGER_MIN_MINUS_ONE = (long)Integer.MIN_VALUE - 1;
    private static final long INTEGER_MAX_PLUS_ONE = (long)Integer.MAX_VALUE + 1;

    @Test
    public void testIntegerPK() throws Exception {
        int[] testNumbers = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1,
                -2, -1, 0, 1, 2, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName,"PKIntValueTest");
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO " + tableName + " VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i = 0; i < testNumbers.length; i++) {
            stmt.setInt(1, testNumbers[i]);
            stmt.execute();
        }
        conn.commit();
        
        String select = "SELECT COUNT(*) from " + tableName ;
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where pk >= " + Integer.MIN_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM " + tableName + " where pk >= " + Integer.MIN_VALUE +
                " GROUP BY pk ORDER BY pk ASC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        // NOTE: This case currently fails with an error message:
        // "Overflow trying to get next key for [-1, -1, -1, -1]"
        select = "SELECT count(*) FROM " + tableName + " where pk <= " + Integer.MAX_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM " + tableName + " where pk <= " + Integer.MAX_VALUE +
                " GROUP BY pk ORDER BY pk DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = testNumbers.length - 1; i >= 0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        // NOTE: This case currently fails since it is not retrieving the negative values.
        select = "SELECT count(*) FROM " + tableName + " where pk >= " + INTEGER_MIN_MINUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM " + tableName + " where pk >= " + INTEGER_MIN_MINUS_ONE +
                " GROUP BY pk ORDER BY pk ASC NULLS LAST ";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        // NOTE: This test case fails because it is not retrieving positive values.
        select = "SELECT count(*) FROM " + tableName + " where pk <= " + INTEGER_MAX_PLUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM " + tableName + " where pk <= " + INTEGER_MAX_PLUS_ONE +
                " GROUP BY pk ORDER BY pk DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = testNumbers.length - 1; i >= 0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testBigIntPK() throws Exception {
      // NOTE: Due to how we parse negative long, -9223372036854775808L, the minimum value of 
      // bigint is not recognizable in the current version. As a result, we start with 
      // Long.MIN_VALUE+1 as the smallest value.
        String tableName = generateUniqueName();
        long[] testNumbers = {Long.MIN_VALUE+1 , Long.MIN_VALUE+2 , 
                -2L, -1L, 0L, 1L, 2L, Long.MAX_VALUE-1, Long.MAX_VALUE};
        ensureTableCreated(getUrl(), tableName, "PKBigIntValueTest" );
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO " + tableName + " VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i=0; i<testNumbers.length; i++) {
            stmt.setLong(1, testNumbers[i]);
            stmt.execute();
        }
        conn.commit();
        
        String select = "SELECT COUNT(*) from " + tableName ;
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where pk >= " + (Long.MIN_VALUE + 1);
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM " + tableName + " WHERE pk >= " + (Long.MIN_VALUE + 1) +
                " GROUP BY pk ORDER BY pk ASC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getLong(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where pk <= " + Long.MAX_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT pk FROM " + tableName + " WHERE pk <= " + Long.MAX_VALUE +
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
        conn.close();
    }

    @Test
    public void testIntegerKV() throws Exception {
        String tableName = generateUniqueName();
        int[] testNumbers = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, 
                -2, -1, 0, 1, 2, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
        ensureTableCreated(getUrl(), tableName, "KVIntValueTest" );
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO " + tableName + " VALUES(?, ?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i=0; i<testNumbers.length; i++) {
            stmt.setInt(1, i);
            stmt.setInt(2, testNumbers[i]);
            stmt.execute();
        }
        conn.commit();
        
        String select = "SELECT COUNT(*) from " + tableName ;
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where kv >= " + Integer.MIN_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM " + tableName + " WHERE kv >= " + Integer.MIN_VALUE +
                " GROUP BY kv ORDER BY kv ASC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i=0; i<testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where kv <= " + Integer.MAX_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM " + tableName + " WHERE kv <= " + Integer.MAX_VALUE +
                " GROUP BY kv ORDER BY kv DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i=testNumbers.length-1; i>=0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where kv >= " + INTEGER_MIN_MINUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM " + tableName + " WHERE kv >= " + INTEGER_MIN_MINUS_ONE +
                " GROUP BY kv ORDER BY kv ASC NULLS LAST ";
        rs = conn.createStatement().executeQuery(select);
        for (int i=0; i<testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where kv <= " + INTEGER_MAX_PLUS_ONE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM " + tableName + " WHERE kv <= " + INTEGER_MAX_PLUS_ONE +
                " GROUP BY kv ORDER BY kv DESC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i=testNumbers.length-1; i>=0; i--) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getInt(1));
        }
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testBigIntKV() throws Exception {
        // NOTE: Due to how we parse negative long, -9223372036854775808L, the minimum value of 
        // bigint is not recognizable in the current version. As a result, we start with 
        // Long.MIN_VALUE+1 as the smallest value.
        String tableName = generateUniqueName();
        long[] testNumbers = {Long.MIN_VALUE+1, Long.MIN_VALUE+2, 
                -2L, -1L, 0L, 1L, 2L, Long.MAX_VALUE-1, Long.MAX_VALUE};
        ensureTableCreated(getUrl(), tableName, "KVBigIntValueTest" );
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO " + tableName + " VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i = 0; i < testNumbers.length; i++) {
            stmt.setLong(1, i);
            stmt.setLong(2, testNumbers[i]);
            stmt.execute();
        }
        conn.commit();
        
        String select = "SELECT COUNT(*) from " + tableName ;
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where kv >= " + (Long.MIN_VALUE+1);
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM " + tableName + " WHERE kv >= " + (Long.MIN_VALUE+1) + 
                " GROUP BY kv ORDER BY kv ASC NULLS LAST";
        rs = conn.createStatement().executeQuery(select);
        for (int i = 0; i < testNumbers.length; i++) {
            assertTrue(rs.next());
            assertEquals(testNumbers[i], rs.getLong(1));
        }
        assertFalse(rs.next());
        
        select = "SELECT count(*) FROM " + tableName + " where kv <= " + Long.MAX_VALUE;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(testNumbers.length, rs.getInt(1));
        assertFalse(rs.next());
        select = "SELECT kv FROM " + tableName + " WHERE kv <= " + Long.MAX_VALUE +
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
        conn.close();
    }

    @Test
    public void testShort() throws Exception {
        List<Short> testData =
                Arrays.asList(Short.MIN_VALUE, Short.MAX_VALUE, (short) (Short.MIN_VALUE + 1),
                    (short) (Short.MAX_VALUE - 1), (short) 0, (short) 1, (short) -1);
        testValues(false, PSmallint.INSTANCE, testData);
        testValues(true, PSmallint.INSTANCE, testData);
    }

    @Test
    public void testBigInt() throws Exception {
        List<Long> testData =
                Arrays.asList(Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE + 1L,
                    Long.MAX_VALUE - 1L, 0L, 1L, -1L);
        testValues(false, PLong.INSTANCE, testData);
        testValues(true, PLong.INSTANCE, testData);
    }

    private <T extends Number> void testValues(boolean immutable, PDataType<?> dataType, List<T> testData) throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                String.format("CREATE %s TABLE %s (K INTEGER PRIMARY KEY, V1 %s)",
                    immutable ? "IMMUTABLE" : "", tableName, dataType.getSqlTypeName());
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            String upsert = "UPSERT INTO " + tableName + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            int id = 1;
            for (T testVal : testData) {
                stmt.setInt(1, id++);
                stmt.setObject(2, testVal, dataType.getSqlType());
                stmt.execute();
            }
            conn.commit();
            String query = String.format("SELECT K,V1 FROM %s ORDER BY K ASC", tableName);
            ResultSet rs = conn.createStatement().executeQuery(query);
            int index = 0;
            boolean failed = false;
            List<String> errors = Lists.newArrayList();
            while (rs.next()) {
                Number resultVal = rs.getObject(2, testData.get(0).getClass());
                T testVal = testData.get(index++);
                if (!testVal.equals(resultVal)) {
                    errors.add(String.format("[expected=%s actual=%s] ",
                        testVal, resultVal));
                    failed = true;
                }
            }
            String errorMsg =
                    String.format("Data in table didn't match input: immutable=%s, dataType=%s, %s",
                        immutable, dataType.getSqlTypeName(), errors);
            assertFalse(errorMsg, failed);
        }
    }

}