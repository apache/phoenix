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

package org.apache.phoenix.end2end.salted;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class SaltedTableVarLengthRowKeyIT extends ParallelStatsDisabledIT {

    private static final String TEST_TABLE = generateUniqueName();

    private static void initTableValues() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            createTestTable(getUrl(), "create table " + TEST_TABLE + " " +
                    " (key_string varchar not null primary key, kv integer) SALT_BUCKETS=4\n");
            String query = "UPSERT INTO " + TEST_TABLE + " VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "a");
            stmt.setInt(2, 1);
            stmt.execute();
            
            stmt.setString(1, "ab");
            stmt.setInt(2, 2);
            stmt.execute();
            
            stmt.setString(1, "abc");
            stmt.setInt(2, 3);
            stmt.execute();
            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValueWithPointKeyQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            initTableValues();
            String query;
            PreparedStatement stmt;
            ResultSet rs;
            
            query = "SELECT * FROM " + TEST_TABLE + " where key_string = 'abc'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSaltedVarbinaryUpperBoundQuery() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName +
                " ( k VARBINARY PRIMARY KEY, a INTEGER ) SALT_BUCKETS = 3";
        String dml = "UPSERT INTO " + tableName + " values (?, ?)";
        String sql2 = "SELECT * FROM " + tableName + " WHERE k = ?";

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setInt(2, 1);

            stmt.setBytes(1, new byte[] { 5 });
            stmt.executeUpdate();
            stmt.setBytes(1, new byte[] { 5, 0 });
            stmt.executeUpdate();
            stmt.setBytes(1, new byte[] { 5, 1 });
            stmt.executeUpdate();
            stmt.close();
            conn.commit();

            stmt = conn.prepareStatement(sql2);
            stmt.setBytes(1, new byte[] { 5 });
            ResultSet rs = stmt.executeQuery();

            assertTrue(rs.next());
            assertArrayEquals(new byte[] {5},rs.getBytes(1));
            assertEquals(1,rs.getInt(2));
            assertFalse(rs.next());
            stmt.close();
        }
    }

    @Test
    public void testSaltedArrayTypeUpperBoundQuery() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName +
                " ( k TINYINT ARRAY[10] PRIMARY KEY, a INTEGER ) SALT_BUCKETS = 3";
        String dml = "UPSERT INTO " + tableName + " values (?, ?)";
        String sql2 = "SELECT * FROM " + tableName + " WHERE k = ?";

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setInt(2, 1);

            Byte[] byteArray1 = ArrayUtils.toObject(new byte[] {5});
            Byte[] byteArray2 = ArrayUtils.toObject(new byte[] {5, -128});
            Byte[] byteArray3 = ArrayUtils.toObject(new byte[] {5, -127});


            Array array1 = conn.createArrayOf("TINYINT", byteArray1);
            Array array2 = conn.createArrayOf("TINYINT", byteArray2);
            Array array3 = conn.createArrayOf("TINYINT", byteArray3);

            stmt.setArray(1,array1);
            stmt.executeUpdate();
            stmt.setArray(1,array2);
            stmt.executeUpdate();
            stmt.setArray(1,array3);
            stmt.executeUpdate();
            stmt.close();
            conn.commit();

            stmt = conn.prepareStatement(sql2);
            stmt.setArray(1, array1);
            ResultSet rs = stmt.executeQuery();

            assertTrue(rs.next());
            byte[] resultByteArray = (byte[])(rs.getArray(1).getArray());
            assertArrayEquals(new byte[]{5},resultByteArray);
            assertEquals(1,rs.getInt(2));
            assertFalse(rs.next());
            stmt.close();
        }
    }
}
