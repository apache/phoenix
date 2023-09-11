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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * 
 * Tests using native HBase types (i.e. Bytes.toBytes methods) for
 * integers and longs. Phoenix can support this if the numbers are
 * positive.
 *
 * 
 * @since 0.1
 */

@Category(ParallelStatsDisabledTest.class)
public class NativeHBaseTypesIT extends ParallelStatsDisabledIT {
    
    private String initTableValues() throws Exception {
        final String tableName = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
        final byte[] tableBytes = tableName.getBytes();
        final byte[] familyName = Bytes.toBytes(SchemaUtil.normalizeIdentifier("1"));
        final byte[][] splits = new byte[][] {Bytes.toBytes(20), Bytes.toBytes(30)};
        Admin admin = driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).getAdmin();
        try {
            admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(tableBytes))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(familyName)
                            .setKeepDeletedCells(KeepDeletedCells.TRUE).build())
                    .build(), splits);
        } finally {
            admin.close();
        }
        
        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        Table hTable = services.getTable(tableBytes);
        try {
            // Insert rows using standard HBase mechanism with standard HBase "types"
            List<Row> mutations = new ArrayList<Row>();
            byte[] family = Bytes.toBytes("1");
            byte[] uintCol = Bytes.toBytes("UINT_COL");
            byte[] ulongCol = Bytes.toBytes("ULONG_COL");
            byte[] key, bKey;
            Put put;
            
            bKey = key = ByteUtil.concat(Bytes.toBytes(20), Bytes.toBytes(200L), Bytes.toBytes("b"));
            put = new Put(key);
            put.addColumn(family, uintCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(5000));
            put.addColumn(family, ulongCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(50000L));
            mutations.add(put);
            // FIXME: the version of the Delete constructor without the lock args was introduced
            // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
            // of the client.
            long ts = EnvironmentEdgeManager.currentTimeMillis();
            Delete del = new Delete(key, ts);
            mutations.add(del);
            put = new Put(key);
            put.addColumn(family, uintCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(2000));
            put.addColumn(family, ulongCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(20000L));
            mutations.add(put);
            
            key = ByteUtil.concat(Bytes.toBytes(10), Bytes.toBytes(100L), Bytes.toBytes("a"));
            put = new Put(key);
            put.addColumn(family, uintCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(5));
            put.addColumn(family, ulongCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(50L));
            mutations.add(put);
            put = new Put(key);
            put.addColumn(family, uintCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(10));
            put.addColumn(family, ulongCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(100L));
            mutations.add(put);
            
            key = ByteUtil.concat(Bytes.toBytes(30), Bytes.toBytes(300L), Bytes.toBytes("c"));
            put = new Put(key);
            put.addColumn(family, uintCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(3000));
            put.addColumn(family, ulongCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(30000L));
            mutations.add(put);
            
            key = ByteUtil.concat(Bytes.toBytes(40), Bytes.toBytes(400L), Bytes.toBytes("d"));
            put = new Put(key);
            put.addColumn(family, uintCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(4000));
            put.addColumn(family, ulongCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(40000L));
            mutations.add(put);
            
            hTable.batch(mutations, null);
            
            Result r = hTable.get(new Get(bKey));
            assertFalse(r.isEmpty());
        } finally {
            hTable.close();
        }
        // Create Phoenix table after HBase table was created through the native APIs
        // The timestamp of the table creation must be later than the timestamp of the data
        String ddl = "create table " + tableName +
                "   (uint_key unsigned_int not null," +
                "    ulong_key unsigned_long not null," +
                "    string_key varchar not null,\n" +
                "    \"1\".uint_col unsigned_int," +
                "    \"1\".ulong_col unsigned_long" +
                "    CONSTRAINT pk PRIMARY KEY (uint_key, ulong_key, string_key))\n" +
                ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE + "'";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.createStatement().execute(ddl);
        } 
        
        return tableName;
    }
    
    @Test
    public void testRangeQuery1() throws Exception {
        String tableName = initTableValues();
        String query = "SELECT uint_key, ulong_key, string_key FROM " + tableName + " WHERE uint_key > 20 and ulong_key >= 400";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(40, rs.getInt(1));
            assertEquals(400L, rs.getLong(2));
            assertEquals("d", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRangeQuery2() throws Exception {
        String tableName = initTableValues();
        String query = "SELECT uint_key, ulong_key, string_key FROM " + tableName + " WHERE uint_key > 20 and uint_key < 40";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            assertEquals(300L, rs.getLong(2));
            assertEquals("c", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRangeQuery3() throws Exception {
        String tableName = initTableValues();
        String query = "SELECT uint_key, ulong_key, string_key FROM " + tableName + " WHERE ulong_key > 200 and ulong_key < 400";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            assertEquals(300L, rs.getLong(2));
            assertEquals("c", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNegativeAgainstUnsignedNone() throws Exception {
        String tableName = initTableValues();
        String query = "SELECT uint_key, ulong_key, string_key FROM " + tableName + " WHERE ulong_key < -1";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNegativeAgainstUnsignedAll() throws Exception {
        String tableName = initTableValues();
        String query = "SELECT string_key FROM " + tableName + " WHERE ulong_key > -100";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNegativeAddNegativeValue() throws Exception {
        String tableName = initTableValues();
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + "(uint_key,ulong_key,string_key, uint_col) VALUES(?,?,?,?)");
            stmt.setInt(1, -1);
            stmt.setLong(2, 2L);
            stmt.setString(3,"foo");
            stmt.setInt(4, 3);
            stmt.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }
    
    @Test
    public void testNegativeCompareNegativeValue() throws Exception {
        String tableName = initTableValues();
        String query = "SELECT string_key FROM " + tableName + " WHERE uint_key > 100000";
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        Table hTable = conn.getQueryServices().getTable(tableName.getBytes());
        
        List<Row> mutations = new ArrayList<Row>();
        byte[] family = Bytes.toBytes("1");
        byte[] uintCol = Bytes.toBytes("UINT_COL");
        byte[] ulongCol = Bytes.toBytes("ULONG_COL");
        byte[] key;
        Put put;
        
        // Need to use native APIs because the Phoenix APIs wouldn't let you insert a
        // negative number for an unsigned type
        key = ByteUtil.concat(Bytes.toBytes(-10), Bytes.toBytes(100L), Bytes.toBytes("e"));
        put = new Put(key);
        put.addColumn(family, uintCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(10));
        put.addColumn(family, ulongCol, HConstants.LATEST_TIMESTAMP, Bytes.toBytes(100L));
        put.addColumn(family, QueryConstants.EMPTY_COLUMN_BYTES, HConstants.LATEST_TIMESTAMP, ByteUtil.EMPTY_BYTE_ARRAY);
        mutations.add(put);
        hTable.batch(mutations, null);
    
        // Demonstrates weakness of HBase Bytes serialization. Negative numbers
        // show up as bigger than positive numbers
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals("e", rs.getString(1));
        assertFalse(rs.next());
    }
}
    
