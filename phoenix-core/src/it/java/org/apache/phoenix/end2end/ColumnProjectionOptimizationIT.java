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

import static org.apache.phoenix.util.TestUtil.A_VALUE;
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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class ColumnProjectionOptimizationIT extends ParallelStatsDisabledIT {

    @Test
    public void testSelect() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = initATableValues(tenantId, getDefaultSplits(tenantId));

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        // Table wildcard query
        String query = "SELECT * FROM " + tableName ;
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(ROW2, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(ROW4, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(ROW5, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(ROW6, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(ROW8, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(2));
            assertFalse(rs.next());

            // Select only specific columns
            query = "SELECT A_STRING, A_INTEGER FROM " + tableName ;
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(B_VALUE, rs.getString(1));
            assertEquals(5, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(B_VALUE, rs.getString(1));
            assertEquals(6, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(B_VALUE, rs.getString(1));
            assertEquals(7, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(B_VALUE, rs.getString(1));
            assertEquals(8, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(C_VALUE, rs.getString(1));
            assertEquals(9, rs.getInt(2));
            assertFalse(rs.next());

            // Select only specific columns with condition on another column (Not in select)
            query = "SELECT B_STRING, A_SHORT FROM " + tableName + " WHERE X_INTEGER = ?";
            statement = conn.prepareStatement(query);
            statement.setInt(1, 4);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(C_VALUE, rs.getString(1));
            assertEquals(135, rs.getShort(2));
            assertFalse(rs.next());

            // Select only specific columns with condition on another column (Not in select) and one row elements are
            // nulls
            query = "SELECT X_LONG, X_INTEGER, Y_INTEGER FROM " + tableName + " WHERE B_STRING = ?";
            statement = conn.prepareStatement(query);
            statement.setString(1, E_VALUE);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getInt(2));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getInt(3));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getInt(2));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getInt(3));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals(Integer.MAX_VALUE + 1L, rs.getLong(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(300, rs.getInt(3));
            assertFalse(rs.next());

            // Select only specific columns with condition on one of the selected column
            query = "SELECT A_STRING, A_INTEGER FROM " + tableName + " WHERE A_INTEGER = ?";
            statement = conn.prepareStatement(query);
            statement.setInt(1, 9);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(C_VALUE, rs.getString(1));
            assertEquals(9, rs.getInt(2));

            // Select all columns with order by on non PK column
            query = "SELECT * FROM " + tableName + " ORDER BY A_INTEGER";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(ROW2, rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(ROW3, rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(ROW4, rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(ROW5, rs.getString(2));
            assertEquals(B_VALUE, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(ROW6, rs.getString(2));
            assertEquals(B_VALUE, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(2));
            assertEquals(B_VALUE, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(ROW8, rs.getString(2));
            assertEquals(B_VALUE, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(2));
            assertEquals(C_VALUE, rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectFromViewOnExistingTable() throws Exception {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(
                PhoenixConnection.class);
        byte[] cfB = Bytes.toBytes(SchemaUtil.normalizeIdentifier("b"));
        byte[] cfC = Bytes.toBytes(SchemaUtil.normalizeIdentifier("c"));
        byte[][] familyNames = new byte[][] { cfB, cfC };
        String table = generateUniqueName();
        byte[] htableName = SchemaUtil.getTableNameAsBytes("", table);
        Admin admin = pconn.getQueryServices().getAdmin();

        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(htableName));
        for (byte[] familyName : familyNames) {
            builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName));
        }
        admin.createTable(builder.build());

        Properties props = new Properties();
        Connection conn1 = DriverManager.getConnection(getUrl(), props);

        String createStmt = "create view " + table + " (id integer not null primary key,"
                + " b.col1 integer, c.col2 bigint, c.col3 varchar(20))";
        conn1.createStatement().execute(createStmt);
        conn1.close();

        PhoenixConnection conn2 = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        byte[] c1 = Bytes.toBytes("COL1");
        byte[] c2 = Bytes.toBytes("COL2");
        byte[] c3 = Bytes.toBytes("COL3");
        Table htable = null;
        try {
            htable = conn2.getQueryServices().getTable(htableName);
            Put put = new Put(PInteger.INSTANCE.toBytes(1));
            put.addColumn(cfB, c1, PInteger.INSTANCE.toBytes(1));
            put.addColumn(cfC, c2, PLong.INSTANCE.toBytes(2));
            htable.put(put);

            put = new Put(PInteger.INSTANCE.toBytes(2));
            put.addColumn(cfC, c2, PLong.INSTANCE.toBytes(10));
            put.addColumn(cfC, c3, PVarchar.INSTANCE.toBytes("abcd"));
            htable.put(put);

            put = new Put(PInteger.INSTANCE.toBytes(3));
            put.addColumn(cfB, c1, PInteger.INSTANCE.toBytes(3));
            put.addColumn(cfC, c2, PLong.INSTANCE.toBytes(10));
            put.addColumn(cfC, c3, PVarchar.INSTANCE.toBytes("abcd"));
            htable.put(put);

            conn2.close();

            Connection conn7 = DriverManager.getConnection(getUrl(), props);
            String select = "SELECT id, b.col1 FROM " + table + " WHERE c.col2=?";
            PreparedStatement ps = conn7.prepareStatement(select);
            ps.setInt(1, 10);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());

            // Select contains only CF wildcards
            select = "SELECT b.* FROM " + table + " WHERE c.col2=?";
            ps = conn7.prepareStatement(select);
            ps.setInt(1, 10);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());

            select = "SELECT b.* FROM " + table;
            ps = conn7.prepareStatement(select);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            if (htable != null) htable.close();
            admin.disableTable(TableName.valueOf(htableName));
            admin.deleteTable(TableName.valueOf(htableName));
            admin.close();
        }
    }

    
    private static String initMultiCFTable() throws Exception {
        String url = getUrl();
        String tableName = generateUniqueName();
        Properties props = new Properties();
        String ddl = "create table " + tableName +
                "   (id char(15) not null primary key,\n" +
                "    a.unique_user_count integer,\n" +
                "    b.unique_org_count integer,\n" +
                "    c.db_cpu_utilization decimal(31,10),\n" +
                "    d.transaction_count bigint,\n" +
                "    e.cpu_utilization decimal(31,10),\n" +
                "    f.response_time bigint,\n" +
                "    g.response_time bigint)";
        try (Connection conn = DriverManager.getConnection(url, props)) {
            conn.createStatement().execute(ddl);
        }
        props = new Properties();
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tableName +
                    "(" +
                    "    id, " +
                    "    a.unique_user_count, " +
                    "    b.unique_org_count, " +
                    "    c.db_cpu_utilization) " +
                    "VALUES (?, ?, ?, ?)");
            stmt.setString(1, "id1");
            stmt.setInt(2, 1);
            stmt.setInt(3, 1);
            stmt.setBigDecimal(4, BigDecimal.valueOf(40.1));
            stmt.execute();

            stmt.setString(1, "id2");
            stmt.setInt(2, 2);
            stmt.setInt(3, 2);
            stmt.setBigDecimal(4, BigDecimal.valueOf(20.9));
            stmt.execute();
            conn.commit();
            return tableName;
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectWithConditionOnMultiCF() throws Exception {
        String tableName = initMultiCFTable();
        
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT c.db_cpu_utilization FROM " + tableName + " WHERE a.unique_user_count = ? and b.unique_org_count = ?";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setInt(1, 1);
            statement.setInt(2, 1);
            ResultSet rs = statement.executeQuery();
            boolean b = rs.next();
            assertTrue(b);
            assertEquals(BigDecimal.valueOf(40.1), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
