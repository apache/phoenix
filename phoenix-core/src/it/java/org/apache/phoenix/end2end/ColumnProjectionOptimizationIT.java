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
import static org.apache.phoenix.util.TestUtil.MDTEST_NAME;
import static org.apache.phoenix.util.TestUtil.MDTEST_SCHEMA_NAME;
import static org.apache.phoenix.util.TestUtil.MULTI_CF_NAME;
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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;


public class ColumnProjectionOptimizationIT extends BaseClientManagedTimeIT {

    @Test
    public void testSelect() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(getUrl(), props);

        // Table wildcard query
        String query = "SELECT * FROM aTable";
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
            query = "SELECT A_STRING, A_INTEGER FROM aTable";
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
            query = "SELECT B_STRING, A_SHORT FROM aTable WHERE X_INTEGER = ?";
            statement = conn.prepareStatement(query);
            statement.setInt(1, 4);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(C_VALUE, rs.getString(1));
            assertEquals(135, rs.getShort(2));
            assertFalse(rs.next());

            // Select only specific columns with condition on another column (Not in select) and one row elements are
            // nulls
            query = "SELECT X_LONG, X_INTEGER, Y_INTEGER FROM aTable WHERE B_STRING = ?";
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
            query = "SELECT A_STRING, A_INTEGER FROM aTable WHERE A_INTEGER = ?";
            statement = conn.prepareStatement(query);
            statement.setInt(1, 9);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(C_VALUE, rs.getString(1));
            assertEquals(9, rs.getInt(2));

            // Select all columns with order by on non PK column
            query = "SELECT * FROM aTable ORDER BY A_INTEGER";
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
        byte[] htableName = SchemaUtil.getTableNameAsBytes(MDTEST_SCHEMA_NAME, MDTEST_NAME);
        HBaseAdmin admin = pconn.getQueryServices().getAdmin();

        @SuppressWarnings("deprecation")
        HTableDescriptor descriptor = new HTableDescriptor(htableName);
        for (byte[] familyName : familyNames) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(familyName);
            descriptor.addFamily(columnDescriptor);
        }
        admin.createTable(descriptor);

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        Connection conn1 = DriverManager.getConnection(getUrl(), props);

        String createStmt = "create view " + MDTEST_NAME + " (id integer not null primary key,"
                + " b.col1 integer, c.col2 bigint, c.col3 varchar(20))";
        conn1.createStatement().execute(createStmt);
        conn1.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 6));
        PhoenixConnection conn2 = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        byte[] c1 = Bytes.toBytes("COL1");
        byte[] c2 = Bytes.toBytes("COL2");
        byte[] c3 = Bytes.toBytes("COL3");
        HTableInterface htable = null;
        try {
            htable = conn2.getQueryServices().getTable(htableName);
            Put put = new Put(PInteger.INSTANCE.toBytes(1));
            put.add(cfB, c1, ts + 6, PInteger.INSTANCE.toBytes(1));
            put.add(cfC, c2, ts + 6, PLong.INSTANCE.toBytes(2));
            htable.put(put);

            put = new Put(PInteger.INSTANCE.toBytes(2));
            put.add(cfC, c2, ts + 6, PLong.INSTANCE.toBytes(10));
            put.add(cfC, c3, ts + 6, PVarchar.INSTANCE.toBytes("abcd"));
            htable.put(put);

            put = new Put(PInteger.INSTANCE.toBytes(3));
            put.add(cfB, c1, ts + 6, PInteger.INSTANCE.toBytes(3));
            put.add(cfC, c2, ts + 6, PLong.INSTANCE.toBytes(10));
            put.add(cfC, c3, ts + 6, PVarchar.INSTANCE.toBytes("abcd"));
            htable.put(put);

            conn2.close();

            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
            Connection conn7 = DriverManager.getConnection(getUrl(), props);
            String select = "SELECT id, b.col1 FROM " + MDTEST_NAME + " WHERE c.col2=?";
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
            select = "SELECT b.* FROM " + MDTEST_NAME + " WHERE c.col2=?";
            ps = conn7.prepareStatement(select);
            ps.setInt(1, 10);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());

            select = "SELECT b.* FROM " + MDTEST_NAME;
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
            admin.disableTable(htableName);
            admin.deleteTable(htableName);
            admin.close();
        }
    }

    
    private static void initMultiCFTable(long ts) throws Exception {
        String url = getUrl();
        ensureTableCreated(url, MULTI_CF_NAME, ts);

        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                    "MULTI_CF(" +
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
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectWithConditionOnMultiCF() throws Exception {
        long ts = nextTimestamp();
        initMultiCFTable(ts);
        
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT c.db_cpu_utilization FROM MULTI_CF WHERE a.unique_user_count = ? and b.unique_org_count = ?";
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
