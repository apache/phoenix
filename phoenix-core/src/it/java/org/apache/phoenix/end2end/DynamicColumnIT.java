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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic tests for Phoenix dynamic upserting
 * 
 * 
 * @since 1.3
 */


public class DynamicColumnIT extends ParallelStatsDisabledIT {
    private static final byte[] FAMILY_NAME_A = Bytes.toBytes(SchemaUtil.normalizeIdentifier("A"));
    private static final byte[] FAMILY_NAME_B = Bytes.toBytes(SchemaUtil.normalizeIdentifier("B"));

    private String tableName;

    @Before
    public void initTable() throws Exception {
        tableName = generateUniqueName();
        try (PhoenixConnection pconn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class)) {
            ConnectionQueryServices services = pconn.getQueryServices();
            try (HBaseAdmin admin = services.getAdmin()) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                htd.addFamily(new HColumnDescriptor(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES));
                htd.addFamily(new HColumnDescriptor(FAMILY_NAME_A));
                htd.addFamily(new HColumnDescriptor(FAMILY_NAME_B));
                admin.createTable(htd);
            }

            try (HTableInterface hTable = services.getTable(Bytes.toBytes(tableName))) {
                // Insert rows using standard HBase mechanism with standard HBase "types"
                List<Row> mutations = new ArrayList<Row>();
                byte[] dv = Bytes.toBytes("DV");
                byte[] first = Bytes.toBytes("F");
                byte[] f1v1 = Bytes.toBytes("F1V1");
                byte[] f1v2 = Bytes.toBytes("F1V2");
                byte[] f2v1 = Bytes.toBytes("F2V1");
                byte[] f2v2 = Bytes.toBytes("F2V2");
                byte[] key = Bytes.toBytes("entry1");

                Put put = new Put(key);
                put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, dv, Bytes.toBytes("default"));
                put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, first, Bytes.toBytes("first"));
                put.add(FAMILY_NAME_A, f1v1, Bytes.toBytes("f1value1"));
                put.add(FAMILY_NAME_A, f1v2, Bytes.toBytes("f1value2"));
                put.add(FAMILY_NAME_B, f2v1, Bytes.toBytes("f2value1"));
                put.add(FAMILY_NAME_B, f2v2, Bytes.toBytes("f2value2"));
                mutations.add(put);

                hTable.batch(mutations);

                // Create Phoenix table after HBase table was created through the native APIs
                // The timestamp of the table creation must be later than the timestamp of the data
                pconn.createStatement().execute("create table " + tableName + 
                "   (entry varchar not null," +
                "    F varchar," +
                "    A.F1v1 varchar," +
                "    A.F1v2 varchar," +
                "    B.F2v1 varchar" +
                "    CONSTRAINT pk PRIMARY KEY (entry))");
            }

        }
    }

    /**
     * Test a simple select with a dynamic Column
     */
    @Test
    public void testDynamicColums() throws Exception {
        String query = "SELECT * FROM " + tableName + " (DV varchar)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("entry1", rs.getString(1));
            assertEquals("first", rs.getString(2));
            assertEquals("f1value1", rs.getString(3));
            assertEquals("f1value2", rs.getString(4));
            assertEquals("f2value1", rs.getString(5));
            assertEquals("default", rs.getString(6));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test a select with a colum family.column dynamic Column
     */
    @Test
    public void testDynamicColumsFamily() throws Exception {
        String query = "SELECT * FROM " + tableName + " (DV varchar,B.F2V2 varchar)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("entry1", rs.getString(1));
            assertEquals("first", rs.getString(2));
            assertEquals("f1value1", rs.getString(3));
            assertEquals("f1value2", rs.getString(4));
            assertEquals("f2value1", rs.getString(5));
            assertEquals("default", rs.getString(6));
            assertEquals("f2value2", rs.getString(7));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test a select with a colum family.column dynamic Column and check the value
     */

    @Test
    public void testDynamicColumsSpecificQuery() throws Exception {
        String query = "SELECT entry,F2V2 FROM " + tableName + " (DV varchar,B.F2V2 varchar)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("entry1", rs.getString(1));
            assertEquals("f2value2", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of prexisting schema defined columns and dynamic ones with different datatypes
     */
    @Test(expected = ColumnAlreadyExistsException.class)
    public void testAmbiguousStaticSelect() throws Exception {
        String upsertquery = "Select * FROM " + tableName + "(A.F1V1 INTEGER)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.executeQuery();
        } finally {
            conn.close();
        }
    }

    /**
     * Test a select of an undefined ColumnFamily dynamic columns
     */
    @Test(expected = ColumnFamilyNotFoundException.class)
    public void testFakeCFDynamicUpsert() throws Exception {
        String upsertquery = "Select * FROM " + tableName + "(fakecf.DynCol VARCHAR)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.executeQuery();
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDynamicColumnOnNewTable() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "create table " + tableName + 
            "   (entry varchar not null," +
            "    F varchar," +
            "    A.F1v1 varchar," +
            "    A.F1v2 varchar," +
            "    B.F2v1 varchar" +
            "    CONSTRAINT pk PRIMARY KEY (entry))";
        String dml = "UPSERT INTO " + tableName + " values (?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            try (PreparedStatement stmt = conn.prepareStatement(dml)) {
                stmt.setString(1, "entry");
                stmt.setString(2, "a");
                stmt.setString(3, "b");
                stmt.setString(4, "c");
                stmt.setString(5, "d");
                stmt.executeUpdate();
                conn.commit();
            }
            dml = "UPSERT INTO " + tableName + "(entry, F, A.F1V1, A.F1v2, B.F2V1, DYNCOL1 VARCHAR, DYNCOL2 VARCHAR) VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(dml)) {
                stmt.setString(1, "dynentry");
                stmt.setString(2, "a");
                stmt.setString(3, "b");
                stmt.setString(4, "c");
                stmt.setString(5, "d");
                stmt.setString(6, "e");
                stmt.setString(7, "f");
                stmt.executeUpdate();
                conn.commit();
            }
            
            // test dynamic column in where clause
            String query = "SELECT entry, F from " + tableName + " (DYNCOL1 VARCHAR, DYNCOL2 VARCHAR) " + " WHERE DYNCOL1 = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, "e");
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals("dynentry", rs.getString(1));
                assertEquals("a", rs.getString(2));
                assertFalse(rs.next());
            }
            
            // test dynamic column with projection
            query = "SELECT entry, dyncol1, dyncol2 from " + tableName + " (DYNCOL1 VARCHAR, DYNCOL2 VARCHAR) ";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals("dynentry", rs.getString(1));
                assertEquals("e", rs.getString(2));
                assertEquals("f", rs.getString(3));
                assertTrue(rs.next());
                assertEquals("entry", rs.getString(1));
                assertEquals(null, rs.getString(2));
                assertEquals(null, rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

}
