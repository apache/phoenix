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

import static org.apache.phoenix.util.TestUtil.HBASE_DYNAMIC_COLUMNS;
import static org.apache.phoenix.util.TestUtil.HBASE_DYNAMIC_COLUMNS_SCHEMA_NAME;
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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Basic tests for Phoenix dynamic upserting
 * 
 * 
 * @since 1.3
 */


public class DynamicColumnIT extends BaseClientManagedTimeIT {
    private static final byte[] HBASE_DYNAMIC_COLUMNS_BYTES = SchemaUtil.getTableNameAsBytes(null, HBASE_DYNAMIC_COLUMNS);
    private static final byte[] FAMILY_NAME = Bytes.toBytes(SchemaUtil.normalizeIdentifier("A"));
    private static final byte[] FAMILY_NAME2 = Bytes.toBytes(SchemaUtil.normalizeIdentifier("B"));

    @BeforeClass
    public static void doBeforeTestSetup() throws Exception {
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).getAdmin();
        try {
            try {
                admin.disableTable(HBASE_DYNAMIC_COLUMNS_BYTES);
                admin.deleteTable(HBASE_DYNAMIC_COLUMNS_BYTES);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {}
            ensureTableCreated(getUrl(), HBASE_DYNAMIC_COLUMNS);
            initTableValues();
        } finally {
            admin.close();
        }
    }

    @SuppressWarnings("deprecation")
    private static void initTableValues() throws Exception {
        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        HTableInterface hTable = services.getTable(SchemaUtil.getTableNameAsBytes(HBASE_DYNAMIC_COLUMNS_SCHEMA_NAME,HBASE_DYNAMIC_COLUMNS));
        try {
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
            put.add(FAMILY_NAME, f1v1, Bytes.toBytes("f1value1"));
            put.add(FAMILY_NAME, f1v2, Bytes.toBytes("f1value2"));
            put.add(FAMILY_NAME2, f2v1, Bytes.toBytes("f2value1"));
            put.add(FAMILY_NAME2, f2v2, Bytes.toBytes("f2value2"));
            mutations.add(put);

            hTable.batch(mutations);

        } finally {
            hTable.close();
        }
        // Create Phoenix table after HBase table was created through the native APIs
        // The timestamp of the table creation must be later than the timestamp of the data
        ensureTableCreated(getUrl(), HBASE_DYNAMIC_COLUMNS);
    }

    /**
     * Test a simple select with a dynamic Column
     */
    @Test
    public void testDynamicColums() throws Exception {
        String query = "SELECT * FROM HBASE_DYNAMIC_COLUMNS (DV varchar)";
        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
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
        String query = "SELECT * FROM HBASE_DYNAMIC_COLUMNS (DV varchar,B.F2V2 varchar)";
        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
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
        String query = "SELECT entry,F2V2 FROM HBASE_DYNAMIC_COLUMNS (DV varchar,B.F2V2 varchar)";
        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
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
        String upsertquery = "Select * FROM HBASE_DYNAMIC_COLUMNS(A.F1V1 INTEGER)";
        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
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
        String upsertquery = "Select * FROM HBASE_DYNAMIC_COLUMNS(fakecf.DynCol VARCHAR)";
        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.executeQuery();
        } finally {
            conn.close();
        }
    }

}
