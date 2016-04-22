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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.HBASE_NATIVE;
import static org.apache.phoenix.util.TestUtil.HBASE_NATIVE_SCHEMA_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class DropMetadataIT extends BaseHBaseManagedTimeIT {
    private static final byte[] HBASE_NATIVE_BYTES = SchemaUtil.getTableNameAsBytes(HBASE_NATIVE_SCHEMA_NAME, HBASE_NATIVE);
    private static final byte[] FAMILY_NAME = Bytes.toBytes(SchemaUtil.normalizeIdentifier("1"));
    public static final String SCHEMA_NAME = "";
    public static final String DATA_TABLE_NAME = "T";
    public static final String INDEX_TABLE_NAME = "I";
    public static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    public static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");
    private final String TENANT_SPECIFIC_URL = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant1";
    
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    @BeforeClass 
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Drop the HBase table metadata for this test
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testDropViewKeepsHTable() throws Exception {
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
        try {
            try {
                admin.disableTable(HBASE_NATIVE_BYTES);
                admin.deleteTable(HBASE_NATIVE_BYTES);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            }
            @SuppressWarnings("deprecation")
            HTableDescriptor descriptor = new HTableDescriptor(HBASE_NATIVE_BYTES);
            HColumnDescriptor columnDescriptor =  new HColumnDescriptor(FAMILY_NAME);
            columnDescriptor.setKeepDeletedCells(true);
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
        } finally {
            admin.close();
        }
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create view " + HBASE_NATIVE +
                "   (uint_key unsigned_int not null," +
                "    ulong_key unsigned_long not null," +
                "    string_key varchar not null,\n" +
                "    \"1\".uint_col unsigned_int," +
                "    \"1\".ulong_col unsigned_long" +
                "    CONSTRAINT pk PRIMARY KEY (uint_key, ulong_key, string_key))\n" +
                     HColumnDescriptor.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE + "'");
        conn.createStatement().execute("drop view " + HBASE_NATIVE);
        
        admin = driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).getAdmin();
        try {
            try {
                admin.disableTable(HBASE_NATIVE_BYTES);
                admin.deleteTable(HBASE_NATIVE_BYTES);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                fail(); // The underlying HBase table should still exist
            }
        } finally {
            admin.close();
        }
    }
    
    @Test
    public void testDroppingIndexedColDropsIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String localIndexTableName1 = "LOCAL_" + INDEX_TABLE_NAME + "_1";
        String localIndexTableName2 = "LOCAL_" + INDEX_TABLE_NAME + "_2";
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            conn.createStatement().execute(
                "CREATE TABLE " + DATA_TABLE_FULL_NAME
                        + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            // create one regular and two local indexes
            conn.createStatement().execute(
                "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v2) INCLUDE (v1)");
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + localIndexTableName1 + " ON " + DATA_TABLE_FULL_NAME + " (v2) INCLUDE (v1)");
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + localIndexTableName2 + " ON " + DATA_TABLE_FULL_NAME + " (k) INCLUDE (v1)");
            
            // upsert a single row
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            conn.commit();
            
            // verify the indexes were created
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PTable dataTable = pconn.getTable(new PTableKey(null, DATA_TABLE_FULL_NAME));
            assertEquals("Unexpected number of indexes ", 3, dataTable.getIndexes().size());
            PTable indexTable = dataTable.getIndexes().get(0);
            byte[] indexTablePhysicalName = indexTable.getPhysicalName().getBytes();
            PName localIndexTablePhysicalName = dataTable.getIndexes().get(1).getPhysicalName();
            
            // drop v2 which causes the regular index and first local index to be dropped
            conn.createStatement().execute(
                "ALTER TABLE " + DATA_TABLE_FULL_NAME + " DROP COLUMN v2 ");

            // verify the both of the indexes' metadata were dropped
            conn.createStatement().execute("SELECT * FROM "+DATA_TABLE_FULL_NAME);
            try {
                conn.createStatement().execute("SELECT * FROM "+INDEX_TABLE_NAME);
                fail("Index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            pconn = conn.unwrap(PhoenixConnection.class);
            dataTable = pconn.getTable(new PTableKey(null, DATA_TABLE_FULL_NAME));
            try {
                pconn.getTable(new PTableKey(null, INDEX_TABLE_NAME));
                fail("index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            try {
                pconn.getTable(new PTableKey(null, localIndexTableName1));
                fail("index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            assertEquals("Unexpected number of indexes ", 1, dataTable.getIndexes().size());
            
            // verify that the regular index physical table was dropped
            try {
                conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(indexTablePhysicalName);
                fail("Index table should have been dropped");
            } catch (TableNotFoundException e) {
            }
            
            // verify that the local index physical table was *not* dropped
            conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(localIndexTablePhysicalName.getBytes());
            
            // there should be a single row belonging to localIndexTableName2 
            Scan scan = new Scan();
            HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(localIndexTablePhysicalName.getBytes());
            ResultScanner results = table.getScanner(scan);
            Result result = results.next();
            assertNotNull(result);
            assertNotNull("localIndexTableName2 row is missing", result.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, 
                IndexUtil.getIndexColumnName(QueryConstants.DEFAULT_COLUMN_FAMILY, "V1").getBytes()));
            assertNull(results.next());
        }
    }
    
    @Test
    public void testDroppingIndexedColDropsViewIndex() throws Exception {
        helpTestDroppingIndexedColDropsViewIndex(false);
    }
    
    @Test
    public void testDroppingIndexedColDropsMultiTenantViewIndex() throws Exception {
        helpTestDroppingIndexedColDropsViewIndex(true);
    }
    
    public void helpTestDroppingIndexedColDropsViewIndex(boolean isMultiTenant) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL) : conn ) {
            conn.setAutoCommit(false);
            viewConn.setAutoCommit(false);
            String ddlFormat = "CREATE TABLE TABLEWITHVIEW (%s k VARCHAR NOT NULL, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR, v4 VARCHAR CONSTRAINT PK PRIMARY KEY(%s k))%s";
            String ddl = String.format(ddlFormat, isMultiTenant ? "TENANT_ID VARCHAR NOT NULL, " : "",
                    isMultiTenant ? "TENANT_ID, " : "", isMultiTenant ? "MULTI_TENANT=true" : "");
            conn.createStatement().execute(ddl);
            viewConn.createStatement()
                    .execute(
                        "CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM TABLEWITHVIEW");
            // create an index with the column that will be dropped
            viewConn.createStatement().execute("CREATE INDEX VIEWINDEX1 ON VIEWOFTABLE(v2) INCLUDE (v4)");
            // create an index without the column that will be dropped
            viewConn.createStatement().execute("CREATE INDEX VIEWINDEX2 ON VIEWOFTABLE(v1) INCLUDE (v4)");
            // verify index was created
            try {
                viewConn.createStatement().execute("SELECT * FROM VIEWINDEX1");
            } catch (TableNotFoundException e) {
                fail("Index on view was not created");
            }
            
            // upsert a single row
            PreparedStatement stmt = viewConn.prepareStatement("UPSERT INTO VIEWOFTABLE VALUES(?,?,?,?,?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "b");
            stmt.setString(3, "c");
            stmt.setString(4, "d");
            stmt.setString(5, "e");
            stmt.setInt(6, 1);
            stmt.setString(7, "g");
            stmt.execute();
            viewConn.commit();

            // verify the index was created
            PhoenixConnection pconn = viewConn.unwrap(PhoenixConnection.class);
            PName tenantId = isMultiTenant ? PNameFactory.newName("tenant1") : null; 
            PTable view = pconn.getTable(new PTableKey(tenantId, "VIEWOFTABLE"));
            PTable viewIndex = pconn.getTable(new PTableKey(tenantId, "VIEWINDEX1"));
            byte[] viewIndexPhysicalTable = viewIndex.getPhysicalName().getBytes();
            assertNotNull("Can't find view index", viewIndex);
            assertEquals("Unexpected number of indexes ", 2, view.getIndexes().size());
            assertEquals("Unexpected index ", "VIEWINDEX1", view.getIndexes().get(0).getName()
                    .getString());
            assertEquals("Unexpected index ", "VIEWINDEX2", view.getIndexes().get(1).getName()
                .getString());
            
            // drop two columns
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW DROP COLUMN v2, v3 ");
            
            // verify columns were dropped
            try {
                conn.createStatement().execute("SELECT v2 FROM TABLEWITHVIEW");
                fail("Column should have been dropped");
            } catch (ColumnNotFoundException e) {
            }
            try {
                conn.createStatement().execute("SELECT v3 FROM TABLEWITHVIEW");
                fail("Column should have been dropped");
            } catch (ColumnNotFoundException e) {
            }
            
            // verify index metadata was dropped
            try {
                viewConn.createStatement().execute("SELECT * FROM VIEWINDEX1");
                fail("Index metadata should have been dropped");
            } catch (TableNotFoundException e) {
            }
            
            pconn = viewConn.unwrap(PhoenixConnection.class);
            view = pconn.getTable(new PTableKey(tenantId, "VIEWOFTABLE"));
            try {
                viewIndex = pconn.getTable(new PTableKey(tenantId, "VIEWINDEX1"));
                fail("View index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            assertEquals("Unexpected number of indexes ", 1, view.getIndexes().size());
            assertEquals("Unexpected index ", "VIEWINDEX2", view.getIndexes().get(0).getName().getString());
            
            // verify that the physical index view table is *not* dropped
            conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(viewIndexPhysicalTable);
            
            // scan the physical table and verify there is a single row for the second local index
            Scan scan = new Scan();
            HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(viewIndexPhysicalTable);
            ResultScanner results = table.getScanner(scan);
            Result result = results.next();
            assertNotNull(result);
            // there should be a single row belonging to VIEWINDEX2 
            assertNotNull("VIEWINDEX2 row is missing", result.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, 
                IndexUtil.getIndexColumnName(QueryConstants.DEFAULT_COLUMN_FAMILY, "V4").getBytes()));
            assertNull(results.next());
        }
    }
}
        
