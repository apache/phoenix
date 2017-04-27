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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DropColumnIT extends ParallelStatsDisabledIT {
    
    private static final String PRINCIPAL = "dropColumn";
    public static final String SCHEMA_NAME = "";
    private final String TENANT_ID = "tenant1";
    private String tableDDLOptions;
    private boolean columnEncoded;
    private boolean mutable;
    
    private Connection getConnection() throws Exception {
        return getConnection(PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
    }
    
    private Connection getConnection(Properties props) throws Exception {
        props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Force real driver to be used as the test one doesn't handle creating
        // more than one ConnectionQueryService
        props.setProperty(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, StringUtil.EMPTY_STRING);
        // Create new ConnectionQueryServices so that we can set DROP_METADATA_ATTRIB
        String url = QueryUtil.getConnectionUrl(props, config, PRINCIPAL);
        return DriverManager.getConnection(url, props);
    }
    
    public DropColumnIT(boolean mutable, boolean columnEncoded) {
        StringBuilder optionBuilder = new StringBuilder();
        if (!columnEncoded) {
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (!mutable) {
            if (optionBuilder.length()>0)
                optionBuilder.append(",");
            optionBuilder.append("IMMUTABLE_ROWS=true");
            if (!columnEncoded) {
                optionBuilder.append(",IMMUTABLE_STORAGE_SCHEME="+PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
            }
        }
        this.mutable = mutable;
        this.columnEncoded = columnEncoded;
        this.tableDDLOptions = optionBuilder.toString();
    }
    
    @Parameters(name="DropColumnIT_mutable={0}, columnEncoded={1}") // name is used by failsafe as file name in reports
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false }, { false, true }, { true, false }, { true, true }, 
           });
    }
    
    @Test
    public void testDropCol() throws Exception {
        String indexTableName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String localIndexTableName = "LOCAL_" + indexTableName;
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            conn.createStatement().execute(
                "CREATE TABLE " + dataTableName
                        + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR) " + tableDDLOptions);
            // create one global and one local index
            conn.createStatement().execute(
                "CREATE INDEX " + indexTableName + " ON " + dataTableName + " (v1) INCLUDE (v2, v3)");
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + localIndexTableName + " ON " + dataTableName + " (v1) INCLUDE (v2, v3)");
            
            // upsert a single row
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.setString(4, "2");
            stmt.execute();
            conn.commit();
            
            // verify v2 exists in the data table
            PTable dataTable = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, dataTableName));
            PColumn dataColumn = dataTable.getColumnForColumnName("V2");
            byte[] dataCq = dataColumn.getColumnQualifierBytes();
            
            // verify v2 exists in the global index table
            PTable globalIndexTable = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, indexTableName));
            PColumn glovalIndexCol = globalIndexTable.getColumnForColumnName("0:V2");
            byte[] globalIndexCq = glovalIndexCol.getColumnQualifierBytes();
            
            // verify v2 exists in the global index table
            PTable localIndexTable = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, localIndexTableName));
            PColumn localIndexCol = localIndexTable.getColumnForColumnName("0:V2");
            byte[] localIndexCq = localIndexCol.getColumnQualifierBytes();
            
            verifyColValue(indexTableName, dataTableName, conn, dataTable, dataColumn, dataCq,
                    globalIndexTable, glovalIndexCol, globalIndexCq, localIndexTable,
                    localIndexCol, localIndexCq);
            
            // drop v2 column
            conn.createStatement().execute("ALTER TABLE " + dataTableName + " DROP COLUMN v2 ");
            conn.createStatement().execute("SELECT * FROM " + dataTableName);

            // verify that the column was dropped from the data table
            dataTable = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, dataTableName));
            try {
                dataTable.getColumnForColumnName("V2");
                fail("Column V2 should have been dropped from data table");
            }
            catch (ColumnNotFoundException e){
            }
            
            // verify that the column was dropped from the global index table
            globalIndexTable = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, indexTableName));
            try {
                globalIndexTable.getColumnForColumnName("V2");
                fail("Column V2 should have been dropped from global index table");
            }
            catch (ColumnNotFoundException e){
            }
            
            // verify that the column was dropped from the local index table
            localIndexTable = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, indexTableName));
            try {
                localIndexTable.getColumnForColumnName("V2");
                fail("Column V2 should have been dropped from global index table");
            }
            catch (ColumnNotFoundException e){
            }
            
            if (mutable || !columnEncoded) {
                byte[] key = Bytes.toBytes("a");
                Scan scan = new Scan();
                scan.setRaw(true);
                scan.setStartRow(key);
                scan.setStopRow(key);
                HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(dataTableName.getBytes());
                ResultScanner results = table.getScanner(scan);
                Result result = results.next();
                assertNotNull(result);
                
                assertEquals("data table column value should have been deleted", KeyValue.Type.DeleteColumn.getCode(), result.getColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, dataCq).get(0).getTypeByte());
                assertNull(results.next());
                
                // key value for v2 should have been deleted from the global index table
                scan = new Scan();
                scan.setRaw(true);
                table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(indexTableName.getBytes());
                results = table.getScanner(scan);
                result = results.next();
                assertNotNull(result);
                assertEquals("data table column value should have been deleted", KeyValue.Type.DeleteColumn.getCode(), result.getColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, globalIndexCq).get(0).getTypeByte());
                assertNull(results.next());
                
                // key value for v2 should have been deleted from the local index table
                scan = new Scan();
                scan.setRaw(true);
                scan.addFamily(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);
                table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(dataTableName.getBytes());
                results = table.getScanner(scan);
                result = results.next();
                assertNotNull(result);
                assertEquals("data table col"
                        + "umn value should have been deleted", KeyValue.Type.DeleteColumn.getCode(), result.getColumn(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES, localIndexCq).get(0).getTypeByte());
                assertNull(results.next()); 
            }
            else {
                // verify we don't issue deletes when we drop a column from an immutable encoded table
                verifyColValue(indexTableName, dataTableName, conn, dataTable, dataColumn, dataCq,
                    globalIndexTable, glovalIndexCol, globalIndexCq, localIndexTable,
                    localIndexCol, localIndexCq);
            }
        }
    }

    private void verifyColValue(String indexTableName, String dataTableName, Connection conn,
            PTable dataTable, PColumn dataColumn, byte[] dataCq, PTable globalIndexTable,
            PColumn glovalIndexCol, byte[] globalIndexCq, PTable localIndexTable,
            PColumn localIndexCol, byte[] localIndexCq)
            throws SQLException, IOException, ArrayComparisonFailure {
        // key value for v2 should exist in the data table
        Scan scan = new Scan();
        scan.setRaw(true);
        byte[] key = Bytes.toBytes("a");
        scan.setStartRow(key);
        scan.setStopRow(key);
        HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(dataTableName.getBytes());
        ResultScanner results = table.getScanner(scan);
        Result result = results.next();
        assertNotNull(result);
        byte[] colValue;
        if (!mutable && columnEncoded) {
            KeyValueColumnExpression colExpression = new SingleCellColumnExpression(dataColumn, "V2", dataTable.getEncodingScheme());
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            colExpression.evaluate(new ResultTuple(result), ptr);
            colValue = ptr.copyBytesIfNecessary();
        }
        else {
            colValue = result.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, dataCq);
        }
        assertArrayEquals("wrong column value for v2", Bytes.toBytes("1"), colValue);
        assertNull(results.next());
        
        // key value for v2 should exist in the global index table
        scan = new Scan();
        scan.setRaw(true);
        table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(indexTableName.getBytes());
        results = table.getScanner(scan);
        result = results.next();
        assertNotNull(result);
        if (!mutable && columnEncoded) {
            KeyValueColumnExpression colExpression = new SingleCellColumnExpression(glovalIndexCol, "0:V2", globalIndexTable.getEncodingScheme());
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            colExpression.evaluate(new ResultTuple(result), ptr);
            colValue = ptr.copyBytesIfNecessary();
        }
        else {
            colValue = result.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, globalIndexCq);
        }
        assertArrayEquals("wrong column value for v2", Bytes.toBytes("1"), colValue);
        assertNull(results.next());
        
        // key value for v2 should exist in the local index table
        scan = new Scan();
        scan.setRaw(true);
        scan.addFamily(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);
        table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(dataTableName.getBytes());
        results = table.getScanner(scan);
        result = results.next();
        assertNotNull(result);
        if (!mutable && columnEncoded) {
            KeyValueColumnExpression colExpression = new SingleCellColumnExpression(localIndexCol, "0:V2", localIndexTable.getEncodingScheme());
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            colExpression.evaluate(new ResultTuple(result), ptr);
            colValue = ptr.copyBytesIfNecessary();
        }
        else {
            colValue = result.getValue(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES, localIndexCq);
        }
        assertArrayEquals("wrong column value for v2", Bytes.toBytes("1"), colValue);
        assertNull(results.next());
    }
    
    @Test
    public void testDroppingIndexedColDropsIndex() throws Exception {
        String indexTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(SCHEMA_NAME, generateUniqueName());
        String localIndexTableName1 = "LOCAL_" + indexTableName + "_1";
        String localIndexTableName2 = "LOCAL_" + indexTableName + "_2";
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            conn.createStatement().execute(
                "CREATE TABLE " + dataTableFullName
                        + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " + tableDDLOptions);
            // create one regular and two local indexes
            conn.createStatement().execute(
                "CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (v2) INCLUDE (v1)");
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + localIndexTableName1 + " ON " + dataTableFullName + " (v2) INCLUDE (v1)");
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + localIndexTableName2 + " ON " + dataTableFullName + " (k) INCLUDE (v1)");
            
            // upsert a single row
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            conn.commit();
            
            // verify the indexes were created
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PTable dataTable = pconn.getTable(new PTableKey(null, dataTableFullName));
            assertEquals("Unexpected number of indexes ", 3, dataTable.getIndexes().size());
            PTable indexTable = dataTable.getIndexes().get(0);
            byte[] indexTablePhysicalName = indexTable.getPhysicalName().getBytes();
            PName localIndexTablePhysicalName = dataTable.getIndexes().get(1).getPhysicalName();
            
            // drop v2 which causes the regular index and first local index to be dropped
            conn.createStatement().execute(
                "ALTER TABLE " + dataTableFullName + " DROP COLUMN v2 ");

            // verify the both of the indexes' metadata were dropped
            conn.createStatement().execute("SELECT * FROM "+dataTableFullName);
            try {
                conn.createStatement().execute("SELECT * FROM "+indexTableName);
                fail("Index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            pconn = conn.unwrap(PhoenixConnection.class);
            dataTable = pconn.getTable(new PTableKey(null, dataTableFullName));
            try {
                pconn.getTable(new PTableKey(null, indexTableName));
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
            PTable localIndex2 = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, localIndexTableName2));
            
            // there should be a single row belonging to localIndexTableName2 
            Scan scan = new Scan();
            scan.addFamily(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);
            HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(localIndexTablePhysicalName.getBytes());
            ResultScanner results = table.getScanner(scan);
            Result result = results.next();
            assertNotNull(result);
            String indexColumnName = IndexUtil.getIndexColumnName(QueryConstants.DEFAULT_COLUMN_FAMILY, "V1");
            PColumn localIndexCol = localIndex2.getColumnForColumnName(indexColumnName);
            byte[] colValue;
            if (!mutable && columnEncoded) {
                KeyValueColumnExpression colExpression = new SingleCellColumnExpression(localIndexCol, indexColumnName, localIndex2.getEncodingScheme());
                ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                colExpression.evaluate(new ResultTuple(result), ptr);
                colValue = ptr.copyBytesIfNecessary();
            }
            else {
                colValue = result.getValue(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES, localIndexCol.getColumnQualifierBytes());
            }
            assertNotNull("localIndexTableName2 row is missing", colValue);
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
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(TENANT_ID_ATTRIB, TENANT_ID);
        try (Connection conn = getConnection();
                Connection viewConn = isMultiTenant ? getConnection(props) : conn ) {
            String tableWithView = generateUniqueName();
            String viewOfTable = generateUniqueName();
            String viewIndex1 = generateUniqueName();
            String viewIndex2 = generateUniqueName();
            
            conn.setAutoCommit(false);
            viewConn.setAutoCommit(false);
            String ddlFormat = "CREATE TABLE " + tableWithView + " (%s k VARCHAR NOT NULL, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR, v4 VARCHAR CONSTRAINT PK PRIMARY KEY(%s k))%s";
            String ddl = String.format(ddlFormat, isMultiTenant ? "TENANT_ID VARCHAR NOT NULL, " : "",
                    isMultiTenant ? "TENANT_ID, " : "", isMultiTenant ? "MULTI_TENANT=true" : "");
            conn.createStatement().execute(ddl);
            viewConn.createStatement()
                    .execute(
                        "CREATE VIEW " + viewOfTable + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM " + tableWithView );
            // create an index with the column that will be dropped
            viewConn.createStatement().execute("CREATE INDEX " + viewIndex1 + " ON " + viewOfTable + "(v2) INCLUDE (v4)");
            // create an index without the column that will be dropped
            viewConn.createStatement().execute("CREATE INDEX " + viewIndex2 + " ON " + viewOfTable + "(v1) INCLUDE (v4)");
            // verify index was created
            try {
                viewConn.createStatement().execute("SELECT * FROM " + viewIndex1 );
            } catch (TableNotFoundException e) {
                fail("Index on view was not created");
            }
            
            // upsert a single row
            PreparedStatement stmt = viewConn.prepareStatement("UPSERT INTO " + viewOfTable + " VALUES(?,?,?,?,?,?,?)");
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
            PTable view = pconn.getTable(new PTableKey(tenantId,  viewOfTable ));
            PTable viewIndex = pconn.getTable(new PTableKey(tenantId,  viewIndex1 ));
            byte[] viewIndexPhysicalTable = viewIndex.getPhysicalName().getBytes();
            assertNotNull("Can't find view index", viewIndex);
            assertEquals("Unexpected number of indexes ", 2, view.getIndexes().size());
            assertEquals("Unexpected index ",  viewIndex1 , view.getIndexes().get(0).getName()
                    .getString());
            assertEquals("Unexpected index ",  viewIndex2 , view.getIndexes().get(1).getName()
                .getString());
            
            // drop two columns
            conn.createStatement().execute("ALTER TABLE " + tableWithView + " DROP COLUMN v2, v3 ");
            
            // verify columns were dropped
            try {
                conn.createStatement().execute("SELECT v2 FROM " + tableWithView );
                fail("Column should have been dropped");
            } catch (ColumnNotFoundException e) {
            }
            try {
                conn.createStatement().execute("SELECT v3 FROM " + tableWithView );
                fail("Column should have been dropped");
            } catch (ColumnNotFoundException e) {
            }
            
            // verify index metadata was dropped
            try {
                viewConn.createStatement().execute("SELECT * FROM " + viewIndex1 );
                fail("Index metadata should have been dropped");
            } catch (TableNotFoundException e) {
            }
            
            pconn = viewConn.unwrap(PhoenixConnection.class);
            view = pconn.getTable(new PTableKey(tenantId,  viewOfTable ));
            try {
                viewIndex = pconn.getTable(new PTableKey(tenantId,  viewIndex1 ));
                fail("View index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            assertEquals("Unexpected number of indexes ", 1, view.getIndexes().size());
            assertEquals("Unexpected index ",  viewIndex2 , view.getIndexes().get(0).getName().getString());
            
            // verify that the physical index view table is *not* dropped
            conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(viewIndexPhysicalTable);
            
            // scan the physical table and verify there is a single row for the second local index
            Scan scan = new Scan();
            HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(viewIndexPhysicalTable);
            ResultScanner results = table.getScanner(scan);
            Result result = results.next();
            assertNotNull(result);
            PTable viewIndexPTable = pconn.getTable(new PTableKey(pconn.getTenantId(), viewIndex2));
            PColumn column = viewIndexPTable.getColumnForColumnName(IndexUtil.getIndexColumnName(QueryConstants.DEFAULT_COLUMN_FAMILY, "V4"));
            byte[] cq = column.getColumnQualifierBytes();
            // there should be a single row belonging to VIEWINDEX2 
            assertNotNull(viewIndex2 + " row is missing", result.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, cq));
            assertNull(results.next());
        }
    }

}
