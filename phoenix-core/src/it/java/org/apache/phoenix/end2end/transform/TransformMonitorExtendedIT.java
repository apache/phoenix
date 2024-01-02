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
package org.apache.phoenix.end2end.transform;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.end2end.transform.TransformMonitorIT.waitForTransformToGetToState;
import static org.apache.phoenix.end2end.transform.TransformToolIT.getTenantConnection;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class TransformMonitorExtendedIT extends BaseTest {
    private static RegionCoprocessorEnvironment taskRegionEnvironment;
    protected String dataTableDdl = "";
    private Properties propsNamespace = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60*60)); // An hour
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());

        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());

        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));

        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo in the tool doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return new Configuration(config);
            }

            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(config);
                copy.addResource(confToClone);
                return copy;
            }
        });
    }

    public TransformMonitorExtendedIT() throws IOException, InterruptedException {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" COLUMN_ENCODED_BYTES=0,IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN");
        this.dataTableDdl = optionBuilder.toString();
        propsNamespace.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));

        TableName taskTable = TableName.valueOf(SYSTEM_CATALOG_SCHEMA + QueryConstants.NAMESPACE_SEPARATOR + SYSTEM_TASK_TABLE);
        taskRegionEnvironment = (RegionCoprocessorEnvironment) getUtility()
                .getRSForFirstRegionInTable(taskTable)
                .getRegions(taskTable)
                .get(0).getCoprocessorHost()
                .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    @Before
    public void setupTest() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME);
            conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);
        }
    }

    @Test
    public void testTransformIndexWithNamespaceEnabled() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String fullDataTableName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        String createIndexStmt = "CREATE INDEX %s ON " + fullDataTableName + " (NAME) INCLUDE (ZIP) ";
        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            TransformToolIT.createTableAndUpsertRows(conn, fullDataTableName, numOfRows, dataTableDdl);
            conn.createStatement().execute(String.format(createIndexStmt, indexName));
            assertEquals(numOfRows, TransformMonitorIT.countRows(conn, fullIndexName));

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, fullIndexName);

            conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + fullDataTableName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, indexName, fullDataTableName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            TransformToolIT.upsertRows(conn, fullDataTableName, 2, 1);

            ResultSet rs = conn.createStatement().executeQuery("SELECT \":ID\", \"0:ZIP\" FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals(95051, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals(95052, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testTransformTableWithNamespaceEnabled() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String fullDataTableName = SchemaUtil.getTableName(schemaName, dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            TransformToolIT.createTableAndUpsertRows(conn, fullDataTableName, numOfRows, dataTableDdl);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, fullDataTableName);

            conn.createStatement().execute("ALTER TABLE " + fullDataTableName +
                    " SET COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            TransformToolIT.upsertRows(conn, fullDataTableName, 2, 1);
            assertEquals(numOfRows + 1, TransformMonitorIT.countRows(conn, fullDataTableName));

            ResultSet rs = conn.createStatement().executeQuery("SELECT ID, ZIP FROM " + fullDataTableName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals(95051, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals(95052, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testTransformImmutableTableWithNamespaceEnabled() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String fullDataTableName = SchemaUtil.getTableName(schemaName, dataTableName);
        dataTableDdl += ", IMMUTABLE_ROWS=true";
        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            TransformToolIT.createTableAndUpsertRows(conn, fullDataTableName, numOfRows, dataTableDdl);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, fullDataTableName);

            conn.createStatement().execute("ALTER TABLE " + fullDataTableName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            TransformToolIT.upsertRows(conn, fullDataTableName, 2, 1);
            assertEquals(numOfRows + 1, TransformMonitorIT.countRows(conn, fullDataTableName));

            ResultSet rs = conn.createStatement().executeQuery("SELECT ID, ZIP FROM " + fullDataTableName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals(95051, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals(95052, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testTransformWithGlobalAndTenantViews() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName1 = generateUniqueName();
        String dataTableFullName1 = SchemaUtil.getTableName(schemaName, dataTableName1);
        String namespaceMappedDataTableName1 = SchemaUtil.getPhysicalHBaseTableName(schemaName, dataTableName1, true).getString();
        String view1Name = SchemaUtil.getTableName(schemaName, "VW1_" + generateUniqueName());
        String view2Name = SchemaUtil.getTableName(schemaName, "VW2_" + generateUniqueName());
        String tenantView = SchemaUtil.getTableName(schemaName, "VWT_" + generateUniqueName());
        String readOnlyTenantView = SchemaUtil.getTableName(schemaName, "ROVWT_" + generateUniqueName());

        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName1, numOfRows, "TABLE_ONLY", dataTableDdl);

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName1);

            String createViewSql = "CREATE VIEW " + view1Name + " ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName1 + " where DATA='GLOBAL_VIEW' ";
            conn.createStatement().execute(createViewSql);
            PreparedStatement stmt1 = conn.prepareStatement(String.format("UPSERT INTO %s VALUES(?, ? , ?, ?, ?,?)", view1Name));
            stmt1.setInt(1, 2);
            stmt1.setString(2, "uname2");
            stmt1.setInt(3, 95053);
            stmt1.setString(4, "GLOBAL_VIEW");
            stmt1.setInt(5, 111);
            stmt1.setString(6, "viewcol2");
            stmt1.executeUpdate();

            createViewSql = "CREATE VIEW " + view2Name + " ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName1 + " where DATA='GLOBAL_VIEW' AND ZIP=95053";
            conn.createStatement().execute(createViewSql);
            stmt1 = conn.prepareStatement(String.format("UPSERT INTO %s VALUES(?, ? , ?, ?, ?,?)", view1Name));
            stmt1.setInt(1, 20);
            stmt1.setString(2, "uname22");
            stmt1.setInt(3, 95053);
            stmt1.setString(4, "GLOBAL_VIEW");
            stmt1.setInt(5, 111);
            stmt1.setString(6, "viewcol22");
            stmt1.executeUpdate();
        }

        try (Connection tenantConn1 = getTenantConnection("tenant1")) {
            tenantConn1.setAutoCommit(true);
            String createViewSql = "CREATE VIEW " + tenantView + " ( VIEW_TCOL1 INTEGER, VIEW_TCOL2 VARCHAR ) " +
                    " AS SELECT * FROM "
                    + dataTableFullName1 + " where DATA='TENANT_VIEW'";
            tenantConn1.createStatement().execute(createViewSql);

            PreparedStatement stmt1 = tenantConn1.prepareStatement(
                    String.format("UPSERT INTO %s (ID, NAME, ZIP, DATA, VIEW_TCOL1, VIEW_TCOL2) " +
                            "VALUES(?, ? , ?, ?, ?, ?)", tenantView));
            stmt1.setInt(1, 4);
            stmt1.setString(2, "uname4");
            stmt1.setInt(3, 95054);
            stmt1.setString(4, "TENANT_VIEW");
            stmt1.setInt(5, 2001);
            stmt1.setString(6, "tenantviewcol");
            stmt1.executeUpdate();

            // ZIP field values are like 95050 + i
            createViewSql = "CREATE VIEW " + readOnlyTenantView + " ( VIEW_TCOL1 INTEGER, VIEW_TCOL2 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName1 + " where DATA='TENANT_VIEW' AND ZIP > 95050";
            tenantConn1.createStatement().execute(createViewSql);
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName1 +
                    " SET COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName1, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                admin.disableTable(TableName.valueOf(namespaceMappedDataTableName1));
                admin.truncateTable(TableName.valueOf(namespaceMappedDataTableName1), true);
            }

            SingleCellIndexIT.dumpTable(schemaName + ":" + dataTableName1 + "_1");

            String sql = "SELECT VIEW_COL1, VIEW_COL2 FROM %s WHERE DATA='GLOBAL_VIEW' ";
            ResultSet rs1 = conn.createStatement().executeQuery(String.format(sql, view1Name));
            assertTrue(rs1.next());
            assertEquals(111, rs1.getInt(1));
            assertEquals("viewcol2", rs1.getString(2));
            assertTrue(rs1.next());
            assertEquals("viewcol22", rs1.getString(2));
            assertFalse(rs1.next());

            rs1 = conn.createStatement().executeQuery(String.format(sql, view2Name));
            assertTrue(rs1.next());
            assertEquals(111, rs1.getInt(1));
            assertEquals("viewcol2", rs1.getString(2));
            assertTrue(rs1.next());
            assertEquals("viewcol22", rs1.getString(2));
            assertFalse(rs1.next());

            sql = "SELECT DATA FROM %s WHERE ID=1";
            rs1 = conn.createStatement().executeQuery(String.format(sql, dataTableFullName1));
            assertFalse(rs1.next());
        }

        try (Connection tenantConn1 = getTenantConnection("tenant1")) {
            String sql = "SELECT VIEW_TCOL1, VIEW_TCOL2 FROM %s ";
            ResultSet rs1 = tenantConn1.createStatement().executeQuery(String.format(sql, tenantView));

            assertTrue(rs1.next());
            assertEquals(2001, rs1.getInt(1));
            assertEquals("tenantviewcol", rs1.getString(2));

            ResultSet rs2 = tenantConn1.createStatement().executeQuery(String.format(sql, readOnlyTenantView));
            assertTrue(rs2.next());
            assertEquals(2001, rs2.getInt(1));
            assertEquals("tenantviewcol", rs2.getString(2));
        }
    }

    @Test
    public void testTransformWithGlobalAndTenantViewsImmutable() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName1 = generateUniqueName();
        String dataTableFullName1 = SchemaUtil.getTableName(schemaName, dataTableName1);
        String namespaceMappedDataTableName1 = SchemaUtil.getPhysicalHBaseTableName(schemaName, dataTableName1, true).getString();
        String view1Name = SchemaUtil.getTableName(schemaName, "VW1_" + generateUniqueName());
        String view2Name = SchemaUtil.getTableName(schemaName, "VW2_" + generateUniqueName());
        String tenantView = SchemaUtil.getTableName(schemaName, "VWT_" + generateUniqueName());
        String readOnlyTenantView = SchemaUtil.getTableName(schemaName, "ROVWT_" + generateUniqueName());
        dataTableDdl += ", IMMUTABLE_ROWS=true";
        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName1, numOfRows, "TABLE_ONLY", dataTableDdl);

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName1);

            String createViewSql = "CREATE VIEW " + view1Name + " ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName1 + " where DATA='GLOBAL_VIEW' ";
            conn.createStatement().execute(createViewSql);
            PreparedStatement stmt1 = conn.prepareStatement(String.format("UPSERT INTO %s VALUES(?, ? , ?, ?, ?,?)", view1Name));
            stmt1.setInt(1, 2);
            stmt1.setString(2, "uname2");
            stmt1.setInt(3, 95053);
            stmt1.setString(4, "GLOBAL_VIEW");
            stmt1.setInt(5, 111);
            stmt1.setString(6, "viewcol2");
            stmt1.executeUpdate();

            createViewSql = "CREATE VIEW " + view2Name + " ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName1 + " where DATA='GLOBAL_VIEW' AND ZIP=95053";
            conn.createStatement().execute(createViewSql);
            stmt1 = conn.prepareStatement(String.format("UPSERT INTO %s VALUES(?, ? , ?, ?, ?,?)", view1Name));
            stmt1.setInt(1, 20);
            stmt1.setString(2, "uname22");
            stmt1.setInt(3, 95053);
            stmt1.setString(4, "GLOBAL_VIEW");
            stmt1.setInt(5, 111);
            stmt1.setString(6, "viewcol22");
            stmt1.executeUpdate();
        }

        try (Connection tenantConn1 = getTenantConnection("tenant1")) {
            tenantConn1.setAutoCommit(true);
            String createViewSql = "CREATE VIEW " + tenantView + " ( VIEW_TCOL1 INTEGER, VIEW_TCOL2 VARCHAR ) " +
                    " AS SELECT * FROM "
                    + dataTableFullName1 + " where DATA='TENANT_VIEW'";
            tenantConn1.createStatement().execute(createViewSql);

            PreparedStatement stmt1 = tenantConn1.prepareStatement(
                    String.format("UPSERT INTO %s (ID, NAME, ZIP, DATA, VIEW_TCOL1, VIEW_TCOL2) " +
                            "VALUES(?, ? , ?, ?, ?, ?)", tenantView));
            stmt1.setInt(1, 4);
            stmt1.setString(2, "uname4");
            stmt1.setInt(3, 95054);
            stmt1.setString(4, "TENANT_VIEW");
            stmt1.setInt(5, 2001);
            stmt1.setString(6, "tenantviewcol");
            stmt1.executeUpdate();

            // ZIP field values are like 95050 + i
            createViewSql = "CREATE VIEW " + readOnlyTenantView + " ( VIEW_TCOL1 INTEGER, VIEW_TCOL2 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName1 + " where DATA='TENANT_VIEW' AND ZIP > 95050";
            tenantConn1.createStatement().execute(createViewSql);
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName1 +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName1, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                admin.disableTable(TableName.valueOf(namespaceMappedDataTableName1));
                admin.truncateTable(TableName.valueOf(namespaceMappedDataTableName1), true);
            }

            SingleCellIndexIT.dumpTable(schemaName + ":" + dataTableName1 + "_1");

            String sql = "SELECT VIEW_COL1, VIEW_COL2 FROM %s WHERE DATA='GLOBAL_VIEW' ";
            ResultSet rs1 = conn.createStatement().executeQuery(String.format(sql, view1Name));
            assertTrue(rs1.next());
            assertEquals(111, rs1.getInt(1));
            assertEquals("viewcol2", rs1.getString(2));
            assertTrue(rs1.next());
            assertEquals("viewcol22", rs1.getString(2));
            assertFalse(rs1.next());

            rs1 = conn.createStatement().executeQuery(String.format(sql, view2Name));
            assertTrue(rs1.next());
            assertEquals(111, rs1.getInt(1));
            assertEquals("viewcol2", rs1.getString(2));
            assertTrue(rs1.next());
            assertEquals("viewcol22", rs1.getString(2));
            assertFalse(rs1.next());

            sql = "SELECT DATA FROM %s WHERE ID=1";
            rs1 = conn.createStatement().executeQuery(String.format(sql, dataTableFullName1));
            assertFalse(rs1.next());
        }

        try (Connection tenantConn1 = getTenantConnection("tenant1")) {
            String sql = "SELECT VIEW_TCOL1, VIEW_TCOL2 FROM %s ";
            ResultSet rs1 = tenantConn1.createStatement().executeQuery(String.format(sql, tenantView));

            assertTrue(rs1.next());
            assertEquals(2001, rs1.getInt(1));
            assertEquals("tenantviewcol", rs1.getString(2));

            ResultSet rs2 = tenantConn1.createStatement().executeQuery(String.format(sql, readOnlyTenantView));
            assertTrue(rs2.next());
            assertEquals(2001, rs2.getInt(1));
            assertEquals("tenantviewcol", rs2.getString(2));
        }
    }

}
