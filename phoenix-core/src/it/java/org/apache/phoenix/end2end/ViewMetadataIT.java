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

import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static org.apache.phoenix.coprocessor.TaskRegionObserver.TASK_DETAILS;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MODIFY_VIEW_PK;
import static org.apache.phoenix.exception.SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.schema.PTable.TaskType.DROP_CHILD_VIEWS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.end2end.ViewIT.TestMetaDataRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

public class ViewMetadataIT extends SplitSystemCatalogIT {

    private static RegionCoprocessorEnvironment TaskRegionEnvironment;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        NUM_SLAVES_BASE = 6;
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        boolean splitSystemCatalog = (driver == null);
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.PHOENIX_ACLS_ENABLED, "true");
        serverProps.put(PhoenixMetaDataCoprocessorHost.PHOENIX_META_DATA_COPROCESSOR_CONF_KEY,
                TestMetaDataRegionObserver.class.getName());
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        // Set this in server properties too since we get a connection on the server and pass in
        // server-side properties when running the drop child views tasks
        serverProps.put(DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
        // Split SYSTEM.CATALOG once after the mini-cluster is started
        if (splitSystemCatalog) {
            // splitSystemCatalog is incompatible with the balancer chore
            getUtility().getHBaseCluster().getMaster().balanceSwitch(false);
            splitSystemCatalog();
        }

        TaskRegionEnvironment =
                (RegionCoprocessorEnvironment)getUtility()
                        .getRSForFirstRegionInTable(
                                PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .getOnlineRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .get(0).getCoprocessorHost()
                        .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    @Test
    public void testCreateViewWithUpdateCacheFrquency() throws Exception {
      Properties props = new Properties();
      Connection conn1 = DriverManager.getConnection(getUrl(), props);
      conn1.setAutoCommit(true);
      String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
      String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
      conn1.createStatement().execute(
        "CREATE TABLE "+tableName+" (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) UPDATE_CACHE_FREQUENCY=1000000");
      conn1.createStatement().execute("upsert into "+tableName+" values ('row1', 'value1', 'key1')");
      conn1.createStatement().execute(
        "CREATE VIEW "+viewName+" (v43 VARCHAR) AS SELECT * FROM "+tableName+" WHERE v1 = 'value1'");

      ResultSet rs = conn1.createStatement()
          .executeQuery("SELECT * FROM "+tableName+" WHERE v1 = 'value1'");
      assertTrue(rs.next());
    }

    @Test
    public void testCreateViewFromHBaseTable() throws Exception {
        String tableNameStr = generateUniqueName();
        String familyNameStr = generateUniqueName();

        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(),
                TestUtil.TEST_PROPERTIES).getAdmin();

        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableNameStr));
        desc.addFamily(new HColumnDescriptor(familyNameStr));
        admin.createTable(desc);
        Connection conn = DriverManager.getConnection(getUrl());

        //PK is not specified, without where clause
        try {
            conn.createStatement().executeUpdate("CREATE VIEW \"" + tableNameStr +
                    "\" (ROWKEY VARCHAR, \"" + familyNameStr + "\".a VARCHAR)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.PRIMARY_KEY_MISSING.getErrorCode(), e.getErrorCode());
        }

        // No error, as PK is specified
        conn.createStatement().executeUpdate("CREATE VIEW \"" + tableNameStr +
                "\" (ROWKEY VARCHAR PRIMARY KEY, \"" + familyNameStr + "\".a VARCHAR)");

        conn.createStatement().executeUpdate("DROP VIEW \"" + tableNameStr + "\"");

        //PK is not specified, with where clause
        try {
            conn.createStatement().executeUpdate("CREATE VIEW \"" + tableNameStr +
                    "\" (ROWKEY VARCHAR, \"" + familyNameStr + "\".a VARCHAR) AS SELECT * FROM \""
                    + tableNameStr + "\" WHERE ROWKEY = '1'");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.PRIMARY_KEY_MISSING.getErrorCode(), e.getErrorCode());
        }

        conn.createStatement().executeUpdate("CREATE VIEW \"" + tableNameStr +
                "\" (ROWKEY VARCHAR PRIMARY KEY, \"" + familyNameStr + "\".a VARCHAR) AS SELECT " +
                "* FROM \"" + tableNameStr + "\" WHERE ROWKEY = '1'");

        conn.createStatement().executeUpdate("DROP VIEW \"" + tableNameStr + "\"");
    }

    @Test
    public void testCreateViewMappedToExistingHbaseTableWithNamespaceMappingEnabled() throws Exception {
        final String NS = "NS_" + generateUniqueName();
        final String TBL = "TBL_" + generateUniqueName();
        final String CF = "CF";

        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {

            conn.createStatement().execute("CREATE SCHEMA " + NS);

            // test for a view that is in non-default schema
            {
                TableName tableName = TableName.valueOf(NS, TBL);
                HTableDescriptor desc = new HTableDescriptor(tableName);
                desc.addFamily(new HColumnDescriptor(CF));
                admin.createTable(desc);

                String view1 = NS + "." + TBL;
                conn.createStatement().execute(
                        "CREATE VIEW " + view1 + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil.getExplainPlan(
                        conn.createStatement().executeQuery("explain select * from " + view1))
                        .contains(NS + ":" + TBL));

                conn.createStatement().execute("DROP VIEW " + view1);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // test for a view whose name contains a dot (e.g. "AAA.BBB") in default schema (for backward compatibility)
            {
                TableName tableName = TableName.valueOf(NS + "." + TBL);
                HTableDescriptor desc = new HTableDescriptor(tableName);
                desc.addFamily(new HColumnDescriptor(CF));
                admin.createTable(desc);

                String view2 = "\"" + NS + "." + TBL + "\"";
                conn.createStatement().execute(
                        "CREATE VIEW " + view2 + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil
                        .getExplainPlan(
                                conn.createStatement().executeQuery("explain select * from " + view2))
                        .contains(NS + "." + TBL));

                conn.createStatement().execute("DROP VIEW " + view2);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // test for a view whose name contains a dot (e.g. "AAA.BBB") in non-default schema
            {
                TableName tableName = TableName.valueOf(NS, NS + "." + TBL);
                HTableDescriptor desc = new HTableDescriptor(tableName);
                desc.addFamily(new HColumnDescriptor(CF));
                admin.createTable(desc);

                String view3 = NS + ".\"" + NS + "." + TBL + "\"";
                conn.createStatement().execute(
                        "CREATE VIEW " + view3 + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil.getExplainPlan(
                        conn.createStatement().executeQuery("explain select * from " + view3))
                        .contains(NS + ":" + NS + "." + TBL));

                conn.createStatement().execute("DROP VIEW " + view3);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            conn.createStatement().execute("DROP SCHEMA " + NS);
        }
    }

    @Test
    public void testRecreateDroppedTableWithChildViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        String tableDdl = "CREATE TABLE " + fullTableName + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        String indexName = generateUniqueName();
        ddl = "CREATE INDEX " + indexName + " on " + fullViewName1 + "(v2)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + "(v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 10";
        conn.createStatement().execute(ddl);

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName + " CASCADE");
        runDropChildViewsTask();

        validateViewDoesNotExist(conn, fullViewName1);
        validateViewDoesNotExist(conn, fullViewName2);

        // recreate the table that was dropped
        conn.createStatement().execute(tableDdl);
        // the two child views should still not exist
        try {
            PhoenixRuntime.getTableNoCache(conn, fullViewName1);
            fail();
        } catch (SQLException e) {
        }
        try {
            PhoenixRuntime.getTableNoCache(conn, fullViewName2);
            fail();
        } catch (SQLException e) {
        }
    }

    private void runDropChildViewsTask() {
        // Run DropChildViewsTask to complete the tasks for dropping child views
        TaskRegionObserver.SelfHealingTask task = new TaskRegionObserver.SelfHealingTask(
                TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        task.run();
    }

    @Test
    public void testRecreateIndexWhoseAncestorWasDropped() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName1 = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullTableName2 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

        String tableDdl = "CREATE TABLE " + fullTableName1 + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        tableDdl = "CREATE TABLE " + fullTableName2 + "  (k INTEGER NOT NULL PRIMARY KEY, v3 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1 + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        String indexName = generateUniqueName();
        ddl = "CREATE INDEX " + indexName + " on " + fullViewName1 + "(v2)";
        conn.createStatement().execute(ddl);
        try {
                // this should fail because an index with this name is present
            ddl = "CREATE INDEX " + indexName + " on " + fullTableName2 + "(v1)";
            conn.createStatement().execute(ddl);
            fail();
        }
        catch(SQLException e) {
        }

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName1 + " CASCADE");
        runDropChildViewsTask();

        // should be able to reuse the index name 
        ddl = "CREATE INDEX " + indexName + " on " + fullTableName2 + "(v3)";
        conn.createStatement().execute(ddl);

        String fullIndexName = SchemaUtil.getTableName(SCHEMA2, indexName);
        PTable index = PhoenixRuntime.getTableNoCache(conn, fullIndexName);
        // the index should have v3 but not v2
        validateCols(index);
    }

    @Test
    public void testRecreateViewWhoseParentWasDropped() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName1 = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullTableName2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        String tableDdl = "CREATE TABLE " + fullTableName1 + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        tableDdl = "CREATE TABLE " + fullTableName2 + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1 + " WHERE k > 5";
        conn.createStatement().execute(ddl);

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName1 + " CASCADE");
        runDropChildViewsTask();

        // should be able to reuse the view name 
        ddl = "CREATE VIEW " + fullViewName1 + " (v3 VARCHAR) AS SELECT * FROM " + fullTableName2 + " WHERE k > 5";
        conn.createStatement().execute(ddl);

        PTable view = PhoenixRuntime.getTableNoCache(conn, fullViewName1);
        // the view should have v3 but not v2
        validateCols(view);
    }

    // Test case to ensure PHOENIX-5546 does not happen
    @Test
    public void testRepeatedCreateAndDropCascadeTableWorks() throws Exception {
        String tableName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, tableName);
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTableViewAndDropCascade(conn, fullTableName, fullViewName, false);
            validateViewDoesNotExist(conn, fullViewName);
            validateSystemTaskContainsCompletedDropChildViewsTasks(conn, SCHEMA1, tableName, 1);

            // Repeat this and check that the view still doesn't exist
            createTableViewAndDropCascade(conn, fullTableName, fullViewName, false);
            validateViewDoesNotExist(conn, fullViewName);
            validateSystemTaskContainsCompletedDropChildViewsTasks(conn, SCHEMA1, tableName, 2);
        }
    }

    // We set DROP_METADATA_ATTRIB to true and check that this does not fail dropping child views
    // that have an index, though their underlying physical table was already dropped.
    // See PHOENIX-5545.
    @Test
    public void testDropTableCascadeWithChildViewWithIndex() throws SQLException {
        String tableName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, tableName);
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTableViewAndDropCascade(conn, fullTableName, fullViewName, true);
            validateViewDoesNotExist(conn, fullViewName);
            validateSystemTaskContainsCompletedDropChildViewsTasks(conn, SCHEMA1, tableName, 1);
        }
    }

    private void createTableViewAndDropCascade(Connection conn, String fullTableName,
            String fullViewName, boolean createViewIndex) throws SQLException {
        String tableDdl = "CREATE TABLE " + fullTableName +
                "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName +
                " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        if (createViewIndex) {
            conn.createStatement().execute("CREATE INDEX " + "INDEX_" + generateUniqueName() +
                    " ON " + fullViewName + "(v2)");
        }
        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName + " CASCADE");
        runDropChildViewsTask();
    }

    private void validateSystemTaskContainsCompletedDropChildViewsTasks(Connection conn,
            String schemaName, String tableName, int numTasks) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + SYSTEM_TASK_NAME +
                " WHERE " + TASK_TYPE + "=" + DROP_CHILD_VIEWS.getSerializedValue() +
                " AND " + TENANT_ID + " IS NULL" +
                " AND " + TABLE_SCHEM + "='" + schemaName +
                "' AND " + TABLE_NAME + "='" + tableName + "'");
        assertTrue(rs.next());
        for (int i = 0; i < numTasks; i++) {
            Timestamp maxTs = new Timestamp(HConstants.LATEST_TIMESTAMP);
            assertNotEquals("Should have got a valid timestamp", maxTs, rs.getTimestamp(2));
            assertTrue("Task should be completed",
                    PTable.TaskStatus.COMPLETED.toString().equals(rs.getString(6)));
            assertNotNull("Task end time should not be null", rs.getTimestamp(7));
            String taskData = rs.getString(9);
            assertTrue("Task data should contain final status", taskData != null &&
                    taskData.contains(TASK_DETAILS) &&
                    taskData.contains(PTable.TaskStatus.COMPLETED.toString()));
        }
    }

    @Test
    public void testViewAndTableInDifferentSchemasWithNamespaceMappingEnabled() throws Exception {
        testViewAndTableInDifferentSchemas(true);
    }

    @Test
    public void testViewAndTableInDifferentSchemas() throws Exception {
        testViewAndTableInDifferentSchemas(false);

    }

    private void testViewAndTableInDifferentSchemas(boolean isNamespaceMapped) throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        Connection conn = DriverManager.getConnection(getUrl(),props);
        String tableName = "T_" + generateUniqueName();
        String schemaName1 = SCHEMA1;
        String fullTableName1 = SchemaUtil.getTableName(schemaName1, tableName);
        String viewName1 = "V_" + generateUniqueName();
        String viewSchemaName = SCHEMA2;
        String fullViewName1 = SchemaUtil.getTableName(viewSchemaName, viewName1);
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3, viewName1);

        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName1);
        }
        String ddl = "CREATE TABLE " + fullTableName1 + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        conn.createStatement().execute(ddl);
        assertTrue(admin.tableExists(SchemaUtil.getPhysicalTableName(SchemaUtil.normalizeIdentifier(fullTableName1),
                conn.unwrap(PhoenixConnection.class).getQueryServices().getProps())));

        ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1 + " WHERE k > 5";
        conn.createStatement().execute(ddl);

        ddl = "CREATE VIEW " + fullViewName2 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1 + " WHERE k > 5";
        conn.createStatement().execute(ddl);

        conn.createStatement().executeQuery("SELECT * FROM " + fullViewName1);
        conn.createStatement().executeQuery("SELECT * FROM " + fullViewName2);
        ddl = "DROP VIEW " + viewName1;
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableNotFoundException ignore) {
        }
        ddl = "DROP VIEW " + fullViewName1;
        conn.createStatement().execute(ddl);
        ddl = "DROP VIEW " + SchemaUtil.getTableName(viewSchemaName, generateUniqueName());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableNotFoundException ignore) {
        }
        ddl = "DROP TABLE " + fullTableName1;
        ddl = "DROP VIEW " + fullViewName2;
        conn.createStatement().execute(ddl);
        ddl = "DROP TABLE " + fullTableName1;
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testViewAndTableAndDropCascade() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        String tableDdl = "CREATE TABLE " + fullTableName + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        String indexName = generateUniqueName();
        ddl = "CREATE LOCAL INDEX " + indexName + " on " + fullViewName1 + "(v2)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + "(v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 10";
        conn.createStatement().execute(ddl);

        // dropping base table without cascade should fail
        try {
            conn.createStatement().execute("DROP TABLE " + fullTableName );
            fail();
        }
        catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName + " CASCADE");
        runDropChildViewsTask();

        validateViewDoesNotExist(conn, fullViewName1);
        validateViewDoesNotExist(conn, fullViewName2);

    }

    @Test
    public void testUpdatingPropertyOnBaseTable() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("create table " + fullTableName
                            + "(tenantId CHAR(15) NOT NULL, pk1 integer NOT NULL, v varchar CONSTRAINT PK PRIMARY KEY "
                            + "(tenantId, pk1)) MULTI_TENANT=true");
            conn.createStatement().execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName);

            conn.createStatement()
                    .execute("ALTER TABLE " + fullTableName + " set IMMUTABLE_ROWS = true");

            // fetch the latest tables
            PTable table = PhoenixRuntime.getTableNoCache(conn, fullTableName);
            PTable view = PhoenixRuntime.getTableNoCache(conn, fullViewName);
            assertEquals("IMMUTABLE_ROWS property set incorrectly", true, table.isImmutableRows());
            assertEquals("IMMUTABLE_ROWS property set incorrectly", true, view.isImmutableRows());
        }
    }

    @Test
    public void testViewAddsPKColumnWhoseParentsLastPKIsVarLength() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 VARCHAR NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY, k4 VARCHAR PRIMARY KEY, v2 INTEGER";
        try {
            conn.createStatement().execute(ddl);
            fail("View cannot extend PK if parent's last PK is variable length. See https://issues.apache.org/jira/browse/PHOENIX-978.");
        } catch (SQLException e) {
            assertEquals(CANNOT_MODIFY_VIEW_PK.getErrorCode(), e.getErrorCode());
        }
        String fullViewName2 = "V_" + generateUniqueName();
        ddl = "CREATE VIEW " + fullViewName2 + " (k3 VARCHAR PRIMARY KEY)  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        try {
            conn.createStatement().execute(ddl);
        } catch (SQLException e) {
            assertEquals(CANNOT_MODIFY_VIEW_PK.getErrorCode(), e.getErrorCode());
        }
    }

    @Test(expected=ColumnAlreadyExistsException.class)
    public void testViewAddsClashingPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY, k2 VARCHAR PRIMARY KEY, v2 INTEGER";
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testQueryWithSeparateConnectionForViewOnTableThatHasIndex() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection conn2 = DriverManager.getConnection(getUrl());
                Statement s = conn.createStatement();
                Statement s2 = conn2.createStatement()) {
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            String indexName = generateUniqueName();
            helpTestQueryForViewOnTableThatHasIndex(s, s2, tableName, viewName, indexName);
        }
    }

    @Test
    public void testQueryForViewOnTableThatHasIndex() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement s = conn.createStatement()) {
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            String indexName = generateUniqueName();
            helpTestQueryForViewOnTableThatHasIndex(s, s, tableName, viewName, indexName);
        }
    }

    private void helpTestQueryForViewOnTableThatHasIndex(Statement s1, Statement s2, String tableName, String viewName, String indexName)
            throws SQLException {
        // Create a table
        s1.execute("create table " + tableName + " (col1 varchar primary key, col2 varchar)");

        // Create a view on the table
        s1.execute("create view " + viewName + " (col3 varchar) as select * from " + tableName);
        s1.executeQuery("select * from " + viewName);
        // Create a index on the table
        s1.execute("create index " + indexName + " ON " + tableName + " (col2)");

        try (ResultSet rs =
                s2.executeQuery("explain select /*+ INDEX(" + viewName + " " + indexName
                        + ") */ * from " + viewName + " where col2 = 'aaa'")) {
            String explainPlan = QueryUtil.getExplainPlan(rs);

            // check if the query uses the index
            assertTrue(explainPlan.contains(indexName));
        }
    }

    @Test
    public void testViewAndTableAndDropCascadeWithIndexes() throws Exception {
        // Setup - Tables and Views with Indexes
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String ddl = "CREATE TABLE " + fullTableName + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(ddl);
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String indexName1 = "I_" + generateUniqueName();
        String indexName2 = "I_" + generateUniqueName();
        String indexName3 = "I_" + generateUniqueName();

        ddl = "CREATE INDEX " + indexName1 + " ON " + fullTableName + " (v1)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX " + indexName2 + " ON " + fullViewName1 + " (v2)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 10";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX " + indexName3 + " ON " + fullViewName2 + " (v2)";
        conn.createStatement().execute(ddl);

        // Execute DROP...CASCADE
        conn.createStatement().execute("DROP TABLE " + fullTableName + " CASCADE");
        runDropChildViewsTask();

        // Validate Views were deleted - Try and delete child views, should throw TableNotFoundException
        validateViewDoesNotExist(conn, fullViewName1);
        validateViewDoesNotExist(conn, fullViewName2);
    }

    @Test
    public void testViewAddsNotNullPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        try {
            ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR NOT NULL PRIMARY KEY"; 
            conn.createStatement().execute(ddl);
            fail("can only add nullable PKs via ALTER VIEW/TABLE");
        } catch (SQLException e) {
            assertEquals(NOT_NULLABLE_COLUMN_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testDisallowDropOfColumnOnParentTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + viewName + "(v2 VARCHAR, v3 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().execute("ALTER TABLE " + fullTableName + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testDisallowDropOfReferencedColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName1 + "(v2 VARCHAR, v3 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + " AS SELECT * FROM " + fullViewName1 + " WHERE v2 != 'foo'";
        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName1 + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_DROP_VIEW_REFERENCED_COL.getErrorCode(),
                    e.getErrorCode());
        }

        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName2 + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_DROP_VIEW_REFERENCED_COL.getErrorCode(),
                    e.getErrorCode());
        }
        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName2 + " DROP COLUMN v2");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_DROP_VIEW_REFERENCED_COL.getErrorCode(),
                    e.getErrorCode());
        }
        conn.createStatement().execute("ALTER VIEW " + fullViewName2 + " DROP COLUMN v3");
    }

    @Test
    public void testViewAddsPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String viewName = generateUniqueName();
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, viewName);

        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY, k4 VARCHAR PRIMARY KEY, v2 INTEGER";
        conn.createStatement().execute(ddl);

        // assert PK metadata
        ResultSet rs = conn.getMetaData().getPrimaryKeys(null, SCHEMA2, viewName);
        assertPKs(rs, new String[] {"K1", "K2", "K3", "K4"});
    }

    @Test
    public void testCreateViewDefinesPKConstraint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "(v2 VARCHAR, k3 VARCHAR, k4 INTEGER NOT NULL, CONSTRAINT PKVEW PRIMARY KEY (k3, k4)) AS SELECT * FROM " + fullTableName + " WHERE K1 = 1";
        conn.createStatement().execute(ddl);

        PhoenixRuntime.getTableNoCache(conn, fullViewName);

        // assert PK metadata
        ResultSet rs =
                conn.getMetaData().getPrimaryKeys(null,
                    SchemaUtil.getSchemaNameFromFullName(fullViewName),
                    SchemaUtil.getTableNameFromFullName(fullViewName));
        assertPKs(rs, new String[] {"K1", "K2", "K3", "K4"});
    }

    private void assertPKs(ResultSet rs, String[] expectedPKs) throws SQLException {
        List<String> pkCols = newArrayListWithExpectedSize(expectedPKs.length);
        while (rs.next()) {
            pkCols.add(rs.getString("COLUMN_NAME"));
        }
        String[] actualPKs = pkCols.toArray(new String[0]);
        assertArrayEquals(expectedPKs, actualPKs);
    }

    private void validateViewDoesNotExist(Connection conn, String fullViewName)    throws SQLException {
        try {
            String ddl1 = "DROP VIEW " + fullViewName;
            conn.createStatement().execute(ddl1);
            fail("View " + fullViewName + " should have been deleted when parent was dropped");
        } catch (TableNotFoundException e) {
            //Expected
        }
    }

    private void validateCols(PTable table) {
        final String prefix = table.getType() == PTableType.INDEX ? "0:" : "";
        Predicate<PColumn> predicate = new Predicate<PColumn>() {
            @Override
            public boolean apply(PColumn col) {
                return col.getName().getString().equals(prefix + "V3")
                        || col.getName().getString().equals(prefix + "V2");
            }
        };
        List<PColumn> colList = table.getColumns();
        Collection<PColumn> filteredCols = Collections2.filter(colList, predicate);
        assertEquals(1, filteredCols.size());
        assertEquals(prefix + "V3", filteredCols.iterator().next().getName().getString());
    }
}
