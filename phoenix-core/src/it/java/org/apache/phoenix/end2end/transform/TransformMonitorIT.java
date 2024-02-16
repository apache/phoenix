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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessor.tasks.TransformMonitorTask;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.ServerTask;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.end2end.IndexRebuildTaskIT.waitForTaskState;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_CREATE_TENANT_SPECIFIC_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getRawRowCount;
import static org.apache.phoenix.util.TestUtil.getRowCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransformMonitorIT extends ParallelStatsDisabledIT {
    private static RegionCoprocessorEnvironment TaskRegionEnvironment;

    private Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    public TransformMonitorIT() throws IOException, InterruptedException {
        testProps.put(QueryServices.DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB, "ONE_CELL_PER_COLUMN");
        testProps.put(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB, "0");
        testProps.put(QueryServices.PHOENIX_ACLS_ENABLED, "true");

        TaskRegionEnvironment = (RegionCoprocessorEnvironment) getUtility()
                .getRSForFirstRegionInTable(
                        PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                .getRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                .get(0).getCoprocessorHost()
                .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    @Before
    public void setupTest() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME);
            conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);
        }
    }

    private void testTransformTable(boolean createIndex, boolean createView, boolean isImmutable) throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String newTableName = dataTableName + "_1";
        String newTableFullName = SchemaUtil.getTableName(schemaName, newTableName);
        String indexName = "IDX_" + generateUniqueName();
        String indexName2 = "IDX_" + generateUniqueName();
        String viewName = "VW_" + generateUniqueName();
        String viewName2 = "VW2_" + generateUniqueName();
        String viewIdxName = "VW_IDX_" + generateUniqueName();
        String viewIdxName2 = "VW_IDX_" + generateUniqueName();
        String view2IdxName1 = "VW2_IDX_" + generateUniqueName();
        String indexFullName = SchemaUtil.getTableName(schemaName, indexName);
        String createIndexStmt = "CREATE INDEX %s ON " + dataTableFullName + " (NAME) INCLUDE (ZIP) ";
        String createViewStmt = "CREATE VIEW %s ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM " + dataTableFullName;
        String createViewIdxSql = "CREATE INDEX  %s ON " + viewName + " (VIEW_COL1) include (VIEW_COL2) ";
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            int numOfRows = 10;
            TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, numOfRows, isImmutable? " IMMUTABLE_ROWS=true" : "");
            if (createIndex) {
                conn.createStatement().execute(String.format(createIndexStmt, indexName));
            }

            if (createView) {
                conn.createStatement().execute(String.format(createViewStmt, viewName));
                conn.createStatement().execute(String.format(createViewIdxSql, viewIdxName));
                conn.createStatement().execute("UPSERT INTO " + viewName + "(ID, NAME, VIEW_COL1, VIEW_COL2) VALUES (1, 'uname11', 100, 'viewCol2')");
            }
            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<Task.TaskRecord> taskRecordList = Task.queryTaskTable(conn, null);
            assertEquals(1, taskRecordList.size());
            assertEquals(PTable.TaskType.TRANSFORM_MONITOR, taskRecordList.get(0).getTaskType());
            assertEquals(schemaName, taskRecordList.get(0).getSchemaName());
            assertEquals(dataTableName, taskRecordList.get(0).getTableName());

            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            // Test that the PhysicalTableName is updated.
            PTable oldTable = conn.getTableNoCache(dataTableFullName);
            assertEquals(newTableName, oldTable.getPhysicalName(true).getString());

            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, dataTableFullName);

            ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
            long newRowCount = countRows(conn, newTableFullName);
            assertEquals(getRawRowCount(cqs.getTable(Bytes.toBytes(dataTableFullName))), newRowCount);

            if (createIndex) {
                assertEquals(newRowCount, countRows(conn, indexFullName));
                int additionalRows = 2;
                // Upsert new rows to new table. Note that after transform is complete, we are using the new table
                TransformToolIT.upsertRows(conn, dataTableFullName, (int)newRowCount+1, additionalRows);
                assertEquals(newRowCount+additionalRows, countRows(conn, indexFullName));
                assertEquals(newRowCount, getRawRowCount(cqs.getTable(Bytes.toBytes(dataTableFullName))));

                // Create another index on the new table and count
                Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
                TableName hTableName = TableName.valueOf(dataTableFullName);
                admin.disableTable(hTableName);
                admin.deleteTable(hTableName);
                conn.createStatement().execute(String.format(createIndexStmt, indexName2));
                assertEquals(newRowCount+additionalRows, countRows(conn, dataTableFullName));
                assertEquals(newRowCount+additionalRows, countRows(conn, SchemaUtil.getTableName(schemaName, indexName2)));
            } else if (createView) {
                assertEquals(numOfRows, countRows(conn, viewName));
                assertEquals(numOfRows, countRowsForViewIndex(conn, dataTableFullName));
                assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, viewName);
                conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

                ResultSet rs = conn.createStatement().executeQuery("SELECT VIEW_COL2 FROM " + viewName + " WHERE VIEW_COL1=100");
                assertTrue(rs.next());
                assertEquals("viewCol2", rs.getString(1));
                assertFalse(rs.next());

                int additionalRows = 2;
                // Upsert new rows to new table. Note that after transform is complete, we are using the new table
                TransformToolIT.upsertRows(conn, viewName, (int)newRowCount+1, additionalRows);
                assertEquals(newRowCount+additionalRows, getRowCount(conn, viewName));
                assertEquals(newRowCount+additionalRows, countRowsForViewIndex(conn, dataTableFullName));

                // Drop view index and create another on the new table and count
                conn.createStatement().execute("DROP INDEX " + viewIdxName + " ON " + viewName);
                Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
                TableName hTableName = TableName.valueOf(dataTableFullName);
                admin.disableTable(hTableName);
                admin.deleteTable(hTableName);
                conn.createStatement().execute(String.format(createViewIdxSql, viewIdxName2));
                assertEquals(newRowCount+additionalRows, countRowsForViewIndex(conn, dataTableFullName));

                // Create another view and have a new index on top
                conn.createStatement().execute(String.format(createViewStmt, viewName2));
                conn.createStatement().execute(String.format(createViewIdxSql, view2IdxName1));
                assertEquals((newRowCount+additionalRows)*2, countRowsForViewIndex(conn, dataTableFullName));

                conn.createStatement().execute("UPSERT INTO " + viewName2 + "(ID, NAME, VIEW_COL1, VIEW_COL2) VALUES (100, 'uname100', 1000, 'viewCol100')");
                rs = conn.createStatement().executeQuery("SELECT VIEW_COL2, NAME FROM " + viewName2 + " WHERE VIEW_COL1=1000");
                assertTrue(rs.next());
                assertEquals("viewCol100", rs.getString(1));
                assertEquals("uname100", rs.getString(2));
                assertFalse(rs.next());
            }

        }
    }

    public static int countRows(Connection conn, String tableFullName) throws SQLException {
        ResultSet count = conn.createStatement().executeQuery("select  /*+ NO_INDEX*/ count(*) from " + tableFullName);
        count.next();
        int numRows = count.getInt(1);
        return numRows;
    }

    protected int countRowsForViewIndex(Connection conn, String baseTable) throws IOException, SQLException {
        String viewIndexTableName = MetaDataUtil.getViewIndexPhysicalName(baseTable);
        ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();

        Table indexHTable = queryServices.getTable(Bytes.toBytes(viewIndexTableName));
        // If there are multiple indexes on this view, this will return rows for others as well. For 1 view index, it is fine.
        return getUtility().countRows(indexHTable);
    }

    @Test
    public void testTransformMonitor_mutableTableWithoutIndex() throws Exception {
        testTransformTable(false, false, false);
    }

    @Test
    public void testTransformMonitor_immutableTableWithoutIndex() throws Exception {
        testTransformTable(false, false, true);
    }

    @Test
    public void testTransformMonitor_immutableTableWithIndex() throws Exception {
        testTransformTable(true, false, true);
    }

    @Test
    public void testTransformMonitor_pausedTransform() throws Exception {
        testTransformMonitor_checkStates(PTable.TransformStatus.PAUSED, PTable.TaskStatus.COMPLETED);
    }

    @Test
    public void testTransformMonitor_completedTransform() throws Exception {
        testTransformMonitor_checkStates(PTable.TransformStatus.COMPLETED, PTable.TaskStatus.COMPLETED);
    }

    @Test
    public void testTransformMonitor_failedTransform() throws Exception {
        testTransformMonitor_checkStates(PTable.TransformStatus.FAILED, PTable.TaskStatus.FAILED);
    }

    private void testTransformMonitor_checkStates(PTable.TransformStatus transformStatus, PTable.TaskStatus taskStatus) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            SystemTransformRecord.SystemTransformBuilder transformBuilder = new SystemTransformRecord.SystemTransformBuilder();
            String logicalTableName = generateUniqueName();
            transformBuilder.setLogicalTableName(logicalTableName);
            transformBuilder.setTransformStatus(transformStatus.name());
            transformBuilder.setNewPhysicalTableName(logicalTableName + "_1");
            Transform.upsertTransform(transformBuilder.build(), conn.unwrap(PhoenixConnection.class));

            TaskRegionObserver.SelfHealingTask task =
                    new TaskRegionObserver.SelfHealingTask(
                            TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);

            Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder()
                    .setConn(conn.unwrap(PhoenixConnection.class))
                    .setTaskType(PTable.TaskType.TRANSFORM_MONITOR)
                    .setTenantId(null)
                    .setSchemaName(null)
                    .setTableName(logicalTableName)
                    .setTaskStatus(PTable.TaskStatus.CREATED.toString())
                    .setData(null)
                    .setPriority(null)
                    .setStartTs(startTs)
                    .setEndTs(null)
                    .build());
            task.run();

            waitForTaskState(conn, PTable.TaskType.TRANSFORM_MONITOR, logicalTableName, taskStatus);
        }
    }

    @Test
    public void testTransformMonitor_pauseAndResumeTransform() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            TransformToolIT.pauseTableTransform(schemaName, dataTableName, conn, "");

            List<String> args = TransformToolIT.getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, true, false, false);

            // This run resumes transform and TransformMonitor task runs and completes it
            TransformToolIT.runTransformTool(args.toArray(new String[0]), 0);
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            List<Task.TaskRecord> taskRecordList = Task.queryTaskTable(conn, null);
            assertEquals(1, taskRecordList.size());
            assertEquals(PTable.TaskType.TRANSFORM_MONITOR, taskRecordList.get(0).getTaskType());
            assertEquals(schemaName, taskRecordList.get(0).getSchemaName());
            assertEquals(dataTableName, taskRecordList.get(0).getTableName());

            waitForTaskState(conn, PTable.TaskType.TRANSFORM_MONITOR, dataTableName, PTable.TaskStatus.COMPLETED);
        }
    }

    @Test
    public void testTransformMonitor_mutableTableWithIndex() throws Exception {
        testTransformTable(true, false, false);
    }

    @Test
    public void testTransformMonitor_tableWithViews() throws Exception {
        testTransformTable(false, true, false);
    }

    @Test
    public void testTransformMonitor_index() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexName = "IDX_" + generateUniqueName();
        String indexFullName = SchemaUtil.getTableName(schemaName, indexName);
        String newTableFullName = indexFullName + "_1";
        String createIndexStmt = "CREATE INDEX " + indexName + " ON " + dataTableFullName + " (ZIP) INCLUDE (NAME) ";
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(),
                testProps)) {
            conn.setAutoCommit(true);
            int numOfRows = 10;
            TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, numOfRows, "");
            conn.createStatement().execute(createIndexStmt);
            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, indexFullName);

            conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + dataTableFullName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, indexName, dataTableFullName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<Task.TaskRecord> taskRecordList = Task.queryTaskTable(conn, null);
            assertEquals(1, taskRecordList.size());
            assertEquals(PTable.TaskType.TRANSFORM_MONITOR, taskRecordList.get(0).getTaskType());
            assertEquals(schemaName, taskRecordList.get(0).getSchemaName());
            assertEquals(indexName, taskRecordList.get(0).getTableName());

            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            // Test that the PhysicalTableName is updated.
            PTable oldTable = conn.getTableNoCache(indexFullName);
            assertEquals(indexName+"_1", oldTable.getPhysicalName(true).getString());
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, newTableFullName);
            ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
            long newRowCount = countRows(conn, newTableFullName);
            assertEquals(getRawRowCount(cqs.getTable(Bytes.toBytes(indexFullName))), newRowCount);
        }
    }

    @Test
    public void testTransformTableWithTenantViews() throws Exception {
        String tenantId = generateUniqueName();
        String dataTableName = generateUniqueName();
        String viewTenantName = "TENANTVW_" + generateUniqueName();
        String createTblStr = "CREATE TABLE %s (TENANT_ID VARCHAR(15) NOT NULL,ID INTEGER NOT NULL"
                + ", NAME VARCHAR, CONSTRAINT PK_1 PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
        String createViewStr = "CREATE VIEW %s  (VIEW_COL1 VARCHAR) AS SELECT * FROM %s";

        String upsertQueryStr = "UPSERT INTO %s (TENANT_ID, ID, NAME, VIEW_COL1) VALUES('%s' , %d, '%s', '%s')";

        Properties props = PropertiesUtil.deepCopy(testProps);
        Connection connGlobal = null;
        Connection connTenant = null;
        try {
            connGlobal = DriverManager.getConnection(getUrl(), props);
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            connTenant = DriverManager.getConnection(getUrl(), props);
            connTenant.setAutoCommit(true);
            String tableStmtGlobal = String.format(createTblStr, dataTableName);
            connGlobal.createStatement().execute(tableStmtGlobal);
            assertMetadata(connGlobal, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableName);

            String viewStmtTenant = String.format(createViewStr, viewTenantName, dataTableName);
            connTenant.createStatement().execute(viewStmtTenant);

            // TODO: Fix this as part of implementing TransformTool so that the tenant view rows could be read from the tool
//            connTenant.createStatement()
//                    .execute(String.format(upsertQueryStr, viewTenantName, tenantId, 1, "x", "xx"));
            try {
                connTenant.createStatement().execute("ALTER TABLE " + dataTableName
                        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
                fail("Tenant connection cannot do alter");
            } catch (SQLException e) {
                assertEquals(CANNOT_CREATE_TENANT_SPECIFIC_TABLE.getErrorCode(), e.getErrorCode());
            }
            connGlobal.createStatement().execute("ALTER TABLE " + dataTableName
                    + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord tableRecord = Transform.getTransformRecord(null, dataTableName, null, null, connGlobal.unwrap(PhoenixConnection.class));
            assertNotNull(tableRecord);

            waitForTransformToGetToState(connGlobal.unwrap(PhoenixConnection.class), tableRecord, PTable.TransformStatus.COMPLETED);

            connTenant.createStatement()
                    .execute(String.format(upsertQueryStr, viewTenantName, tenantId, 2, "y", "yy"));

            ResultSet rs = connTenant.createStatement().executeQuery("SELECT /*+ NO_INDEX */ VIEW_COL1 FROM " + viewTenantName);
            assertTrue(rs.next());
//            assertEquals("xx", rs.getString(1));
//            assertTrue(rs.next());
            assertEquals("yy", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            if (connGlobal != null) {
                connGlobal.close();
            }
            if (connTenant != null) {
                connTenant.close();
            }
        }
    }

    @Test
    public void testTransformAlreadyTransformedIndex() throws Exception {
        String dataTableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String createIndexStmt = "CREATE INDEX %s ON " + dataTableName + " (NAME) INCLUDE (ZIP) ";
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            TransformToolIT.createTableAndUpsertRows(conn, dataTableName, numOfRows, "");
            conn.createStatement().execute(String.format(createIndexStmt, indexName));
            assertEquals(numOfRows, countRows(conn, indexName));

            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, indexName);

            conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + dataTableName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(null, indexName, dataTableName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            TransformToolIT.upsertRows(conn, dataTableName, 2, 1);

            // Removing this so that we are sure that we are not picking up the old transform record.
            Transform.removeTransformRecord(record, conn.unwrap(PhoenixConnection.class));
            conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + dataTableName +
                    " ACTIVE SET IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0");
            record = Transform.getTransformRecord(null, indexName, dataTableName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            TransformToolIT.upsertRows(conn, dataTableName, 3, 1);
            assertEquals(numOfRows + 2, countRows(conn, indexName));
            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, record.getNewPhysicalTableName());
            ResultSet rs = conn.createStatement().executeQuery("SELECT \":ID\", \"0:ZIP\" FROM " + indexName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals( 95051, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals( 95052, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("3", rs.getString(1));
            assertEquals( 95053, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testTransformAlreadyTransformedTable() throws Exception {
        String dataTableName = "TBL_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            String stmString1 =
                    "CREATE TABLE IF NOT EXISTS " + dataTableName
                            + " (ID INTEGER NOT NULL, CITY_PK VARCHAR NOT NULL, NAME_PK VARCHAR NOT NULL,NAME VARCHAR, ZIP INTEGER CONSTRAINT PK PRIMARY KEY(ID, CITY_PK, NAME_PK)) ";
            conn.createStatement().execute(stmString1);

            String upsertQuery = "UPSERT INTO %s VALUES(%d, '%s', '%s', '%s', %d)";

            // insert rows
            conn.createStatement().execute(String.format(upsertQuery, dataTableName, 1, "city1", "name1", "uname1", 95051));

            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableName);

            conn.createStatement().execute("ALTER TABLE " + dataTableName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(null, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            conn.createStatement().execute(String.format(upsertQuery, dataTableName, 2, "city2", "name2", "uname2", 95052));

            assertEquals(numOfRows+1, countRows(conn, dataTableName));

            // Make sure that we are not accessing the original table. We are supposed to read from the new table above
            Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            TableName hTableName = TableName.valueOf(dataTableName);
            admin.disableTable(hTableName);
            admin.deleteTable(hTableName);

            // Removing this so that we are sure that we are not picking up the old transform record.
            Transform.removeTransformRecord(record, conn.unwrap(PhoenixConnection.class));
            conn.createStatement().execute("ALTER TABLE " + dataTableName +
                    " SET IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0");
            record = Transform.getTransformRecord(null, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);

            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, record.getNewPhysicalTableName());
            conn.createStatement().execute(String.format(upsertQuery, dataTableName, 3, "city3", "name3", "uname3", 95053));
            assertEquals(numOfRows+2, countRows(conn, dataTableName));

            ResultSet rs = conn.createStatement().executeQuery("SELECT ID, ZIP, NAME, NAME_PK, CITY_PK FROM " + dataTableName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals( 95051, rs.getInt(2));
            assertEquals( "uname1", rs.getString(3));
            assertEquals( "name1", rs.getString(4));
            assertEquals( "city1", rs.getString(5));
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals( 95052, rs.getInt(2));
            assertEquals( "uname2", rs.getString(3));
            assertEquals( "name2", rs.getString(4));
            assertEquals( "city2", rs.getString(5));
            assertTrue(rs.next());
            assertEquals("3", rs.getString(1));
            assertEquals( 95053, rs.getInt(2));
            assertEquals( "uname3", rs.getString(3));
            assertEquals( "name3", rs.getString(4));
            assertEquals( "city3", rs.getString(5));
            assertFalse(rs.next());
        }
    }

    public void testDifferentClientAccessTransformedTable(boolean isImmutable) throws Exception {
        String dataTableName = "TBL_" + generateUniqueName();
        try (Connection conn1 = DriverManager.getConnection(getUrl(), testProps)) {
            conn1.setAutoCommit(true);
            int numOfRows = 1;
            TransformToolIT.createTableAndUpsertRows(conn1, dataTableName, numOfRows, isImmutable ? " IMMUTABLE_ROWS=true" : "");

            String url2 = ConnectionInfo.create(url, null, null).withPrincipal("LongRunningQueries").toUrl();
            try (Connection conn2 = DriverManager.getConnection(url2, PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
                conn2.setAutoCommit(true);
                TransformToolIT.upsertRows(conn2, dataTableName, 2, 1);

                conn1.createStatement().execute("ALTER TABLE " + dataTableName +
                        " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
                SystemTransformRecord record = Transform.getTransformRecord(null, dataTableName, null, null, conn1.unwrap(PhoenixConnection.class));
                assertNotNull(record);
                waitForTransformToGetToState(conn1.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
                assertMetadata(conn1, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

                // A connection does transform and another connection doesn't try to upsert into old table
                TransformToolIT.upsertRows(conn2, dataTableName, 3, 1);

                ResultSet rs = conn2.createStatement().executeQuery("SELECT ID, NAME, ZIP FROM " + dataTableName);
                assertTrue(rs.next());
                assertEquals("1", rs.getString(1));
                assertEquals("uname1", rs.getString(2));
                assertEquals( 95051, rs.getInt(3));
                assertTrue(rs.next());
                assertEquals("2", rs.getString(1));
                assertEquals("uname2", rs.getString(2));
                assertEquals( 95052, rs.getInt(3));
                assertTrue(rs.next());
                assertEquals("3", rs.getString(1));
                assertEquals("uname3", rs.getString(2));
                assertEquals( 95053, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testDifferentClientAccessTransformedTable_mutable() throws Exception {
        // A connection does transform and another connection doesn't try to upsert into old table
        testDifferentClientAccessTransformedTable(false);
    }

    @Test
    public void testDifferentClientAccessTransformedTable_immutable() throws Exception {
        // A connection does transform and another connection doesn't try to upsert into old table
        testDifferentClientAccessTransformedTable(true);
    }

    @Test
    public void testTransformTable_cutoverNotAuto() throws Exception {
        // Transform index and see it is not auto cutover
        String schemaName = generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            TransformMonitorTask.disableTransformMonitorTask(true);
            conn.setAutoCommit(true);
            int numOfRows = 1;
            TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, numOfRows, "");
            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            // Wait for task to fail
            waitForTaskState(conn, PTable.TaskType.TRANSFORM_MONITOR, dataTableName, PTable.TaskStatus.FAILED);
        } finally {
            TransformMonitorTask.disableTransformMonitorTask(false);
        }
    }

    @Test
    public void testTransformMonitor_tableWithViews_OnOldAndNew() throws Exception {
        // Create view before and after transform with different select statements and check
        String schemaName = "S_" + generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String fullDataTableName = SchemaUtil.getTableName(schemaName, dataTableName);
        String view1 = "VW1_" + generateUniqueName();
        String view2 = "VW2_" + generateUniqueName();
        String createTblStr = "CREATE TABLE %s (ID INTEGER NOT NULL, PK1 VARCHAR NOT NULL"
                + ", NAME VARCHAR CONSTRAINT PK_1 PRIMARY KEY (ID, PK1)) ";
        String createViewStr = "CREATE VIEW %s  (VIEW_COL1 VARCHAR) AS SELECT * FROM %s WHERE NAME='%s'";

        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)){
            conn.setAutoCommit(true);
            conn.createStatement().execute(String.format(createTblStr, fullDataTableName));

            int numOfRows=2;
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", fullDataTableName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            for (int i=1; i <= numOfRows; i++) {
                stmt1.setInt(1, i);
                stmt1.setString(2, "pk" + i);
                stmt1.setString(3, "name"+ i);
                stmt1.execute();
            }
            conn.createStatement().execute(String.format(createViewStr, view1, fullDataTableName, "name1"));

            assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, fullDataTableName);

            conn.createStatement().execute("ALTER TABLE " + fullDataTableName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            conn.createStatement().execute(String.format(createViewStr, view2, fullDataTableName, "name2"));

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + view2);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("pk2", rs.getString(2));
            assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("SELECT * FROM " + view1);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("pk1", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    public static void waitForTransformToGetToState(PhoenixConnection conn, SystemTransformRecord record, PTable.TransformStatus status) throws InterruptedException, SQLException {
        int maxTries = 250, nTries = 0;
        String lastStatus = "";
        do {
            if (status.name().equals(record.getTransformStatus())) {
                return;
            }
            Thread.sleep(500);
            record = Transform.getTransformRecord(record.getSchemaName(), record.getLogicalTableName(), record.getLogicalParentName(), record.getTenantId(), conn);
            lastStatus = record.getTransformStatus();
        } while (++nTries < maxTries);
        try {
            SingleCellIndexIT.dumpTable("SYSTEM.TASK");
        } catch (Exception e) {

        }
        fail("Ran out of time waiting for transform state to become " + status + " but it was " + lastStatus);
    }

}
