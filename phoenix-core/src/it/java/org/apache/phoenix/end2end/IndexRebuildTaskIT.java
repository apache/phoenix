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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.waitForIndexRebuild;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
public class IndexRebuildTaskIT extends BaseTest {
    protected static String TENANT1 = "tenant1";
    private static RegionCoprocessorEnvironment TaskRegionEnvironment;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        HashMap<String, String> props = new HashMap<>();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        TaskRegionEnvironment =
                getUtility()
                        .getRSForFirstRegionInTable(
                                PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .getRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .get(0).getCoprocessorHost()
                        .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    private String generateDDL(String format) {
        StringBuilder optionsBuilder = new StringBuilder();

        if (optionsBuilder.length() != 0) optionsBuilder.append(",");
        optionsBuilder.append("MULTI_TENANT=true");

        return String.format(format, "TENANT_ID VARCHAR NOT NULL, ", "TENANT_ID, ", optionsBuilder.toString());
    }

    @Test
    public void testIndexRebuildTask() throws Throwable {
        String baseTable = generateUniqueName();
        String viewName = generateUniqueName();
        Connection conn = null;
        Connection tenantConn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            conn.setAutoCommit(false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, TENANT1);

            tenantConn =DriverManager.getConnection(getUrl(), props);
            String ddlFormat =
                    "CREATE TABLE IF NOT EXISTS " + baseTable + "  ("
                            + " %s PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s PK2)" + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            conn.commit();
            // Create a view
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
            tenantConn.createStatement().execute(viewDDL);

            // Create index
            String indexName = generateUniqueName();
            String idxSDDL = String.format("CREATE INDEX %s ON %s (V1)", indexName, viewName);

            tenantConn.createStatement().execute(idxSDDL);

            // Insert rows
            int numOfValues = 1000;
            for (int i=0; i < numOfValues; i++){
                tenantConn.createStatement().execute(
                        String.format("UPSERT INTO %s VALUES('%s', '%s', '%s')", viewName, String.valueOf(i), "y",
                                "z"));
            }
            tenantConn.commit();

            waitForIndexRebuild(conn, indexName, PIndexState.ACTIVE);
            String viewIndexTableName = MetaDataUtil.getViewIndexPhysicalName(baseTable);
            ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();

            Table indexHTable = queryServices.getTable(Bytes.toBytes(viewIndexTableName));
            int count = getUtility().countRows(indexHTable);
            assertEquals(numOfValues, count);

            // Alter to Unusable makes the index status inactive.
            // If I Alter to DISABLE, it fails to in Index tool while setting state to active due to Invalid transition.
            tenantConn.createStatement().execute(
                    String.format("ALTER INDEX %s ON %s UNUSABLE", indexName, viewName));
            tenantConn.commit();

            // Remove index contents and try again
            Admin admin = queryServices.getAdmin();
            TableName tableName = TableName.valueOf(viewIndexTableName);
            admin.disableTable(tableName);
            admin.truncateTable(tableName, false);

            count = getUtility().countRows(indexHTable);
            assertEquals(0, count);

            String data = "{\"IndexName\":\"" + indexName + "\"}";

            // Run IndexRebuildTask
            TaskRegionObserver.SelfHealingTask task =
                    new TaskRegionObserver.SelfHealingTask(
                            TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);

            Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            Task.addTask(new SystemTaskParams.SystemTaskParamsBuilder()
                .setConn(conn.unwrap(PhoenixConnection.class))
                .setTaskType(PTable.TaskType.INDEX_REBUILD)
                .setTenantId(TENANT1)
                .setSchemaName(null)
                .setTableName(viewName)
                .setTaskStatus(PTable.TaskStatus.CREATED.toString())
                .setData(data)
                .setPriority(null)
                .setStartTs(startTs)
                .setEndTs(null)
                .setAccessCheckEnabled(true)
                .build());
            task.run();

            // Check task status and other column values.
            waitForTaskState(conn, PTable.TaskType.INDEX_REBUILD, viewName, PTable.TaskStatus.COMPLETED);

            // See that index is rebuilt and confirm index has rows
            count = getUtility().countRows(indexHTable);
            assertEquals(numOfValues, count);
        } finally {
            if (conn != null) {
                conn.createStatement().execute("DELETE " + " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME
                        + " WHERE TABLE_NAME ='" + viewName  + "'");
                conn.commit();
                conn.close();
            }
            if (tenantConn != null) {
                tenantConn.close();
            }
        }
    }

    public static void waitForTaskState(Connection conn, PTable.TaskType taskType, String expectedTableName,
            PTable.TaskStatus expectedTaskStatus) throws InterruptedException,
            SQLException {
        int maxTries = 200, nTries = 0;
        String taskStatus = "";
        String taskData = "";
        do {
            Thread.sleep(2000);
            String stmt = "SELECT * " +
                    " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME +
                    " WHERE " + PhoenixDatabaseMetaData.TASK_TYPE + " = " +
                    taskType.getSerializedValue();
            if (expectedTableName != null) {
                stmt += " AND " + PhoenixDatabaseMetaData.TABLE_NAME + "='" + expectedTableName + "'";
            }

            ResultSet rs = conn.createStatement().executeQuery(stmt);

            while (rs.next()) {
                taskStatus = rs.getString(PhoenixDatabaseMetaData.TASK_STATUS);
                taskData = rs.getString(PhoenixDatabaseMetaData.TASK_DATA);
                boolean matchesExpected = (expectedTaskStatus.toString().equals(taskStatus));
                if (matchesExpected) {
                    return;
                }
            }
        } while (++nTries < maxTries);
        fail(String.format("Ran out of time waiting for current task state %s to become %s. TaskData: %s", taskStatus, expectedTaskStatus, taskData));
    }
}
