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
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexRebuildTaskIT extends BaseUniqueNamesOwnClusterIT {
    protected static String TENANT1 = "tenant1";
    private static RegionCoprocessorEnvironment TaskRegionEnvironment;

    @BeforeClass
    public static void doSetup() throws Exception {
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
        Connection conn = null;
        Connection viewConn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, TENANT1);

            viewConn =DriverManager.getConnection(getUrl(), props);
            String ddlFormat =
                    "CREATE TABLE IF NOT EXISTS " + baseTable + "  ("
                            + " %s PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s PK2)" + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            conn.commit();
            // Create a view
            String viewName = generateUniqueName();
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
            viewConn.createStatement().execute(viewDDL);

            // Create index
            String indexName = generateUniqueName();
            String idxSDDL = String.format("CREATE INDEX %s ON %s (V1)", indexName, viewName);

            viewConn.createStatement().execute(idxSDDL);

            // Insert rows
            int numOfValues = 1;
            for (int i=0; i < numOfValues; i++){
                viewConn.createStatement().execute(
                        String.format("UPSERT INTO %s VALUES('%s', '%s', '%s')", viewName, String.valueOf(i), "y",
                                "z"));
            }
            viewConn.commit();

            String data = "{IndexName:" + indexName + "}";
            // Run IndexRebuildTask
            TaskRegionObserver.SelfHealingTask task =
                    new TaskRegionObserver.SelfHealingTask(
                            TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);

            Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            // Add a task to System.Task to build indexes
            Task.addTask(conn.unwrap(PhoenixConnection.class), PTable.TaskType.INDEX_REBUILD,
                    TENANT1, null, viewName,
                    PTable.TaskStatus.CREATED.toString(), data, null, startTs, null, true);


            task.run();

            String viewIndexTableName = MetaDataUtil.getViewIndexPhysicalName(baseTable);
            ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();
            int count = getUtility().countRows(queryServices.getTable(Bytes.toBytes(viewIndexTableName)));
            assertTrue(count == numOfValues);


            // Remove index contents and try again
            Admin admin = queryServices.getAdmin();
            TableName tableName = TableName.valueOf(viewIndexTableName);
            admin.disableTable(tableName);
            admin.truncateTable(tableName, false);

            data = "{IndexName:" + indexName + ", DisableBefore:true}";

            // Add a new task (update status to created) to System.Task to rebuild indexes
            Task.addTask(conn.unwrap(PhoenixConnection.class), PTable.TaskType.INDEX_REBUILD,
                    TENANT1, null, viewName,
                    PTable.TaskStatus.CREATED.toString(), data, null, startTs, null, true);
            task.run();

            Table systemHTable= queryServices.getTable(Bytes.toBytes("SYSTEM."+PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE));
            count = getUtility().countRows(systemHTable);
            assertEquals(1, count);

            // Check task status and other column values.
            waitForTaskState(conn, PTable.TaskType.INDEX_REBUILD, PTable.TaskStatus.COMPLETED);

            // See that index is rebuilt and confirm index has rows
            Table htable= queryServices.getTable(Bytes.toBytes(viewIndexTableName));
            count = getUtility().countRows(htable);
            assertEquals(numOfValues, count);
        } finally {
            conn.createStatement().execute("DELETE " + " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);
            conn.commit();
            if (conn != null) {
                conn.close();
            }
            if (viewConn != null) {
                viewConn.close();
            }
        }
    }

    public static void waitForTaskState(Connection conn, PTable.TaskType taskType, PTable.TaskStatus expectedTaskStatus) throws InterruptedException,
            SQLException {
        int maxTries = 100, nTries = 0;
        do {
            Thread.sleep(2000);
            ResultSet rs = conn.createStatement().executeQuery("SELECT * " +
                    " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME +
                    " WHERE " + PhoenixDatabaseMetaData.TASK_TYPE + " = " +
                    taskType.getSerializedValue());

            String taskStatus = null;

            if (rs.next()) {
                taskStatus = rs.getString(PhoenixDatabaseMetaData.TASK_STATUS);
                boolean matchesExpected = (expectedTaskStatus.toString().equals(taskStatus));
                if (matchesExpected) {
                    return;
                }
            }
        } while (++nTries < maxTries);
        fail("Ran out of time waiting for task state to become " + expectedTaskStatus);
    }
}
