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

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.TableViewFinderResult;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.util.ViewUtil;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;

import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class DropTableWithViewsIT extends SplitSystemCatalogIT {

    private final boolean isMultiTenant;
    private final boolean columnEncoded;
    private final String TENANT_SPECIFIC_URL1 = getUrl() + ';' + TENANT_ID_ATTRIB + "=" + TENANT1;
    public static final Logger LOGGER = LoggerFactory.getLogger(DropTableWithViewsIT.class);
    private static RegionCoprocessorEnvironment TaskRegionEnvironment;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        serverProps.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        SplitSystemCatalogIT.doSetup(serverProps);
        TaskRegionEnvironment =
                getUtility()
                        .getRSForFirstRegionInTable(
                                PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .getRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .get(0).getCoprocessorHost()
                        .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    public DropTableWithViewsIT(boolean isMultiTenant, boolean columnEncoded) {
        this.isMultiTenant = isMultiTenant;
        this.columnEncoded = columnEncoded;
    }

    @Parameters(name="DropTableWithViewsIT_multiTenant={0}, columnEncoded={1}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false }, { false, true },
                { true, false }, { true, true } });
    }

    private String generateDDL(String format) {
        return generateDDL("", format);
    }

    private String generateDDL(String options, String format) {
        StringBuilder optionsBuilder = new StringBuilder(options);
        if (!columnEncoded) {
            if (optionsBuilder.length() != 0)
                optionsBuilder.append(",");
            optionsBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (isMultiTenant) {
            if (optionsBuilder.length() !=0 )
                optionsBuilder.append(",");
            optionsBuilder.append("MULTI_TENANT=true");
        }
        return String.format(format, isMultiTenant ? "TENANT_ID VARCHAR NOT NULL, " : "",
            isMultiTenant ? "TENANT_ID, " : "", optionsBuilder.toString());
    }

    @Test
    public void testDropTableWithChildViews() throws Exception {
        String baseTable = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn =
                        isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn) {
            conn.setAutoCommit(true);
            viewConn.setAutoCommit(true);
            // Empty the task table first.
            conn.createStatement().execute("DELETE " + " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);

            String ddlFormat =
                    "CREATE TABLE IF NOT EXISTS " + baseTable + "  ("
                            + " %s PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s PK2)" + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));

            // Create a view tree (i.e., tree of views) with depth of 2 and fanout factor of 4
            for (int  i = 0; i < 4; i++) {
                String childView = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
                String childViewDDL = "CREATE VIEW " + childView + " AS SELECT * FROM " + baseTable;
                viewConn.createStatement().execute(childViewDDL);
                for (int j = 0; j < 4; j++) {
                    String grandChildView = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
                    String grandChildViewDDL = "CREATE VIEW " + grandChildView + " AS SELECT * FROM " + childView;
                    viewConn.createStatement().execute(grandChildViewDDL);
                }
            }
            // Drop the base table
            String dropTable = String.format("DROP TABLE IF EXISTS %s CASCADE", baseTable);
            conn.createStatement().execute(dropTable);
            // Run DropChildViewsTask to complete the tasks for dropping child views. The depth of the view tree is 2,
            // so we expect that this will be done in two task handling runs as each non-root level will be processed
            // in one run
            TaskRegionObserver.SelfHealingTask task =
                    new TaskRegionObserver.SelfHealingTask(
                            TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
            task.run();
            task.run();

            assertTaskColumns(conn, PTable.TaskStatus.COMPLETED.toString(), PTable.TaskType.DROP_CHILD_VIEWS,
                    null, null, null, null, null);

            // Views should be dropped by now
            TableName linkTable = TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES);
            TableViewFinderResult childViewsResult = new TableViewFinderResult();
            ViewUtil.findAllRelatives(getUtility().getConnection().getTable(linkTable),
                    HConstants.EMPTY_BYTE_ARRAY,
                    SchemaUtil.getSchemaNameFromFullName(baseTable).getBytes(),
                    SchemaUtil.getTableNameFromFullName(baseTable).getBytes(),
                    PTable.LinkType.CHILD_TABLE,
                    childViewsResult);
            assertTrue(childViewsResult.getLinks().size() == 0);
            // There should not be any orphan views
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME +
                    " WHERE " + PhoenixDatabaseMetaData.TABLE_SCHEM + " = '" + SCHEMA2 +"'");
            assertFalse(rs.next());
        }
    }

    public static void assertTaskColumns(Connection conn, String expectedStatus, PTable.TaskType taskType,
            String expectedTableName, String expectedTenantId, String expectedSchema, Timestamp expectedTs,
            String expectedIndexName)
            throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT * " +
                " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME +
                " WHERE " + PhoenixDatabaseMetaData.TASK_TYPE + " = " +
                taskType.getSerializedValue());
        assertTrue(rs.next());
        String taskStatus = rs.getString(PhoenixDatabaseMetaData.TASK_STATUS);
        assertEquals(expectedStatus, taskStatus);

        if (expectedTableName != null) {
            String tableName = rs.getString(PhoenixDatabaseMetaData.TABLE_NAME);
            assertEquals(expectedTableName, tableName);
        }

        if (expectedTenantId != null) {
            String tenantId = rs.getString(PhoenixDatabaseMetaData.TENANT_ID);
            assertEquals(expectedTenantId, tenantId);
        }

        if (expectedSchema != null) {
            String schema = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM);
            assertEquals(expectedSchema, schema);
        }

        if (expectedTs != null) {
            Timestamp ts = rs.getTimestamp(PhoenixDatabaseMetaData.TASK_TS);
            assertEquals(expectedTs, ts);
        }

        if (expectedIndexName != null) {
            String data = rs.getString(PhoenixDatabaseMetaData.TASK_DATA);
            assertEquals(true, data.contains("\"IndexName\":\"" + expectedIndexName));
        }
    }
}
