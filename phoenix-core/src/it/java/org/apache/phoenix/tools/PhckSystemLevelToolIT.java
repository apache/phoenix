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
package org.apache.phoenix.tools;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;

@Category(NeedsOwnMiniClusterTest.class)
public class PhckSystemLevelToolIT extends ParallelStatsEnabledIT {

    @Test
    public void testTableIsNotExistAndRecreation() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool(config);

        try (Connection connection = ConnectionUtil.getInputConnection(config);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
             Admin admin = phoenixConnection.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {

            assertTrue(tool.isTableQueryable(SYSTEM_TASK_TABLE));
            assertTrue(tool.isTableExisted(SYSTEM_TASK_TABLE, admin));

            admin.disableTable(tool.getHBaseTableName(SYSTEM_TASK_TABLE));
            admin.deleteTable(tool.getHBaseTableName(SYSTEM_TASK_TABLE));
            assertFalse(tool.isTableExisted(SYSTEM_TASK_TABLE, admin));

            tool.setFixTableNotExistMode(true);
            tool.fixTableNotExist(SYSTEM_TASK_TABLE);
            assertTrue(tool.isTableExisted(SYSTEM_TASK_TABLE, admin));
        }
    }

    @Test
    public void testTableDisabled() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool(config);

        try (Connection connection = ConnectionUtil.getInputConnection(config);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
             Admin admin = phoenixConnection.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            assertTrue(tool.isTableQueryable(SYSTEM_CHILD_LINK_TABLE));
            assertFalse(tool.isTableDisabled(SYSTEM_CHILD_LINK_TABLE, admin));

            admin.disableTable(tool.getHBaseTableName(SYSTEM_CHILD_LINK_TABLE));
            assertTrue(tool.isTableDisabled(SYSTEM_CHILD_LINK_TABLE, admin));

            tool.setFixTableDisabledMode(true);
            tool.fixDisabledTable(SYSTEM_CHILD_LINK_TABLE);
            assertFalse(tool.isTableDisabled(SYSTEM_CHILD_LINK_TABLE, admin));
        }
    }

    @Test
    public void testTableColumnRowsGreaterThanHeadRowCount() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool(config);

        Properties props = new Properties();
        String tenantIdProperty = "TENANT1";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantIdProperty);

        try (Connection conn = ConnectionUtil.getInputConnection(config);
            Connection tenantConn = ConnectionUtil.getInputConnection(config, props)) {
            assertTrue(tool.isTableRowCountMatches(SYSTEM_LOG_TABLE));

            conn.createStatement().executeUpdate(
                    "UPSERT INTO SYSTEM.CATALOG (TABLE_SCHEM, TABLE_NAME, COLUMN_NAME) " +
                            "VALUES ('SYSTEM', 'LOG', 'DUMMY')");
            conn.commit();

            assertFalse(tool.isTableRowCountMatches(SYSTEM_LOG_TABLE));
            try {
                tenantConn.createStatement().executeQuery("SELECT * FROM SYSTEM.LOG");
                fail();
            } catch (Exception e) {

            }

            tool.setFixMismatchedRowCountMode(true);
            tool.fixMismatchedRowCount(SYSTEM_LOG_TABLE);
            assertTrue(tool.isTableRowCountMatches(SYSTEM_LOG_TABLE));
            assertTrue(tool.isTableQueryable(SYSTEM_LOG_TABLE));

            try {
                tenantConn.createStatement().executeQuery("SELECT * FROM SYSTEM.LOG");

            } catch (Exception e) {
                fail();
            }
        }
    }

    @Test
    public void testTableColumnRowsLessThanHeadRowCount() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool(config);

        try (Connection conn = ConnectionUtil.getInputConnection(config)) {
            assertTrue(tool.isTableRowCountMatches(SYSTEM_STATS_TABLE));

            conn.createStatement().executeUpdate(
                    "DELETE FROM SYSTEM.CATALOG WHERE TABLE_SCHEM ='SYSTEM' AND " +
                            "TABLE_NAME= 'STATS' AND COLUMN_NAME= 'PHYSICAL_NAME'");
            conn.commit();
            assertFalse(tool.isTableRowCountMatches(SYSTEM_STATS_TABLE));

            tool.setFixMismatchedRowCountMode(true);
            tool.fixMismatchedRowCount(SYSTEM_STATS_TABLE);
            assertTrue(tool.isTableRowCountMatches(SYSTEM_STATS_TABLE));
        }
    }

    @Test
    public void testForNoCorruptionInMonitorMode() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool(config);
        tool.setMonitorMode(true);
        tool.run();
        assertEquals(0, tool.getMissingTables().size());
        assertEquals(0, tool.getDisabledTables().size());
        assertEquals(0, tool.getMismatchedRowCountTables().size());
    }
}
