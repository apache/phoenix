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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

//FIXME this class has no @Category, and is never run by maven
public class GlobalConnectionTenantTableIT extends BaseTest {

    private static final String SCHEMA_NAME = "SCHEMA1";
    private static final String TABLE_NAME = generateUniqueName();
    private static final String TENANT_NAME = "TENANT_A";
    private static final String VIEW_NAME = "VIEW1";
    private static final String INDEX_NAME = "INDEX1";
    private static final String VIEW_INDEX_COL = "v2";
    private static final String FULL_VIEW_NAME = SchemaUtil.getTableName(SCHEMA_NAME, VIEW_NAME);
    private static final String FULL_INDEX_NAME = SchemaUtil.getTableName(SCHEMA_NAME, INDEX_NAME);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        createBaseTable(SCHEMA_NAME, TABLE_NAME, true, null, null);
        try (Connection conn = getTenantConnection(TENANT_NAME)) {
            createView(conn, SCHEMA_NAME, VIEW_NAME, TABLE_NAME);
            createViewIndex(conn, SCHEMA_NAME, INDEX_NAME, VIEW_NAME, VIEW_INDEX_COL);
        }
    }

    @Test
    public void testGetLatestTenantTable() throws SQLException {
        try (Connection conn = getConnection()) {
            PTable table = PhoenixRuntime.getTable(conn, TENANT_NAME, FULL_VIEW_NAME, null);
            assertNotNull(table);
            table = null;
            table = PhoenixRuntime.getTable(conn, TENANT_NAME, FULL_INDEX_NAME, null);
            assertNotNull(table);
        }
    }

    @Test
    public void testGetTenantViewAtTimestamp() throws SQLException {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = getConnection()) {
            PTable table = PhoenixRuntime.getTable(conn, TENANT_NAME, FULL_VIEW_NAME, null);
            long tableTimestamp = table.getTimeStamp();
            // Alter table
            try (Connection tenantConn = getTenantConnection(TENANT_NAME)) {
                String alterView = "ALTER VIEW " + FULL_VIEW_NAME + " ADD new_col INTEGER";
                tenantConn.createStatement().execute(alterView);
            }
            // Get the altered table and verify
            PTable newTable = PhoenixRuntime.getTable(conn, TENANT_NAME, FULL_VIEW_NAME);
            assertNotNull(newTable);
            assertTrue(newTable.getTimeStamp() > tableTimestamp);
            assertEquals(newTable.getColumns().size(), (table.getColumns().size() + 1));
            // Now get the old table and verify
            PTable oldTable = PhoenixRuntime.getTable(conn, TENANT_NAME, FULL_VIEW_NAME, startTime);
            assertNotNull(oldTable);
            assertEquals(oldTable.getTimeStamp(), tableTimestamp);
        }
    }

    @Test
    public void testGetTableWithoutTenantId() throws SQLException {
        try (Connection conn = getConnection()) {
            PTable table =
                    PhoenixRuntime.getTable(conn, null,
                        SchemaUtil.getTableName(SCHEMA_NAME, TABLE_NAME));
            assertNotNull(table);

            try {
                table = PhoenixRuntime.getTable(conn, null, FULL_VIEW_NAME);
                fail(
                    "Expected TableNotFoundException for trying to get tenant specific view without tenantid");
            } catch (SQLException e) {
                assertEquals(e.getErrorCode(), SQLExceptionCode.TABLE_UNDEFINED.getErrorCode());
            }
        }
    }

    @Test
    public void testTableNotFound() throws SQLException {
        try (Connection conn = getConnection()) {
            try {
                PTable table = PhoenixRuntime.getTable(conn, TENANT_NAME, FULL_VIEW_NAME, 1L);
                fail("Expected TableNotFoundException");
            } catch (SQLException e) {
                assertEquals(e.getErrorCode(), SQLExceptionCode.TABLE_UNDEFINED.getErrorCode());
            }
        }

    }

    @Test
    public void testGetTableFromCache() throws SQLException {
        try (Connection conn = getConnection()) {
            PTable table = PhoenixRuntime.getTable(conn, TENANT_NAME, FULL_VIEW_NAME, null);
            PTable newTable = PhoenixRuntime.getTable(conn, TENANT_NAME, FULL_VIEW_NAME, null);
            assertNotNull(newTable);
            assertTrue(newTable == table);
        }
    }

    private static void createBaseTable(String schemaName, String tableName, boolean multiTenant,
            Integer saltBuckets, String splits) throws SQLException {
        Connection conn = getConnection();
        String ddl =
                "CREATE TABLE " + SchemaUtil.getTableName(schemaName, tableName)
                        + " (t_id VARCHAR NOT NULL,\n" + "k1 VARCHAR NOT NULL,\n"
                        + "k2 INTEGER NOT NULL,\n" + "v1 VARCHAR,\n" + VIEW_INDEX_COL
                        + " INTEGER,\n" + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n";
        String ddlOptions = multiTenant ? "MULTI_TENANT=true" : "";
        if (saltBuckets != null) {
            ddlOptions =
                    ddlOptions + (ddlOptions.isEmpty() ? "" : ",") + "salt_buckets=" + saltBuckets;
        }
        if (splits != null) {
            ddlOptions = ddlOptions + (ddlOptions.isEmpty() ? "" : ",") + "splits=" + splits;
        }
        conn.createStatement().execute(ddl + ddlOptions);
        conn.close();
    }

    private static void createView(Connection conn, String schemaName, String viewName,
            String baseTableName) throws SQLException {
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
        conn.createStatement()
                .execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName);
        conn.commit();
    }

    private static void createViewIndex(Connection conn, String schemaName, String indexName,
            String viewName, String indexColumn) throws SQLException {
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        conn.createStatement().execute(
            "CREATE INDEX " + indexName + " ON " + fullViewName + "(" + indexColumn + ")");
        conn.commit();
    }

    private static Connection getTenantConnection(String tenant) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenant);
        return DriverManager.getConnection(getUrl(), props);
    }

    private static Connection getConnection() throws SQLException {
        Properties props = new Properties();
        return DriverManager.getConnection(getUrl(), props);
    }

}
