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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests various scenarios when
 * {@link QueryServices#ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK}
 * is set to true and SYSTEM.CATALOG should not be allowed to split.
 * Note that this config must be set on both the client and server
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SystemCatalogRollbackEnabledIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = new HashMap<>(2);
        Map<String, String> clientProps = new HashMap<>(1);
        serverProps.put(QueryServices.SYSTEM_CATALOG_SPLITTABLE, "false");
        serverProps.put(QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK,
                "true");
        clientProps.put(QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK,
                "true");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    private void createTableAndTenantViews(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement();) {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            stmt.execute("CREATE TABLE " + tableName +
                    " (TENANT_ID VARCHAR NOT NULL, " +
                    "PK1 VARCHAR NOT NULL, V1 VARCHAR CONSTRAINT PK " +
                    "PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT=true");
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + tableName +
                        "_view1 AS SELECT * FROM " + tableName;
                tenant1Conn.createStatement().execute(view1DDL);
            }
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                String view1DDL = "CREATE VIEW " + tableName +
                        "_view2 AS SELECT * FROM " + tableName;
                tenant2Conn.createStatement().execute(view1DDL);
            }
            conn.commit();
        }
    }

    private Connection getTenantConnection(String tenantId)
            throws SQLException {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }

    private void assertNumRegions(HBaseTestingUtility testUtil,
            TableName tableName, int expectedNumRegions) throws IOException {
        RegionLocator rl = testUtil.getConnection().getRegionLocator(tableName);
        assertEquals(expectedNumRegions, rl.getAllRegionLocations().size());
    }

    /**
     * Make sure that SYSTEM.CATALOG cannot be split if
     * {@link QueryServices#SYSTEM_CATALOG_SPLITTABLE} is false
     */
    @Test
    public void testSystemCatalogDoesNotSplit() throws Exception {
        HBaseTestingUtility testUtil = getUtility();
        for (int i=0; i<10; i++) {
            createTableAndTenantViews("schema"+i+".table_"+i);
        }
        TableName systemCatalog = TableName.valueOf(
                PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
        assertNumRegions(testUtil, systemCatalog, 1);

        try {
            // now attempt to split SYSTEM.CATALOG
            testUtil.getAdmin().split(systemCatalog);
            // make sure the split finishes (in hbase 2.x the Admin.split() API
            // is asynchronous)
            testUtil.getAdmin().disableTable(systemCatalog);
            testUtil.getAdmin().enableTable(systemCatalog);
            fail(String.format("Splitting %s should have failed",
                    systemCatalog.getNameAsString()));
        } catch (DoNotRetryIOException e) {
            // In hbase 2.x, if splitting is disabled for a table,
            // the split request will throw an exception.
            assertTrue(e.getMessage().contains("NOT splittable"));
        }

        // test again... Must still be exactly one region.
        assertNumRegions(testUtil, systemCatalog, 1);
    }

    /**
     * Ensure that we cannot add a column to a parent table or view if
     * {@link QueryServices#ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK} is true
     */
    @Test
    public void testAddColumnOnParentFails() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final String parentTableName = SchemaUtil.getTableName(
                    generateUniqueName(), generateUniqueName());
            final String parentViewName = SchemaUtil.getTableName(
                    generateUniqueName(), generateUniqueName());
            final String childViewName = SchemaUtil.getTableName(
                    generateUniqueName(), generateUniqueName());
            // create parent table
            String ddl = "CREATE TABLE " + parentTableName
                    + " (col1 INTEGER NOT NULL,"
                    + " col2 INTEGER " + "CONSTRAINT pk PRIMARY KEY (col1))";
            conn.createStatement().execute(ddl);

            // create view on table
            ddl = "CREATE VIEW " + parentViewName + " AS SELECT * FROM "
                    + parentTableName;
            conn.createStatement().execute(ddl);
            try {
                ddl = "ALTER TABLE " + parentTableName + " ADD col4 INTEGER";
                conn.createStatement().execute(ddl);
                fail("ALTER TABLE ADD should not be allowed on parent table");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE
                        .getErrorCode(), e.getErrorCode());
            }

            // create child view on above view
            ddl = "CREATE VIEW " + childViewName
                    + "(col3 INTEGER) AS SELECT * FROM " + parentViewName;
            conn.createStatement().execute(ddl);
            try {
                ddl = "ALTER VIEW " + parentViewName + " ADD col4 INTEGER";
                conn.createStatement().execute(ddl);
                fail("ALTER VIEW ADD should not be allowed on parent view");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE
                        .getErrorCode(), e.getErrorCode());
            }

            // alter child view with add column should be allowed
            ddl = "ALTER VIEW " + childViewName + " ADD col4 INTEGER";
            conn.createStatement().execute(ddl);
        }
    }

    /**
     * Ensure that we cannot drop a column from a parent table or view if
     * {@link QueryServices#ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK} is true
     */
    @Test
    public void testDropColumnOnParentFails() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final String parentTableName = SchemaUtil.getTableName(
                    generateUniqueName(), generateUniqueName());
            final String parentViewName = SchemaUtil.getTableName(
                    generateUniqueName(), generateUniqueName());
            final String childViewName = SchemaUtil.getTableName(
                    generateUniqueName(), generateUniqueName());
            // create parent table
            String ddl = "CREATE TABLE " + parentTableName
                    + " (col1 INTEGER NOT NULL, col2 INTEGER, col3 VARCHAR "
                    + "CONSTRAINT pk PRIMARY KEY (col1))";
            conn.createStatement().execute(ddl);

            // create view on table
            ddl = "CREATE VIEW " + parentViewName + " AS SELECT * FROM "
                    + parentTableName;
            conn.createStatement().execute(ddl);
            try {
                ddl = "ALTER TABLE " + parentTableName + " DROP COLUMN col2";
                conn.createStatement().execute(ddl);
                fail("ALTER TABLE DROP COLUMN should not be allowed "
                        + "on parent table");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE
                        .getErrorCode(), e.getErrorCode());
            }

            // create child view on above view
            ddl = "CREATE VIEW " + childViewName
                    + "(col5 INTEGER) AS SELECT * FROM " + parentViewName;
            conn.createStatement().execute(ddl);
            try {
                ddl = "ALTER VIEW " + parentViewName + " DROP COLUMN col2";
                conn.createStatement().execute(ddl);
                fail("ALTER VIEW DROP COLUMN should not be allowed "
                        + "on parent view");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE
                        .getErrorCode(), e.getErrorCode());
            }

            // alter child view with drop column should be allowed
            ddl = "ALTER VIEW " + childViewName + " DROP COLUMN col2";
            conn.createStatement().execute(ddl);
        }
    }

    // Test for PHOENIX-6032
    @Test
    public void testViewDoesNotSeeDataForDroppedColumn() throws Exception {
        final String parentName = "T_" + SchemaUtil.getTableName(
                generateUniqueName(), generateUniqueName());
        final String viewName = "V_" + SchemaUtil.getTableName(
                generateUniqueName(), generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + parentName
                    + " (A INTEGER PRIMARY KEY, B INTEGER, C"
                    + " VARCHAR, D INTEGER)");
            conn.createStatement().execute("CREATE VIEW " + viewName
                    + " (VA INTEGER, VB INTEGER)"
                    + " AS SELECT * FROM " + parentName + " WHERE B=200");
            // Upsert some data via the view
            conn.createStatement().execute("UPSERT INTO " + viewName
                    + " (A,B,C,D,VA,VB) VALUES"
                    + " (2, 200, 'def', -20, 91, 101)");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Query from the parent table and assert expected values
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT A,B,C,D FROM " + parentName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(200, rs.getInt(2));
            assertEquals("def", rs.getString(3));
            assertEquals(-20, rs.getInt(4));
            assertFalse(rs.next());

            // Query from the view and assert expected values
            rs = conn.createStatement().executeQuery(
                    "SELECT A,B,C,D,VA,VB FROM " + viewName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(200, rs.getInt(2));
            assertEquals("def", rs.getString(3));
            assertEquals(-20, rs.getInt(4));
            assertEquals(91, rs.getInt(5));
            assertEquals(101, rs.getInt(6));
            assertFalse(rs.next());
        }

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Drop a parent column from the view
            conn.createStatement().execute("ALTER VIEW " + viewName +
                    " DROP COLUMN C");
            try {
                conn.createStatement().executeQuery("SELECT C FROM "
                        + viewName);
                fail("Expected a ColumnNotFoundException for C since it "
                        + "was dropped");
            } catch (ColumnNotFoundException ignore) { }
        }
    }

}