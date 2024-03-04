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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost.PHOENIX_META_DATA_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UpdatableViewRestrictionsIT extends SplitSystemCatalogIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        NUM_SLAVES_BASE = 6;
        boolean splitSystemCatalog = (driver == null);
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.PHOENIX_ACLS_ENABLED, "true");
        serverProps.put(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY,
                ViewConcurrencyAndFailureIT.TestMetaDataRegionObserver.class
                        .getName());
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                ReadOnlyProps.EMPTY_PROPS);
        // Split SYSTEM.CATALOG once after the mini-cluster is started
        if (splitSystemCatalog) {
            // splitSystemCatalog is incompatible with the balancer chore
            getUtility().getHBaseCluster().getMaster().balanceSwitch(false);
            splitSystemCatalog();
        }
    }

    private void createTable(Connection conn, String tableName) throws Exception {
        String ddl = "CREATE TABLE " + tableName
                + " (k1 INTEGER NOT NULL, k2 DECIMAL, k3 INTEGER NOT NULL, s VARCHAR "
                + "CONSTRAINT pk PRIMARY KEY (k1, k2, k3))";
        conn.createStatement().execute(ddl);
    }

    private void createMultiTenantTable(Connection conn, String tableName) throws Exception {
        String ddl = "CREATE TABLE " + tableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true";
        conn.createStatement().execute(ddl);
    }

    private Connection getTenantConnection(final String tenantId) throws Exception {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        tenantProps.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        return DriverManager.getConnection(getUrl(), tenantProps);
    }

    private void verifyNumberOfRows(String tableName, String tenantId, int expectedRows,
                                    Connection conn) throws Exception {
        String query = "SELECT COUNT(*) FROM " + tableName;
        if (tenantId != null) {
            query = query + " WHERE TENANT_ID = '" + tenantId + "'";
        }
        try (Statement stm = conn.createStatement()) {
            ResultSet rs = stm.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(expectedRows, rs.getInt(1));
        }
    }

    @Test
    public void testReadOnlyViewWithNonPkInWhere() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTable(conn, fullTableName);

            Statement stmt = conn.createStatement();
            String viewDDL = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName +
                    " WHERE s = 'a'";
            stmt.execute(viewDDL);
            try {
                stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 3, 'a')");
                fail();
            } catch (ReadOnlyTableException ignored) {
            }
        }
    }

    @Test
    public void testReadOnlyViewWithPkNotInOrderInWhere1() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTable(conn, fullTableName);

            Statement stmt = conn.createStatement();
            String viewDDL = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName +
                    " WHERE k1 = 1 AND k3 = 3";
            stmt.execute(viewDDL);
            try {
                stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 3, 'a')");
                fail();
            } catch (ReadOnlyTableException ignored) {
            }
        }
    }

    @Test
    public void testReadOnlyViewWithPkNotInOrderInWhere2() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTable(conn, fullTableName);

            Statement stmt = conn.createStatement();
            String viewDDL = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName +
                    " WHERE k1 = 1 AND k2 = 2 AND s = 'a'";
            stmt.execute(viewDDL);
            try {
                stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 3, 'a')");
                fail();
            } catch (ReadOnlyTableException ignored) {
            }
        }
    }

    @Test
    public void testReadOnlyViewWithPkNotSameInWhere() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullGlobalViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String fullSiblingViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTable(conn, fullTableName);

            Statement stmt = conn.createStatement();
            String globalViewDDL =
                    "CREATE VIEW " + fullGlobalViewName + " AS SELECT * FROM " + fullTableName +
                            " WHERE k1 = 1";
            stmt.execute(globalViewDDL);

            String siblingViewDDL = "CREATE VIEW " + fullSiblingViewName + " AS SELECT * FROM " +
                    fullGlobalViewName + " WHERE k2 = 1";
            stmt.execute(siblingViewDDL);
            String viewDDL =
                    "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullGlobalViewName
                            + " WHERE k2 = 2 AND k3 = 109";
            stmt.execute(viewDDL);
            try {
                stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 109, 'a')");
                fail();
            } catch (ReadOnlyTableException ignored) {
            }
        }
    }

    @Test
    public void testReadOnlyViewWithNonMultiTenantAndNotStartFromFirstPKCol() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTable(conn, fullTableName);

            Statement stmt = conn.createStatement();
            String viewDDL =
                    "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName +
                            " WHERE k2 = 2";
            stmt.execute(viewDDL);
            try {
                stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 109, 'a')");
                fail();
            } catch (ReadOnlyTableException ignored) {
            }
        }
    }

    @Test
    public void testUpdatableViewWithNonMultiTenantAndStartFromFirstPKCol() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTable(conn, fullTableName);

            Statement stmt = conn.createStatement();
            String viewDDL =
                    "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName +
                            " WHERE k1 = 1";
            stmt.execute(viewDDL);

            stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 109, 'a')");
            conn.commit();

            ResultSet rs = stmt.executeQuery("SELECT k1, k2, k3 FROM " + fullViewName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(109, rs.getInt(3));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testReadOnlyViewWithMultiTenantAndNotStartFromSecondPKCol() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String tenantViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String tenantId = TENANT1;

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection globalConn = DriverManager.getConnection(getUrl(), props);
             Connection tenantConn = getTenantConnection(tenantId)) {
            createMultiTenantTable(globalConn, fullTableName);

            final Statement tenantStmt = tenantConn.createStatement();
            tenantStmt.execute("CREATE VIEW " + tenantViewName + " AS SELECT * FROM " +
                    fullTableName + " WHERE COL2 = 'col2'");
            try {
                tenantStmt.execute(String.format("UPSERT INTO %s VALUES" +
                        "('col1', 'col2', 'col3', 'col4')", tenantViewName));
                fail();
            } catch (ReadOnlyTableException ignored) {
            }
        }
    }

    @Test
    public void testUpdatableViewWithMultiTenantAndStartFromSecondPKCol() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String tenantViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String tenantId = TENANT1;

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection globalConn = DriverManager.getConnection(getUrl(), props);
             Connection tenantConn = getTenantConnection(tenantId)) {
            createMultiTenantTable(globalConn, fullTableName);

            tenantConn.createStatement().execute("CREATE VIEW " + tenantViewName + " AS SELECT * FROM " +
                    fullTableName + " WHERE COL1 = 'col1'");
            tenantConn.setAutoCommit(true);

            tenantConn.createStatement().execute(String.format("UPSERT INTO %s VALUES" +
                    "('col1', 'col2', 'col3', 'col4')", tenantViewName));

            verifyNumberOfRows(fullTableName, tenantId, 1, globalConn);
            ResultSet rs =
                    tenantConn.createStatement().executeQuery("SELECT COL1, COL2 FROM " +  tenantViewName);
            assertTrue(rs.next());
            assertEquals("col1", rs.getString(1));
            assertEquals("col2", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpdatableViewOnView() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullGlobalViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String fullLeafViewName1 = SchemaUtil.getTableName(SCHEMA4, generateUniqueName());
        String fullLeafViewName2 = SchemaUtil.getTableName(SCHEMA4, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTable(conn, fullTableName);

            Statement stmt = conn.createStatement();
            String globalViewDDL = "CREATE VIEW " + fullGlobalViewName + " AS SELECT * FROM "
                    + fullTableName + " WHERE k1 = 1";
            stmt.execute(globalViewDDL);
            String viewDDL =
                    "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullGlobalViewName
                            + " WHERE k2 = 1";
            stmt.execute(viewDDL);
            String leafView1DDL = "CREATE VIEW " + fullLeafViewName1 + " AS SELECT * FROM "
                    + fullViewName + " WHERE k3 = 101";
            stmt.execute(leafView1DDL);
            String leafView2DDL = "CREATE VIEW " + fullLeafViewName2 + " AS SELECT * FROM "
                    + fullViewName + " WHERE k3 = 105";
            stmt.execute(leafView2DDL);

            for (int i = 0; i < 10; i++) {
                stmt.execute("UPSERT INTO " + fullTableName
                        + " VALUES(" + (i % 4) + "," + (i > 5 ? 2 : 1) + "," + (i + 100) + ")");
            }
            conn.commit();

            ResultSet rs;
            rs = stmt.executeQuery("SELECT count(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            rs = stmt.executeQuery("SELECT count(*) FROM " + fullGlobalViewName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            rs = stmt.executeQuery("SELECT count(*) FROM " + fullViewName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            rs = stmt.executeQuery("SELECT k1, k2, k3 FROM " + fullLeafViewName1);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertEquals(101, rs.getInt(3));
            assertFalse(rs.next());
            rs = stmt.executeQuery("SELECT k1, k2, k3 FROM " + fullLeafViewName2);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertEquals(105, rs.getInt(3));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testReadOnlyViewOnUpdatableView() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String ddl = "CREATE VIEW " + fullViewName2 + " AS SELECT * FROM " + fullViewName1 +
                " WHERE k2 = 101";
        ViewIT.testUpdatableView(
                fullTableName, fullViewName1, fullViewName2, ddl, null, "");

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(101, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertFalse(rs.next());

        conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k3) VALUES(10)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2 + " " +
                "WHERE k3 >= 10");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(101, rs.getInt(2));
        assertEquals(10, rs.getInt(3));
        assertFalse(rs.next());

        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k2,k3) VALUES(123,3)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k2,k3) select 102, k3 from " + fullViewName1);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testUpdatableViewWithMultiTenantTable() throws Exception {
        final String tableName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(SCHEMA1, tableName);
        final String view01 = SchemaUtil.getTableName(SCHEMA2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(SCHEMA3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(SCHEMA4, "v03_" + tableName);

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                    + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                    + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                    + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT1)) {
                final Statement tenantStmt = tenantConn.createStatement();

                stmt.execute("CREATE VIEW " + view01
                        + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                        + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute("CREATE VIEW " + view02 + " AS SELECT * FROM " + view01
                                + " WHERE COL2 = 'col2'");
                tenantStmt.execute("CREATE VIEW " + view03 + " AS SELECT * FROM " + view02
                        + " WHERE VCOL1 = 'vcol1'");

                tenantConn.setAutoCommit(true);
                tenantStmt.execute(String.format("UPSERT INTO %s (VCOL1,COL3,COL4,COL5)" +
                        " VALUES('vcol2', 'col3', 'col4', 'col5')", view02));
                tenantStmt.execute(String.format("UPSERT INTO %s (COL3,COL4,COL5)" +
                        " VALUES('col3', 'col4', 'col5')", view03));

                verifyNumberOfRows(fullTableName, TENANT1, 2, conn);
                ResultSet rs = tenantConn.createStatement().executeQuery(
                        "SELECT COL1, COL2, VCOL1 FROM " + view02);
                assertTrue(rs.next());
                assertEquals("col1", rs.getString(1));
                assertEquals("col2", rs.getString(2));
                assertEquals("vcol1", rs.getString(3));
                assertTrue(rs.next());
                assertEquals("col1", rs.getString(1));
                assertEquals("col2", rs.getString(2));
                assertEquals("vcol2", rs.getString(3));
                assertFalse(rs.next());

                rs = tenantConn.createStatement().executeQuery(
                        "SELECT COL1, COL2, VCOL1 FROM " + view03);
                assertTrue(rs.next());
                assertEquals("col1", rs.getString(1));
                assertEquals("col2", rs.getString(2));
                assertEquals("vcol1", rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }
}
