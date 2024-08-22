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

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost.PHOENIX_META_DATA_COPROCESSOR_CONF_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for restrictions associated with updatable view.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class UpdatableViewRestrictionsIT extends SplitSystemCatalogIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatableViewRestrictionsIT.class);

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

    private void createTable(
            Connection conn, String tableSQL, Map<String, Object> tableProps) throws Exception {
        List<String> props = new ArrayList<>();
        Boolean multitenant = (Boolean) TableProperty.MULTI_TENANT.getValue(tableProps);
        if (multitenant != null && multitenant) {
            props.add(TableProperty.MULTI_TENANT.getPropertyName() + "=" + multitenant);
        }
        Integer nSaltBuckets = (Integer) TableProperty.SALT_BUCKETS.getValue(tableProps);
        if (nSaltBuckets != null) {
            props.add(TableProperty.SALT_BUCKETS.getPropertyName() + "=" + nSaltBuckets);
        }
        tableSQL += " " + String.join(", ", props);
        LOGGER.debug("Creating table with SQL: " + tableSQL);
        conn.createStatement().execute(tableSQL);
    }

    private void createTable(Connection conn, String tableName,
                             boolean multitenant, Integer nSaltBuckets) throws Exception {
        String tableSQL = "CREATE TABLE " + tableName + " ("
                + (multitenant ? "TENANT_ID VARCHAR NOT NULL, " : "")
                + "k1 INTEGER NOT NULL, k2 DECIMAL, k3 INTEGER NOT NULL, s VARCHAR "
                + "CONSTRAINT pk PRIMARY KEY ("
                + (multitenant ? "TENANT_ID, " : "")
                + "k1, k2, k3))";
        createTable(conn, tableSQL, new HashMap<String, Object>() {{
            put(TableProperty.MULTI_TENANT.getPropertyName(), multitenant);
            put(TableProperty.SALT_BUCKETS.getPropertyName(), nSaltBuckets);
        }});
    }

    private void createTable(Connection conn, String tableName) throws Exception {
        createTable(conn, tableName, false, null);
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

    /**
     * Test that the view type is READ_ONLY if there are non-PK columns in the WHERE clause.
     */
    @Test
    public void testReadOnlyViewWithNonPkInWhere() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
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

    /**
     * Test that the view type is READ_ONLY if PK columns in the WHERE clause are not in the order
     * they are defined: primary key k2 is missing in this case.
     */
    @Test
    public void testReadOnlyViewWithPkNotInOrderInWhere1() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
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

    /**
     * Test that the view type is READ_ONLY if PK columns in the WHERE clause are not in the order
     * they are defined: primary key k3 is missing in this case.
     */
    @Test
    public void testReadOnlyViewWithPkNotInOrderInWhere2() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("CREATE TABLE " + fullTableName
                    + " (k1 INTEGER NOT NULL, k2 DECIMAL, k3 INTEGER NOT NULL, s VARCHAR "
                    + "CONSTRAINT pk PRIMARY KEY (k1, k2, k3, s))");

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

    /**
     * Test that the view type is READ_ONLY if the set of PK columns in the WHERE clause are not
     * same as its sibling view's: primary key k3 is redundant in this case.
     */
    @Test
    public void testReadOnlyViewWithPkNotSameInWhere1() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullGlobalViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String fullSiblingViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
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

    /**
     * Test that the view type is READ_ONLY if the set of PK columns in the WHERE clause are not
     * same as its sibling view's: primary key k3 is missing in this case.
     */
    @Test
    public void testReadOnlyViewWithPkNotSameInWhere2() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullGlobalViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String fullSiblingViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            createTable(conn, fullTableName);

            Statement stmt = conn.createStatement();
            String globalViewDDL =
                    "CREATE VIEW " + fullGlobalViewName + " AS SELECT * FROM " + fullTableName +
                            " WHERE k1 = 1";
            stmt.execute(globalViewDDL);

            String siblingViewDDL = "CREATE VIEW " + fullSiblingViewName + " AS SELECT * FROM " +
                    fullGlobalViewName + " WHERE k2 = 1 AND k3 = 109";
            stmt.execute(siblingViewDDL);
            String viewDDL =
                    "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullGlobalViewName
                            + " WHERE k2 = 2";
            stmt.execute(viewDDL);
            try {
                stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 109, 'a')");
                fail();
            } catch (ReadOnlyTableException ignored) {
            }
        }
    }

    /**
     * Test that the view type is UPDATABLE if the view statement WHERE clause
     * starts from the first PK column (ignore the prefix PK columns, TENANT_ID and/or _SALTED,
     * if the parent table is multi-tenant and/or salted),
     * and satisfies all other criteria.
     * @param multitenant Whether the parent table is multi-tenant
     * @param nSaltBuckets Number of salt buckets
     * @throws Exception
     */
    private void testUpdatableViewStartFromFirstPK(boolean multitenant, Integer nSaltBuckets) throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            createTable(conn, fullTableName, multitenant, nSaltBuckets);
            String tenantId = null;
            if (multitenant) {
                tenantId = TENANT1;
                try (Connection tenantConn = getTenantConnection(tenantId)) {
                    createAndVerifyUpdatableView(fullTableName, tenantConn);
                }
            } else {
                createAndVerifyUpdatableView(fullTableName, conn);
            }
            verifyNumberOfRows(fullTableName, tenantId, 1, conn);
        }
    }

    /**
     * Test that the view type is READ_ONLY if the view statement WHERE clause
     * does not start from the first PK column (ignore the prefix PK columns, TENANT_ID and/or
     * _SALTED, if the parent table is multi-tenant and/or salted).
     * @param multitenant Whether the parent table is multi-tenant
     * @param nSaltBuckets Number of salt buckets
     * @throws Exception
     */
    private void testReadOnlyViewNotStartFromFirstPK(boolean multitenant, Integer nSaltBuckets) throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String tenantId = TENANT1;

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        try (Connection globalConn = DriverManager.getConnection(getUrl(), props);
             Connection tenantConn = getTenantConnection(tenantId)) {
            globalConn.setAutoCommit(true);
            tenantConn.setAutoCommit(true);

            createTable(globalConn, fullTableName, multitenant, nSaltBuckets);

            final Statement stmt = multitenant ? tenantConn.createStatement() :
                    globalConn.createStatement();
            stmt.execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " +
                    fullTableName + " WHERE k2 = 2");
            try {
                stmt.execute(String.format("UPSERT INTO %s VALUES" +
                        "(1, 2, 3, 's')", fullViewName));
                fail();
            } catch (ReadOnlyTableException ignored) {
            }
        }
    }

    /**
     * Test that the view type is READ_ONLY if the view statement WHERE clause does not start
     * from the first PK column when the parent table is neither multi-tenant nor salted.
     */
    @Test
    public void testReadOnlyViewOnNonMultitenantNonSaltedTableNotStartFromFirstPK() throws Exception {
        testReadOnlyViewNotStartFromFirstPK(false, null);
    }

    /**
     * Test that the view type is READ_ONLY if the view statement WHERE clause does not start
     * from the first PK column when the parent table is multi-tenant and not salted (ignore the
     * prefix PK column TENANT_ID).
     */
    @Test
    public void testReadOnlyViewOnMultitenantNonSaltedTableNotStartFromFirstPK() throws Exception {
        testReadOnlyViewNotStartFromFirstPK(true, null);
    }

    /**
     * Test that the view type is READ_ONLY if the view statement WHERE clause does not start
     * from the first PK column when the parent table is not multi-tenant but salted (ignore
     * the prefix PK column _SALTED).
     */
    @Test
    public void testReadOnlyViewOnNonMultitenantSaltedTableNotStartFromFirstPK() throws Exception {
        testReadOnlyViewNotStartFromFirstPK(false, 3);
    }

    /**
     * Test that the view type is READ_ONLY if the view statement WHERE clause does not start
     * from the first PK column when the parent table is both multi-tenant and salted (ignore
     * the prefix PK columns TENANT_ID and _SALTED).
     */
    @Test
    public void testReadOnlyViewOnMultitenantSaltedTableNotStartFromFirstPK() throws Exception {
        testReadOnlyViewNotStartFromFirstPK(true, 3);
    }

    /**
     * Test that the view type is UPDATABLE if the view statement WHERE clause
     * starts from the first PK column when the parent table is neither multi-tenant nor salted,
     * and satisfies all other criteria.
     */
    @Test
    public void testUpdatableViewOnNonMultitenantNonSaltedTableStartFromFirstPK() throws Exception {
        testUpdatableViewStartFromFirstPK(false, null);
    }

    /**
     * Test that the view type is UPDATABLE if the view statement WHERE clause
     * starts from the first PK column when the parent table is multi-tenant and not salted
     * (ignore the prefix PK column TENANT_ID),
     * and satisfies all other criteria.
     */
    @Test
    public void testUpdatableViewOnMultitenantNonSaltedTableStartFromFirstPK() throws Exception {
        testUpdatableViewStartFromFirstPK(true, null);
    }

    /**
     * Test that the view type is UPDATABLE if the view statement WHERE clause
     * starts from the first PK column when the parent table is not multi-tenant but salted
     * (ignore the prefix PK column _SALTED),
     * and satisfies all other criteria.
     */
    @Test
    public void testUpdatableViewOnNonMultitenantSaltedTableStartFromFirstPK() throws Exception {
        testUpdatableViewStartFromFirstPK(false, 3);
    }

    /**
     * Test that the view type is UPDATABLE if the view statement WHERE clause
     * starts from the first PK column when the parent table is both multi-tenant and salted
     * (ignore the prefix PK columns TENANT_ID and _SALTED),
     * and satisfies all other criteria.
     */
    @Test
    public void testUpdatableViewOnMultitenantSaltedTableStartFromFirstPK() throws Exception {
        testUpdatableViewStartFromFirstPK(true, 3);
    }

    private void createAndVerifyUpdatableView(
            String fullTableName, Connection conn) throws Exception {
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName +
                " WHERE k1 = 1");
        stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 109, 'a')");
        conn.commit();

        ResultSet rs = stmt.executeQuery("SELECT k1, k2, k3 FROM " + fullViewName);

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(109, rs.getInt(3));
        assertFalse(rs.next());
    }

    /**
     * Test that the view type is READ_ONLY when it's created on an updatable view and PK columns
     * in the WHERE clause are not in the order they are defined.
     */
    @Test
    public void testReadOnlyViewOnUpdatableView1() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullChildViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);

        createTable(conn, fullTableName);
        Statement stmt = conn.createStatement();

        String viewDDL = "CREATE VIEW " + fullViewName +
                " AS SELECT * FROM " + fullTableName + " WHERE k1 = 1";
        stmt.execute(viewDDL);

        String childViewDDL = "CREATE VIEW " + fullChildViewName +
                " AS SELECT * FROM " + fullViewName + " WHERE k3 = 3";
        stmt.execute(childViewDDL);

        try {
            stmt.execute("UPSERT INTO " + fullChildViewName + " VALUES(1, 2, 3, 'a')");
            fail();
        } catch (ReadOnlyTableException ignored) {
        }
    }

    /**
     * Test that the view type is READ_ONLY when it's created on an updatable view and there are
     * non-PK columns in the WHERE clause.
     */
    @Test
    public void testReadOnlyViewOnUpdatableView2() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullChildViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);

        createTable(conn, fullTableName);
        Statement stmt = conn.createStatement();

        String viewDDL = "CREATE VIEW " + fullViewName +
                " AS SELECT * FROM " + fullTableName + " WHERE k1 = 1";
        stmt.execute(viewDDL);

        String childViewDDL = "CREATE VIEW " + fullChildViewName +
                " AS SELECT * FROM " + fullViewName + " WHERE k2 = 2 AND k3 = 3 AND s = 'a'";
        stmt.execute(childViewDDL);

        try {
            stmt.execute("UPSERT INTO " + fullChildViewName + " VALUES(1, 2, 3, 'a')");
            fail();
        } catch (ReadOnlyTableException ignored) {
        }
    }

    @Test
    public void testUpdatableViewOnUpdatableView() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullChildViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        Properties props = new Properties();
        props.setProperty(QueryServices.PHOENIX_UPDATABLE_VIEW_RESTRICTION_ENABLED, "true");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);

        createTable(conn, fullTableName);
        Statement stmt = conn.createStatement();

        String viewDDL = "CREATE VIEW " + fullViewName +
                " AS SELECT * FROM " + fullTableName + " WHERE k1 = 1";
        stmt.execute(viewDDL);

        String childViewDDL = "CREATE VIEW " + fullChildViewName +
                " AS SELECT * FROM " + fullViewName + " WHERE k2 = 2 AND k3 = 3";
        stmt.execute(childViewDDL);

        stmt.execute("UPSERT INTO " + fullChildViewName + " VALUES(1, 2, 3, 'a')");

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + fullChildViewName);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(3, rs.getInt(3));
        assertEquals("a", rs.getString(4));
        assertFalse(rs.next());
    }

    @Test
    public void testSiblingsUpdatableOnUpdatableView() throws Exception {
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

            stmt.execute("UPSERT INTO " + fullLeafViewName1 + " VALUES(1, 1, 101, 'leaf1')");
            stmt.execute("UPSERT INTO " + fullLeafViewName2 + " VALUES(1, 1, 105, 'leaf2')");
            conn.commit();
            rs = stmt.executeQuery("SELECT s FROM " + fullLeafViewName1);
            assertTrue(rs.next());
            assertEquals("leaf1", rs.getString(1));
            assertFalse(rs.next());
            rs = stmt.executeQuery("SELECT s FROM " + fullLeafViewName2);
            assertTrue(rs.next());
            assertEquals("leaf2", rs.getString(1));
            assertFalse(rs.next());
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
                tenantConn.setAutoCommit(true);
                final Statement tenantStmt = tenantConn.createStatement();

                stmt.execute("CREATE VIEW " + view01
                        + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                        + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute("CREATE VIEW " + view02 + " AS SELECT * FROM " + view01
                                + " WHERE COL2 = 'col2'");
                tenantStmt.execute("CREATE VIEW " + view03 + " AS SELECT * FROM " + view02
                        + " WHERE VCOL1 = 'vcol1'");

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
