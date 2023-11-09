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

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK;
import static org.apache.phoenix.exception.SQLExceptionCode.VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES;
import static org.apache.phoenix.query.QueryServices.DISABLE_VIEW_SUBTREE_VALIDATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for restrictions associated with view extending primary key of its parent.
 */
@Category(ParallelStatsDisabledTest.class)
public class ViewExtendsPkRestrictionsIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViewExtendsPkRestrictionsIT.class);

    private static final String TENANT_ID = "tenant_01";

    private Connection getTenantConnection(final String tenantId) throws Exception {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }

    private Connection getTenantConnection(final String tenantId,
        final boolean disableCreateIndexCheck) throws Exception {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        tenantProps.setProperty(DISABLE_VIEW_SUBTREE_VALIDATION,
            Boolean.toString(disableCreateIndexCheck));
        return DriverManager.getConnection(getUrl(), tenantProps);
    }

    @Test
    public void testViewExtendsPkWithParentTableIndex1() {
        final String tableName = generateUniqueName();
        final String indexName = "idx_" + tableName;
        final String view01 = "v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute(
                "CREATE INDEX " + indexName + " ON " + tableName + " (COL3) INCLUDE " + "(COL4)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL PRIMARY KEY, COL5 VARCHAR) AS SELECT * FROM "
                + tableName + " WHERE COL1 = 'col1'");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testSchemaViewExtendsPkWithParentTableIndex1() {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String indexName = "idx_" + tableName;
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (COL3) INCLUDE "
                + "(COL4)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL PRIMARY KEY, COL5 VARCHAR) AS SELECT * FROM "
                + fullTableName + " WHERE COL1 = 'col1'");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewExtendsPkWithParentTableIndex1() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String indexName = "idx_" + tableName;
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");
            stmt.execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (COL3) INCLUDE "
                + "(COL4)");
            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();
                tenantStmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL PRIMARY KEY, COL5 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE COL1 = 'col1'");
                fail();
            } catch (SQLException e) {
                try {
                    assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                        e.getErrorCode());
                    assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                        e.getSQLState());
                    assertTrue(e.getMessage()
                        .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
                } catch (AssertionError ae) {
                    LOGGER.error("Exception: ", e);
                    throw ae;
                }
            }
        }
    }

    @Test
    public void testViewExtendsPkWithParentTableIndex2() {
        final String tableName = generateUniqueName();
        final String indexName = "idx_" + tableName;
        final String view01 = "v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute(
                "CREATE INDEX " + indexName + " ON " + tableName + " (COL3) INCLUDE " + "(COL4)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testViewExtendsPkWithViewIndex1() throws Exception {
        final String tableName = generateUniqueName();
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
            stmt.execute(
                "CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)" + " AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                + "(COL1, COL2, COL3)");
        }
    }

    @Test
    public void testViewExtendsPkWithViewIndex2() {
        final String tableName = generateUniqueName();
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR CONSTRAINT pk "
                + "PRIMARY KEY(VCOL2)) AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testSchemaViewExtendsPkWithViewIndex2() {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR CONSTRAINT pk "
                + "PRIMARY KEY(VCOL2)) AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewExtendsPkWithViewIndex2() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();

                tenantStmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute(
                    "CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                        + "(COL1, COL2, COL3)");
                allStmtExecuted = true;
                tenantStmt.execute(
                    "CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR CONSTRAINT pk "
                        + "PRIMARY KEY(VCOL2)) AS SELECT * FROM " + view01
                        + " WHERE VCOL1 = 'vcol1'");
                fail();
            }
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testViewExtendsPkWithViewIndex3() {
        final String tableName = generateUniqueName();
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String view03 = "v03_" + tableName;
        final String view04 = "v04_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            stmt.execute(
                "CREATE VIEW " + view02 + " (VCOL2 CHAR(8), COL6 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'col2'");
            stmt.execute(
                "CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewExtendsPkWithViewIndex3() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();

                stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(8), COL6 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col2'");
                tenantStmt.execute("CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
                stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
                allStmtExecuted = true;
                tenantStmt.execute(
                    "CREATE VIEW " + view04 + " (VCOL2 CHAR(10), " + "COL6 VARCHAR PRIMARY KEY) AS "
                        + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
                fail();
            }
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewExtendsPkWithViewIndex4() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String schemaName6 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String view05 = SchemaUtil.getTableName(schemaName6, "v05_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();

                stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(8), COL6 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col2'");
                tenantStmt.execute("CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
                tenantStmt.execute(
                    "CREATE VIEW " + view04 + " (VCOL4 CHAR(10), " + "COL8 VARCHAR) AS "
                        + "SELECT * FROM " + view02 + " WHERE VCOL1 = 'vcol4'");
                stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
                allStmtExecuted = true;
                tenantStmt.execute("CREATE VIEW " + view05 + " (VCOL5 CHAR(10), "
                    + "COL9 VARCHAR, COL10 INTEGER CONSTRAINT pk PRIMARY KEY(VCOL5)) AS "
                    + "SELECT * FROM " + view04 + " WHERE VCOL4 = 'vcol4'");
                fail();
            }
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewExtendsPkWithViewIndex5() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String schemaName6 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String view05 = SchemaUtil.getTableName(schemaName6, "v05_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();

                stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(8), COL6 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col2'");
                tenantStmt.execute("CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
                tenantStmt.execute(
                    "CREATE VIEW " + view04 + " (VCOL4 CHAR(10), " + "COL8 VARCHAR) AS "
                        + "SELECT * FROM " + view02 + " WHERE VCOL1 = 'vcol4'");
                stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
                tenantStmt.execute("CREATE VIEW " + view05 + " (VCOL5 CHAR(10), "
                    + "COL9 VARCHAR, COL10 INTEGER) AS " + "SELECT * FROM " + view04
                    + " WHERE VCOL4 = 'vcol4'");
            }
        }
    }

    @Test
    public void testTenantSchemaViewExtendsPkWithViewIndex6() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String schemaName6 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String view05 = SchemaUtil.getTableName(schemaName6, "v05_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();

                tenantStmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(8), COL6 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col2'");
                tenantStmt.execute("CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
                tenantStmt.execute(
                    "CREATE VIEW " + view04 + " (VCOL4 CHAR(10), " + "COL8 VARCHAR) AS "
                        + "SELECT * FROM " + view02 + " WHERE VCOL1 = 'vcol4'");
                tenantStmt.execute(
                    "CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                        + "(COL1, COL2, COL3)");
                allStmtExecuted = true;
                tenantStmt.execute("CREATE VIEW " + view05 + " (VCOL5 CHAR(10), "
                    + "COL9 VARCHAR, COL10 INTEGER CONSTRAINT pk PRIMARY KEY(VCOL5)) AS "
                    + "SELECT * FROM " + view04 + " WHERE VCOL4 = 'vcol4'");
                fail();
            }
        } catch (SQLException e) {
            try {
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    e.getErrorCode());
                assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewExtendsPkWithViewIndex7() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String schemaName6 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String view05 = SchemaUtil.getTableName(schemaName6, "v05_" + tableName);
        final String index_table = "idx_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();

                tenantStmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(8), COL6 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col2'");
                tenantStmt.execute("CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
                tenantStmt.execute(
                    "CREATE VIEW " + view04 + " (VCOL4 CHAR(10), " + "COL8 VARCHAR) AS "
                        + "SELECT * FROM " + view02 + " WHERE VCOL1 = 'vcol4'");
                allStmtExecuted = true;
                stmt.execute(
                    "CREATE INDEX " + index_table + " ON " + fullTableName + " (COL4) INCLUDE "
                        + "(COL2)");
                tenantStmt.execute("CREATE VIEW " + view05 + " (VCOL5 CHAR(10), "
                    + "COL9 VARCHAR, COL10 INTEGER CONSTRAINT pk PRIMARY KEY(VCOL5)) AS "
                    + "SELECT * FROM " + view04 + " WHERE VCOL4 = 'vcol4'");
                fail();
            }
        } catch (SQLException e) {
            try {
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewExtendsPkWithViewIndex8() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String index_table = "idx_" + tableName;

        Properties props = new Properties();
        props.setProperty(DISABLE_VIEW_SUBTREE_VALIDATION, "true");

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID, true)) {
                final Statement tenantStmt = tenantConn.createStatement();

                tenantStmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(8), COL6 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col2'");
                tenantStmt.execute("CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
                tenantStmt.execute(
                    "CREATE VIEW " + view04 + " (VCOL4 CHAR(10), " + "COL8 VARCHAR) AS "
                        + "SELECT * FROM " + view02 + " WHERE VCOL1 = 'vcol4'");
                stmt.execute(
                    "CREATE INDEX " + index_table + " ON " + fullTableName + " (COL4) INCLUDE "
                        + "(COL2)");
            }
        }
    }

    @Test
    public void testViewIndexWithChildViewExtendedPk1() throws Exception {
        final String tableName = generateUniqueName();
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String view03 = "v03_" + tableName;
        final String view04 = "v04_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;

        Properties props = new Properties();
        props.setProperty(DISABLE_VIEW_SUBTREE_VALIDATION, "true");

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10) PRIMARY KEY, COL6 VARCHAR)"
                + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                + "(COL1, COL2, COL3)");
            stmt.execute(
                "CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
        }
    }

    @Test
    public void testViewIndexWithChildViewExtendedPk4() {
        final String tableName = generateUniqueName();
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String view03 = "v03_" + tableName;
        final String view04 = "v04_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10) PRIMARY KEY, COL6 VARCHAR)"
                + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                + "(COL1, COL2, COL3)");
            stmt.execute(
                "CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            allStmtExecuted = true;
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testSchemaViewIndexWithChildViewExtendedPk1() {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10) PRIMARY KEY, COL6 VARCHAR)"
                + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                + "(COL1, COL2, COL3)");
            stmt.execute(
                "CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            allStmtExecuted = true;
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewIndexWithChildViewExtendedPk1() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();

                tenantStmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute(
                    "CREATE VIEW " + view02 + " (VCOL2 CHAR(10) PRIMARY KEY, COL6 VARCHAR)"
                        + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
                tenantStmt.execute(
                    "CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                        + "(COL1, COL2, COL3)");
                tenantStmt.execute("CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
                tenantStmt.execute(
                    "CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                        + "SELECT * FROM " + view03 + " WHERE VCOL1 = 'vcol4'");
                allStmtExecuted = true;
                tenantStmt.execute(
                    "CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                        + "(COL1, COL2, COL3)");
                fail();
            }
        } catch (SQLException e) {
            try {
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testViewIndexWithChildViewExtendedPk2() {
        final String tableName = generateUniqueName();
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String view03 = "v03_" + tableName;
        final String view04 = "v04_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR CONSTRAINT pk "
                + "PRIMARY KEY(COL6)) AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                + "(COL1, COL2, COL3)");
            stmt.execute(
                "CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            allStmtExecuted = true;
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testSchemaViewIndexWithChildViewExtendedPk2() {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR CONSTRAINT pk "
                + "PRIMARY KEY(COL6)) AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                + "(COL1, COL2, COL3)");
            stmt.execute(
                "CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            allStmtExecuted = true;
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
            fail();
        } catch (SQLException e) {
            try {
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testTenantSchemaViewIndexWithChildViewExtendedPk2() throws Exception {
        final String tableName = generateUniqueName();
        final String schemaName1 = generateUniqueName();
        final String schemaName2 = generateUniqueName();
        final String schemaName3 = generateUniqueName();
        final String schemaName4 = generateUniqueName();
        final String schemaName5 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName1, tableName);
        final String view01 = SchemaUtil.getTableName(schemaName2, "v01_" + tableName);
        final String view02 = SchemaUtil.getTableName(schemaName3, "v02_" + tableName);
        final String view03 = SchemaUtil.getTableName(schemaName4, "v03_" + tableName);
        final String view04 = SchemaUtil.getTableName(schemaName5, "v04_" + tableName);
        final String index_view01 = "idx_v01_" + tableName;
        boolean allStmtExecuted = false;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT "
                + "NULL, COL3 VARCHAR, COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(TENANT_ID, "
                + "COL1, COL2)) MULTI_TENANT = true");

            try (Connection tenantConn = getTenantConnection(TENANT_ID)) {
                final Statement tenantStmt = tenantConn.createStatement();

                stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1))"
                    + " AS SELECT * FROM " + fullTableName + " WHERE COL1 = 'col1'");
                tenantStmt.execute(
                    "CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR CONSTRAINT pk "
                        + "PRIMARY KEY(VCOL2)) AS SELECT * FROM " + view01
                        + " WHERE VCOL1 = 'vcol1'");
                tenantStmt.execute("CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
                tenantStmt.execute(
                    "CREATE VIEW " + view04 + " (VCOL3 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                        + "SELECT * FROM " + view02 + " WHERE VCOL1 = 'vcol4'");
                allStmtExecuted = true;
                stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
                fail();
            }
        } catch (SQLException e) {
            try {
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
                assertEquals(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
                assertTrue(e.getMessage()
                    .contains(CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK.getMessage()));
            } catch (AssertionError ae) {
                LOGGER.error("Exception: ", e);
                throw ae;
            }
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testViewIndexWithChildViewExtendedPk3() throws Exception {
        final String tableName = generateUniqueName();
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String view03 = "v03_" + tableName;
        final String view04 = "v04_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
            stmt.execute("CREATE VIEW " + view01
                + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            stmt.execute(
                "CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                + "(COL1, COL2, COL3)");
            stmt.execute(
                "CREATE VIEW " + view03 + " (VCOL3 CHAR(8), COL7 VARCHAR) " + "AS SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute(
                "CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR) AS " + "SELECT * FROM "
                    + view01 + " WHERE VCOL1 = 'vcol4'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                + "(COL1, COL2, COL3)");
        }
    }

}
