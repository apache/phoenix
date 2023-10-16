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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_CREATE_VIEW_INDEX_CHILD_VIEWS_EXTEND_PK;
import static org.apache.phoenix.exception.SQLExceptionCode.VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for restrictions associated with view extending primary key of its parent.
 */
@Category(ParallelStatsDisabledTest.class)
public class ViewExtendsPkRestrictionsIT extends ParallelStatsDisabledIT {

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
            stmt.execute("CREATE INDEX " + indexName + " ON " + tableName + " (COL3) INCLUDE " +
                    "(COL4)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL PRIMARY KEY, COL5 VARCHAR) AS SELECT * FROM " +
                    tableName + " WHERE COL1 = 'col1'");
            fail();
        } catch (SQLException e) {
            assertEquals(VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES.getErrorCode(), e.getErrorCode());
            assertEquals(VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES.getSQLState(), e.getSQLState());
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
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
            stmt.execute("CREATE INDEX " + indexName + " ON " + tableName + " (COL3) INCLUDE "
                    + "(COL4)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1)) "
                    + "AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
            fail();
        } catch (SQLException e) {
            assertEquals(VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES.getErrorCode(), e.getErrorCode());
            assertEquals(VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES.getSQLState(), e.getSQLState());
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
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)"
                    + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
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
                    + "PRIMARY KEY(VCOL2)) AS SELECT * FROM " + view01
                    + " WHERE VCOL1 = 'vcol1'");
            fail();
        } catch (SQLException e) {
            assertEquals(VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES.getErrorCode(), e.getErrorCode());
            assertEquals(VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES.getSQLState(), e.getSQLState());
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
            stmt.execute("CREATE VIEW " + view02
                    + " (VCOL2 CHAR(8), COL6 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col2'");
            stmt.execute("CREATE VIEW " + view03
                    + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
            allStmtExecuted = true;
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                    + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            fail();
        } catch (SQLException e) {
            assertEquals(VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES.getErrorCode(), e.getErrorCode());
            assertEquals(VIEW_CANNOT_EXTEND_PK_VIEW_INDEXES.getSQLState(), e.getSQLState());
        }
        assertTrue("All statements could not be executed", allStmtExecuted);
    }

    @Test
    public void testViewIndexWithChildViewExtendedPk1() {
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
            stmt.execute("CREATE VIEW " + view03
                    + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                    + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            allStmtExecuted = true;
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
            fail();
        } catch (SQLException e) {
            assertEquals(CANNOT_CREATE_VIEW_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
            assertEquals(CANNOT_CREATE_VIEW_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
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
            stmt.execute("CREATE VIEW " + view03
                    + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR PRIMARY KEY) AS "
                    + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            allStmtExecuted = true;
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
            fail();
        } catch (SQLException e) {
            assertEquals(CANNOT_CREATE_VIEW_INDEX_CHILD_VIEWS_EXTEND_PK.getErrorCode(),
                    e.getErrorCode());
            assertEquals(CANNOT_CREATE_VIEW_INDEX_CHILD_VIEWS_EXTEND_PK.getSQLState(),
                    e.getSQLState());
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
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                    + "(COL1, COL2, COL3)");
            stmt.execute("CREATE VIEW " + view03
                    + " (VCOL3 CHAR(8), COL7 VARCHAR) "
                    + "AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'col3'");
            stmt.execute("CREATE VIEW " + view04 + " (VCOL2 CHAR(10), COL6 VARCHAR) AS "
                    + "SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol4'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
        }
    }

}
