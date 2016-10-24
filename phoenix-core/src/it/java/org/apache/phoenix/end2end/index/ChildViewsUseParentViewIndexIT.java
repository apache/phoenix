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
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class ChildViewsUseParentViewIndexIT extends ParallelStatsDisabledIT {

    @Test
    public void testIndexOnParentViewWithTenantSpecificConnection() throws Exception {
        final String baseTableName = generateUniqueName();
        final String globalViewName = generateUniqueName();
        final String globalViewIdxName = generateUniqueName();
        final String tenantViewName1 = generateUniqueName();
        final String tenantViewName2 = generateUniqueName();

        // Set up props with TenantId
        Properties props  = new Properties();
        props.setProperty("TenantId", "00Dxxxxxxxxxxx1");
        
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
            // Create Base table
            createBaseTable(baseTableName, conn);
            
            // Create the Global View on the Base Table for a value of KP
            createGlobalView(globalViewName, baseTableName, conn);
    
            // Create Global Index on View
            createGlobalIndexOnView(globalViewName, globalViewIdxName, conn);
    
            // Create tenant specific view which is a child of global view
            createTenantSpecificView(globalViewName, tenantViewName1, tenantConn);
            
            // Create tenant specific view which is a child of previous tenant view
            createTenantSpecificView(tenantViewName1, tenantViewName2, tenantConn);
    
            int rowCount = 0;
            insertRowIntoView(globalViewName, tenantConn, ++rowCount);
            insertRowIntoView(globalViewName, tenantConn, ++rowCount);
            insertRowIntoView(globalViewName, tenantConn, ++rowCount);
            assertQueryIndex(globalViewName, baseTableName, tenantConn, 3);
    
            insertRowIntoView(tenantViewName1, tenantConn, ++rowCount);
            insertRowIntoView(tenantViewName2, tenantConn, ++rowCount);
    
            // assert that we get 5 rows while querying via tenant specific views and that we use the index
            assertQueryIndex(tenantViewName1, baseTableName, tenantConn, 5);
            assertQueryIndex(tenantViewName2, baseTableName, tenantConn, 5);
    
            // assert that we get 5 rows while querying via global specific view and that we use the index
            assertQueryIndex(globalViewName, baseTableName, tenantConn, 5);
        }
    }
    
    
    @Test
    public void testParentViewIndexWithSpecializedChildViews() throws Exception {
        final String baseTableName = generateUniqueName();
        final String parentViewName = generateUniqueName();
        final String parentViewIdxName = generateUniqueName();
        final String childViewName1 = generateUniqueName();
        final String childViewName2 = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // create base table
            String baseTableDdl = "CREATE TABLE " + baseTableName + " (" +
                    "A0 CHAR(1) NOT NULL PRIMARY KEY," +
                    "A1 CHAR(1)," +
                    "A2 CHAR(1)," +
                    "A3 CHAR(1)," +
                    "A4 CHAR(1))";
            conn.createStatement().execute(baseTableDdl);
            
            // create the parent view on the base table for a value of A
            String globalViewDdl = "CREATE VIEW " + parentViewName + " AS SELECT * FROM " + baseTableName + " WHERE A1 = 'X'";
            conn.createStatement().execute(globalViewDdl);
    
            // create index on parent view
            conn.createStatement().execute("CREATE INDEX " + parentViewIdxName + " ON " + parentViewName + "(A4, A2)");
    
            // create child of parent view that should be able to use the parent's index
            String childViewDdl = "CREATE VIEW " + childViewName1 + " AS SELECT * FROM " + parentViewName + " WHERE A2 = 'Y'";
            conn.createStatement().execute(childViewDdl);
            
            // create child of parent view that should *not* be able to use the parent's index
            String grandChildViewDdl1 = "CREATE VIEW " + childViewName2 + " AS SELECT * FROM " + childViewName1 + " WHERE A3 = 'Z'";
            conn.createStatement().execute(grandChildViewDdl1);

            // upsert row using parent view
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + parentViewName + " (A0, A2, A3, A4) VALUES(?,?,?,?)");
            stmt.setString(1, "1");
            stmt.setString(2, "Y");
            stmt.setString(3, "Z");
            stmt.setString(4, "1");
            stmt.execute();
            conn.commit();
            
            // upsert row using first child view
            stmt = conn.prepareStatement("UPSERT INTO " + childViewName1 + " (A0, A3, A4) VALUES(?,?,?)");
            stmt.setString(1, "2");
            stmt.setString(2, "Z");
            stmt.setString(3, "2");
            stmt.execute();
            conn.commit();
            
            // upsert row using second child view
            stmt = conn.prepareStatement("UPSERT INTO " + childViewName2 + " (A0, A4) VALUES(?,?)");
            stmt.setString(1, "3");
            stmt.setString(2, "3");
            stmt.execute();
            conn.commit();
            
            // assert that we get 2 rows while querying via parent views and that we use the index
            assertQueryUsesIndex(baseTableName, parentViewName, conn, false);
            
            // assert that we get 2 rows while querying via the first child view and that we use the index
            assertQueryUsesIndex(baseTableName, childViewName1, conn, true);
            
            // assert that we get 3 rows while querying via the second child view and that we use the base table
            assertQueryUsesBaseTable(baseTableName, childViewName2, conn);
        }
    }

    private void assertQueryUsesIndex(final String baseTableName, final String viewName, Connection conn, boolean isChildView) throws SQLException {
        String sql = "SELECT A0, A1, A2, A4 FROM " + viewName +" WHERE A4 IN ('1', '2', '3') ORDER BY A4, A2";
        ResultSet rs = conn.prepareStatement("EXPLAIN " + sql).executeQuery();
        String childViewScanKey = isChildView ? ",'Y'" : "";
        assertEquals(
            "CLIENT PARALLEL 1-WAY SKIP SCAN ON 3 KEYS OVER _IDX_" + baseTableName + " [-32768,'1'" + childViewScanKey + "] - [-32768,'3'" + childViewScanKey + "]\n" +
            "    SERVER FILTER BY FIRST KEY ONLY",
            QueryUtil.getExplainPlan(rs));
        
        rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("X", rs.getString(2));
        assertEquals("Y", rs.getString(3));
        assertEquals("1", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
        assertEquals("X", rs.getString(2));
        assertEquals("Y", rs.getString(3));
        assertEquals("2", rs.getString(4));
        assertFalse(rs.next());
    }
    
    private void assertQueryUsesBaseTable(final String baseTableName, final String viewName, Connection conn) throws SQLException {
        String sql = "SELECT A0, A1, A2, A4 FROM " + viewName +" WHERE A4 IN ('1', '2', '3') ";
        ResultSet rs = conn.prepareStatement("EXPLAIN " + sql).executeQuery();
        assertEquals(
            "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + baseTableName + "\n" +
            "    SERVER FILTER BY (A4 IN ('1','2','3') AND ((A1 = 'X' AND A2 = 'Y') AND A3 = 'Z'))",
            QueryUtil.getExplainPlan(rs));
        
        rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("X", rs.getString(2));
        assertEquals("Y", rs.getString(3));
        assertEquals("1", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
        assertEquals("X", rs.getString(2));
        assertEquals("Y", rs.getString(3));
        assertEquals("2", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("3", rs.getString(1));
        assertEquals("X", rs.getString(2));
        assertEquals("Y", rs.getString(3));
        assertEquals("3", rs.getString(4));
        assertFalse(rs.next());
    }

    private void createBaseTable(String baseTableName, Connection conn) throws SQLException {
        String baseTableDdl = "CREATE TABLE " + baseTableName + " (" +
                "OID CHAR(15) NOT NULL,\n" +
                "KP CHAR(3) NOT NULL\n," +
                "CREATED_DATE DATE\n," +
                "CREATED_BY CHAR(3)\n," +
                "CONSTRAINT PK PRIMARY KEY (oid, kp))\n";
        String ddlOptions = "MULTI_TENANT=true, IMMUTABLE_ROWS=TRUE, VERSIONS=1";
        conn.createStatement().execute(baseTableDdl + ddlOptions);
    }

    private void createGlobalView(String globalViewName, String baseTableName, Connection conn) throws SQLException {
        String globalViewDdl = "CREATE VIEW IF NOT EXISTS " + globalViewName + " (" +
                "A_DATE DATE NOT NULL," +
                "WO_ID CHAR(15) NOT NULL," +
                "WA_ID CHAR(15) NOT NULL," +
                "C_TYPE VARCHAR NOT NULL," +
                "CA_TYPE VARCHAR NOT NULL," +
                "V_ID CHAR(15)," +
                "C_CTX VARCHAR," +
                "CONSTRAINT PKVIEW PRIMARY KEY (A_DATE, WO_ID, WA_ID, C_TYPE, CA_TYPE))" +
                "AS SELECT * FROM " + baseTableName + " WHERE KP = 'xyz'";
        conn.createStatement().execute(globalViewDdl);
    }

    private void createGlobalIndexOnView(String globalViewName, String globalViewIdxName, Connection conn) throws SQLException {
        String globalViewIdxDdl = "CREATE INDEX IF NOT EXISTS " + globalViewIdxName +
                " ON " + globalViewName + "(WO_ID, A_DATE DESC)";
        conn.createStatement().execute(globalViewIdxDdl);
    }

    private void createTenantSpecificView(String parentViewName, String tenantViewName, Connection conn) throws SQLException {
        String tenantSpecificViewDdl = "CREATE VIEW IF NOT EXISTS " +
                tenantViewName + " AS SELECT * FROM " + parentViewName;
        conn.createStatement().
                execute(tenantSpecificViewDdl);
    }
    
    private void insertRowIntoView(String viewName, Connection conn, int rowCount) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
                "UPSERT INTO " + viewName + " (A_DATE, WO_ID, WA_ID, C_TYPE, CA_TYPE, V_ID) VALUES(?,?,?,?,?,?)");
        stmt.setDate(1, new Date(System.currentTimeMillis()));
        stmt.setString(2, "003xxxxxxxxxxx" + rowCount);
        stmt.setString(3, "701xxxxxxxxxxx" + rowCount);
        stmt.setString(4, "ctype1");
        stmt.setString(5, "catype1");
        stmt.setString(6, "xyzxxxxxxxxxxx" + rowCount);
        stmt.execute();
        conn.commit();
    }

    private void assertQueryIndex(String viewName, String baseTableName, Connection conn, int expectedRows) throws SQLException {
        String sql = "SELECT WO_ID FROM " + viewName +
                " WHERE WO_ID IN ('003xxxxxxxxxxx1', '003xxxxxxxxxxx2', '003xxxxxxxxxxx3', '003xxxxxxxxxxx4', '003xxxxxxxxxxx5') " +
                " AND (A_DATE > TO_DATE('2016-01-01 06:00:00.0')) " +
                " ORDER BY WO_ID, A_DATE DESC";
        ResultSet rs = conn.prepareStatement("EXPLAIN " + sql).executeQuery();
        assertEquals(
            "CLIENT PARALLEL 1-WAY SKIP SCAN ON 5 RANGES OVER _IDX_" + baseTableName + " [-32768,'00Dxxxxxxxxxxx1','003xxxxxxxxxxx1',*] - [-32768,'00Dxxxxxxxxxxx1','003xxxxxxxxxxx5',~'2016-01-01 06:00:00.000']\n" + 
            "    SERVER FILTER BY FIRST KEY ONLY",
            QueryUtil.getExplainPlan(rs));
        
        rs = conn.createStatement().executeQuery(sql);
        for (int i=0; i<expectedRows; ++i)
            assertTrue(rs.next());
        assertFalse(rs.next());
    }
}
