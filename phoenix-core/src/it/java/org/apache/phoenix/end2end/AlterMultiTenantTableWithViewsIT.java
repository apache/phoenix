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

import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.query.QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import com.google.common.base.Objects;

public class AlterMultiTenantTableWithViewsIT extends ParallelStatsDisabledIT {

    private Connection getTenantConnection(String tenantId) throws Exception {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }
    
    private static long getTableSequenceNumber(PhoenixConnection conn, String tableName) throws SQLException {
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), SchemaUtil.normalizeIdentifier(tableName)));
        return table.getSequenceNumber();
    }
    
    private static short getMaxKeySequenceNumber(PhoenixConnection conn, String tableName) throws SQLException {
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), SchemaUtil.normalizeIdentifier(tableName)));
        return SchemaUtil.getMaxKeySeq(table);
    }
    
    private static void verifyNewColumns(ResultSet rs, String ... values) throws SQLException {
        assertTrue(rs.next());
        int i = 1;
        for (String value : values) {
            assertEquals(value, rs.getString(i++));
        }
        assertFalse(rs.next());
        assertEquals(values.length, i - 1);
    }
    
    @Test
    public void testAddDropColumnToBaseTablePropagatesToEntireViewHierarchy() throws Exception {
        String baseTable = "testViewHierarchy";
        String baseViewName = generateUniqueName();
        String view1 = baseViewName + "_VIEW1";
        String view2 = baseViewName + "_VIEW2";
        String view3 = baseViewName + "_VIEW3";
        String view4 = baseViewName + "_VIEW4";
        /*                                     baseTable
                                 /                  |               \ 
                         view1(tenant1)    view3(tenant2)          view4(global)
                          /
                        view2(tenant1)  
        */
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);
            
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
                
                String view2DDL = "CREATE VIEW " + view2 + " AS SELECT * FROM " + view1;
                tenant1Conn.createStatement().execute(view2DDL);
            }
            
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                String view3DDL = "CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable;
                tenant2Conn.createStatement().execute(view3DDL);
            }
            
            String view4DDL = "CREATE VIEW " + view4 + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(view4DDL);
            
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD V3 VARCHAR";
            conn.createStatement().execute(alterBaseTable);
            
            // verify that the column is visible to view4
            conn.createStatement().execute("SELECT V3 FROM " + view4);
            
            // verify that the column is visible to view1 and view2
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                tenant1Conn.createStatement().execute("SELECT V3 from " + view1);
                tenant1Conn.createStatement().execute("SELECT V3 from " + view2);
            }
            
            // verify that the column is visible to view3
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                tenant2Conn.createStatement().execute("SELECT V3 from " + view3);
            }

            alterBaseTable = "ALTER TABLE " + baseTable + " DROP COLUMN V1";
            conn.createStatement().execute(alterBaseTable);

            // verify that the column is not visible to view4
            try {
                conn.createStatement().execute("SELECT V1 FROM " + view4);
                fail();
            } catch (ColumnNotFoundException e) {
            }
            // verify that the column is not visible to view1 and view2
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                try {
                    tenant1Conn.createStatement().execute("SELECT V1 from " + view1);
                    fail();
                } catch (ColumnNotFoundException e) {
                }
                try {
                    tenant1Conn.createStatement().execute("SELECT V1 from " + view2);
                    fail();
                } catch (ColumnNotFoundException e) {
                }
            }

            // verify that the column is not visible to view3
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                try {
                    tenant2Conn.createStatement().execute("SELECT V1 from " + view3);
                    fail();
                } catch (ColumnNotFoundException e) {
                }
            }
        }
           
    }
    
    @Test
    public void testChangingPKOfBaseTableChangesPKForAllViews() throws Exception {
        String baseTable = "testChangePKOfBaseTable";
        String baseViewName = generateUniqueName();
        String view1 = baseViewName + "_VIEW1";
        String view2 = baseViewName + "_VIEW2";
        String view3 = baseViewName + "_VIEW3";
        String view4 = baseViewName + "_VIEW4";
        /*                                     baseTable
                                 /                  |               \ 
                         view1(tenant1)    view3(tenant2)          view4(global)
                          /
                        view2(tenant1)  
         */
        Connection tenant1Conn = null, tenant2Conn = null;
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE "
                    + baseTable
                    + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            globalConn.createStatement().execute(baseTableDDL);

            tenant1Conn = getTenantConnection("tenant1");
            String view1DDL = "CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable;
            tenant1Conn.createStatement().execute(view1DDL);

            String view2DDL = "CREATE VIEW " + view2 + " AS SELECT * FROM " + view1;
            tenant1Conn.createStatement().execute(view2DDL);

            tenant2Conn = getTenantConnection("tenant2");
            String view3DDL = "CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable;
            tenant2Conn.createStatement().execute(view3DDL);

            String view4DDL = "CREATE VIEW " + view4 + " AS SELECT * FROM " + baseTable;
            globalConn.createStatement().execute(view4DDL);

            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD NEW_PK varchar primary key ";
            globalConn.createStatement().execute(alterBaseTable);
            
            // verify that the new column new_pk is now part of the primary key for the entire hierarchy
            
            globalConn.createStatement().execute("SELECT * FROM " + baseTable);
            assertTrue(checkColumnPartOfPk(globalConn.unwrap(PhoenixConnection.class), "NEW_PK", baseTable));
            
            tenant1Conn.createStatement().execute("SELECT * FROM " + view1);
            assertTrue(checkColumnPartOfPk(tenant1Conn.unwrap(PhoenixConnection.class), "NEW_PK", view1));
            
            tenant1Conn.createStatement().execute("SELECT * FROM " + view2);
            assertTrue(checkColumnPartOfPk(tenant1Conn.unwrap(PhoenixConnection.class), "NEW_PK", view2));
            
            tenant2Conn.createStatement().execute("SELECT * FROM " + view3);
            assertTrue(checkColumnPartOfPk(tenant2Conn.unwrap(PhoenixConnection.class), "NEW_PK", view3));
            
            globalConn.createStatement().execute("SELECT * FROM " + view4);
            assertTrue(checkColumnPartOfPk(globalConn.unwrap(PhoenixConnection.class), "NEW_PK", view4));

        } finally {
            if (tenant1Conn != null) {
                try {
                    tenant1Conn.close();
                } catch (Throwable ignore) {}
            }
            if (tenant2Conn != null) {
                try {
                    tenant2Conn.close();
                } catch (Throwable ignore) {}
            }
        }

    }
    
    private boolean checkColumnPartOfPk(PhoenixConnection conn, String columnName, String tableName) throws SQLException {
        String normalizedTableName = SchemaUtil.normalizeIdentifier(tableName);
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), normalizedTableName));
        List<PColumn> pkCols = table.getPKColumns();
        String normalizedColumnName = SchemaUtil.normalizeIdentifier(columnName);
        for (PColumn pkCol : pkCols) {
            if (pkCol.getName().getString().equals(normalizedColumnName)) {
                return true;
            }
        }
        return false;
    }
    
    private int getIndexOfPkColumn(PhoenixConnection conn, String columnName, String tableName) throws SQLException {
        String normalizedTableName = SchemaUtil.normalizeIdentifier(tableName);
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), normalizedTableName));
        List<PColumn> pkCols = table.getPKColumns();
        String normalizedColumnName = SchemaUtil.normalizeIdentifier(columnName);
        int i = 0;
        for (PColumn pkCol : pkCols) {
            if (pkCol.getName().getString().equals(normalizedColumnName)) {
                return i;
            }
            i++;
        }
        return -1;
    }
    
    @Test
    public void testAddPKColumnToBaseTableWhoseViewsHaveIndices() throws Exception {
        String baseTable = "testAddPKColumnToBaseTableWhoseViewsHaveIndices";
        String baseViewName = generateUniqueName();
        String view1 = baseViewName + "_VIEW1";
        String view2 = baseViewName + "_VIEW2";
        String view3 = baseViewName + "_VIEW3";
        String tenant1 = baseViewName + "_T1";
        String tenant2 = baseViewName + "_T2";
        String view2Index = view2 + "_IDX";
        String view3Index = view3 + "_IDX";
        /*                          baseTable(mutli-tenant)
                                 /                           \                
                         view1(tenant1)                  view3(tenant2, index) 
                          /
                        view2(tenant1, index)  
         */
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            // make sure that the tables are empty, but reachable
            globalConn
            .createStatement()
            .execute(
                    "CREATE TABLE "
                            + baseTable
                            + " (TENANT_ID VARCHAR NOT NULL, K1 varchar not null, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, K1)) MULTI_TENANT = true ");

        }
        try (Connection viewConn = getTenantConnection(tenant1)) {
            // create tenant specific view for tenant1 - view1
            viewConn.createStatement().execute("CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable);
            PhoenixConnection phxConn = viewConn.unwrap(PhoenixConnection.class);
            assertEquals(0, getTableSequenceNumber(phxConn, view1));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view1));

            // create a view - view2 on view - view1
            viewConn.createStatement().execute("CREATE VIEW " + view2 + " AS SELECT * FROM " + view1);
            assertEquals(0, getTableSequenceNumber(phxConn, view2));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view2));


            // create an index on view2
            viewConn.createStatement().execute("CREATE INDEX " + view2Index + " ON " + view2 + " (v1) include (v2)");
            assertEquals(0, getTableSequenceNumber(phxConn, view2Index));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view2Index));
        }
        try (Connection viewConn = getTenantConnection(tenant2)) {
            // create tenant specific view for tenant2 - view3
            viewConn.createStatement().execute("CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable);
            PhoenixConnection phxConn = viewConn.unwrap(PhoenixConnection.class);
            assertEquals(0, getTableSequenceNumber(phxConn, view3));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view3));


            // create an index on view3
            viewConn.createStatement().execute("CREATE INDEX " + view3Index + " ON " + view3 + " (v1) include (v2)");
            assertEquals(0, getTableSequenceNumber(phxConn, view3Index));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view3Index));


        }

        // alter the base table by adding 1 non-pk and 2 pk columns
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            globalConn.createStatement().execute("ALTER TABLE " + baseTable + " ADD v3 VARCHAR, k2 VARCHAR PRIMARY KEY, k3 VARCHAR PRIMARY KEY");
            assertEquals(4, getMaxKeySequenceNumber(globalConn.unwrap(PhoenixConnection.class), baseTable));

            // Upsert records in the base table
            String upsert = "UPSERT INTO " + baseTable + " (TENANT_ID, K1, K2, K3, V1, V2, V3) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = globalConn.prepareStatement(upsert);
            stmt.setString(1, tenant1);
            stmt.setString(2, "K1");
            stmt.setString(3, "K2");
            stmt.setString(4, "K3");
            stmt.setString(5, "V1");
            stmt.setString(6, "V2");
            stmt.setString(7, "V3");
            stmt.executeUpdate();
            stmt.setString(1, tenant2);
            stmt.setString(2, "K11");
            stmt.setString(3, "K22");
            stmt.setString(4, "K33");
            stmt.setString(5, "V11");
            stmt.setString(6, "V22");
            stmt.setString(7, "V33");
            stmt.executeUpdate();
            globalConn.commit();
        }

        // Verify now that the sequence number of data table, indexes and views have changed.
        // Also verify that the newly added pk columns show up as pk columns of data table, indexes and views.
        try (Connection viewConn = getTenantConnection(tenant1)) {

            ResultSet rs = viewConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view1);
            PhoenixConnection phxConn = viewConn.unwrap(PhoenixConnection.class);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view1));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view1));
            assertEquals(1, getTableSequenceNumber(phxConn, view1));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view1));
            verifyNewColumns(rs, "K2", "K3", "V3");


            rs = viewConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view2);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view2));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view2));
            assertEquals(1, getTableSequenceNumber(phxConn, view2));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view2));
            verifyNewColumns(rs, "K2", "K3", "V3");

            assertEquals(4, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k2"), view2Index));
            assertEquals(5, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k3"), view2Index));
            assertEquals(1, getTableSequenceNumber(phxConn, view2Index));
            assertEquals(6, getMaxKeySequenceNumber(phxConn, view2Index));
        }
        try (Connection viewConn = getTenantConnection(tenant2)) {
            ResultSet rs = viewConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view3);
            PhoenixConnection phxConn = viewConn.unwrap(PhoenixConnection.class);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view3));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view3));
            assertEquals(1, getTableSequenceNumber(phxConn, view3));
            verifyNewColumns(rs, "K22", "K33", "V33");

            assertEquals(4, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k2"), view3Index));
            assertEquals(5, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k3"), view3Index));
            assertEquals(1, getTableSequenceNumber(phxConn, view3Index));
            assertEquals(6, getMaxKeySequenceNumber(phxConn, view3Index));
        }
        // Verify that the index is actually being used when using newly added pk col
        try (Connection viewConn = getTenantConnection(tenant1)) {
            String upsert = "UPSERT INTO " + view2 + " (K1, K2, K3, V1, V2, V3) VALUES ('key1', 'key2', 'key3', 'value1', 'value2', 'value3')";
            viewConn.createStatement().executeUpdate(upsert);
            viewConn.commit();
            Statement stmt = viewConn.createStatement();
            String sql = "SELECT V2 FROM " + view2 + " WHERE V1 = 'value1' AND K3 = 'key3'";
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql);
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(SchemaUtil.normalizeIdentifier(view2Index)));
            ResultSet rs = viewConn.createStatement().executeQuery(sql);
            verifyNewColumns(rs, "value2");
        }

    }
    
    @Test
    public void testAddingPkAndKeyValueColumnsToBaseTableWithDivergedView() throws Exception {
        String baseTable = "testAlteringPkOfBaseTableWithDivergedView".toUpperCase();
        String view1 = generateUniqueName();
        String divergedView = generateUniqueName();
        String divergedViewIndex = divergedView + "_IDX";
        /*                                     baseTable
                                 /                  |                
                         view1(tenant1)         divergedView(tenant2)    
                            
        */
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);
            
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
            }
            
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                String divergedViewDDL = "CREATE VIEW " + divergedView + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant2Conn.createStatement().execute(divergedViewDDL);
                // Drop column V2 from the view to have it diverge from the base table
                tenant2Conn.createStatement().execute("ALTER VIEW " + divergedView + " DROP COLUMN V2");
                
                // create an index on the diverged view
                String indexDDL = "CREATE INDEX " + divergedViewIndex + " ON " + divergedView + " (V1) include (V3)";
                tenant2Conn.createStatement().execute(indexDDL);
            }
            
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD KV VARCHAR, PK2 VARCHAR PRIMARY KEY";
            conn.createStatement().execute(alterBaseTable);
            
            
            // verify that the both columns were added to view1
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                tenant1Conn.createStatement().execute("SELECT KV from " + view1);
                tenant1Conn.createStatement().execute("SELECT PK2 from " + view1);
            }
            
            // verify that only the primary key column PK2 was added to diverged view
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                tenant2Conn.createStatement().execute("SELECT PK2 from " + divergedView);
                try {
                    tenant2Conn.createStatement().execute("SELECT KV FROM " + divergedView);
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
                }
            }
            
            // Upsert records in diverged view. Verify that the PK column was added to the index on it.
            String upsert = "UPSERT INTO " + divergedView + " (PK1, PK2, V1, V3) VALUES ('PK1', 'PK2', 'V1', 'V3')";
            try (Connection viewConn = getTenantConnection("tenant2")) {
                viewConn.createStatement().executeUpdate(upsert);
                viewConn.commit();
                Statement stmt = viewConn.createStatement();
                String sql = "SELECT V3 FROM " + divergedView + " WHERE V1 = 'V1' AND PK2 = 'PK2'";
                QueryPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql);
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(SchemaUtil.normalizeIdentifier(divergedViewIndex)));
                ResultSet rs = viewConn.createStatement().executeQuery(sql);
                verifyNewColumns(rs, "V3");
            }
            
            // For non-diverged view, base table columns will be added at the same position as base table
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 1, 9, 7, "TENANT_ID", "PK1", "V1", "V2", "V3", "KV", "PK2", "VIEW_COL1", "VIEW_COL2");
            // For a diverged view, only base table's pk column will be added and that too at the end.
            assertTableDefinition(conn, divergedView, PTableType.VIEW, baseTable, 2, 7, DIVERGED_VIEW_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V3", "VIEW_COL1", "VIEW_COL2", "PK2");
            
            // Adding existing column VIEW_COL2 to the base table isn't allowed.
            try {
                alterBaseTable = "ALTER TABLE " + baseTable + " ADD VIEW_COL2 CHAR(256)";
                conn.createStatement().execute(alterBaseTable);
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test
    public void testAddColumnsToSaltedBaseTableWithViews() throws Exception {
        String baseTable = "testAddColumnsToSaltedBaseTableWithViews".toUpperCase();
        String view1 = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);

            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
            }

            assertTableDefinition(conn, baseTable, PTableType.TABLE, null, 1, 5, BASE_TABLE_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V2", "V3");
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 0, 7, 5, "TENANT_ID", "PK1", "V1", "V2", "V3", "VIEW_COL1", "VIEW_COL2");

            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD KV VARCHAR, PK2 VARCHAR PRIMARY KEY";
            conn.createStatement().execute(alterBaseTable);

            assertTableDefinition(conn, baseTable, PTableType.TABLE, null, 2, 7, BASE_TABLE_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V2", "V3", "KV", "PK2");
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 1, 9, 7, "TENANT_ID", "PK1", "V1", "V2", "V3", "KV", "PK2", "VIEW_COL1", "VIEW_COL2");

            // verify that the both columns were added to view1
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                tenant1Conn.createStatement().execute("SELECT KV from " + view1);
                tenant1Conn.createStatement().execute("SELECT PK2 from " + view1);
            }
        }
    }
    
    @Test
    public void testDropColumnsFromSaltedBaseTableWithViews() throws Exception {
        String baseTable = "testDropColumnsFromSaltedBaseTableWithViews".toUpperCase();
        String view1 = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);

            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
            }

            assertTableDefinition(conn, baseTable, PTableType.TABLE, null, 1, 5, BASE_TABLE_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V2", "V3");
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 0, 7, 5, "TENANT_ID", "PK1", "V1", "V2", "V3", "VIEW_COL1", "VIEW_COL2");

            String alterBaseTable = "ALTER TABLE " + baseTable + " DROP COLUMN V2";
            conn.createStatement().execute(alterBaseTable);

            assertTableDefinition(conn, baseTable, PTableType.TABLE, null, 2, 4, BASE_TABLE_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V3");
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 1, 6, 4, "TENANT_ID", "PK1", "V1", "V3", "VIEW_COL1", "VIEW_COL2");

            // verify that the dropped columns aren't visible
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                try {
                    tenant1Conn.createStatement().execute("SELECT KV from " + view1);
                    fail();
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
                }
                try {
                    tenant1Conn.createStatement().execute("SELECT PK2 from " + view1);
                    fail();
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
                }
            }
        }
    }
    
    @Test
    public void testAlteringViewConditionallyModifiesHTableMetadata() throws Exception {
        String baseTable = "testAlteringViewConditionallyModifiesBaseTable".toUpperCase();
        String view1 = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);
            HTableDescriptor tableDesc1 = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin().getTableDescriptor(Bytes.toBytes(baseTable)); 
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
                // This should not modify the base table
                String alterView = "ALTER VIEW " + view1 + " ADD NEWCOL1 VARCHAR";
                tenant1Conn.createStatement().execute(alterView);
                HTableDescriptor tableDesc2 = tenant1Conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin().getTableDescriptor(Bytes.toBytes(baseTable));
                assertEquals(tableDesc1, tableDesc2);
                
                // Add a new column family that doesn't already exist in the base table
                alterView = "ALTER VIEW " + view1 + " ADD CF.NEWCOL2 VARCHAR";
                tenant1Conn.createStatement().execute(alterView);
                
                // Verify that the column family now shows up in the base table descriptor
                tableDesc2 = tenant1Conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin().getTableDescriptor(Bytes.toBytes(baseTable));
                assertFalse(tableDesc2.equals(tableDesc1));
                assertNotNull(tableDesc2.getFamily(Bytes.toBytes("CF")));
                
                // Add a column with an existing column family. This shouldn't modify the base table.
                alterView = "ALTER VIEW " + view1 + " ADD CF.NEWCOL3 VARCHAR";
                tenant1Conn.createStatement().execute(alterView);
                HTableDescriptor tableDesc3 = tenant1Conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin().getTableDescriptor(Bytes.toBytes(baseTable));
                assertTrue(tableDesc3.equals(tableDesc2));
                assertNotNull(tableDesc3.getFamily(Bytes.toBytes("CF")));
            }
        }
    }
    
    @Test
    public void testCacheInvalidatedAfterAddingColumnToBaseTableWithViews() throws Exception {
        String baseTable = "testCacheInvalidatedAfterAddingColumnToBaseTableWithViews";
        String viewName = baseTable + "_view";
        String tenantId = "tenantId";
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String tableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true" ;
            globalConn.createStatement().execute(tableDDL);
            Properties tenantProps = new Properties();
            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            // create a tenant specific view
            try (Connection tenantConn =  DriverManager.getConnection(getUrl(), tenantProps)) {
                String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
                tenantConn.createStatement().execute(viewDDL);
                
                // Add a column to the base table using global connection
                globalConn.createStatement().execute("ALTER TABLE " + baseTable + " ADD NEW_COL VARCHAR");

                // Check now whether the tenant connection can see the column that was added
                tenantConn.createStatement().execute("SELECT NEW_COL FROM " + viewName);
                tenantConn.createStatement().execute("SELECT NEW_COL FROM " + baseTable);
            }
        }
    }
    
    @Test
    public void testCacheInvalidatedAfterDroppingColumnFromBaseTableWithViews() throws Exception {
        String baseTable = "testCacheInvalidatedAfterDroppingColumnFromBaseTableWithViews";
        String viewName = baseTable + "_view";
        String tenantId = "tenantId";
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String tableDDL =
                    "CREATE TABLE "
                            + baseTable
                            + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true" ;
            globalConn.createStatement().execute(tableDDL);
            Properties tenantProps = new Properties();
            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            // create a tenant specific view
            try (Connection tenantConn =  DriverManager.getConnection(getUrl(), tenantProps)) {
                String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
                tenantConn.createStatement().execute(viewDDL);
    
                // Add a column to the base table using global connection
                globalConn.createStatement()
                        .execute("ALTER TABLE " + baseTable + " DROP COLUMN V1");
    
                // Check now whether the tenant connection can see the column that was dropped
                try {
                    tenantConn.createStatement().execute("SELECT V1 FROM " + viewName);
                    fail();
                } catch (ColumnNotFoundException e) {
                }
                try {
                    tenantConn.createStatement().execute("SELECT V1 FROM " + baseTable);
                    fail();
                } catch (ColumnNotFoundException e) {
                }
            }
        }
    }

    public static void assertTableDefinition(Connection conn, String tableName, PTableType tableType, String parentTableName, int sequenceNumber, int columnCount, int baseColumnCount, String... columnName) throws Exception {
        PreparedStatement p = conn.prepareStatement("SELECT * FROM \"SYSTEM\".\"CATALOG\" WHERE TABLE_NAME=? AND TABLE_TYPE=?");
        p.setString(1, tableName);
        p.setString(2, tableType.getSerializedValue());
        ResultSet rs = p.executeQuery();
        assertTrue(rs.next());
        assertEquals(AlterTableWithViewsIT.getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in BaseColumnCount"), baseColumnCount, rs.getInt("BASE_COLUMN_COUNT"));
        assertEquals(AlterTableWithViewsIT.getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in columnCount"), columnCount, rs.getInt("COLUMN_COUNT"));
        assertEquals(AlterTableWithViewsIT.getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in sequenceNumber"), sequenceNumber, rs.getInt("TABLE_SEQ_NUM"));
        rs.close();
    
        ResultSet parentTableColumnsRs = null; 
        if (parentTableName != null) {
            parentTableColumnsRs = conn.getMetaData().getColumns(null, null, parentTableName, null);
            parentTableColumnsRs.next();
        }
        
        ResultSet viewColumnsRs = conn.getMetaData().getColumns(null, null, tableName, null);
        for (int i = 0; i < columnName.length; i++) {
            if (columnName[i] != null) {
                assertTrue(viewColumnsRs.next());
                assertEquals(AlterTableWithViewsIT.getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in columnName: i=" + i), columnName[i], viewColumnsRs.getString(PhoenixDatabaseMetaData.COLUMN_NAME));
                int viewColOrdinalPos = viewColumnsRs.getInt(PhoenixDatabaseMetaData.ORDINAL_POSITION);
                assertEquals(AlterTableWithViewsIT.getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in ordinalPosition: i=" + i), i+1, viewColOrdinalPos);
                // validate that all the columns in the base table are present in the view   
                if (parentTableColumnsRs != null && !parentTableColumnsRs.isAfterLast()) {
                    ResultSetMetaData parentTableColumnsMetadata = parentTableColumnsRs.getMetaData();
                    assertEquals(parentTableColumnsMetadata.getColumnCount(), viewColumnsRs.getMetaData().getColumnCount());
                    int parentTableColOrdinalRs = parentTableColumnsRs.getInt(PhoenixDatabaseMetaData.ORDINAL_POSITION);
                    assertEquals(AlterTableWithViewsIT.getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in ordinalPosition of view and base table for i=" + i), parentTableColOrdinalRs, viewColOrdinalPos);
                    for (int columnIndex = 1; columnIndex < parentTableColumnsMetadata.getColumnCount(); columnIndex++) {
                        String viewColumnValue = viewColumnsRs.getString(columnIndex);
                        String parentTableColumnValue = parentTableColumnsRs.getString(columnIndex);
                        if (!Objects.equal(viewColumnValue, parentTableColumnValue)) {
                            if (parentTableColumnsMetadata.getColumnName(columnIndex).equals(PhoenixDatabaseMetaData.TABLE_NAME)) {
                                assertEquals(parentTableName, parentTableColumnValue);
                                assertEquals(tableName, viewColumnValue);
                            } 
                        }
                    }
                    parentTableColumnsRs.next();
                }
            }
        }
        assertFalse(AlterTableWithViewsIT.getSystemCatalogEntriesForTable(conn, tableName, ""), viewColumnsRs.next());
    }
}
