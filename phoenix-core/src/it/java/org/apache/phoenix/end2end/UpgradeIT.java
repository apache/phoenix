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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.query.QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;
import static org.apache.phoenix.util.UpgradeUtil.SELECT_BASE_COLUMN_COUNT_FROM_HEADER_ROW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.Test;

public class UpgradeIT extends BaseHBaseManagedTimeIT {

    private static String TENANT_ID = "tenantId";

    @Test
    public void testUpgradeForTenantViewWithSameColumnsAsBaseTable() throws Exception {
        testViewUpgrade(true, TENANT_ID, null, "TABLEWITHVIEW1", null, "VIEW1", ColumnDiff.EQUAL);
        testViewUpgrade(true, TENANT_ID, "TABLESCHEMA", "TABLEWITHVIEW", null, "VIEW2",
            ColumnDiff.EQUAL);
        testViewUpgrade(true, TENANT_ID, null, "TABLEWITHVIEW3", "VIEWSCHEMA", "VIEW3",
            ColumnDiff.EQUAL);
        testViewUpgrade(true, TENANT_ID, "TABLESCHEMA", "TABLEWITHVIEW4", "VIEWSCHEMA", "VIEW4",
            ColumnDiff.EQUAL);
        testViewUpgrade(true, TENANT_ID, "SAMESCHEMA", "TABLEWITHVIEW5", "SAMESCHEMA", "VIEW5",
            ColumnDiff.EQUAL);
    }

    @Test
    public void testUpgradeForTenantViewWithMoreColumnsThanBaseTable() throws Exception {
        testViewUpgrade(true, TENANT_ID, null, "TABLEWITHVIEW1", null, "VIEW1", ColumnDiff.MORE);
        testViewUpgrade(true, TENANT_ID, "TABLESCHEMA", "TABLEWITHVIEW", null, "VIEW2",
            ColumnDiff.MORE);
        testViewUpgrade(true, TENANT_ID, null, "TABLEWITHVIEW3", "VIEWSCHEMA", "VIEW3",
            ColumnDiff.MORE);
        testViewUpgrade(true, TENANT_ID, "TABLESCHEMA", "TABLEWITHVIEW4", "VIEWSCHEMA", "VIEW4",
            ColumnDiff.MORE);
        testViewUpgrade(true, TENANT_ID, "SAMESCHEMA", "TABLEWITHVIEW5", "SAMESCHEMA", "VIEW5",
            ColumnDiff.MORE);
    }

    @Test
    public void testUpgradeForViewWithSameColumnsAsBaseTable() throws Exception {
        testViewUpgrade(false, null, null, "TABLEWITHVIEW1", null, "VIEW1", ColumnDiff.EQUAL);
        testViewUpgrade(false, null, "TABLESCHEMA", "TABLEWITHVIEW", null, "VIEW2",
            ColumnDiff.EQUAL);
        testViewUpgrade(false, null, null, "TABLEWITHVIEW3", "VIEWSCHEMA", "VIEW3",
            ColumnDiff.EQUAL);
        testViewUpgrade(false, null, "TABLESCHEMA", "TABLEWITHVIEW4", "VIEWSCHEMA", "VIEW4",
            ColumnDiff.EQUAL);
        testViewUpgrade(false, null, "SAMESCHEMA", "TABLEWITHVIEW5", "SAMESCHEMA", "VIEW5",
            ColumnDiff.EQUAL);
    }

    @Test
    public void testUpgradeForViewWithMoreColumnsThanBaseTable() throws Exception {
        testViewUpgrade(false, null, null, "TABLEWITHVIEW1", null, "VIEW1", ColumnDiff.MORE);
        testViewUpgrade(false, null, "TABLESCHEMA", "TABLEWITHVIEW", null, "VIEW2", ColumnDiff.MORE);
        testViewUpgrade(false, null, null, "TABLEWITHVIEW3", "VIEWSCHEMA", "VIEW3", ColumnDiff.MORE);
        testViewUpgrade(false, null, "TABLESCHEMA", "TABLEWITHVIEW4", "VIEWSCHEMA", "VIEW4",
            ColumnDiff.MORE);
        testViewUpgrade(false, null, "SAMESCHEMA", "TABLEWITHVIEW5", "SAMESCHEMA", "VIEW5",
            ColumnDiff.MORE);
    }

    @Test
    public void testSettingBaseColumnCountWhenBaseTableColumnDropped() throws Exception {
        testViewUpgrade(true, TENANT_ID, null, "TABLEWITHVIEW1", null, "VIEW1", ColumnDiff.MORE);
        testViewUpgrade(true, TENANT_ID, "TABLESCHEMA", "TABLEWITHVIEW", null, "VIEW2",
            ColumnDiff.LESS);
        testViewUpgrade(true, TENANT_ID, null, "TABLEWITHVIEW3", "VIEWSCHEMA", "VIEW3",
            ColumnDiff.LESS);
        testViewUpgrade(true, TENANT_ID, "TABLESCHEMA", "TABLEWITHVIEW4", "VIEWSCHEMA", "VIEW4",
            ColumnDiff.LESS);
        testViewUpgrade(true, TENANT_ID, "SAMESCHEMA", "TABLEWITHVIEW5", "SAMESCHEMA", "VIEW5",
            ColumnDiff.LESS);
    }

    @Test
    public void testMapTableToNamespaceDuringUpgrade()
            throws SQLException, IOException, IllegalArgumentException, InterruptedException {
        String[] strings = new String[] { "a", "b", "c", "d" };

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String schemaName = "TEST";
            String phoenixFullTableName = schemaName + ".S_NEW";
            String indexName = "IDX";
            String localIndexName = "LIDX";
            String[] tableNames = new String[] { phoenixFullTableName, schemaName + "." + indexName,
                    schemaName + "." + localIndexName, "diff.v", "test.v","v"};
            String[] viewIndexes = new String[] { "diff.v_idx", "test.v_idx" };
            conn.createStatement().execute("CREATE TABLE " + phoenixFullTableName
                    + "(k VARCHAR PRIMARY KEY, v INTEGER, f INTEGER, g INTEGER NULL, h INTEGER NULL)");
            PreparedStatement upsertStmt = conn
                    .prepareStatement("UPSERT INTO " + phoenixFullTableName + " VALUES(?, ?, 0, 0, 0)");
            int i = 1;
            for (String str : strings) {
                upsertStmt.setString(1, str);
                upsertStmt.setInt(2, i++);
                upsertStmt.execute();
            }
            conn.commit();
            // creating local index
            conn.createStatement()
                    .execute("create local index " + localIndexName + " on " + phoenixFullTableName + "(K)");
            // creating global index
            conn.createStatement().execute("create index " + indexName + " on " + phoenixFullTableName + "(k)");
            // creating view in schema 'diff'
            conn.createStatement().execute("CREATE VIEW diff.v (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // creating view in schema 'test'
            conn.createStatement().execute("CREATE VIEW test.v (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            conn.createStatement().execute("CREATE VIEW v (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // Creating index on views
            conn.createStatement().execute("create index v_idx on diff.v(col)");
            conn.createStatement().execute("create index v_idx on test.v(col)");

            // validate data
            for (String tableName : tableNames) {
                ResultSet rs = conn.createStatement().executeQuery("select * from " + tableName);
                for (String str : strings) {
                    assertTrue(rs.next());
                    assertEquals(str, rs.getString(1));
                }
            }

            // validate view Index data
            for (String viewIndex : viewIndexes) {
                ResultSet rs = conn.createStatement().executeQuery("select * from " + viewIndex);
                for (String str : strings) {
                    assertTrue(rs.next());
                    assertEquals(str, rs.getString(2));
                }
            }

            HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            assertTrue(admin.tableExists(phoenixFullTableName));
            assertTrue(admin.tableExists(schemaName + QueryConstants.NAME_SEPARATOR + indexName));
            assertTrue(admin.tableExists(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(phoenixFullTableName))));
            Properties props = new Properties();
            props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
            props.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.toString(false));
            admin.close();
            PhoenixConnection phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
            UpgradeUtil.upgradeTable(phxConn, phoenixFullTableName);
            Set<String> viewNames = MetaDataUtil.getViewNames(phxConn, phoenixFullTableName);
            for (String viewName : viewNames) {
                UpgradeUtil.upgradeTable(phxConn, viewName);
            }
            admin = phxConn.getQueryServices().getAdmin();
            String hbaseTableName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(phoenixFullTableName), true)
                    .getNameAsString();
            assertTrue(admin.tableExists(hbaseTableName));
            assertTrue(admin.tableExists(Bytes.toBytes(hbaseTableName)));
            assertTrue(admin.tableExists(schemaName + QueryConstants.NAMESPACE_SEPARATOR + indexName));
            assertTrue(admin.tableExists(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(hbaseTableName))));
            i = 0;
            // validate data
            for (String tableName : tableNames) {
                ResultSet rs = phxConn.createStatement().executeQuery("select * from " + tableName);
                for (String str : strings) {
                    assertTrue(rs.next());
                    assertEquals(str, rs.getString(1));
                }
            }
            // validate view Index data
            for (String viewIndex : viewIndexes) {
                ResultSet rs = conn.createStatement().executeQuery("select * from " + viewIndex);
                for (String str : strings) {
                    assertTrue(rs.next());
                    assertEquals(str, rs.getString(2));
                }
            }
            PName tenantId = phxConn.getTenantId();
            PName physicalName = PNameFactory.newName(hbaseTableName);
            String oldSchemaName = MetaDataUtil.getViewIndexSequenceSchemaName(PNameFactory.newName(phoenixFullTableName),
                    false);
            String newSchemaName = MetaDataUtil.getViewIndexSequenceSchemaName(physicalName, true);
            String newSequenceName = MetaDataUtil.getViewIndexSequenceName(physicalName, tenantId, true);
            ResultSet rs = phxConn.createStatement()
                    .executeQuery("SELECT " + PhoenixDatabaseMetaData.CURRENT_VALUE + "  FROM "
                            + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE + " WHERE " + PhoenixDatabaseMetaData.TENANT_ID
                            + " IS NULL AND " + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + " = '" + newSchemaName
                            + "' AND " + PhoenixDatabaseMetaData.SEQUENCE_NAME + "='" + newSequenceName + "'");
            assertTrue(rs.next());
            assertEquals("-32765", rs.getString(1));
            rs = phxConn.createStatement().executeQuery("SELECT " + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + ","
                    + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + "," + PhoenixDatabaseMetaData.CURRENT_VALUE + "  FROM "
                    + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE + " WHERE " + PhoenixDatabaseMetaData.TENANT_ID
                    + " IS NULL AND " + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + " = '" + oldSchemaName + "'");
            assertFalse(rs.next());
            phxConn.close();
            admin.close();
   
        }
    }
    
    @Test
    public void testSettingBaseColumnCountForMultipleViewsOnTable() throws Exception {
        String baseSchema = "XYZ";
        String baseTable = "BASE_TABLE";
        String fullBaseTableName = SchemaUtil.getTableName(baseSchema, baseTable);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + fullBaseTableName + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 INTEGER, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true";
            conn.createStatement().execute(baseTableDDL);
            
            for (int i = 1; i <=2; i++) {
                // Create views for tenants;
                String tenant = "tenant" + i;
                try (Connection tenantConn = createTenantConnection(tenant)) {
                    String view = "TENANT_VIEW1";
                    
                    // view with its own column
                    String viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                    tenantConn.createStatement().execute(viewDDL);
                    String addCols = "ALTER VIEW " + view + " ADD COL1 VARCHAR ";
                    tenantConn.createStatement().execute(addCols);
                    removeBaseColumnCountKV(tenant, null, view);

                    // view that has the last base table column removed
                    view = "TENANT_VIEW2";
                    viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                    tenantConn.createStatement().execute(viewDDL);
                    String droplastBaseCol = "ALTER VIEW " + view + " DROP COLUMN V2";
                    tenantConn.createStatement().execute(droplastBaseCol);
                    removeBaseColumnCountKV(tenant, null, view);

                    // view that has the middle base table column removed
                    view = "TENANT_VIEW3";
                    viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                    tenantConn.createStatement().execute(viewDDL);
                    String dropMiddileBaseCol = "ALTER VIEW " + view + " DROP COLUMN V1";
                    tenantConn.createStatement().execute(dropMiddileBaseCol);
                    removeBaseColumnCountKV(tenant, null, view);
                }
            }
            
            // create global views
            try (Connection globalConn = DriverManager.getConnection(getUrl())) {
                String view = "GLOBAL_VIEW1";
                
                // view with its own column
                String viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                globalConn.createStatement().execute(viewDDL);
                String addCols = "ALTER VIEW " + view + " ADD COL1 VARCHAR ";
                globalConn.createStatement().execute(addCols);
                removeBaseColumnCountKV(null, null, view);

                // view that has the last base table column removed
                view = "GLOBAL_VIEW2";
                viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                globalConn.createStatement().execute(viewDDL);
                String droplastBaseCol = "ALTER VIEW " + view + " DROP COLUMN V2";
                globalConn.createStatement().execute(droplastBaseCol);
                removeBaseColumnCountKV(null, null, view);

                // view that has the middle base table column removed
                view = "GLOBAL_VIEW3";
                viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                globalConn.createStatement().execute(viewDDL);
                String dropMiddileBaseCol = "ALTER VIEW " + view + " DROP COLUMN V1";
                globalConn.createStatement().execute(dropMiddileBaseCol);
                removeBaseColumnCountKV(null, null, view);
            }
            
            // run upgrade
            UpgradeUtil.upgradeTo4_5_0(conn.unwrap(PhoenixConnection.class));
            
            // Verify base column counts for tenant specific views
            for (int i = 1; i <=2 ; i++) {
                String tenantId = "tenant" + i;
                checkBaseColumnCount(tenantId, null, "TENANT_VIEW1", 4);
                checkBaseColumnCount(tenantId, null, "TENANT_VIEW2", DIVERGED_VIEW_BASE_COLUMN_COUNT);
                checkBaseColumnCount(tenantId, null, "TENANT_VIEW3", DIVERGED_VIEW_BASE_COLUMN_COUNT);
            }
            
            // Verify base column count for global views
            checkBaseColumnCount(null, null, "GLOBAL_VIEW1", 4);
            checkBaseColumnCount(null, null, "GLOBAL_VIEW2", DIVERGED_VIEW_BASE_COLUMN_COUNT);
            checkBaseColumnCount(null, null, "GLOBAL_VIEW3", DIVERGED_VIEW_BASE_COLUMN_COUNT);
        }
        
        
    }
    
    private enum ColumnDiff {
        MORE, EQUAL, LESS
    };

    private void testViewUpgrade(boolean tenantView, String tenantId, String baseTableSchema,
            String baseTableName, String viewSchema, String viewName, ColumnDiff diff)
            throws Exception {
        if (tenantView) {
            checkNotNull(tenantId);
        } else {
            checkArgument(tenantId == null);
        }
        Connection conn = DriverManager.getConnection(getUrl());
        String fullViewName = SchemaUtil.getTableName(viewSchema, viewName);
        String fullBaseTableName = SchemaUtil.getTableName(baseTableSchema, baseTableName);
        try {
            int expectedBaseColumnCount;
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS " + fullBaseTableName + " ("
                        + " TENANT_ID CHAR(15) NOT NULL, " + " PK1 integer NOT NULL, "
                        + "PK2 bigint NOT NULL, " + "CF1.V1 VARCHAR, " + "CF2.V2 VARCHAR, "
                        + "V3 CHAR(100) ARRAY[4] "
                        + " CONSTRAINT NAME_PK PRIMARY KEY (TENANT_ID, PK1, PK2)"
                        + " ) MULTI_TENANT= true");
            
            // create a view with same columns as base table.
            try (Connection conn2 = getConnection(tenantView, tenantId)) {
                conn2.createStatement().execute(
                    "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullBaseTableName);
            }

            if (diff == ColumnDiff.MORE) {
                    // add a column to the view
                    try (Connection conn3 = getConnection(tenantView, tenantId)) {
                        conn3.createStatement().execute(
                            "ALTER VIEW " + fullViewName + " ADD VIEW_COL1 VARCHAR");
                    }
            }
            if (diff == ColumnDiff.LESS) {
                try (Connection conn3 = getConnection(tenantView, tenantId)) {
                    conn3.createStatement().execute(
                        "ALTER VIEW " + fullViewName + " DROP COLUMN CF2.V2");
                }
                expectedBaseColumnCount = DIVERGED_VIEW_BASE_COLUMN_COUNT;
            } else {
                expectedBaseColumnCount = 6;
            }

            checkBaseColumnCount(tenantId, viewSchema, viewName, expectedBaseColumnCount);
            checkBaseColumnCount(null, baseTableSchema, baseTableName, BASE_TABLE_BASE_COLUMN_COUNT);
            
            // remove base column count kv so we can check whether the upgrade code is setting the 
            // base column count correctly.
            removeBaseColumnCountKV(tenantId, viewSchema, viewName);
            removeBaseColumnCountKV(null, baseTableSchema, baseTableName);

            // assert that the removing base column count key value worked correctly.
            checkBaseColumnCount(tenantId, viewSchema, viewName, 0);
            checkBaseColumnCount(null, baseTableSchema, baseTableName, 0);
            
            // run upgrade
            UpgradeUtil.upgradeTo4_5_0(conn.unwrap(PhoenixConnection.class));

            checkBaseColumnCount(tenantId, viewSchema, viewName, expectedBaseColumnCount);
            checkBaseColumnCount(null, baseTableSchema, baseTableName, BASE_TABLE_BASE_COLUMN_COUNT);
        } finally {
            conn.close();
        }
    }

    private static void checkBaseColumnCount(String tenantId, String schemaName, String tableName,
            int expectedBaseColumnCount) throws Exception {
        checkNotNull(tableName);
        Connection conn = DriverManager.getConnection(getUrl());
        String sql = SELECT_BASE_COLUMN_COUNT_FROM_HEADER_ROW;
        sql =
                String.format(sql, tenantId == null ? " IS NULL " : " = ? ",
                    schemaName == null ? "IS NULL" : " = ? ");
        int paramIndex = 1;
        PreparedStatement stmt = conn.prepareStatement(sql);
        if (tenantId != null) {
            stmt.setString(paramIndex++, tenantId);
        }
        if (schemaName != null) {
            stmt.setString(paramIndex++, schemaName);
        }
        stmt.setString(paramIndex, tableName);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(expectedBaseColumnCount, rs.getInt(1));
        assertFalse(rs.next());
    }

    private static void
            removeBaseColumnCountKV(String tenantId, String schemaName, String tableName)
                    throws Exception {
        byte[] rowKey =
                SchemaUtil.getTableKey(tenantId == null ? new byte[0] : Bytes.toBytes(tenantId),
                    schemaName == null ? new byte[0] : Bytes.toBytes(schemaName),
                    Bytes.toBytes(tableName));
        Put viewColumnDefinitionPut = new Put(rowKey, HConstants.LATEST_TIMESTAMP);
        viewColumnDefinitionPut.add(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
            PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES, HConstants.LATEST_TIMESTAMP, null);

        try (PhoenixConnection conn =
                (DriverManager.getConnection(getUrl())).unwrap(PhoenixConnection.class)) {
            try (HTableInterface htable =
                    conn.getQueryServices().getTable(
                        Bytes.toBytes(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME))) {
                RowMutations mutations = new RowMutations(rowKey);
                mutations.add(viewColumnDefinitionPut);
                htable.mutateRow(mutations);
            }
        }
    }

    private Connection createTenantConnection(String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }

    private Connection getConnection(boolean tenantSpecific, String tenantId) throws SQLException {
        if (tenantSpecific) {
            checkNotNull(tenantId);
            return createTenantConnection(tenantId);
        }
        return DriverManager.getConnection(getUrl());
    }
    
}
