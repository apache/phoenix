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
import static org.apache.phoenix.query.ConnectionQueryServicesImpl.UPGRADE_MUTEX;
import static org.apache.phoenix.query.ConnectionQueryServicesImpl.UPGRADE_MUTEX_LOCKED;
import static org.apache.phoenix.query.ConnectionQueryServicesImpl.UPGRADE_MUTEX_UNLOCKED;
import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.query.QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;
import static org.apache.phoenix.util.UpgradeUtil.SELECT_BASE_COLUMN_COUNT_FROM_HEADER_ROW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.UpgradeInProgressException;
import org.apache.phoenix.exception.UpgradeRequiredException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.Before;
import org.junit.Test;

public class UpgradeIT extends ParallelStatsDisabledIT {

    private String tenantId;
    
    @Before
    public void generateTenantId() {
        tenantId = "T_" + generateUniqueName();
    }

    @Test
    public void testUpgradeForTenantViewWithSameColumnsAsBaseTable() throws Exception {
        String tableWithViewName = generateUniqueName();
        String viewTableName = generateUniqueName();
        testViewUpgrade(true, tenantId, null, tableWithViewName + "1", null, viewTableName + "1", ColumnDiff.EQUAL);
        testViewUpgrade(true, tenantId, "TABLESCHEMA", tableWithViewName + "", null, viewTableName + "2",
            ColumnDiff.EQUAL);
        testViewUpgrade(true, tenantId, null, tableWithViewName + "3", viewTableName + "SCHEMA", viewTableName + "3",
            ColumnDiff.EQUAL);
        testViewUpgrade(true, tenantId, "TABLESCHEMA", tableWithViewName + "4", viewTableName + "SCHEMA", viewTableName + "4",
            ColumnDiff.EQUAL);
        testViewUpgrade(true, tenantId, "SAMESCHEMA", tableWithViewName + "5", "SAMESCHEMA", viewTableName + "5",
            ColumnDiff.EQUAL);
    }

    @Test
    public void testUpgradeForTenantViewWithMoreColumnsThanBaseTable() throws Exception {
        String tableWithViewName = generateUniqueName();
        String viewTableName = generateUniqueName();
        testViewUpgrade(true, tenantId, null, tableWithViewName + "1", null, viewTableName + "1", ColumnDiff.MORE);
        testViewUpgrade(true, tenantId, "TABLESCHEMA", tableWithViewName + "", null, viewTableName + "2",
            ColumnDiff.MORE);
        testViewUpgrade(true, tenantId, null, tableWithViewName + "3", "VIEWSCHEMA", viewTableName + "3",
            ColumnDiff.MORE);
        testViewUpgrade(true, tenantId, "TABLESCHEMA", tableWithViewName + "4", "VIEWSCHEMA", viewTableName + "4",
            ColumnDiff.MORE);
        testViewUpgrade(true, tenantId, "SAMESCHEMA", tableWithViewName + "5", "SAMESCHEMA", viewTableName + "5",
            ColumnDiff.MORE);
    }

    @Test
    public void testUpgradeForViewWithSameColumnsAsBaseTable() throws Exception {
        String tableWithViewName = generateUniqueName();
        String viewTableName = generateUniqueName();
        testViewUpgrade(false, null, null, tableWithViewName + "1", null, viewTableName + "1", ColumnDiff.EQUAL);
        testViewUpgrade(false, null, "TABLESCHEMA", tableWithViewName + "", null, viewTableName + "2",
            ColumnDiff.EQUAL);
        testViewUpgrade(false, null, null, tableWithViewName + "3", "VIEWSCHEMA", viewTableName + "3",
            ColumnDiff.EQUAL);
        testViewUpgrade(false, null, "TABLESCHEMA", tableWithViewName + "4", "VIEWSCHEMA", viewTableName + "4",
            ColumnDiff.EQUAL);
        testViewUpgrade(false, null, "SAMESCHEMA", tableWithViewName + "5", "SAMESCHEMA", viewTableName + "5",
            ColumnDiff.EQUAL);
    }

    @Test
    public void testUpgradeForViewWithMoreColumnsThanBaseTable() throws Exception {
        String tableWithViewName = generateUniqueName();
        String viewTableName = generateUniqueName();
        testViewUpgrade(false, null, null, tableWithViewName + "1", null, viewTableName + "1", ColumnDiff.MORE);
        testViewUpgrade(false, null, "TABLESCHEMA", tableWithViewName + "", null, viewTableName + "2", ColumnDiff.MORE);
        testViewUpgrade(false, null, null, tableWithViewName + "3", "VIEWSCHEMA", viewTableName + "3", ColumnDiff.MORE);
        testViewUpgrade(false, null, "TABLESCHEMA", tableWithViewName + "4", "VIEWSCHEMA", viewTableName + "4",
            ColumnDiff.MORE);
        testViewUpgrade(false, null, "SAMESCHEMA", tableWithViewName + "5", "SAMESCHEMA", viewTableName + "5",
            ColumnDiff.MORE);
    }

    @Test
    public void testSettingBaseColumnCountWhenBaseTableColumnDropped() throws Exception {
        String tableWithViewName = generateUniqueName();
        String viewTableName = generateUniqueName();
        testViewUpgrade(true, tenantId, null, tableWithViewName + "1", null, viewTableName + "1", ColumnDiff.MORE);
        testViewUpgrade(true, tenantId, "TABLESCHEMA", tableWithViewName + "", null, viewTableName + "2",
            ColumnDiff.LESS);
        testViewUpgrade(true, tenantId, null, tableWithViewName + "3", "VIEWSCHEMA", viewTableName + "3",
            ColumnDiff.LESS);
        testViewUpgrade(true, tenantId, "TABLESCHEMA", tableWithViewName + "4", "VIEWSCHEMA", viewTableName + "4",
            ColumnDiff.LESS);
        testViewUpgrade(true, tenantId, "SAMESCHEMA", tableWithViewName + "5", "SAMESCHEMA", viewTableName + "5",
            ColumnDiff.LESS);
    }

    @Test
    public void testMapTableToNamespaceDuringUpgrade()
            throws SQLException, IOException, IllegalArgumentException, InterruptedException {
        String[] strings = new String[] { "a", "b", "c", "d" };

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String schemaName = "TEST";
            String phoenixFullTableName = schemaName + "." + generateUniqueName();
            String indexName = "IDX_" + generateUniqueName();
            String localIndexName = "LIDX_" + generateUniqueName();

            String viewName = "VIEW_" + generateUniqueName();
            String viewIndexName = "VIDX_" + generateUniqueName();

            String[] tableNames = new String[] { phoenixFullTableName, schemaName + "." + indexName,
                    schemaName + "." + localIndexName, "diff." + viewName, "test." + viewName, viewName};
            String[] viewIndexes = new String[] { "diff." + viewIndexName, "test." + viewIndexName };
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
            conn.createStatement().execute("CREATE VIEW diff." + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // creating view in schema 'test'
            conn.createStatement().execute("CREATE VIEW test." + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            conn.createStatement().execute("CREATE VIEW " + viewName + "(col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // Creating index on views
            conn.createStatement().execute("create index " + viewIndexName + "  on diff." + viewName + "(col)");
            conn.createStatement().execute("create index " + viewIndexName + " on test." + viewName + "(col)");

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
            UpgradeUtil.mapChildViewsToNamespace(phxConn, phoenixFullTableName,props);
            phxConn.close();
            props = new Properties();
            phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
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
    public void testMapMultiTenantTableToNamespaceDuringUpgrade() throws SQLException, SnapshotCreationException,
            IllegalArgumentException, IOException, InterruptedException {
        String[] strings = new String[] { "a", "b", "c", "d" };
        String schemaName1 = "S_" +generateUniqueName(); // TEST
        String schemaName2 = "S_" +generateUniqueName(); // DIFF
        String phoenixFullTableName = schemaName1 + "." + generateUniqueName();
        String hbaseTableName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(phoenixFullTableName), true)
                .getNameAsString();
        String indexName = "IDX_" + generateUniqueName();
        String viewName = "V_" + generateUniqueName();
        String viewName1 = "V1_" + generateUniqueName();
        String viewIndexName = "V_IDX_" + generateUniqueName();
        String tenantViewIndexName = "V1_IDX_" + generateUniqueName();

        String[] tableNames = new String[] { phoenixFullTableName, schemaName2 + "." + viewName1, schemaName1 + "." + viewName1, viewName1 };
        String[] viewIndexes = new String[] { schemaName1 + "." + viewIndexName, schemaName2 + "." + viewIndexName };
        String[] tenantViewIndexes = new String[] { schemaName1 + "." + tenantViewIndexName, schemaName2 + "." + tenantViewIndexName };
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + phoenixFullTableName
                    + "(k VARCHAR not null, v INTEGER not null, f INTEGER, g INTEGER NULL, h INTEGER NULL CONSTRAINT pk PRIMARY KEY(k,v)) MULTI_TENANT=true");
            PreparedStatement upsertStmt = conn
                    .prepareStatement("UPSERT INTO " + phoenixFullTableName + " VALUES(?, ?, 0, 0, 0)");
            int i = 1;
            for (String str : strings) {
                upsertStmt.setString(1, str);
                upsertStmt.setInt(2, i++);
                upsertStmt.execute();
            }
            conn.commit();

            // creating global index
            conn.createStatement().execute("create index " + indexName + " on " + phoenixFullTableName + "(f)");
            // creating view in schema 'diff'
            conn.createStatement().execute("CREATE VIEW " + schemaName2 + "." + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // creating view in schema 'test'
            conn.createStatement().execute("CREATE VIEW " + schemaName1 + "." + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            conn.createStatement().execute("CREATE VIEW " + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // Creating index on views
            conn.createStatement().execute("create local index " + viewIndexName + " on " + schemaName2 + "." + viewName + "(col)");
            conn.createStatement().execute("create local index " + viewIndexName + " on " + schemaName1 + "." + viewName + "(col)");
        }
        Properties props = new Properties();
        String tenantId = strings[0];
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement upsertStmt = conn
                    .prepareStatement("UPSERT INTO " + phoenixFullTableName + "(k,v,f,g,h)  VALUES(?, ?, 0, 0, 0)");
            int i = 1;
            for (String str : strings) {
                upsertStmt.setString(1, str);
                upsertStmt.setInt(2, i++);
                upsertStmt.execute();
            }
            conn.commit();
            // creating view in schema 'diff'
            conn.createStatement()
                    .execute("CREATE VIEW " + schemaName2 + "." + viewName1 + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // creating view in schema 'test'
            conn.createStatement()
                    .execute("CREATE VIEW " + schemaName1 + "." + viewName1 + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            conn.createStatement().execute("CREATE VIEW " + viewName1 + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // Creating index on views
            conn.createStatement().execute("create index " + tenantViewIndexName + " on " + schemaName2 + "." + viewName1 + "(col)");
            conn.createStatement().execute("create index " + tenantViewIndexName + " on " + schemaName1 + "." + viewName1 + "(col)");
        }

        props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        props.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.toString(false));
        PhoenixConnection phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        UpgradeUtil.upgradeTable(phxConn, phoenixFullTableName);
        UpgradeUtil.mapChildViewsToNamespace(phxConn,phoenixFullTableName,props);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        int i = 1;
        String indexPhysicalTableName = Bytes
                .toString(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(hbaseTableName)));
        // validate data with tenant
        for (String tableName : tableNames) {
            assertTableUsed(phxConn, tableName, hbaseTableName);
            ResultSet rs = phxConn.createStatement().executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            do {
                assertEquals(i++, rs.getInt(1));
            } while (rs.next());
            i = 1;
        }
        // validate view Index data
        for (String viewIndex : tenantViewIndexes) {
            assertTableUsed(phxConn, viewIndex, indexPhysicalTableName);
            ResultSet rs = phxConn.createStatement().executeQuery("select * from " + viewIndex);
            assertTrue(rs.next());
            do {
                assertEquals(i++, rs.getInt(2));
            } while (rs.next());
            i = 1;
        }
        phxConn.close();
        props.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
        phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);

        // validate view Index data
        for (String viewIndex : viewIndexes) {
            assertTableUsed(phxConn, viewIndex, hbaseTableName);
            ResultSet rs = phxConn.createStatement().executeQuery("select * from " + viewIndex);
            for (String str : strings) {
                assertTrue(rs.next());
                assertEquals(str, rs.getString(1));
            }
        }
        phxConn.close();
    }

    public void assertTableUsed(Connection conn, String phoenixTableName, String hbaseTableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + phoenixTableName);
        assertTrue(rs.next());
        assertTrue(rs.getString(1).contains(hbaseTableName));
    }
    

    @Test
    public void testSettingBaseColumnCountForMultipleViewsOnTable() throws Exception {
        String baseSchema = "S_" + generateUniqueName();
        String baseTable = "T_" + generateUniqueName();
        String fullBaseTableName = SchemaUtil.getTableName(baseSchema, baseTable);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + fullBaseTableName + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 INTEGER, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true";
            conn.createStatement().execute(baseTableDDL);

            String[] tenants = new String[] {"T_" + generateUniqueName(), "T_" + generateUniqueName()};
            Collections.sort(Arrays.asList(tenants));
            String[] tenantViews = new String[] {"V_" + generateUniqueName(), "V_" + generateUniqueName(), "V_" + generateUniqueName()};
            Collections.sort(Arrays.asList(tenantViews));
            String[] globalViews = new String[] {"G_" + generateUniqueName(), "G_" + generateUniqueName(), "G_" + generateUniqueName()};
            Collections.sort(Arrays.asList(globalViews));
            for (int i = 0; i < 2; i++) {
                // Create views for tenants;
                String tenant = tenants[i];
                try (Connection tenantConn = createTenantConnection(tenant)) {
                    String view = tenantViews[0];
                    // view with its own column
                    String viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                    tenantConn.createStatement().execute(viewDDL);
                    String addCols = "ALTER VIEW " + view + " ADD COL1 VARCHAR ";
                    tenantConn.createStatement().execute(addCols);
                    removeBaseColumnCountKV(tenant, null, view);

                    // view that has the last base table column removed
                    view = tenantViews[1];
                    viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                    tenantConn.createStatement().execute(viewDDL);
                    String droplastBaseCol = "ALTER VIEW " + view + " DROP COLUMN V2";
                    tenantConn.createStatement().execute(droplastBaseCol);
                    removeBaseColumnCountKV(tenant, null, view);

                    // view that has the middle base table column removed
                    view = tenantViews[2];
                    viewDDL = "CREATE VIEW " + view + " AS SELECT * FROM " + fullBaseTableName;
                    tenantConn.createStatement().execute(viewDDL);
                    String dropMiddileBaseCol = "ALTER VIEW " + view + " DROP COLUMN V1";
                    tenantConn.createStatement().execute(dropMiddileBaseCol);
                    removeBaseColumnCountKV(tenant, null, view);
                }
            }

            // create global views
            try (Connection globalConn = DriverManager.getConnection(getUrl())) {
                String globalView = globalViews[0];
                // view with its own column
                String viewDDL = "CREATE VIEW " + globalView + " AS SELECT * FROM " + fullBaseTableName;
                globalConn.createStatement().execute(viewDDL);
                String addCols = "ALTER VIEW " + globalView + " ADD COL1 VARCHAR ";
                globalConn.createStatement().execute(addCols);
                removeBaseColumnCountKV(null, null, globalView);

                // view that has the last base table column removed
                globalView = globalViews[1];
                viewDDL = "CREATE VIEW " + globalView + " AS SELECT * FROM " + fullBaseTableName;
                globalConn.createStatement().execute(viewDDL);
                String droplastBaseCol = "ALTER VIEW " + globalView + " DROP COLUMN V2";
                globalConn.createStatement().execute(droplastBaseCol);
                removeBaseColumnCountKV(null, null, globalView);

                // view that has the middle base table column removed
                globalView = globalViews[2];
                viewDDL = "CREATE VIEW " + globalView + " AS SELECT * FROM " + fullBaseTableName;
                globalConn.createStatement().execute(viewDDL);
                String dropMiddileBaseCol = "ALTER VIEW " + globalView + " DROP COLUMN V1";
                globalConn.createStatement().execute(dropMiddileBaseCol);
                removeBaseColumnCountKV(null, null, globalView);
            }
            
            // run upgrade
            UpgradeUtil.upgradeTo4_5_0(conn.unwrap(PhoenixConnection.class));
            
            // Verify base column counts for tenant specific views
            for (int i = 0; i < 2 ; i++) {
                String tenantId = tenants[i];
                checkBaseColumnCount(tenantId, null, tenantViews[0], 4);
                checkBaseColumnCount(tenantId, null, tenantViews[1], DIVERGED_VIEW_BASE_COLUMN_COUNT);
                checkBaseColumnCount(tenantId, null, tenantViews[2], DIVERGED_VIEW_BASE_COLUMN_COUNT);
            }
            
            // Verify base column count for global views
            checkBaseColumnCount(null, null, globalViews[0], 4);
            checkBaseColumnCount(null, null, globalViews[1], DIVERGED_VIEW_BASE_COLUMN_COUNT);
            checkBaseColumnCount(null, null, globalViews[2], DIVERGED_VIEW_BASE_COLUMN_COUNT);
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
    
    @Test
    public void testUpgradeRequiredPreventsSQL() throws SQLException {
        String tableName = generateUniqueName();
        try (Connection conn = getConnection(false, null)) {
            conn.createStatement().execute(
                    "CREATE TABLE " + tableName
                            + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2))");
            final ConnectionQueryServices delegate = conn.unwrap(PhoenixConnection.class).getQueryServices();
            ConnectionQueryServices servicesWithUpgrade = new DelegateConnectionQueryServices(delegate) {
                @Override
                public boolean isUpgradeRequired() {
                    return true;
                }
            };
            try (PhoenixConnection phxConn = new PhoenixConnection(servicesWithUpgrade,
                    conn.unwrap(PhoenixConnection.class), HConstants.LATEST_TIMESTAMP)) {
                try {
                    phxConn.createStatement().execute(
                            "CREATE TABLE " + generateUniqueName()
                                    + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT PK PRIMARY KEY(K1,K2))");
                    fail("CREATE TABLE should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                try {
                    phxConn.createStatement().execute("SELECT * FROM " + tableName);
                    fail("SELECT should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                try {
                    phxConn.createStatement().execute("DELETE FROM " + tableName);
                    fail("DELETE should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                try {
                    phxConn.createStatement().execute(
                            "CREATE INDEX " + tableName + "_IDX ON " + tableName + " (KV1) INCLUDE (KV2)" );
                    fail("CREATE INDEX should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                try {
                    phxConn.createStatement().execute(
                            "UPSERT INTO " + tableName + " VALUES ('PK1', 'PK2', 'KV1', 'KV2')" );
                    fail("UPSERT VALUES should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
            }
        }
    }
    
    @Test
    public void testUpgradingConnectionBypassesUpgradeRequiredCheck() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = getConnection(false, null)) {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE "
                                    + tableName
                                    + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2))");
            final ConnectionQueryServices delegate = conn.unwrap(PhoenixConnection.class).getQueryServices();
            ConnectionQueryServices servicesWithUpgrade = new DelegateConnectionQueryServices(delegate) {
                @Override
                public boolean isUpgradeRequired() {
                    return true;
                }
            };
            try (PhoenixConnection phxConn = new PhoenixConnection(servicesWithUpgrade,
                    conn.unwrap(PhoenixConnection.class), HConstants.LATEST_TIMESTAMP)) {
                // Because upgrade is required, this SQL should fail.
                try {
                    phxConn.createStatement().executeQuery("SELECT * FROM " + tableName);
                    fail("SELECT should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                // Marking connection as the one running upgrade should let SQL execute fine.
                phxConn.setRunningUpgrade(true);
                phxConn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES ('PK1', 'PK2', 'KV1', 'KV2')" );
                phxConn.commit();
                try (ResultSet rs = phxConn.createStatement().executeQuery("SELECT * FROM " + tableName)) {
                    assertTrue(rs.next());
                    assertFalse(rs.next());
                }
            }
        }
    }
    
    private void putUnlockKVInSysMutex(byte[] row) throws Exception {
        try (Connection conn = getConnection(false, null)) {
            ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
            try (HTableInterface sysMutexTable = services.getTable(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME_BYTES)) {
                byte[] family = PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
                byte[] qualifier = UPGRADE_MUTEX;
                Put put = new Put(row);
                put.add(family, qualifier, UPGRADE_MUTEX_UNLOCKED);
                sysMutexTable.put(put);
                sysMutexTable.flushCommits();
            }
        }
    }
    
    @Test
    public void testAcquiringAndReleasingUpgradeMutex() throws Exception {
        ConnectionQueryServices services = null;
        byte[] mutexRowKey = SchemaUtil.getTableKey(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA,
                generateUniqueName());
        try (Connection conn = getConnection(false, null)) {
            services = conn.unwrap(PhoenixConnection.class).getQueryServices();
            putUnlockKVInSysMutex(mutexRowKey);
            assertTrue(((ConnectionQueryServicesImpl)services)
                    .acquireUpgradeMutex(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0, mutexRowKey));
            try {
                ((ConnectionQueryServicesImpl)services)
                        .acquireUpgradeMutex(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0, mutexRowKey);
                fail();
            } catch (UpgradeInProgressException expected) {

            }
            assertTrue(((ConnectionQueryServicesImpl)services).releaseUpgradeMutex(mutexRowKey));
            assertFalse(((ConnectionQueryServicesImpl)services).releaseUpgradeMutex(mutexRowKey));
        }
    }
    
    @Test
    public void testConcurrentUpgradeThrowsUprgadeInProgressException() throws Exception {
        final AtomicBoolean mutexStatus1 = new AtomicBoolean(false);
        final AtomicBoolean mutexStatus2 = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicInteger numExceptions = new AtomicInteger(0);
        ConnectionQueryServices services = null;
        final byte[] mutexKey = Bytes.toBytes(generateUniqueName());
        try (Connection conn = getConnection(false, null)) {
            services = conn.unwrap(PhoenixConnection.class).getQueryServices();
            putUnlockKVInSysMutex(mutexKey);
            FutureTask<Void> task1 = new FutureTask<>(new AcquireMutexRunnable(mutexStatus1, services, latch, numExceptions, mutexKey));
            FutureTask<Void> task2 = new FutureTask<>(new AcquireMutexRunnable(mutexStatus2, services, latch, numExceptions, mutexKey));
            Thread t1 = new Thread(task1);
            t1.setDaemon(true);
            Thread t2 = new Thread(task2);
            t2.setDaemon(true);
            t1.start();
            t2.start();
            latch.await();
            // make sure tasks didn't fail by calling get()
            task1.get();
            task2.get();
            assertTrue("One of the threads should have acquired the mutex", mutexStatus1.get() || mutexStatus2.get());
            assertNotEquals("One and only one thread should have acquired the mutex ", mutexStatus1.get(),
                    mutexStatus2.get());
            assertEquals("One and only one thread should have caught UpgradeRequiredException ", 1, numExceptions.get());
        }
    }
    
    private static class AcquireMutexRunnable implements Callable<Void> {
        
        private final AtomicBoolean acquireStatus;
        private final ConnectionQueryServices services;
        private final CountDownLatch latch;
        private final AtomicInteger numExceptions;
        private final byte[] mutexRowKey;
        public AcquireMutexRunnable(AtomicBoolean acquireStatus, ConnectionQueryServices services, CountDownLatch latch, AtomicInteger numExceptions, byte[] mutexKey) {
            this.acquireStatus = acquireStatus;
            this.services = services;
            this.latch = latch;
            this.numExceptions = numExceptions;
            this.mutexRowKey = mutexKey;
        }
        @Override
        public Void call() throws Exception {
            try {
                ((ConnectionQueryServicesImpl)services).acquireUpgradeMutex(
                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0, mutexRowKey);
                acquireStatus.set(true);
            } catch (UpgradeInProgressException e) {
                numExceptions.incrementAndGet();
            } finally {
                latch.countDown();
            }
            return null;
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
