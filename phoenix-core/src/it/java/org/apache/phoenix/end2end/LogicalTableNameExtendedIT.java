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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

@Category(NeedsOwnMiniClusterTest.class)
public class LogicalTableNameExtendedIT extends LogicalTableNameBaseIT {
    private Properties propsNamespace = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        initCluster(true);
    }

    public LogicalTableNameExtendedIT()  {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" ,IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN");
        this.dataTableDdl = optionBuilder.toString();
        propsNamespace.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
    }

    @Test
    public void testUpdatePhysicalTableName_namespaceMapped() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String view1Name = "VW1_" + generateUniqueName();
        String view1IndexName1 = "VW1IDX1_" + generateUniqueName();
        String view1IndexName2 = "VW1IDX2_" + generateUniqueName();
        String view2Name = "VW2_" + generateUniqueName();
        String view2IndexName1 = "VW2IDX1_" + generateUniqueName();

         try (Connection conn = getConnection(propsNamespace)) {
            try (Connection conn2 = getConnection(propsNamespace)) {
                conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
                testWithViewsAndIndex_BaseTableChange(conn, conn2, null, schemaName, tableName, view1Name,
                        view1IndexName1, view1IndexName2, view2Name, view2IndexName1, true, false);

                populateView(conn, (schemaName+"."+view2Name), 10, 1);
                ResultSet rs = conn2.createStatement().executeQuery("SELECT * FROM " + (schemaName + "." + view2IndexName1) + " WHERE \":PK1\"='PK10'");
                assertEquals(true, rs.next());

            }
        }
    }

    private void test_bothTableAndIndexHaveDifferentNames(Connection conn, Connection conn2, String schemaName, String tableName, String indexName) throws Exception {
        String fullTableHName = schemaName + ":" + tableName;
        String fullIndexHName = schemaName + ":" + indexName;

        conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        // Create tables and change physical index table
        test_IndexTableChange(conn, conn2, schemaName, tableName, indexName,
                QueryConstants.UNVERIFIED_BYTES, true);
        // Now change physical data table
        createAndPointToNewPhysicalTable(conn, fullTableHName, true);
        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getAdmin()) {
            assertEquals(false, admin.tableExists(TableName.valueOf(fullTableHName)));
            assertEquals(false, admin.tableExists(TableName.valueOf(fullIndexHName)));
        }
    }

    @Test
    public void testUpdatePhysicalTableName_bothTableAndIndexHaveDifferentNames() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullTableHName = schemaName + ":" + tableName;
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        String fullIndexHName = schemaName + ":" + indexName;
        String fullNewTableHName = schemaName + ":NEW_TBL_" + tableName;
        try (Connection conn = getConnection(propsNamespace)) {
            try (Connection conn2 = getConnection(propsNamespace)) {
                test_bothTableAndIndexHaveDifferentNames(conn, conn2, schemaName, tableName, indexName);
                try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices()
                        .getAdmin()) {
                    conn2.setAutoCommit(true);
                    // Add row and check
                    populateTable(conn2, fullTableName, 10, 1);
                    ResultSet
                            rs =
                            conn2.createStatement().executeQuery("SELECT * FROM " + fullIndexName + " WHERE \":PK1\"='PK10'");
                    assertEquals(true, rs.next());
                    rs = conn.createStatement().executeQuery("SELECT * FROM " + fullTableName + " WHERE PK1='PK10'");
                    assertEquals(true, rs.next());

                    // Drop row and check
                    conn.createStatement().execute("DELETE from " + fullTableName + " WHERE PK1='PK10'");
                    rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullIndexName + " WHERE \":PK1\"='PK10'");
                    assertEquals(false, rs.next());
                    rs = conn.createStatement().executeQuery("SELECT * FROM " + fullTableName + " WHERE PK1='PK10'");
                    assertEquals(false, rs.next());

                    // Add a row and run IndexTool to check that the row is there on the other side
                    rs = conn.createStatement().executeQuery("SELECT * FROM " +  fullIndexName + " WHERE \":PK1\"='PK30'");
                    assertEquals(false, rs.next());
                    try (Table htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullNewTableHName))) {
                        Put put = new Put(ByteUtil.concat(Bytes.toBytes("PK30")));
                        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
                        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"),
                                Bytes.toBytes("V30"));
                        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V2"),
                                PInteger.INSTANCE.toBytes(32));
                        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V3"),
                                PInteger.INSTANCE.toBytes(33));
                        htable.put(put);
                    }

                    IndexToolIT.runIndexTool(false, schemaName, tableName, indexName);
                    rs = conn.createStatement().executeQuery("SELECT * FROM " +  fullIndexName + " WHERE \":PK1\"='PK30'");
                    assertEquals(true, rs.next());

                    // Drop tables
                    conn2.createStatement().execute("DROP TABLE " + fullTableName);
                    // check that the physical data table is dropped
                    assertEquals(false, admin.tableExists(TableName.valueOf(fullNewTableHName)));

                    // check that index is dropped
                    assertEquals(false, admin.tableExists(TableName.valueOf((schemaName + ":NEW_IDXTBL_" + indexName))));
                }
            }
        }
    }

    @Test
    public void testUpdatePhysicalTableName_alterTable() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullTableHName = schemaName + ":" + tableName;
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        String fullIndexHName = schemaName + ":" + indexName;
        String fullNewTableHName = schemaName + ":NEW_TBL_" + tableName;
        try (Connection conn = getConnection(propsNamespace)) {
            try (Connection conn2 = getConnection(propsNamespace)) {
                test_bothTableAndIndexHaveDifferentNames(conn, conn2, schemaName, tableName, indexName);
                conn2.setAutoCommit(true);

                conn2.createStatement().execute("ALTER TABLE " + fullTableName + " ADD new_column_1 VARCHAR(64) CASCADE INDEX ALL");
                conn2.createStatement().execute("UPSERT INTO " + fullTableName + " (PK1, V1, new_column_1) VALUES ('a', 'v1', 'new_col_val')");
                ResultSet
                        rs =
                        conn2.createStatement().executeQuery("SELECT \"0:NEW_COLUMN_1\" FROM " + fullIndexName);
                assertEquals(true, rs.next());
                rs = conn.createStatement().executeQuery("SELECT NEW_COLUMN_1 FROM " + fullTableName + " WHERE NEW_COLUMN_1 IS NOT NULL");
                assertEquals(true, rs.next());
                assertEquals(false, rs.next());

                // Drop column, check is that there are no exceptions
                conn.createStatement().execute("ALTER TABLE " + fullTableName + " DROP COLUMN NEW_COLUMN_1");
            }
        }
    }

    @Test
    public void testHint() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexName2 = "IDX2_" + generateUniqueName();
        try (Connection conn = getConnection(propsNamespace)) {
            conn.setAutoCommit(true);
            createTable(conn, tableName);
            createIndexOnTable(conn, tableName, indexName);
            createIndexOnTable(conn, tableName, indexName2);
            populateTable(conn, tableName, 1, 2);

            // Test hint
            String tableSelect = "SELECT V1,V2,V3 FROM " + tableName;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + tableSelect);
            assertEquals(true, QueryUtil.getExplainPlan(rs).contains(indexName));
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices()
                    .getAdmin()) {
                String snapshotName = new StringBuilder(indexName2).append("-Snapshot").toString();
                admin.snapshot(snapshotName, TableName.valueOf(indexName2));
                String newName = "NEW_" + indexName2;
                admin.cloneSnapshot(snapshotName, TableName.valueOf(newName));
                renameAndDropPhysicalTable(conn, "NULL", null, indexName2, newName, true);
            }
            String indexSelect = "SELECT /*+ INDEX(" + tableName + " " + indexName2 + ")*/ V1,V2,V3 FROM " + tableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + indexSelect);
            assertEquals(true, QueryUtil.getExplainPlan(rs).contains(indexName2));
            rs = conn.createStatement().executeQuery(indexSelect);
            assertEquals(true, rs.next());
        }
    }

    @Test
    public void testUpdatePhysicalTableName_tenantViews() throws Exception {

        try (Connection conn = getConnection(propsNamespace)) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS TEST_ENTITY");
        }
        testGlobalViewAndTenantView(false, true);
        testGlobalViewAndTenantView(true, true);
    }

    @Test
    public void testUpdatePhysicalTableName_localIndex() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "LCL_IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        String fullHTableName = schemaName + ":" + tableName;

        try (Connection conn = getConnection(propsNamespace)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            createTable(conn, fullTableName);

            createIndexOnTable(conn, fullTableName, indexName, true);
            HashMap<String, ArrayList<String>> expected = populateTable(conn, fullTableName, 1, 2);
            createAndPointToNewPhysicalTable(conn, fullHTableName, true);

            String select = "SELECT * FROM " + fullIndexName;
            ResultSet rs = conn.createStatement().executeQuery( select);
            assertEquals(true, rs.next());
            validateIndex(conn, fullIndexName,false, expected);

            // Drop and recreate
            conn.createStatement().execute("DROP INDEX " + indexName + " ON " + fullTableName);
            createIndexOnTable(conn, fullTableName, indexName, true);
            rs = conn.createStatement().executeQuery(select);
            assertEquals(true, rs.next());
            validateIndex(conn, fullIndexName,false, expected);
        }
    }

    @Test
    public void testUpdatePhysicalTableName_viewIndexSequence() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String viewName = "VW1_" + generateUniqueName();
        String viewName2 = "VW2_" + generateUniqueName();
        String viewIndexName1 = "VWIDX1_" + generateUniqueName();
        String viewIndexName2 = "VWIDX2_" + generateUniqueName();
        String view2IndexName1 = "VW2IDX1_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        String fullViewName2 = SchemaUtil.getTableName(schemaName, viewName2);
        String fullViewIndex1Name = SchemaUtil.getTableName(schemaName, viewIndexName1);
        String fullViewIndex2Name = SchemaUtil.getTableName(schemaName, viewIndexName2);
        String fullView2Index1Name = SchemaUtil.getTableName(schemaName, view2IndexName1);
        String fullTableHName = schemaName + ":" + tableName;
        try (Connection conn = getConnection(propsNamespace)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            createTable(conn, fullTableName);
            createViewAndIndex(conn, schemaName, tableName, viewName, viewIndexName1);
            HashMap<String, ArrayList<String>> expected = populateView(conn, fullViewName, 1, 1);
            createAndPointToNewPhysicalTable(conn, fullTableHName, true);
            validateIndex(conn, fullViewIndex1Name,true, expected);
            String indexDDL = "CREATE INDEX IF NOT EXISTS " + viewIndexName2 + " ON " + fullViewName + " (VIEW_COL1) include (VIEW_COL2) ";
            conn.createStatement().execute(indexDDL);
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + fullViewIndex2Name);
            assertEquals(true, rs.next());
            assertEquals("VIEW_COL1_1", rs.getString(1));
            assertEquals("PK1", rs.getString(2));
            assertEquals("VIEW_COL2_1", rs.getString(3));
            assertEquals(false, rs.next());
            expected.putAll(populateView(conn, fullViewName, 10, 1));

            validateIndex(conn, fullViewIndex1Name, true, expected);
            rs = conn.createStatement().executeQuery("SELECT * FROM " + fullViewIndex2Name + " WHERE \"0:VIEW_COL1\"='VIEW_COL1_10'");
            assertEquals(true, rs.next());
            assertEquals("VIEW_COL1_10", rs.getString(1));
            assertEquals("PK10", rs.getString(2));
            assertEquals("VIEW_COL2_10", rs.getString(3));
            assertEquals(false, rs.next());

            conn.createStatement().execute("CREATE VIEW " + fullViewName2 + "  (VIEW_COL1 VARCHAR, VIEW_COL2 VARCHAR) AS SELECT * FROM "
            + fullTableName);
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + view2IndexName1 + " ON " + fullViewName2 + " (VIEW_COL1) include (VIEW_COL2)");
            populateView(conn, fullViewName2, 20, 1);
            rs = conn.createStatement().executeQuery("SELECT * FROM " + fullView2Index1Name + " WHERE \"0:VIEW_COL1\"='VIEW_COL1_10'");
            assertEquals(true, rs.next());
            assertEquals("VIEW_COL1_10", rs.getString(1));
            assertEquals("PK10", rs.getString(2));
            assertEquals("VIEW_COL2_10", rs.getString(3));
            assertEquals(false, rs.next());
        }
    }
}
