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
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.end2end.join.HashJoinGlobalIndexIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.query.QueryConstants;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.INVALID_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.VALID_ROW_COUNT;
import static org.apache.phoenix.util.MetaDataUtil.VIEW_INDEX_TABLE_PREFIX;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class LogicalTableNameIT extends LogicalTableNameBaseIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableNameIT.class);

    protected boolean createChildAfterRename;
    private boolean immutable;
    private Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
       initCluster(false);
    }

    public LogicalTableNameIT(boolean createChildAfterRename, boolean immutable)  {
        this.createChildAfterRename = createChildAfterRename;
        this.immutable = immutable;
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" ,IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN");
        if (immutable) {
            optionBuilder.append(" , IMMUTABLE_ROWS=true");
        }
        this.dataTableDdl = optionBuilder.toString();
    }

    @Parameterized.Parameters(
            name = "createChildAfterRename={0}, immutable={1}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(2);
        boolean[] Booleans = new boolean[] { false, true };
        for (boolean immutable : Booleans) {
            for (boolean createAfter : Booleans) {
                list.add(new Object[] { createAfter, immutable });
            }
        }

        return list;
    }

    @Test
    public void testUpdatePhysicalTableNameWithIndex() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);

        try (Connection conn = getConnection(props)) {
            try (Connection conn2 = getConnection(props)) {
                HashMap<String, ArrayList<String>> expected = testBaseTableWithIndex_BaseTableChange(conn, conn2, schemaName, tableName,
                        indexName, false, createChildAfterRename);

                // We have to rebuild index for this to work
                IndexToolIT.runIndexTool(false, schemaName, tableName, indexName);

                validateTable(conn, fullTableName);
                validateTable(conn2, fullTableName);
                validateIndex(conn, fullIndexName, false, expected);
                validateIndex(conn2, fullIndexName, false, expected);

                // Add row and check
                populateTable(conn, fullTableName, 10, 1);
                ResultSet rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullIndexName + " WHERE \":PK1\"='PK10'");
                assertEquals(true, rs.next());
                rs = conn.createStatement().executeQuery("SELECT * FROM " + fullTableName  + " WHERE PK1='PK10'");
                assertEquals(true, rs.next());

                // Drop row and check
                conn.createStatement().execute("DELETE from " + fullTableName + " WHERE PK1='PK10'");
                rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullIndexName + " WHERE \":PK1\"='PK10'");
                assertEquals(false, rs.next());
                rs = conn.createStatement().executeQuery("SELECT * FROM " + fullTableName  + " WHERE PK1='PK10'");
                assertEquals(false, rs.next());

                conn2.createStatement().execute("DROP TABLE " + fullTableName);
                // check that the physical data table is dropped
                try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                    assertEquals(false, admin.tableExists(TableName.valueOf(SchemaUtil.getTableName(schemaName, NEW_TABLE_PREFIX + tableName))));

                    // check that index is dropped
                    assertEquals(false, admin.tableExists(TableName.valueOf(fullIndexName)));
                }
            }
        }
    }

    @Test
    public void testUpdatePhysicalTableNameWithIndex_runScrutiny() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        try (Connection conn = getConnection(props)) {
            try (Connection conn2 = getConnection(props)) {
                testBaseTableWithIndex_BaseTableChange(conn, conn2, schemaName, tableName, indexName, false, createChildAfterRename);

                List<Job>
                        completedJobs =
                        IndexScrutinyToolBaseIT.runScrutinyTool(schemaName, tableName, indexName, 1L,
                                IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE);

                Job job = completedJobs.get(0);
                assertTrue(job.isSuccessful());

                Counters counters = job.getCounters();
                if (createChildAfterRename) {
                    assertEquals(3, counters.findCounter(VALID_ROW_COUNT).getValue());
                    assertEquals(0, counters.findCounter(INVALID_ROW_COUNT).getValue());
                } else {
                    // Since we didn't build the index, we expect 1 missing index row
                    assertEquals(2, counters.findCounter(VALID_ROW_COUNT).getValue());
                    assertEquals(1, counters.findCounter(INVALID_ROW_COUNT).getValue());
                }
            }
        }
    }

    @Test
    public void testUpdatePhysicalIndexTableName() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = getConnection(props)) {
            try (Connection conn2 = getConnection(props)) {
                HashMap<String, ArrayList<String>> expected = test_IndexTableChange(conn, conn2, schemaName, tableName, indexName, QueryConstants.VERIFIED_BYTES, false);

                validateIndex(conn, fullIndexName, false, expected);
                validateIndex(conn2, fullIndexName, false, expected);

                // create another index and drop the first index and validate the second one
                String indexName2 = "IDX2_" + generateUniqueName();
                String fullIndexName2 = SchemaUtil.getTableName(schemaName, indexName2);
                if (createChildAfterRename) {
                    createIndexOnTable(conn2, fullTableName, indexName2);
                }
                dropIndex(conn2, fullTableName, indexName);
                if (!createChildAfterRename) {
                    createIndexOnTable(conn2, fullTableName, indexName2);
                }
                // The new index doesn't have the new row
                expected.remove("PK3");
                validateIndex(conn, fullIndexName2, false, expected);
                validateIndex(conn2, fullIndexName2, false, expected);
            }
        }
    }

    @Test
    public void testUpdatePhysicalIndexTableName_runScrutiny() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = getConnection(props)) {
            try (Connection conn2 = getConnection(props)) {
                test_IndexTableChange(conn, conn2, schemaName, tableName, indexName, QueryConstants.VERIFIED_BYTES, false);
                List<Job>
                        completedJobs =
                        IndexScrutinyToolBaseIT.runScrutinyTool(schemaName, tableName, indexName, 1L,
                                IndexScrutinyTool.SourceTable.INDEX_TABLE_SOURCE);

                Job job = completedJobs.get(0);
                assertTrue(job.isSuccessful());

                Counters counters = job.getCounters();

                // Since we didn't build the index, we expect 1 missing index row
                assertEquals(2, counters.findCounter(VALID_ROW_COUNT).getValue());
                assertEquals(1, counters.findCounter(INVALID_ROW_COUNT).getValue());

                // Try with unverified bytes
                String tableName2 = "TBL_" + generateUniqueName();
                String indexName2 = "IDX_" + generateUniqueName();
                test_IndexTableChange(conn, conn2, schemaName, tableName2, indexName2, QueryConstants.UNVERIFIED_BYTES, false);

                completedJobs =
                        IndexScrutinyToolBaseIT.runScrutinyTool(schemaName, tableName2, indexName2, 1L,
                                IndexScrutinyTool.SourceTable.INDEX_TABLE_SOURCE);

                job = completedJobs.get(0);
                assertTrue(job.isSuccessful());

                counters = job.getCounters();

                // Since we didn't build the index, we expect 1 missing index row
                assertEquals(2, counters.findCounter(VALID_ROW_COUNT).getValue());
                assertEquals(0, counters.findCounter(INVALID_ROW_COUNT).getValue());

            }
        }
    }

    @Test
    public void testUpdatePhysicalTableNameWithViews() throws Exception {
        try (Connection conn = getConnection(props)) {
            try (Connection conn2 = getConnection(props)) {
                String schemaName = "S_" + generateUniqueName();
                String tableName = "TBL_" + generateUniqueName();
                String view1Name = "VW1_" + generateUniqueName();
                String view1IndexName1 = "VW1IDX1_" + generateUniqueName();
                String view1IndexName2 = "VW1IDX2_" + generateUniqueName();
                String fullView1IndexName1 = SchemaUtil.getTableName(schemaName, view1IndexName1);
                String fullView1IndexName2 =  SchemaUtil.getTableName(schemaName, view1IndexName2);
                String view2Name = "VW2_" + generateUniqueName();
                String view2IndexName1 = "VW2IDX1_" + generateUniqueName();
                String fullView1Name = SchemaUtil.getTableName(schemaName, view1Name);
                String fullView2Name = SchemaUtil.getTableName(schemaName, view2Name);
                String fullView2IndexName1 =  SchemaUtil.getTableName(schemaName, view2IndexName1);

                HashMap<String, ArrayList<String>> expected = testWithViewsAndIndex_BaseTableChange(conn, conn2, null,
                        schemaName, tableName, view1Name, view1IndexName1, view1IndexName2, view2Name, view2IndexName1, false, createChildAfterRename);

                // We have to rebuild index for this to work
                IndexToolIT.runIndexTool(false, schemaName, view1Name, view1IndexName1);
                IndexToolIT.runIndexTool(false, schemaName, view1Name, view1IndexName2);
                IndexToolIT.runIndexTool(false, schemaName, view2Name, view2IndexName1);

                validateIndex(conn, fullView1IndexName1, true, expected);
                validateIndex(conn2, fullView1IndexName2, true, expected);

                // Add row and check
                populateView(conn, fullView2Name, 20, 1);
                ResultSet rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullView2IndexName1 + " WHERE \":PK1\"='PK20'");
                assertEquals(true, rs.next());
                rs = conn.createStatement().executeQuery("SELECT * FROM " + fullView2Name  + " WHERE PK1='PK20'");
                assertEquals(true, rs.next());

                // Drop row and check
                conn.createStatement().execute("DELETE from " + fullView2Name + " WHERE PK1='PK20'");
                rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullView2IndexName1 + " WHERE \":PK1\"='PK20'");
                assertEquals(false, rs.next());
                rs = conn.createStatement().executeQuery("SELECT * FROM " + fullView2Name  + " WHERE PK1='PK20'");
                assertEquals(false, rs.next());

                conn2.createStatement().execute("DROP VIEW " + fullView2Name);
                // check that this view is dropped but the other is there
                rs = conn.createStatement().executeQuery("SELECT * FROM " + fullView1Name);
                assertEquals(true, rs.next());
                boolean failed = true;
                try (ResultSet rsFail = conn.createStatement().executeQuery("SELECT * FROM " + fullView2Name)) {
                    rsFail.next();
                    failed = false;
                } catch (SQLException e){

                }
                assertEquals(true, failed);

                // check that first index is there but second index is dropped
                rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullView1IndexName1);
                assertEquals(true, rs.next());
                try {
                    rs = conn.createStatement().executeQuery("SELECT * FROM " + fullView2IndexName1);
                    rs.next();
                    failed = false;
                } catch (SQLException e){

                }
                assertEquals(true, failed);
            }
        }
    }

    @Test
    public void testUpdatePhysicalTableNameWithViews_runScrutiny() throws Exception {
        try (Connection conn = getConnection(props)) {
            try (Connection conn2 = getConnection(props)) {
                String schemaName = "S_" + generateUniqueName();
                String tableName = "TBL_" + generateUniqueName();
                String view1Name = "VW1_" + generateUniqueName();
                String view1IndexName1 = "VW1IDX1_" + generateUniqueName();
                String view1IndexName2 = "VW1IDX2_" + generateUniqueName();
                String view2Name = "VW2_" + generateUniqueName();
                String view2IndexName1 = "VW2IDX1_" + generateUniqueName();

                testWithViewsAndIndex_BaseTableChange(conn, conn2, null,schemaName, tableName, view1Name,
                        view1IndexName1, view1IndexName2, view2Name, view2IndexName1, false, createChildAfterRename);

                List<Job>
                        completedJobs =
                        IndexScrutinyToolBaseIT.runScrutinyTool(schemaName, view2Name, view2IndexName1, 1L,
                                IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE);

                Job job = completedJobs.get(0);
                assertTrue(job.isSuccessful());

                Counters counters = job.getCounters();
                if (createChildAfterRename) {
                    assertEquals(3, counters.findCounter(VALID_ROW_COUNT).getValue());
                    assertEquals(2, counters.findCounter(INVALID_ROW_COUNT).getValue());
                } else {
                    // Since we didn't build the index, we expect 1 missing index row and 2 are from the other index
                    assertEquals(2, counters.findCounter(VALID_ROW_COUNT).getValue());
                    assertEquals(3, counters.findCounter(INVALID_ROW_COUNT).getValue());
                }

            }
        }
    }

    @Test
    public void testWith2LevelViewsBaseTablePhysicalNameChange() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String view1Name = "VW1_" + generateUniqueName();
        String level2ViewName = "VW1_CH1_" + generateUniqueName();
        String fullLevel2ViewName = SchemaUtil.getTableName(schemaName, level2ViewName);
        String view1IndexName1 = "VW1IDX1_" + generateUniqueName();
        String level2ViewIndexName = "VW1_CH1IDX_" + generateUniqueName();
        String fullView1Name = SchemaUtil.getTableName(schemaName, view1Name);
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try (Connection conn = getConnection(props)) {
            try (Connection conn2 = getConnection(props)) {
                conn.setAutoCommit(true);
                conn2.setAutoCommit(true);
                HashMap<String, ArrayList<String>> expected = new HashMap<>();
                createTable(conn, fullTableName);
                createViewAndIndex(conn2, schemaName, tableName, view1Name, view1IndexName1);
                createViewAndIndex(conn2, schemaName, tableName, view1Name, view1IndexName1);
                expected.putAll(populateView(conn, fullView1Name, 1, 2));

                String ddl = "CREATE VIEW " + fullLevel2ViewName + "(chv2 VARCHAR) AS SELECT * FROM " + fullView1Name;
                String
                        indexDdl =
                        "CREATE INDEX " + level2ViewIndexName + " ON " + fullLevel2ViewName + " (chv2) INCLUDE (v1, VIEW_COL1)";
                if (!createChildAfterRename) {
                    conn.createStatement().execute(ddl);
                    conn.createStatement().execute(indexDdl);
                }

                String newTableName = NEW_TABLE_PREFIX + generateUniqueName();
                String fullTableHbaseName = SchemaUtil.getTableName(schemaName, tableName);
                createAndPointToNewPhysicalTable(conn, fullTableHbaseName, newTableName, false);

                conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
                if (createChildAfterRename) {
                    conn.createStatement().execute(ddl);
                    conn.createStatement().execute(indexDdl);
                }

                // Add new row to child view
                String upsert = "UPSERT INTO " + fullLevel2ViewName + " (PK1, V1, VIEW_COL1, CHV2) VALUES (?,?,?,?)";
                PreparedStatement upsertStmt = conn.prepareStatement(upsert);
                ArrayList<String> row = new ArrayList<>();
                upsertStmt.setString(1, "PK10");
                upsertStmt.setString(2, "V10");
                upsertStmt.setString(3, "VIEW_COL1_10");
                upsertStmt.setString(4, "CHV210");
                upsertStmt.executeUpdate();

                String selectFromL2View = "SELECT /*+ NO_INDEX */ * FROM " + fullLevel2ViewName + " WHERE chv2='CHV210'";
                ResultSet
                        rs =
                        conn2.createStatement().executeQuery(selectFromL2View);
                assertEquals(true, rs.next());
                assertEquals(false, rs.next());

                String indexSelect = "SELECT chv2, V1, VIEW_COL1 FROM " + fullLevel2ViewName + " WHERE chv2='CHV210'";
                rs =
                        conn2.createStatement().executeQuery("EXPLAIN " + indexSelect);
                assertEquals(true, QueryUtil.getExplainPlan(rs).contains(VIEW_INDEX_TABLE_PREFIX));
                rs = conn2.createStatement().executeQuery(indexSelect);
                assertEquals(true, rs.next());
                assertEquals(false, rs.next());

                // Drop row and check
                conn2.createStatement().execute("DELETE FROM " + fullLevel2ViewName + " WHERE chv2='CHV210'");
                rs = conn2.createStatement().executeQuery(indexSelect);
                assertEquals(false, rs.next());
            }
        }
    }

    @Test @Ignore("Requires PHOENIX-6722")
    public void testChangeDetectionAfterTableNameChange() throws Exception {
        try(PhoenixConnection conn = (PhoenixConnection) getConnection(props)) {
            String schemaName = "S_" + generateUniqueName();
            String tableName = "T_" + generateUniqueName();
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            createTable(conn, fullTableName);
            String alterDdl = "ALTER TABLE " + fullTableName + " SET CHANGE_DETECTION_ENABLED=true";
            conn.createStatement().execute(alterDdl);

            PTable table = conn.getTableNoCache(fullTableName);
            assertTrue(table.isChangeDetectionEnabled());
            assertNotNull(table.getExternalSchemaId());
            AlterTableIT.verifySchemaExport(table, getUtility().getConfiguration());

            String newTableName = "T_" + generateUniqueName();
            String fullNewTableName = SchemaUtil.getTableName(schemaName, newTableName);
            LogicalTableNameIT.createAndPointToNewPhysicalTable(conn, fullTableName, fullNewTableName, false);

            //logical table name should still be the same for PTable lookup
            PTable newTable = conn.getTableNoCache(fullTableName);
            //but the schema in the registry should match the new PTable
            AlterTableIT.verifySchemaExport(newTable, getUtility().getConfiguration());
        }
    }

    @Test
    public void testHashJoin() throws Exception {
        if (immutable || createChildAfterRename) {
            return;
        }
        Object[] arr = HashJoinGlobalIndexIT.data().toArray();
        String[] indexDDL = ((String[][])arr[0])[0];
        String[] plans = ((String[][])arr[0])[1];
        HashJoinGlobalIndexIT hjgit = new HashJoinGlobalIndexIT(indexDDL, plans);
        hjgit.createSchema();
        hjgit.testInnerJoin(false);
    }
}
