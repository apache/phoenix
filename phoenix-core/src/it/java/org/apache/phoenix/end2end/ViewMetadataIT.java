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

import static org.apache.phoenix.thirdparty.com.google.common.collect.Lists
        .newArrayListWithExpectedSize;
import static org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost
        .PHOENIX_META_DATA_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.coprocessor.TaskRegionObserver.TASK_DETAILS;
import static org.apache.phoenix.exception.SQLExceptionCode
        .CANNOT_MODIFY_VIEW_PK;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MUTATE_TABLE;
import static org.apache.phoenix.exception.SQLExceptionCode
        .NOT_NULLABLE_COLUMN_IN_ROW_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData
        .SYSTEM_LINK_HBASE_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData
        .SYSTEM_TASK_HBASE_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions
        .DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS;
import static org.apache.phoenix.schema.PTable.TaskType.DROP_CHILD_VIEWS;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.TableInfo;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ViewUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.base.Predicate;
import org.apache.phoenix.thirdparty.com.google.common.collect.Collections2;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.junit.experimental.categories.Category;

/**
 * Test suite related to view metadata
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ViewMetadataIT extends SplitSystemCatalogIT {

    private static RegionCoprocessorEnvironment TaskRegionEnvironment;
    final static String BASE_TABLE_SCHEMA = "S";
    final static String CHILD_VIEW_LEVEL_1_SCHEMA = "S1";
    private final static String CHILD_VIEW_LEVEL_2_SCHEMA = "S2";
    private final static String CHILD_VIEW_LEVEL_3_SCHEMA = "S3";
    final static String CREATE_BASE_TABLE_DDL =
            "CREATE TABLE %s.%s (A INTEGER NOT NULL PRIMARY KEY, "
                    + "B INTEGER, C INTEGER)";
    final static String CREATE_CHILD_VIEW_LEVEL_1_DDL =
            "CREATE VIEW %s.%s (NEW_COL1 INTEGER) AS SELECT * FROM %s.%s "
                    + "WHERE B > 10";
    final static String CREATE_CHILD_VIEW_LEVEL_2_DDL =
            "CREATE VIEW %s.%s (NEW_COL2 INTEGER) AS SELECT * FROM %s.%s "
                    + "WHERE NEW_COL1=5";
    final static String CREATE_CHILD_VIEW_LEVEL_3_DDL =
            "CREATE VIEW %s.%s (NEW_COL3 INTEGER) AS SELECT * FROM %s.%s "
                    + "WHERE NEW_COL2=10";

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        NUM_SLAVES_BASE = 6;
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        boolean splitSystemCatalog = (driver == null);
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.PHOENIX_ACLS_ENABLED, "true");
        serverProps.put(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY,
                ViewConcurrencyAndFailureIT.TestMetaDataRegionObserver.class
                        .getName());
        serverProps.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        serverProps.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        // Set this in server properties too since we get a connection on the
        // server and pass in server-side properties when running the drop
        // child views tasks
        serverProps.put(DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
        // Split SYSTEM.CATALOG once after the mini-cluster is started
        if (splitSystemCatalog) {
            // splitSystemCatalog is incompatible with the balancer chore
            getUtility().getHBaseCluster().getMaster().balanceSwitch(false);
            splitSystemCatalog();
        }

        TaskRegionEnvironment = (RegionCoprocessorEnvironment)getUtility()
                .getRSForFirstRegionInTable(SYSTEM_TASK_HBASE_TABLE_NAME)
                .getRegions(SYSTEM_TASK_HBASE_TABLE_NAME)
                .get(0).getCoprocessorHost()
                .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    @Test
    public void testCreateViewWithUpdateCacheFrquency() throws Exception {
      Properties props = new Properties();
      Connection conn1 = DriverManager.getConnection(getUrl(), props);
      conn1.setAutoCommit(true);
      String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
      String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
      conn1.createStatement().execute(
        "CREATE TABLE "+tableName+" (k VARCHAR PRIMARY KEY, "
                + "v1 VARCHAR, v2 VARCHAR) UPDATE_CACHE_FREQUENCY=1000000");
      conn1.createStatement().execute("upsert into "+tableName
              +" values ('row1', 'value1', 'key1')");
      conn1.createStatement().execute(
        "CREATE VIEW "+viewName+" (v43 VARCHAR) AS SELECT * FROM "+tableName
                +" WHERE v1 = 'value1'");

      ResultSet rs = conn1.createStatement()
          .executeQuery("SELECT * FROM "+tableName+" WHERE v1 = 'value1'");
      assertTrue(rs.next());
    }

    @Test
    public void testCreateViewFromHBaseTable() throws Exception {
        String tableNameStr = generateUniqueName();
        String familyNameStr = generateUniqueName();

        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(
                TableName.valueOf(tableNameStr));
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(
                familyNameStr));

        HBaseTestingUtility testUtil = getUtility();
        Admin admin = testUtil.getAdmin();
        admin.createTable(builder.build());
        Connection conn = DriverManager.getConnection(getUrl());

        //PK is not specified, without where clause
        try {
            conn.createStatement().executeUpdate("CREATE VIEW \""
                    + tableNameStr +
                    "\" (ROWKEY VARCHAR, \"" + familyNameStr + "\".a VARCHAR)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.PRIMARY_KEY_MISSING.getErrorCode(),
                    e.getErrorCode());
        }

        // No error, as PK is specified
        conn.createStatement().executeUpdate("CREATE VIEW \"" + tableNameStr +
                "\" (ROWKEY VARCHAR PRIMARY KEY, \"" + familyNameStr
                + "\".a VARCHAR)");

        conn.createStatement().executeUpdate("DROP VIEW \"" + tableNameStr
                + "\"");

        //PK is not specified, with where clause
        try {
            conn.createStatement().executeUpdate("CREATE VIEW \""
                    + tableNameStr +
                    "\" (ROWKEY VARCHAR, \"" + familyNameStr + "\".a VARCHAR)"
                    + " AS SELECT * FROM \""
                    + tableNameStr + "\" WHERE ROWKEY = '1'");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.PRIMARY_KEY_MISSING.getErrorCode(),
                    e.getErrorCode());
        }

        conn.createStatement().executeUpdate("CREATE VIEW \"" + tableNameStr +
                "\" (ROWKEY VARCHAR PRIMARY KEY, \"" + familyNameStr
                + "\".a VARCHAR) AS SELECT " +
                "* FROM \"" + tableNameStr + "\" WHERE ROWKEY = '1'");

        conn.createStatement().executeUpdate("DROP VIEW \"" + tableNameStr
                + "\"");
    }

    @Test
    public void testCreateViewMappedToExistingHbaseTableWithNSMappingEnabled()
            throws Exception {
        final String NS = "NS_" + generateUniqueName();
        final String TBL = "TBL_" + generateUniqueName();
        final String CF = "CF";

        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
                Boolean.TRUE.toString());

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Admin admin = conn.unwrap(PhoenixConnection.class)
                        .getQueryServices().getAdmin()) {

            conn.createStatement().execute("CREATE SCHEMA " + NS);

            // test for a view that is in non-default schema
            {
                TableName tableName = TableName.valueOf(NS, TBL);
                TableDescriptorBuilder builder = TableDescriptorBuilder
                        .newBuilder(tableName);
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF));
                admin.createTable(builder.build());

                String view1 = NS + "." + TBL;
                conn.createStatement().execute(
                        "CREATE VIEW " + view1 + " (PK VARCHAR PRIMARY KEY, "
                                + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil.getExplainPlan(
                        conn.createStatement().executeQuery(
                                "explain select * from " + view1))
                        .contains(NS + ":" + TBL));

                conn.createStatement().execute("DROP VIEW " + view1);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // test for a view whose name contains a dot (e.g. "AAA.BBB") in
            // default schema (for backward compatibility)
            {
                TableName tableName = TableName.valueOf(NS + "." + TBL);
                TableDescriptorBuilder builder = TableDescriptorBuilder
                        .newBuilder(tableName);
                builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(CF));
                admin.createTable(builder.build());

                String view2 = "\"" + NS + "." + TBL + "\"";
                conn.createStatement().execute(
                        "CREATE VIEW " + view2 + " (PK VARCHAR PRIMARY KEY, "
                                + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil.getExplainPlan(
                        conn.createStatement().executeQuery(
                                "explain select * from " + view2))
                        .contains(NS + "." + TBL));

                conn.createStatement().execute("DROP VIEW " + view2);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // test for a view whose name contains a dot (e.g. "AAA.BBB") in
            // non-default schema
            {
                TableName tableName = TableName.valueOf(NS, NS + "." + TBL);
                TableDescriptorBuilder builder = TableDescriptorBuilder
                        .newBuilder(tableName);
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF));
                admin.createTable(builder.build());

                String view3 = NS + ".\"" + NS + "." + TBL + "\"";
                conn.createStatement().execute(
                        "CREATE VIEW " + view3 + " (PK VARCHAR PRIMARY KEY, "
                                + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil.getExplainPlan(
                        conn.createStatement().executeQuery(
                                "explain select * from " + view3))
                        .contains(NS + ":" + NS + "." + TBL));

                conn.createStatement().execute("DROP VIEW " + view3);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            conn.createStatement().execute("DROP SCHEMA " + NS);
        }
    }

    @Test
    public void testRecreateDroppedTableWithChildViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                generateUniqueName());

        String tableDdl = "CREATE TABLE " + fullTableName
                + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName1
                + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName
                + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        String indexName = generateUniqueName();
        ddl = "CREATE INDEX " + indexName + " on " + fullViewName1 + "(v2)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + "(v2 VARCHAR) AS SELECT * FROM "
                + fullTableName + " WHERE k > 10";
        conn.createStatement().execute(ddl);

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName
                + " CASCADE");
        runDropChildViewsTask();

        validateViewDoesNotExist(conn, fullViewName1);
        validateViewDoesNotExist(conn, fullViewName2);

        // recreate the table that was dropped
        conn.createStatement().execute(tableDdl);
        // the two child views should still not exist
        try {
            PhoenixRuntime.getTableNoCache(conn, fullViewName1);
            fail();
        } catch (SQLException ignored) {
        }
        try {
            PhoenixRuntime.getTableNoCache(conn, fullViewName2);
            fail();
        } catch (SQLException ignored) {
        }
    }

    @Test
    public void testAlterTableIsResilientToOrphanLinks() throws SQLException {
        final String parent1TableName = generateUniqueName();
        final String parent2TableName = generateUniqueName();
        final String viewName = "V_" + generateUniqueName();
        // Note that this column name is the same as the new column on the
        // child view
        final String alterTableDDL = "ALTER TABLE %s ADD NEW_COL1 VARCHAR";
        createOrphanLink(SCHEMA1, parent1TableName, parent2TableName, SCHEMA2,
                viewName);

        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            // Should not fail since this table is unrelated to the view
            // in spite of the orphan parent->child link
            stmt.execute(String.format(alterTableDDL,
                    SchemaUtil.getTableName(SCHEMA1, parent2TableName)));
            try {
                stmt.execute(String.format(alterTableDDL,
                        SchemaUtil.getTableName(SCHEMA1, parent1TableName)));
                fail("Adding column should be disallowed since there is a "
                        + "conflicting column type on the child view");
            } catch (SQLException sqlEx) {
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(),
                        sqlEx.getErrorCode());
            }
        }
    }

    @Test
    public void testDropTableIsResilientToOrphanLinks() throws SQLException {
        final String parent1TableName = generateUniqueName();
        final String parent2TableName = generateUniqueName();
        final String viewName = "V_" + generateUniqueName();
        final String dropTableNoCascadeDDL = "DROP TABLE %s ";
        createOrphanLink(SCHEMA1, parent1TableName, parent2TableName, SCHEMA2,
                viewName);

        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            // Should not fail since this table is unrelated to the view
            // in spite of the orphan parent->child link
            stmt.execute(String.format(dropTableNoCascadeDDL,
                    SchemaUtil.getTableName(SCHEMA1, parent2TableName)));
            try {
                stmt.execute(String.format(dropTableNoCascadeDDL,
                        SchemaUtil.getTableName(SCHEMA1, parent1TableName)));
                fail("Drop table without cascade should fail since there is a"
                        + " child view");
            } catch (SQLException sqlEx) {
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(),
                        sqlEx.getErrorCode());
            }
        }
    }

    /**
     * Create a view hierarchy:
     *
     *              _____ parent1 ____
     *             /         |        \
     *    level1view1   level1view3  level1view4
     *         |
     *  t001.level2view1
     *
     *                      And
     *
     *              _____ parent2 ____
     *             /         |        \
     *    level1view2   level1view5  level1view6
     *         |
     *  t001.level2view2
     *         |
     *  t001.level3view1
     *
     * We induce orphan links by recreating the same view names on top of
     * different parents
     */
    @Test
    public void testViewHierarchyWithOrphanLinks() throws Exception {
        final List<TableInfo> expectedLegitChildViewsListForParent1 =
                new ArrayList<>();
        final List<TableInfo> expectedLegitChildViewsListForParent2 =
                new ArrayList<>();
        final String tenantId = "t001";
        final String parent1TableName = "P1_" + generateUniqueName();
        final String parent2TableName = "P2_" + generateUniqueName();

        final String level1ViewName1 = "L1_V_1_" + generateUniqueName();
        final String level1ViewName2 = "L1_V_2_" + generateUniqueName();
        final String level1ViewName3 = "L1_V_3_" + generateUniqueName();
        final String level1ViewName4 = "L1_V_4_" + generateUniqueName();
        final String level1ViewName5 = "L1_V_5_" + generateUniqueName();
        final String level1ViewName6 = "L1_V_6_" + generateUniqueName();

        final String level2ViewName1 = "L2_V_1_" + generateUniqueName();
        final String level2ViewName2 = "L2_V_2_" + generateUniqueName();

        final String level3ViewName1 = "L3_V_1_" + generateUniqueName();
        createOrphanLink(BASE_TABLE_SCHEMA, parent1TableName, parent2TableName,
                CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName1);

        // Create other legit views on top of parent1 and parent2
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(String.format(
                    CREATE_CHILD_VIEW_LEVEL_1_DDL,
                    CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName3,
                    BASE_TABLE_SCHEMA, parent1TableName));
            conn.createStatement().execute(String.format(
                    CREATE_CHILD_VIEW_LEVEL_1_DDL,
                    CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName4,
                    BASE_TABLE_SCHEMA, parent1TableName));

            conn.createStatement().execute(String.format(
                    CREATE_CHILD_VIEW_LEVEL_1_DDL,
                    CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName2,
                    BASE_TABLE_SCHEMA, parent2TableName));
            conn.createStatement().execute(String.format(
                    CREATE_CHILD_VIEW_LEVEL_1_DDL,
                    CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName5,
                    BASE_TABLE_SCHEMA, parent2TableName));
            conn.createStatement().execute(String.format(
                    CREATE_CHILD_VIEW_LEVEL_1_DDL,
                    CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName6,
                    BASE_TABLE_SCHEMA, parent2TableName));
        }
        Properties props = new Properties();
        props.put(TENANT_ID_ATTRIB, tenantId);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(String.format(
                    CREATE_CHILD_VIEW_LEVEL_2_DDL,
                    CHILD_VIEW_LEVEL_2_SCHEMA, level2ViewName1,
                    CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName1));
            conn.createStatement().execute(String.format(
                    CREATE_CHILD_VIEW_LEVEL_2_DDL,
                    CHILD_VIEW_LEVEL_2_SCHEMA, level2ViewName2,
                    CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName2));

            // Try to recreate the same view on a different global view to
            // create an orphan link
            try {
                conn.createStatement().execute(String.format(
                        CREATE_CHILD_VIEW_LEVEL_2_DDL,
                        CHILD_VIEW_LEVEL_2_SCHEMA, level2ViewName2,
                        CHILD_VIEW_LEVEL_1_SCHEMA, level1ViewName1));
                fail("Creating the same view again should have failed");
            } catch (TableAlreadyExistsException ignored) {
                // expected
            }
            // Create a third level view
            conn.createStatement().execute(String.format(
                    CREATE_CHILD_VIEW_LEVEL_3_DDL,
                    CHILD_VIEW_LEVEL_3_SCHEMA, level3ViewName1,
                    CHILD_VIEW_LEVEL_2_SCHEMA, level2ViewName2));
        }
        // Populate all expected legitimate views in depth-first order
        expectedLegitChildViewsListForParent1.add(new TableInfo(null,
                CHILD_VIEW_LEVEL_1_SCHEMA.getBytes(),
                level1ViewName1.getBytes()));
        expectedLegitChildViewsListForParent1.add(
                new TableInfo(tenantId.getBytes(),
                CHILD_VIEW_LEVEL_2_SCHEMA.getBytes(),
                level2ViewName1.getBytes()));
        expectedLegitChildViewsListForParent1.add(new TableInfo(null,
                CHILD_VIEW_LEVEL_1_SCHEMA.getBytes(),
                level1ViewName3.getBytes()));
        expectedLegitChildViewsListForParent1.add(new TableInfo(null,
                CHILD_VIEW_LEVEL_1_SCHEMA.getBytes(),
                level1ViewName4.getBytes()));

        expectedLegitChildViewsListForParent2.add(new TableInfo(null,
                CHILD_VIEW_LEVEL_1_SCHEMA.getBytes(),
                level1ViewName2.getBytes()));
        expectedLegitChildViewsListForParent2.add(
                new TableInfo(tenantId.getBytes(),
                CHILD_VIEW_LEVEL_2_SCHEMA.getBytes(),
                        level2ViewName2.getBytes()));
        expectedLegitChildViewsListForParent2.add(
                new TableInfo(tenantId.getBytes(),
                CHILD_VIEW_LEVEL_3_SCHEMA.getBytes(),
                        level3ViewName1.getBytes()));
        expectedLegitChildViewsListForParent2.add(new TableInfo(null,
                CHILD_VIEW_LEVEL_1_SCHEMA.getBytes(),
                level1ViewName5.getBytes()));
        expectedLegitChildViewsListForParent2.add(new TableInfo(null,
                CHILD_VIEW_LEVEL_1_SCHEMA.getBytes(),
                level1ViewName6.getBytes()));

        /*
            After this setup, SYSTEM.CHILD_LINK parent->child linking rows
            will look like this:
            parent1->level1view1
            parent1->level1view3
            parent1->level1view4

            parent2->level1view1 (orphan)
            parent2->level1view2
            parent2->level1view5
            parent2->level1view6

            level1view1->t001.level2view1
            level1view1->t001.level2view2 (orphan)
            level1view2->t001.level2view2

            t001.level2view2->t001.level3view1
         */

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class)
                    .getQueryServices();
            try (Table childLinkTable = cqs.getTable(SchemaUtil.getPhysicalName(
                    SYSTEM_LINK_HBASE_TABLE_NAME.toBytes(),
                    cqs.getProps()).getName())) {
                Pair<List<PTable>, List<TableInfo>> allDescendants =
                        ViewUtil.findAllDescendantViews(childLinkTable,
                                cqs.getConfiguration(),
                                EMPTY_BYTE_ARRAY, BASE_TABLE_SCHEMA.getBytes(),
                                parent1TableName.getBytes(),
                                HConstants.LATEST_TIMESTAMP, false);
                List<PTable> legitChildViews = allDescendants.getFirst();
                List<TableInfo> orphanViews = allDescendants.getSecond();
                // All of the orphan links are legit views of the other parent
                // so they don't count as orphan views for this parent
                assertTrue(orphanViews.isEmpty());
                assertLegitChildViews(expectedLegitChildViewsListForParent1,
                        legitChildViews);

                allDescendants = ViewUtil.findAllDescendantViews(childLinkTable,
                        cqs.getConfiguration(), EMPTY_BYTE_ARRAY,
                        BASE_TABLE_SCHEMA.getBytes(),
                        parent2TableName.getBytes(),
                        HConstants.LATEST_TIMESTAMP, false);
                legitChildViews = allDescendants.getFirst();
                orphanViews = allDescendants.getSecond();
                // All of the orphan links are legit views of the other parent
                // so they don't count as orphan views for this parent
                assertTrue(orphanViews.isEmpty());
                assertLegitChildViews(expectedLegitChildViewsListForParent2,
                        legitChildViews);

                // Drop one of the legitimate level 1 views that was on top of
                // parent1
                conn.createStatement().execute(String.format(
                        "DROP VIEW %s.%s CASCADE", CHILD_VIEW_LEVEL_1_SCHEMA,
                        level1ViewName1));
                // The view hierarchy rooted at this view is 2 levels deep so
                // we must run the DropChildViewsTask twice to clear out views
                // level by level
                runDropChildViewsTask();
                runDropChildViewsTask();

                expectedLegitChildViewsListForParent1.clear();
                expectedLegitChildViewsListForParent1.add(new TableInfo(
                        null, CHILD_VIEW_LEVEL_1_SCHEMA.getBytes(),
                        level1ViewName3.getBytes()));
                expectedLegitChildViewsListForParent1.add(new TableInfo(
                        null, CHILD_VIEW_LEVEL_1_SCHEMA.getBytes(),
                        level1ViewName4.getBytes()));

                allDescendants = ViewUtil.findAllDescendantViews(childLinkTable,
                        cqs.getConfiguration(), EMPTY_BYTE_ARRAY,
                        BASE_TABLE_SCHEMA.getBytes(),
                        parent1TableName.getBytes(),
                        HConstants.LATEST_TIMESTAMP, false);
                legitChildViews = allDescendants.getFirst();
                orphanViews = allDescendants.getSecond();
                assertLegitChildViews(expectedLegitChildViewsListForParent1,
                        legitChildViews);
                assertTrue(orphanViews.isEmpty());

                allDescendants = ViewUtil.findAllDescendantViews(childLinkTable,
                        cqs.getConfiguration(), EMPTY_BYTE_ARRAY,
                        BASE_TABLE_SCHEMA.getBytes(),
                        parent2TableName.getBytes(),
                        HConstants.LATEST_TIMESTAMP, false);
                legitChildViews = allDescendants.getFirst();
                orphanViews = allDescendants.getSecond();

                // We prune orphan branches and so we will not explore any
                // more orphan links that stem from the first found orphan
                assertEquals(1, orphanViews.size());
                assertEquals(0, orphanViews.get(0).getTenantId().length);
                assertEquals(CHILD_VIEW_LEVEL_1_SCHEMA,
                        Bytes.toString(orphanViews.get(0).getSchemaName()));
                assertEquals(level1ViewName1, Bytes.toString(
                        orphanViews.get(0).getTableName()));
                assertLegitChildViews(expectedLegitChildViewsListForParent2,
                        legitChildViews);
            }
        }
    }

    private void assertLegitChildViews(List<TableInfo> expectedList,
            List<PTable> actualList) {
        assertEquals(expectedList.size(), actualList.size());
        for (int i=0; i<expectedList.size(); i++) {
            TableInfo expectedChild = expectedList.get(i);
            byte[] expectedTenantId = expectedChild.getTenantId();
            PName actualTenantId = actualList.get(i).getTenantId();
            assertTrue((expectedTenantId == null && actualTenantId == null) ||
                    ((actualTenantId != null && expectedTenantId != null) &&
                            Arrays.equals(actualTenantId.getBytes(),
                                    expectedTenantId)));
            assertEquals(Bytes.toString(expectedChild.getSchemaName()),
                    actualList.get(i).getSchemaName().getString());
            assertEquals(Bytes.toString(expectedChild.getTableName()),
                    actualList.get(i).getTableName().getString());
        }
    }

    // Create 2 base tables and attempt to create the same view on top of both.
    // The second view creation will fail, however an orphan parent->child link
    // will be created inside SYSTEM.CHILD_LINK between parent2 and the view
    static void createOrphanLink(String parentSchema, String parent1,
            String parent2, String viewSchema, String viewName)
            throws SQLException {

        final String querySysChildLink =
                "SELECT * FROM SYSTEM.CHILD_LINK WHERE TABLE_SCHEM='%s' AND "
                        + "TABLE_NAME='%s' AND COLUMN_FAMILY='%s' AND "
                        + LINK_TYPE + " = " +
                        PTable.LinkType.CHILD_TABLE.getSerializedValue();
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(CREATE_BASE_TABLE_DDL, parentSchema,
                    parent1));
            stmt.execute(String.format(CREATE_BASE_TABLE_DDL, parentSchema,
                    parent2));
            stmt.execute(String.format(CREATE_CHILD_VIEW_LEVEL_1_DDL,
                    viewSchema, viewName,  parentSchema, parent1));
            try {
                stmt.execute(String.format(CREATE_CHILD_VIEW_LEVEL_1_DDL,
                        viewSchema, viewName, parentSchema, parent2));
                fail("Creating the same view again should have failed");
            } catch (TableAlreadyExistsException ignored) {
                // expected
            }

            // Confirm that the orphan parent->child link exists after the
            // second view creation
            ResultSet rs = stmt.executeQuery(String.format(querySysChildLink,
                    parentSchema,  parent2, SchemaUtil.getTableName(
                            viewSchema, viewName)));
            assertTrue(rs.next());
        }
    }

    void runDropChildViewsTask() {
        // Run DropChildViewsTask to complete the tasks for dropping child views
        TaskRegionObserver.SelfHealingTask task =
                new TaskRegionObserver.SelfHealingTask(TaskRegionEnvironment,
                        DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        task.run();
    }

    @Test
    public void testRecreateIndexWhoseAncestorWasDropped() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName1 = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String fullTableName2 = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());

        String tableDdl = "CREATE TABLE " + fullTableName1
                + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        tableDdl = "CREATE TABLE " + fullTableName2
                + "  (k INTEGER NOT NULL PRIMARY KEY, v3 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName1
                + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1
                + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        String indexName = generateUniqueName();
        ddl = "CREATE INDEX " + indexName + " on " + fullViewName1 + "(v2)";
        conn.createStatement().execute(ddl);
        try {
                // this should fail because an index with this name is present
            ddl = "CREATE INDEX " + indexName + " on " + fullTableName2
                    + "(v1)";
            conn.createStatement().execute(ddl);
            fail();
        }
        catch(SQLException ignored) {
        }

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName1
                + " CASCADE");
        runDropChildViewsTask();

        // should be able to reuse the index name 
        ddl = "CREATE INDEX " + indexName + " on " + fullTableName2 + "(v3)";
        conn.createStatement().execute(ddl);

        String fullIndexName = SchemaUtil.getTableName(SCHEMA2, indexName);
        PTable index = PhoenixRuntime.getTableNoCache(conn, fullIndexName);
        // the index should have v3 but not v2
        validateCols(index);
    }

    @Test
    public void testRecreateViewWhoseParentWasDropped() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName1 = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String fullTableName2 = SchemaUtil.getTableName(SCHEMA3,
                generateUniqueName());

        String tableDdl = "CREATE TABLE " + fullTableName1
                + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        tableDdl = "CREATE TABLE " + fullTableName2
                + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName1
                + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1
                + " WHERE k > 5";
        conn.createStatement().execute(ddl);

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName1
                + " CASCADE");
        runDropChildViewsTask();

        // should be able to reuse the view name 
        ddl = "CREATE VIEW " + fullViewName1
                + " (v3 VARCHAR) AS SELECT * FROM " + fullTableName2
                + " WHERE k > 5";
        conn.createStatement().execute(ddl);

        PTable view = PhoenixRuntime.getTableNoCache(conn, fullViewName1);
        // the view should have v3 but not v2
        validateCols(view);
    }

    // Test case to ensure PHOENIX-5546 does not happen
    @Test
    public void testRepeatedCreateAndDropCascadeTableWorks() throws Exception {
        String tableName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, tableName);
        String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTableViewAndDropCascade(conn, fullTableName, fullViewName,
                    false);
            validateViewDoesNotExist(conn, fullViewName);
            validateSystemTaskContainsCompletedDropChildViewsTasks(conn,
                    SCHEMA1, tableName, 1);

            // Repeat this and check that the view still doesn't exist
            createTableViewAndDropCascade(conn, fullTableName, fullViewName,
                    false);
            validateViewDoesNotExist(conn, fullViewName);
            validateSystemTaskContainsCompletedDropChildViewsTasks(conn,
                    SCHEMA1, tableName, 2);
        }
    }

    // We set DROP_METADATA_ATTRIB to true and check that this does not fail
    // dropping child views that have an index, though their underlying
    // physical table was already dropped.
    // See PHOENIX-5545.
    @Test
    public void testDropTableCascadeWithChildViewWithIndex()
            throws SQLException {
        String tableName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, tableName);
        String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTableViewAndDropCascade(conn, fullTableName, fullViewName,
                    true);
            validateViewDoesNotExist(conn, fullViewName);
            validateSystemTaskContainsCompletedDropChildViewsTasks(conn,
                    SCHEMA1, tableName, 1);
        }
    }

    private void createTableViewAndDropCascade(Connection conn,
            String fullTableName, String fullViewName, boolean createViewIndex)
            throws SQLException {
        String tableDdl = "CREATE TABLE " + fullTableName +
                "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName +
                " (v2 VARCHAR) AS SELECT * FROM " + fullTableName
                + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        if (createViewIndex) {
            conn.createStatement().execute("CREATE INDEX " + "INDEX_"
                    + generateUniqueName() +
                    " ON " + fullViewName + "(v2)");
        }
        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName
                + " CASCADE");
        runDropChildViewsTask();
    }

    private void validateSystemTaskContainsCompletedDropChildViewsTasks(
            Connection conn,  String schemaName, String tableName,
            int numTasks) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM "
                + SYSTEM_TASK_NAME +
                " WHERE " + TASK_TYPE + "=" +
                DROP_CHILD_VIEWS.getSerializedValue() +
                " AND " + TENANT_ID + " IS NULL" +
                " AND " + TABLE_SCHEM + "='" + schemaName +
                "' AND " + TABLE_NAME + "='" + tableName + "'");
        assertTrue(rs.next());
        for (int i = 0; i < numTasks; i++) {
            Timestamp maxTs = new Timestamp(HConstants.LATEST_TIMESTAMP);
            assertNotEquals("Should have got a valid timestamp", maxTs,
                    rs.getTimestamp(2));
            assertEquals("Task should be completed",
                    PTable.TaskStatus.COMPLETED.toString(),
                    rs.getString(6));
            assertNotNull("Task end time should not be null",
                    rs.getTimestamp(7));
            String taskData = rs.getString(9);
            assertTrue("Task data should contain final status",
                    taskData != null &&
                    taskData.contains(TASK_DETAILS) &&
                    taskData.contains(PTable.TaskStatus.COMPLETED.toString()));
        }
    }

    @Test
    public void testViewAndTableInDifferentSchemasWithNamespaceMappingEnabled()
            throws Exception {
        testViewAndTableInDifferentSchemas(true);
    }

    @Test
    public void testViewAndTableInDifferentSchemas() throws Exception {
        testViewAndTableInDifferentSchemas(false);

    }

    private void testViewAndTableInDifferentSchemas(boolean isNamespaceMapped)
            throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
                Boolean.toString(isNamespaceMapped));
        Connection conn = DriverManager.getConnection(getUrl(),props);
        String tableName = "T_" + generateUniqueName();
        String schemaName1 = SCHEMA1;
        String fullTableName1 = SchemaUtil.getTableName(schemaName1, tableName);
        String viewName1 = "V_" + generateUniqueName();
        String viewSchemaName = SCHEMA2;
        String fullViewName1 = SchemaUtil.getTableName(viewSchemaName,
                viewName1);
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3, viewName1);

        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS "
                    + schemaName1);
        }
        String ddl = "CREATE TABLE " + fullTableName1
                + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getAdmin();
        conn.createStatement().execute(ddl);
        assertTrue(admin.tableExists(SchemaUtil.getPhysicalTableName(
                SchemaUtil.normalizeIdentifier(fullTableName1),
                conn.unwrap(PhoenixConnection.class).getQueryServices()
                        .getProps())));

        ddl = "CREATE VIEW " + fullViewName1
                + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1
                + " WHERE k > 5";
        conn.createStatement().execute(ddl);

        ddl = "CREATE VIEW " + fullViewName2
                + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1
                + " WHERE k > 5";
        conn.createStatement().execute(ddl);

        conn.createStatement().executeQuery("SELECT * FROM " + fullViewName1);
        conn.createStatement().executeQuery("SELECT * FROM " + fullViewName2);
        ddl = "DROP VIEW " + viewName1;
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableNotFoundException ignore) {
        }
        ddl = "DROP VIEW " + fullViewName1;
        conn.createStatement().execute(ddl);
        ddl = "DROP VIEW " + SchemaUtil.getTableName(viewSchemaName,
                generateUniqueName());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableNotFoundException ignore) {
        }
        ddl = "DROP TABLE " + fullTableName1;
        ddl = "DROP VIEW " + fullViewName2;
        conn.createStatement().execute(ddl);
        ddl = "DROP TABLE " + fullTableName1;
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testViewAndTableAndDropCascade() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                generateUniqueName());

        String tableDdl = "CREATE TABLE " + fullTableName
                + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(tableDdl);
        String ddl = "CREATE VIEW " + fullViewName1
                + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName
                + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        String indexName = generateUniqueName();
        ddl = "CREATE LOCAL INDEX " + indexName + " on " + fullViewName1
                + "(v2)";
        conn.createStatement().execute(ddl);

        ddl = "CREATE VIEW " + fullViewName2 + "(v2 VARCHAR) AS SELECT * FROM "
                + fullTableName + " WHERE k > 10";
        conn.createStatement().execute(ddl);

        // dropping base table without cascade should fail
        try {
            conn.createStatement().execute("DROP TABLE " + fullTableName );
            fail();
        }
        catch (SQLException e) {
            assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName
                + " CASCADE");
        runDropChildViewsTask();

        validateViewDoesNotExist(conn, fullViewName1);
        validateViewDoesNotExist(conn, fullViewName2);

    }

    @Test
    public void testUpdatingPropertyOnBaseTable() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("create table " + fullTableName
                            + "(tenantId CHAR(15) NOT NULL, pk1 integer "
                            + "NOT NULL, v varchar CONSTRAINT PK PRIMARY KEY "
                            + "(tenantId, pk1)) MULTI_TENANT=true");
            conn.createStatement().execute("CREATE VIEW " + fullViewName
                    + " AS SELECT * FROM " + fullTableName);

            conn.createStatement()
                    .execute("ALTER TABLE " + fullTableName
                            + " set IMMUTABLE_ROWS = true");

            // fetch the latest tables
            PTable table = PhoenixRuntime.getTableNoCache(conn, fullTableName);
            PTable view = PhoenixRuntime.getTableNoCache(conn, fullViewName);
            assertTrue("IMMUTABLE_ROWS property set incorrectly",
                    table.isImmutableRows());
            assertTrue("IMMUTABLE_ROWS property set incorrectly",
                    view.isImmutableRows());
        }
    }

    @Test
    public void testViewAddsPKColumnWhoseParentsLastPKIsVarLength()
            throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());

        String ddl = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 VARCHAR NOT NULL, v1 DECIMAL, "
                + "CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM "
                + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY,"
                + " k4 VARCHAR PRIMARY KEY, v2 INTEGER";
        try {
            conn.createStatement().execute(ddl);
            fail("View cannot extend PK if parent's last PK is variable length."
                    + " See "
                    + "https://issues.apache.org/jira/browse/PHOENIX-978.");
        } catch (SQLException e) {
            assertEquals(CANNOT_MODIFY_VIEW_PK.getErrorCode(),
                    e.getErrorCode());
        }
        String fullViewName2 = "V_" + generateUniqueName();
        ddl = "CREATE VIEW " + fullViewName2 + " (k3 VARCHAR PRIMARY KEY)  "
                + "AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        try {
            conn.createStatement().execute(ddl);
        } catch (SQLException e) {
            assertEquals(CANNOT_MODIFY_VIEW_PK.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test(expected=ColumnAlreadyExistsException.class)
    public void testViewAddsClashingPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());

        String ddl = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL,"
                + " CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM "
                + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY, "
                + "k2 VARCHAR PRIMARY KEY, v2 INTEGER";
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testQueryWithSeparateConnectionForViewOnTableThatHasIndex()
            throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection conn2 = DriverManager.getConnection(getUrl());
                Statement s = conn.createStatement();
                Statement s2 = conn2.createStatement()) {
            String tableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String viewName = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());
            String indexName = generateUniqueName();
            helpTestQueryForViewOnTableThatHasIndex(s, s2, tableName, viewName,
                    indexName);
        }
    }

    @Test
    public void testQueryForViewOnTableThatHasIndex() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement s = conn.createStatement()) {
            String tableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String viewName = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());
            String indexName = generateUniqueName();
            helpTestQueryForViewOnTableThatHasIndex(s, s, tableName, viewName,
                    indexName);
        }
    }

    private void helpTestQueryForViewOnTableThatHasIndex(Statement s1,
            Statement s2, String tableName, String viewName, String indexName)
            throws SQLException {
        // Create a table
        s1.execute("create table " + tableName
                + " (col1 varchar primary key, col2 varchar)");

        // Create a view on the table
        s1.execute("create view " + viewName
                + " (col3 varchar) as select * from " + tableName);
        s1.executeQuery("select * from " + viewName);
        // Create a index on the table
        s1.execute("create index " + indexName + " ON " + tableName
                + " (col2)");

        try (ResultSet rs = s2.executeQuery("explain select /*+ INDEX("
                + viewName + " " + indexName + ") */ * from "
                + viewName + " where col2 = 'aaa'")) {
            String explainPlan = QueryUtil.getExplainPlan(rs);

            // check if the query uses the index
            assertTrue(explainPlan.contains(indexName));
        }
    }

    @Test
    public void testViewAndTableAndDropCascadeWithIndexes() throws Exception {
        // Setup - Tables and Views with Indexes
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String ddl = "CREATE TABLE " + fullTableName
                + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(ddl);
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                generateUniqueName());
        String indexName1 = "I_" + generateUniqueName();
        String indexName2 = "I_" + generateUniqueName();
        String indexName3 = "I_" + generateUniqueName();

        ddl = "CREATE INDEX " + indexName1 + " ON " + fullTableName + " (v1)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM "
                + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX " + indexName2 + " ON " + fullViewName1 + " (v2)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + " (v2 VARCHAR) AS SELECT * FROM "
                + fullTableName + " WHERE k > 10";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX " + indexName3 + " ON " + fullViewName2 + " (v2)";
        conn.createStatement().execute(ddl);


        // Execute DROP...CASCADE
        conn.createStatement().execute("DROP TABLE " + fullTableName
                + " CASCADE");
        runDropChildViewsTask();

        // Validate Views were deleted - Try and delete child views,
        // should throw TableNotFoundException
        validateViewDoesNotExist(conn, fullViewName1);
        validateViewDoesNotExist(conn, fullViewName2);
    }

    @Test
    public void testViewAddsNotNullPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String ddl = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL,"
                + " CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM "
                + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        try {
            ddl = "ALTER VIEW " + fullViewName
                    + " ADD k3 VARCHAR NOT NULL PRIMARY KEY";
            conn.createStatement().execute(ddl);
            fail("can only add nullable PKs via ALTER VIEW/TABLE");
        } catch (SQLException e) {
            assertEquals(NOT_NULLABLE_COLUMN_IN_ROW_KEY.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testDisallowDropOfColumnOnParentTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String viewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String ddl = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, "
                + "CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + viewName + "(v2 VARCHAR, v3 VARCHAR) "
                + "AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().execute("ALTER TABLE " + fullTableName
                    + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testDisallowDropOfReferencedColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                generateUniqueName());

        String ddl = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, "
                + "CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName1 + "(v2 VARCHAR, v3 VARCHAR)"
                + " AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + " AS SELECT * FROM "
                + fullViewName1 + " WHERE v2 != 'foo'";
        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName1
                    + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_DROP_VIEW_REFERENCED_COL
                            .getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName2
                    + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_DROP_VIEW_REFERENCED_COL
                            .getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName2
                    + " DROP COLUMN v2");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_DROP_VIEW_REFERENCED_COL
                            .getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("ALTER VIEW " + fullViewName2
                + " DROP COLUMN v3");
    }

    @Test
    public void testViewAddsPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String viewName = generateUniqueName();
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, viewName);

        String ddl = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, "
                + "CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM "
                + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);

        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY, "
                + "k4 VARCHAR PRIMARY KEY, v2 INTEGER";
        conn.createStatement().execute(ddl);

        // assert PK metadata
        ResultSet rs = conn.getMetaData().getPrimaryKeys(null, SCHEMA2,
                viewName);
        assertPKs(rs, new String[] {"K1", "K2", "K3", "K4"});
    }

    @Test
    public void testCreateViewDefinesPKConstraint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());

        String ddl = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL,"
                + " CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "(v2 VARCHAR, k3 VARCHAR, "
                + "k4 INTEGER NOT NULL, CONSTRAINT PKVEW PRIMARY KEY (k3, k4))"
                + " AS SELECT * FROM " + fullTableName + " WHERE K1 = 1";
        conn.createStatement().execute(ddl);

        PhoenixRuntime.getTableNoCache(conn, fullViewName);

        // assert PK metadata
        ResultSet rs =
                conn.getMetaData().getPrimaryKeys(null,
                    SchemaUtil.getSchemaNameFromFullName(fullViewName),
                    SchemaUtil.getTableNameFromFullName(fullViewName));
        assertPKs(rs, new String[] {"K1", "K2", "K3", "K4"});
    }

    private void assertPKs(ResultSet rs, String[] expectedPKs)
            throws SQLException {
        List<String> pkCols = newArrayListWithExpectedSize(expectedPKs.length);
        while (rs.next()) {
            pkCols.add(rs.getString("COLUMN_NAME"));
        }
        String[] actualPKs = pkCols.toArray(new String[0]);
        assertArrayEquals(expectedPKs, actualPKs);
    }

    private void validateViewDoesNotExist(Connection conn, String fullViewName)
            throws SQLException {
        try {
            String ddl1 = "DROP VIEW " + fullViewName;
            conn.createStatement().execute(ddl1);
            fail("View " + fullViewName + " should have been deleted when "
                    + "parent was dropped");
        } catch (TableNotFoundException e) {
            //Expected
        }
    }

    private void validateCols(PTable table) {
        final String prefix = table.getType() == PTableType.INDEX ? "0:" : "";
        Predicate<PColumn> predicate = new Predicate<PColumn>() {
            @Override
            public boolean apply(PColumn col) {
                return col.getName().getString().equals(prefix + "V3")
                        || col.getName().getString().equals(prefix + "V2");
            }
        };
        List<PColumn> colList = table.getColumns();
        Collection<PColumn> filteredCols = Collections2.filter(colList,
                predicate);
        assertEquals(1, filteredCols.size());
        assertEquals(prefix + "V3", filteredCols.iterator().next().getName()
                .getString());
    }


}
