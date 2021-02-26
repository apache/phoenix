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

import org.apache.curator.shaded.com.google.common.base.Joiner;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.ScanInfoUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.INVALID_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.VALID_ROW_COUNT;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.MAX_ROWS;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class LogicalTableNameIT extends BaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableNameIT.class);

    private final boolean createChildAfterTransform;
    private final boolean immutable;
    private String dataTableDdl;
    public static final String NEW_TABLE_PREFIX = "NEW_TBL_";
    private Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newConcurrentMap();
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        props.put(ScanInfoUtil.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60*1000)); // An hour
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    public LogicalTableNameIT(boolean createChildAfterTransform, boolean immutable)  {
        this.createChildAfterTransform = createChildAfterTransform;
        this.immutable = immutable;
        StringBuilder optionBuilder = new StringBuilder();
        if (immutable) {
            optionBuilder.append(" ,IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, IMMUTABLE_ROWS=true");
        }
        this.dataTableDdl = optionBuilder.toString();
    }

    @Parameterized.Parameters(
            name = "createChildAfterTransform={0}, immutable={1}")
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

    private Connection getConnection(Properties props) throws Exception {
        props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Force real driver to be used as the test one doesn't handle creating
        // more than one ConnectionQueryService
        props.setProperty(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, StringUtil.EMPTY_STRING);
        // Create new ConnectionQueryServices so that we can set DROP_METADATA_ATTRIB
        String url = QueryUtil.getConnectionUrl(props, config, "PRINCIPAL");
        return DriverManager.getConnection(url, props);
    }

    private  HashMap<String, ArrayList<String>> testBaseTableWithIndex_BaseTableChange(Connection conn, Connection conn2, String schemaName,
                                                                                       String tableName, String indexName) throws Exception {
        conn.setAutoCommit(true);
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        createTable(conn, fullTableName);
        if (!createChildAfterTransform) {
            createIndexOnTable(conn, fullTableName, indexName);
        }
        HashMap<String, ArrayList<String>> expected = populateTable(conn, fullTableName, 1, 2);

        // Create another hbase table and add 1 more row
        String newTableName =  NEW_TABLE_PREFIX + tableName;
        String fullNewTableName = SchemaUtil.getTableName(schemaName, newTableName);
        try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            String snapshotName = new StringBuilder(fullTableName).append("-Snapshot").toString();
            admin.snapshot(snapshotName, TableName.valueOf(fullTableName));
            admin.cloneSnapshot(Bytes.toBytes(snapshotName), Bytes.toBytes(fullNewTableName));

            try (HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullNewTableName))) {
                Put put = new Put(ByteUtil.concat(Bytes.toBytes("PK3")));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                        QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"), Bytes.toBytes("V13"));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V2"),
                        PInteger.INSTANCE.toBytes(3));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V3"),
                        PInteger.INSTANCE.toBytes(4));
                htable.put(put);
                expected.put("PK3", Lists.newArrayList("PK3", "V13", "3", "4"));
            }
        }

        // Query to cache on the second connection
        String selectTable1 = "SELECT PK1, V1, V2, V3 FROM " + fullTableName + " ORDER BY PK1 DESC";
        ResultSet rs1 = conn2.createStatement().executeQuery(selectTable1);
        assertTrue(rs1.next());

        // Rename table to point to the new hbase table
        renameAndDropPhysicalTable(conn, "NULL", schemaName, tableName, newTableName);

        if (createChildAfterTransform) {
            createIndexOnTable(conn, fullTableName, indexName);
        }

        return expected;
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
                HashMap<String, ArrayList<String>> expected = testBaseTableWithIndex_BaseTableChange(conn, conn2, schemaName, tableName, indexName);

                // We have to rebuild index for this to work
                IndexToolIT.runIndexTool(true, false, schemaName, tableName, indexName);

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
                Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
                assertEquals(false, admin.tableExists(TableName.valueOf(SchemaUtil.getTableName(schemaName,NEW_TABLE_PREFIX + tableName))));

                // check that index is dropped
                assertEquals(false, admin.tableExists(TableName.valueOf(fullIndexName)));

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
                testBaseTableWithIndex_BaseTableChange(conn, conn2, schemaName, tableName, indexName);

                List<Job>
                        completedJobs =
                        IndexScrutinyToolBaseIT.runScrutinyTool(schemaName, tableName, indexName, 1L,
                                IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE);

                Job job = completedJobs.get(0);
                assertTrue(job.isSuccessful());

                Counters counters = job.getCounters();
                if (createChildAfterTransform) {
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

    private  HashMap<String, ArrayList<String>> test_IndexTableChange(Connection conn, Connection conn2, String schemaName, String tableName, String indexName, byte[] verifiedBytes) throws Exception {
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        conn.setAutoCommit(true);
        createTable(conn, fullTableName);
        createIndexOnTable(conn, fullTableName, indexName);
        HashMap<String, ArrayList<String>> expected = populateTable(conn, fullTableName, 1, 2);

        // Create another hbase table for index and add 1 more row
        String newTableName = "NEW_IDXTBL_" + generateUniqueName();
        String fullNewTableName = SchemaUtil.getTableName(schemaName, newTableName);
        try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getAdmin()) {
            String snapshotName = new StringBuilder(indexName).append("-Snapshot").toString();
            admin.snapshot(snapshotName, TableName.valueOf(fullIndexName));
            admin.cloneSnapshot(Bytes.toBytes(snapshotName), Bytes.toBytes(fullNewTableName));

            try (HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullNewTableName))) {
                Put
                        put =
                        new Put(ByteUtil.concat(Bytes.toBytes("V13"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes("PK3")));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                        verifiedBytes);
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("0:V2"),
                        PInteger.INSTANCE.toBytes(3));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("0:V3"),
                        PInteger.INSTANCE.toBytes(4));
                htable.put(put);
                expected.put("PK3", Lists.newArrayList("PK3", "V13", "3", "4"));
            }
        }

        // Query to cache on the second connection
        String selectTable1 = "SELECT * FROM " + fullIndexName;
        ResultSet rs1 = conn2.createStatement().executeQuery(selectTable1);
        assertTrue(rs1.next());

        // Rename table to point to the new hbase table
        renameAndDropPhysicalTable(conn, "NULL", schemaName, indexName, newTableName);

        return expected;
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
                HashMap<String, ArrayList<String>> expected = test_IndexTableChange(conn, conn2, schemaName, tableName, indexName, IndexRegionObserver.VERIFIED_BYTES);

                validateIndex(conn, fullIndexName, false, expected);
                validateIndex(conn2, fullIndexName, false, expected);

                // create another index and drop the first index and validate the second one
                String indexName2 = "IDX2_" + generateUniqueName();
                String fullIndexName2 = SchemaUtil.getTableName(schemaName, indexName2);
                if (createChildAfterTransform) {
                    createIndexOnTable(conn2, fullTableName, indexName2);
                }
                dropIndex(conn2, fullTableName, indexName);
                if (!createChildAfterTransform) {
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
                test_IndexTableChange(conn, conn2, schemaName, tableName, indexName, IndexRegionObserver.VERIFIED_BYTES);
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
                test_IndexTableChange(conn, conn2, schemaName, tableName2, indexName2, IndexRegionObserver.UNVERIFIED_BYTES);

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

    private HashMap<String, ArrayList<String>> testWithViewsAndIndex_BaseTableChange(Connection conn, Connection conn2, String schemaName, String tableName, String viewName1, String v1_indexName1, String v1_indexName2, String viewName2, String v2_indexName1) throws Exception {
        conn.setAutoCommit(true);
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullViewName1 = SchemaUtil.getTableName(schemaName, viewName1);
        String fullViewName2 = SchemaUtil.getTableName(schemaName, viewName2);
        createTable(conn, fullTableName);
        HashMap<String, ArrayList<String>> expected = new HashMap<>();
        if (!createChildAfterTransform) {
            createViewAndIndex(conn, schemaName, tableName, viewName1, v1_indexName1);
            createViewAndIndex(conn, schemaName, tableName, viewName1, v1_indexName2);
            createViewAndIndex(conn, schemaName, tableName, viewName2, v2_indexName1);
            expected.putAll(populateView(conn, fullViewName1, 1,2));
            expected.putAll(populateView(conn, fullViewName2, 10,2));
        }

        // Create another hbase table and add 1 more row
        String newTableName = "NEW_TBL_" + generateUniqueName();
        String fullNewTableName = SchemaUtil.getTableName(schemaName, newTableName);
        try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getAdmin()) {
            String snapshotName = new StringBuilder(fullTableName).append("-Snapshot").toString();
            admin.snapshot(snapshotName, TableName.valueOf(fullTableName));
            admin.cloneSnapshot(Bytes.toBytes(snapshotName), Bytes.toBytes(fullNewTableName));

            try (HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullNewTableName))) {
                Put put = new Put(ByteUtil.concat(Bytes.toBytes("PK3")));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                        QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"),
                        Bytes.toBytes("V13"));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V2"),
                        PInteger.INSTANCE.toBytes(3));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V3"),
                        PInteger.INSTANCE.toBytes(4));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("VIEW_COL1"),
                        Bytes.toBytes("VIEW_COL1_3"));
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("VIEW_COL2"),
                        Bytes.toBytes("VIEW_COL2_3"));
                htable.put(put);
                expected.put("PK3", Lists.newArrayList("PK3", "V13", "3", "4", "VIEW_COL1_3", "VIEW_COL2_3"));
            }
        }

        // Query to cache on the second connection
        String selectTable1 = "SELECT PK1, V1, V2, V3 FROM " + fullTableName + " ORDER BY PK1 DESC";
        ResultSet rs1 = conn2.createStatement().executeQuery(selectTable1);
        if (!createChildAfterTransform) {
            assertTrue(rs1.next());
        }

        // Rename table to point to hbase table
        renameAndDropPhysicalTable(conn, "NULL", schemaName, tableName, newTableName);

        conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
        if (createChildAfterTransform) {
            createViewAndIndex(conn, schemaName, tableName, viewName1, v1_indexName1);
            createViewAndIndex(conn, schemaName, tableName, viewName1, v1_indexName2);
            createViewAndIndex(conn, schemaName, tableName, viewName2, v2_indexName1);
            expected.putAll(populateView(conn, fullViewName1, 1,2));
            expected.putAll(populateView(conn, fullViewName2, 10,2));
        }

        return expected;
    }


    private PhoenixTestBuilder.SchemaBuilder createGlobalViewAndTenantView() throws Exception {
        int numOfRows = 5;
        PhoenixTestBuilder.SchemaBuilder.TableOptions tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();
        tableOptions.setTableProps(" MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0 "+this.dataTableDdl);

        PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions globalViewOptions = PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions.withDefaults();

        PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions globalViewIndexOptions =
                PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions tenantViewOptions = new PhoenixTestBuilder.SchemaBuilder.TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions.setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        PhoenixTestBuilder.SchemaBuilder.OtherOptions testCaseWhenAllCFMatchAndAllDefault = new PhoenixTestBuilder.SchemaBuilder.OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        // Define the test schema.
        PhoenixTestBuilder.SchemaBuilder schemaBuilder = null;
        if (!createChildAfterTransform) {
            schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
            schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                    .withGlobalViewIndexOptions(globalViewIndexOptions)
                    .withTenantViewOptions(tenantViewOptions)
                    .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).build();
        }  else {
            schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
            schemaBuilder.withTableOptions(tableOptions).build();
        }

        PTable table = schemaBuilder.getBaseTable();
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        String newBaseTableName = "NEW_TBL_" + tableName;
        String fullNewBaseTableName = SchemaUtil.getTableName(schemaName, newBaseTableName);
        String fullTableName = table.getName().getString();

        try (Connection conn = getConnection(props)) {

            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                String snapshotName = new StringBuilder(fullTableName).append("-Snapshot").toString();
                admin.snapshot(snapshotName, TableName.valueOf(fullTableName));
                admin.cloneSnapshot(Bytes.toBytes(snapshotName), Bytes.toBytes(fullNewBaseTableName));
            }

            renameAndDropPhysicalTable(conn, null, schemaName, tableName, newBaseTableName);
        }

        // TODO: this still creates a new table.
        if (createChildAfterTransform) {
            schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
            schemaBuilder.withDataOptions(schemaBuilder.getDataOptions())
                    .withTableOptions(tableOptions)
                    .withGlobalViewOptions(globalViewOptions)
                    .withGlobalViewIndexOptions(globalViewIndexOptions)
                    .withTenantViewOptions(tenantViewOptions)
                    .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).build();
        }

        // Define the test data.
        PhoenixTestBuilder.DataSupplier dataSupplier = new PhoenixTestBuilder.DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String id = String.format(ViewTTLIT.ID_FMT, rowIndex);
                String zid = String.format(ViewTTLIT.ZID_FMT, rowIndex);
                String col4 = String.format(ViewTTLIT.COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col5 = String.format(ViewTTLIT.COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format(ViewTTLIT.COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format(ViewTTLIT.COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format(ViewTTLIT.COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format(ViewTTLIT.COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                return Lists.newArrayList(
                        new Object[] { id, zid, col4, col5, col6, col7, col8, col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        PhoenixTestBuilder.DataWriter dataWriter = new PhoenixTestBuilder.BasicDataWriter();
        List<String> columns =
                Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");

        String tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();

        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectUrl)) {
            tenantConnection.setAutoCommit(true);
            dataWriter.setConnection(tenantConnection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.setUpsertColumns(columns);
            dataWriter.setRowKeyColumns(rowKeyColumns);
            dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());
            dataWriter.upsertRows(1, numOfRows);
            com.google.common.collect.Table<String, String, Object> upsertedData = dataWriter.getDataTable();;

            PhoenixTestBuilder.DataReader dataReader = new PhoenixTestBuilder.BasicDataReader();
            dataReader.setValidationColumns(columns);
            dataReader.setRowKeyColumns(rowKeyColumns);
            dataReader.setDML(String.format("SELECT %s from %s", Joiner.on(",").join(columns),
                    schemaBuilder.getEntityTenantViewName()));
            dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());
            dataReader.setConnection(tenantConnection);
            dataReader.readRows();
            com.google.common.collect.Table<String, String, Object> fetchedData
                    = dataReader.getDataTable();
            assertNotNull("Fetched data should not be null", fetchedData);
            ViewTTLIT.verifyRowsBeforeTTLExpiration(upsertedData, fetchedData);

        }
        return schemaBuilder;
    }

    @Test
    public void testWith2LevelViewsBaseTablePhysicalNameChange() throws Exception {
        // TODO: use namespace in one of the cases
        PhoenixTestBuilder.SchemaBuilder schemaBuilder = createGlobalViewAndTenantView();
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

                HashMap<String, ArrayList<String>> expected = testWithViewsAndIndex_BaseTableChange(conn, conn2, schemaName, tableName, view1Name, view1IndexName1, view1IndexName2, view2Name, view2IndexName1);

                // We have to rebuild index for this to work
                IndexToolIT.runIndexTool(true, false, schemaName, view1Name, view1IndexName1);
                IndexToolIT.runIndexTool(true, false, schemaName, view1Name, view1IndexName2);
                IndexToolIT.runIndexTool(true, false, schemaName, view2Name, view2IndexName1);

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

                testWithViewsAndIndex_BaseTableChange(conn, conn2,schemaName, tableName, view1Name,
                        view1IndexName1, view1IndexName2, view2Name, view2IndexName1);

                List<Job>
                        completedJobs =
                        IndexScrutinyToolBaseIT.runScrutinyTool(schemaName, view2Name, view2IndexName1, 1L,
                                IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE);

                Job job = completedJobs.get(0);
                assertTrue(job.isSuccessful());

                Counters counters = job.getCounters();
                if (createChildAfterTransform) {
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

    private void createTable(Connection conn, String tableName) throws Exception {
        String createTableSql = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 INTEGER, V3 INTEGER "
                + "CONSTRAINT NAME_PK PRIMARY KEY(PK1)) COLUMN_ENCODED_BYTES=0 " + dataTableDdl;
        LOGGER.debug(createTableSql);
        conn.createStatement().execute(createTableSql);
    }

    private void createIndexOnTable(Connection conn, String tableName, String indexName)
            throws SQLException {
        String createIndexSql = "CREATE INDEX " + indexName + " ON " + tableName + " (V1) INCLUDE (V2, V3) ";
        LOGGER.debug(createIndexSql);
        conn.createStatement().execute(createIndexSql);
    }

    private void dropIndex(Connection conn, String tableName, String indexName)
            throws SQLException {
        String sql = "DROP INDEX " + indexName + " ON " + tableName ;
        conn.createStatement().execute(sql);
    }

    private HashMap<String, ArrayList<String>> populateTable(Connection conn, String tableName, int startnum, int numOfRows)
            throws SQLException {
        String upsert = "UPSERT INTO " + tableName + " (PK1, V1,  V2, V3) VALUES (?,?,?,?)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        HashMap<String, ArrayList<String>> result = new HashMap<>();
        for (int i=startnum; i < startnum + numOfRows; i++) {
            ArrayList<String> row = new ArrayList<>();
            upsertStmt.setString(1, "PK" + i);
            row.add("PK" + i);
            upsertStmt.setString(2, "V1" + i);
            row.add("V1" + i);
            upsertStmt.setInt(3, i);
            row.add(String.valueOf(i));
            upsertStmt.setInt(4, i + 1);
            row.add(String.valueOf(i + 1));
            upsertStmt.executeUpdate();
            result.put("PK" + i, row);
        }
        return result;
    }

    private HashMap<String, ArrayList<String>> populateView(Connection conn, String viewName, int startNum, int numOfRows) throws SQLException {
        String upsert = "UPSERT INTO " + viewName + " (PK1, V1,  V2, V3, VIEW_COL1, VIEW_COL2) VALUES (?,?,?,?,?,?)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        HashMap<String, ArrayList<String>> result = new HashMap<>();
        for (int i=startNum; i < startNum + numOfRows; i++) {
            ArrayList<String> row = new ArrayList<>();
            upsertStmt.setString(1, "PK"+i);
            row.add("PK"+i);
            upsertStmt.setString(2, "V1"+i);
            row.add("V1"+i);
            upsertStmt.setInt(3, i);
            row.add(String.valueOf(i));
            upsertStmt.setInt(4, i+1);
            row.add(String.valueOf(i+1));
            upsertStmt.setString(5, "VIEW_COL1_"+i);
            row.add("VIEW_COL1_"+i);
            upsertStmt.setString(6, "VIEW_COL2_"+i);
            row.add("VIEW_COL2_"+i);
            upsertStmt.executeUpdate();
            result.put("PK"+i, row);
        }
        return result;
    }

    private void createViewAndIndex(Connection conn, String schemaName, String tableName, String viewName, String viewIndexName)
            throws SQLException {
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        String
                view1DDL =
                "CREATE VIEW IF NOT EXISTS " + fullViewName + " ( VIEW_COL1 VARCHAR, VIEW_COL2 VARCHAR) AS SELECT * FROM "
                        + fullTableName;
        conn.createStatement().execute(view1DDL);
        String indexDDL = "CREATE INDEX IF NOT EXISTS " + viewIndexName + " ON " + fullViewName + " (V1) include (V2, V3, VIEW_COL2) ";
        conn.createStatement().execute(indexDDL);
        conn.commit();
    }

    private void validateTable(Connection connection, String tableName) throws SQLException {
        String selectTable = "SELECT PK1, V1, V2, V3 FROM " + tableName + " ORDER BY PK1 DESC";
        ResultSet rs = connection.createStatement().executeQuery(selectTable);
        assertTrue(rs.next());
        assertEquals("PK3", rs.getString(1));
        assertEquals("V13", rs.getString(2));
        assertEquals(3, rs.getInt(3));
        assertEquals(4, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals("PK2", rs.getString(1));
        assertEquals("V12", rs.getString(2));
        assertEquals(2, rs.getInt(3));
        assertEquals(3, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals("PK1", rs.getString(1));
        assertEquals("V11", rs.getString(2));
        assertEquals(1, rs.getInt(3));
        assertEquals(2, rs.getInt(4));
    }

    private void validateIndex(Connection connection, String tableName, boolean isViewIndex, HashMap<String, ArrayList<String>> expected) throws SQLException {
        String selectTable = "SELECT * FROM " + tableName;
        ResultSet rs = connection.createStatement().executeQuery(selectTable);
        int cnt = 0;
        while (rs.next()) {
            String pk = rs.getString(2);
            assertTrue(expected.containsKey(pk));
            ArrayList<String> row = expected.get(pk);
            assertEquals(row.get(1), rs.getString(1));
            assertEquals(row.get(2), rs.getString(3));
            assertEquals(row.get(3), rs.getString(4));
            if (isViewIndex) {
                assertEquals(row.get(5), rs.getString(5));
            }
            cnt++;
        }
        assertEquals(cnt, expected.size());
    }

    public static void renameAndDropPhysicalTable(Connection conn, String tenantId, String schema, String tableName, String physicalName) throws Exception {
        String
                changeName =
                String.format(
                        "UPSERT INTO SYSTEM.CATALOG (TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY, PHYSICAL_TABLE_NAME) VALUES (%s, '%s', '%s', NULL, NULL, '%s')",
                        tenantId, schema, tableName, physicalName);
        conn.createStatement().execute(changeName);
        conn.commit();

        String fullTableName = SchemaUtil.getTableName(schema, tableName);
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        TableName hTableName = TableName.valueOf(fullTableName);
        admin.disableTable(hTableName);
        admin.deleteTable(hTableName);
        conn.unwrap(PhoenixConnection.class).getQueryServices()
                .clearCache();
    }
}
