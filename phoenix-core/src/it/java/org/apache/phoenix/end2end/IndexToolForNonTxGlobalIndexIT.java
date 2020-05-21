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

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRow;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.mapreduce.PhoenixJobCounters.INPUT_RECORDS;
import static org.apache.phoenix.mapreduce.index.IndexTool.RETRY_VERIFY_NOT_APPLICABLE;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.INDEX_TOOL_RUN_STATUS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RESULT_TABLE_COLUMN_FAMILY;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RESULT_TABLE_NAME_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.ROW_KEY_SEPARATOR;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RUN_STATUS_EXECUTED;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RUN_STATUS_SKIPPED;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.CURRENT_SCN_VALUE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class IndexToolForNonTxGlobalIndexIT extends BaseUniqueNamesOwnClusterIT {

    private final String tableDDLOptions;
    private boolean directApi = true;
    private boolean useSnapshot = false;
    private boolean mutable;
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    public IndexToolForNonTxGlobalIndexIT(boolean mutable) {
        StringBuilder optionBuilder = new StringBuilder();
        this.mutable = mutable;
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameterized.Parameters(name = "mutable={0}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {true},
                {false} });
    }

    @BeforeClass
    public static synchronized void setup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
                QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(8));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        clientProps.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @After
    public void cleanup() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME_BYTES));
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationResultRepository.RESULT_TABLE_NAME));
        }
    }

    @Test
    public void testWithSetNull() throws Exception {
        // This tests the cases where a column having a null value is overwritten with a not null value and vice versa;
        // and after that the index table is still rebuilt correctly
        if(!this.mutable) {
            return;
        }
        final int NROWS = 2 * 3 * 5 * 7;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) "
                    + tableDDLOptions);
            String upsertStmt = "UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(upsertStmt);
            IndexToolIT.setEveryNthRowWithNull(NROWS, 2, stmt);
            conn.commit();
            IndexToolIT.setEveryNthRowWithNull(NROWS, 3, stmt);
            conn.commit();
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2) ASYNC ", indexTableName, dataTableFullName));
            // Run the index MR job and verify that the index table is built correctly
            IndexTool
                    indexTool = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0, new String[0]);
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertTrue("Index rebuild failed!", indexTool.getJob().isSuccessful());
            TestUtil.assertIndexState(conn, indexTableFullName, PIndexState.ACTIVE, null);
            long actualRowCount = IndexScrutiny
                    .scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            IndexToolIT.setEveryNthRowWithNull(NROWS, 5, stmt);
            conn.commit();
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            IndexToolIT.setEveryNthRowWithNull(NROWS, 7, stmt);
            conn.commit();
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            indexTool = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null,
                    0, IndexTool.IndexVerifyType.ONLY, new String[0]);
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
        }
    }

    @Test
    public void testIndexToolVerifyWithExpiredIndexRows() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, CODE VARCHAR) COLUMN_ENCODED_BYTES=0");
            // Insert a row
            conn.createStatement()
                    .execute("upsert into " + dataTableFullName + " values (1, 'Phoenix', 'A')");
            conn.commit();
            conn.createStatement()
                    .execute(String.format("CREATE INDEX %s ON %s (NAME) INCLUDE (CODE) ASYNC",
                            indexTableName, dataTableFullName));
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0,
                    IndexTool.IndexVerifyType.ONLY);
            Cell cell =
                    IndexToolIT.getErrorMessageFromIndexToolOutputTable(conn, dataTableFullName,
                            indexTableFullName);
            try {
                String expectedErrorMsg = IndexRebuildRegionScanner.ERROR_MESSAGE_MISSING_INDEX_ROW;
                String actualErrorMsg = Bytes
                        .toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                assertTrue(expectedErrorMsg.equals(actualErrorMsg));
            } catch(Exception ex) {
                Assert.fail("Fail to parsing the error message from IndexToolOutputTable");
            }

            // Run the index tool to populate the index while verifying rows
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0,
                    IndexTool.IndexVerifyType.AFTER);

            // Set ttl of index table ridiculously low so that all data is expired
            Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            TableName indexTable = TableName.valueOf(indexTableFullName);
            HColumnDescriptor desc = admin.getTableDescriptor(indexTable).getColumnFamilies()[0];
            desc.setTimeToLive(1);
            admin.modifyColumn(indexTable, desc);
            Thread.sleep(1000);
            Pair<Integer, Integer> status = admin.getAlterStatus(indexTable);
            int retry = 0;
            while (retry < 20 && status.getFirst() != 0) {
                Thread.sleep(2000);
                status = admin.getAlterStatus(indexTable);
            }
            assertTrue(status.getFirst() == 0);

            TableName indexToolOutputTable = TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME_BYTES);
            admin.disableTable(indexToolOutputTable);
            admin.deleteTable(indexToolOutputTable);
            // Run the index tool using the only-verify option, verify it gives no mismatch
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0,
                    IndexTool.IndexVerifyType.ONLY);
            Scan scan = new Scan();
            Table hIndexToolTable =
                    conn.unwrap(PhoenixConnection.class).getQueryServices()
                            .getTable(indexToolOutputTable.getName());
            Result r = hIndexToolTable.getScanner(scan).next();
            assertTrue(r == null);
        }
    }

    @Test
    public void testSecondaryGlobalIndexFailure() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String stmString1 =
                    "CREATE TABLE " + dataTableFullName
                            + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                            + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            // Insert two rows
            IndexToolIT.upsertRow(stmt1, 1);
            IndexToolIT.upsertRow(stmt1, 2);
            conn.commit();

            String stmtString2 =
                    String.format(
                            "CREATE INDEX %s ON %s  (LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz') ASYNC ", indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);

            // Run the index MR job.
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName);

            String qIndexTableName = SchemaUtil.getQualifiedTableName(schemaName, indexTableName);

            // Verify that the index table is in the ACTIVE state
            assertEquals(PIndexState.ACTIVE, TestUtil.getIndexState(conn, qIndexTableName));

            ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();
            Admin admin = queryServices.getAdmin();
            TableName tableName = TableName.valueOf(qIndexTableName);
            admin.disableTable(tableName);

            // Run the index MR job and it should fail (return -1)
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, -1, new String[0]);

            // Verify that the index table should be still in the ACTIVE state
            assertEquals(PIndexState.ACTIVE, TestUtil.getIndexState(conn, qIndexTableName));
        }
    }

    @Test
    public void testBuildSecondaryIndexAndScrutinize() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String stmString1 =
                    "CREATE TABLE " + dataTableFullName
                            + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                            + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            // Insert NROWS rows
            final int NROWS = 1000;
            for (int i = 0; i < NROWS; i++) {
                IndexToolIT.upsertRow(stmt1, i);
            }
            conn.commit();
            String stmtString2 =
                    String.format(
                            "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP) ASYNC ", indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);

            // Run the index MR job and verify that the index table is built correctly
            IndexTool indexTool = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0, IndexTool.IndexVerifyType.BEFORE, new String[0]);
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);

            // Add more rows and make sure that these rows will be visible to IndexTool
            for (int i = NROWS; i < 2 * NROWS; i++) {
                IndexToolIT.upsertRow(stmt1, i);
            }
            conn.commit();
            indexTool = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0, IndexTool.IndexVerifyType.BOTH, new String[0]);
            assertEquals(2 * NROWS, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(2 * NROWS, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(2 * NROWS, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(2 * NROWS, actualRowCount);
        }
    }

    @Test
    public void testIndexToolVerifyBeforeAndBothOptions() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String schemaName = generateUniqueName();
            String dataTableName = generateUniqueName();
            String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
            String indexTableName = generateUniqueName();
            String viewName = generateUniqueName();
            String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                    + tableDDLOptions);
            conn.commit();
            conn.createStatement().execute("CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName);
            conn.commit();
            // Insert a row
            conn.createStatement().execute("upsert into " + viewFullName + " values (1, 'Phoenix', 12345)");
            conn.commit();
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP) ASYNC", indexTableName, viewFullName));
            TestUtil.addCoprocessor(conn, "_IDX_" + dataTableFullName, IndexToolIT.MutationCountingRegionObserver.class);
            // Run the index MR job and verify that the index table rebuild succeeds
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.AFTER);
            assertEquals(1, IndexToolIT.MutationCountingRegionObserver.getMutationCount());
            IndexToolIT.MutationCountingRegionObserver.setMutationCount(0);
            // Since all the rows are in the index table, running the index tool with the "-v BEFORE" option should not
            // write any index rows
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.BEFORE);
            assertEquals(0, IndexToolIT.MutationCountingRegionObserver.getMutationCount());
            // The "-v BOTH" option should not write any index rows either
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.BOTH);
            assertEquals(0, IndexToolIT.MutationCountingRegionObserver.getMutationCount());
        }
    }

    @Test
    public void testIndexToolVerifyAfterOption() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String schemaName = generateUniqueName();
            String dataTableName = generateUniqueName();
            String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
            String indexTableName = generateUniqueName();
            String viewName = generateUniqueName();
            String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                    + tableDDLOptions);
            conn.commit();
            conn.createStatement().execute("CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName);
            conn.commit();
            // Insert a row
            conn.createStatement().execute("upsert into " + viewFullName + " values (1, 'Phoenix', 12345)");
            conn.commit();
            // Configure IndexRegionObserver to fail the first write phase. This should not
            // lead to any change on index and thus the index verify during index rebuild should fail
            IndexRegionObserver.setIgnoreIndexRebuildForTesting(true);
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP) ASYNC", indexTableName, viewFullName));
            // Run the index MR job and verify that the index table rebuild fails
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName, indexTableName,
                    null, -1, IndexTool.IndexVerifyType.AFTER);
            // The index tool output table should report that there is a missing index row
            Cell cell = IndexToolIT.getErrorMessageFromIndexToolOutputTable(conn, dataTableFullName, "_IDX_" + dataTableFullName);
            try {
                String expectedErrorMsg = IndexRebuildRegionScanner.ERROR_MESSAGE_MISSING_INDEX_ROW;
                String actualErrorMsg = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                assertTrue(expectedErrorMsg.equals(actualErrorMsg));
            } catch(Exception ex){
                Assert.fail("Fail to parsing the error message from IndexToolOutputTable");
            }
            IndexRegionObserver.setIgnoreIndexRebuildForTesting(false);
        }
    }

    @Test
    public void testIndexToolOnlyVerifyOption() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, CODE VARCHAR) COLUMN_ENCODED_BYTES=0");
            // Insert a row
            conn.createStatement().execute("upsert into " + dataTableFullName + " values (1, 'Phoenix', 'A')");
            conn.commit();
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (CODE) ASYNC", indexTableName, dataTableFullName));
            // Run the index MR job to only verify that each data table row has a corresponding index row
            // IndexTool will go through each data table row and record the mismatches in the output table
            // called PHOENIX_INDEX_TOOL
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);
            Cell cell = IndexToolIT.getErrorMessageFromIndexToolOutputTable(conn, dataTableFullName, indexTableFullName);
            try {
                String expectedErrorMsg = IndexRebuildRegionScanner.ERROR_MESSAGE_MISSING_INDEX_ROW;
                String actualErrorMsg = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                assertTrue(expectedErrorMsg.equals(actualErrorMsg));
            } catch(Exception ex) {
                Assert.fail("Fail to parsing the error message from IndexToolOutputTable");
            }

            // VERIFY option should not change the index state.
            Assert.assertEquals(PIndexState.BUILDING, TestUtil.getIndexState(conn, indexTableFullName));

            // Delete the output table for the next test
            IndexToolIT.dropIndexToolTables(conn);
            // Run the index tool to populate the index while verifying rows
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.AFTER);
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);
        }
    }

    @Test
    public void testIndexToolForIncrementalRebuild() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        List<String> expectedStatus = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, CODE VARCHAR) "+tableDDLOptions);
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (CODE)", indexTableName, dataTableFullName));

            conn.createStatement().execute("upsert into " + dataTableFullName + " values (1, 'Phoenix', 'A')");
            conn.createStatement().execute("upsert into " + dataTableFullName + " values (2, 'Phoenix1', 'B')");
            conn.commit();

            IndexTool it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.AFTER);
            Long scn = it.getJob().getConfiguration().getLong(CURRENT_SCN_VALUE, 1L);

            expectedStatus.add(RUN_STATUS_EXECUTED);
            expectedStatus.add(RUN_STATUS_EXECUTED);
            expectedStatus.add(RUN_STATUS_EXECUTED);

            verifyRunStatusFromResultTable(conn, scn, indexTableFullName, 3, expectedStatus);

            deleteOneRowFromResultTable(conn, scn, indexTableFullName);

            it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.AFTER, "-rv", Long.toString(scn));
            scn = it.getJob().getConfiguration().getLong(CURRENT_SCN_VALUE, 1L);

            expectedStatus.set(0, RUN_STATUS_EXECUTED);
            expectedStatus.set(1, RUN_STATUS_SKIPPED);
            expectedStatus.set(2, RUN_STATUS_SKIPPED);

            verifyRunStatusFromResultTable(conn, scn, indexTableFullName, 5, expectedStatus);

            conn.createStatement().execute( "DELETE FROM "+indexTableFullName);
            conn.commit();
            TestUtil.doMajorCompaction(conn, indexTableFullName);

            expectedStatus.set(0, RUN_STATUS_SKIPPED);
            expectedStatus.set(1, RUN_STATUS_SKIPPED);
            expectedStatus.set(2, RUN_STATUS_SKIPPED);

            it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.AFTER, "-rv", Long.toString(scn));
            scn = it.getJob().getConfiguration().getLong(CURRENT_SCN_VALUE, 1L);
            verifyRunStatusFromResultTable(conn, scn, indexTableFullName, 8, expectedStatus);

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + indexTableFullName);
            Assert.assertFalse(rs.next());

            //testing the dependent method
            Assert.assertFalse(it.isValidLastVerifyTime(10L));
            Assert.assertFalse(it.isValidLastVerifyTime(EnvironmentEdgeManager.currentTimeMillis() - 1000L));
            Assert.assertTrue(it.isValidLastVerifyTime(scn));
        }
    }

    @Test
    public void testDisableOutputLogging() throws Exception {
        if (!mutable || useSnapshot) {
            return;
        }

        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String stmString1 =
                "CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                    + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            // insert two rows
            IndexToolIT.upsertRow(stmt1, 1);
            IndexToolIT.upsertRow(stmt1, 2);
            conn.commit();

            //create ASYNC
            String stmtString2 =
                String.format(
                    "CREATE INDEX %s ON %s (LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz') ASYNC ",
                    indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);
            conn.commit();

            // run the index MR job as ONLY so the index doesn't get rebuilt. Should be 2 missing
            //rows. We pass in --disable-logging BEFORE to silence the output logging to
            // PHOENIX_INDEX_TOOL, since ONLY logs BEFORE the (non-existent in this case)
            // rebuild
            assertDisableLogging(conn, 0, IndexTool.IndexVerifyType.ONLY,
                IndexTool.IndexDisableLoggingType.BEFORE, null, schemaName, dataTableName, indexTableName,
                indexTableFullName, 0);

            //now check that disabling logging AFTER leaves only the BEFORE logs on a BOTH run
            assertDisableLogging(conn, 2, IndexTool.IndexVerifyType.BOTH,
                IndexTool.IndexDisableLoggingType.AFTER,
                IndexVerificationOutputRepository.PHASE_BEFORE_VALUE, schemaName,
                dataTableName, indexTableName,
                indexTableFullName, 0);

            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
            deleteAllRows(conn, TableName.valueOf(indexTableFullName));

            //now check that disabling logging BEFORE creates only the AFTER logs on a BOTH run
            //the index tool run fails validation at the end because we suppressed the BEFORE logs
            //which prevented the rebuild from working properly, but that's ok for this test.
            assertDisableLogging(conn, 2, IndexTool.IndexVerifyType.BOTH,
                IndexTool.IndexDisableLoggingType.BEFORE,
                IndexVerificationOutputRepository.PHASE_AFTER_VALUE, schemaName,
                dataTableName, indexTableName,
                indexTableFullName, -1);

            deleteAllRows(conn, TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
            deleteAllRows(conn, TableName.valueOf(indexTableFullName));

            //now check that disabling logging BOTH creates no logs on a BOTH run
            assertDisableLogging(conn, 0, IndexTool.IndexVerifyType.BOTH,
                IndexTool.IndexDisableLoggingType.BOTH,
                IndexVerificationOutputRepository.PHASE_AFTER_VALUE, schemaName,
                dataTableName, indexTableName,
                indexTableFullName, -1);

        }
    }

    public void deleteAllRows(Connection conn, TableName tableName) throws SQLException,
        IOException {
        Scan scan = new Scan();
        HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().
            getAdmin();
        HConnection hbaseConn = admin.getConnection();
        HTableInterface table = hbaseConn.getTable(tableName);
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result r : scanner) {
                Delete del = new Delete(r.getRow());
                table.delete(del);
            }
        }
    }

    private void assertDisableLogging(Connection conn, int expectedRows,
                                      IndexTool.IndexVerifyType verifyType,
                                      IndexTool.IndexDisableLoggingType disableLoggingType,
                                      byte[] expectedPhase,
                                      String schemaName, String dataTableName,
                                      String indexTableName, String indexTableFullName,
                                      int expectedStatus) throws Exception {
        IndexTool tool = IndexToolIT.runIndexTool(true, false, schemaName, dataTableName,
            indexTableName,
            null,
            expectedStatus, verifyType,  "-et",
            Long.toString(EnvironmentEdgeManager.currentTimeMillis()),"-dl", disableLoggingType.toString());
        assertNotNull(tool);
        assertNotNull(tool.getEndTime());
        byte[] indexTableFullNameBytes = Bytes.toBytes(indexTableFullName);

        IndexVerificationOutputRepository outputRepository =
            new IndexVerificationOutputRepository(indexTableFullNameBytes, conn);
        List<IndexVerificationOutputRow> rows =
            outputRepository.getOutputRows(tool.getEndTime(),
                indexTableFullNameBytes);
        assertEquals(expectedRows, rows.size());
        if (expectedRows > 0) {
            assertArrayEquals(expectedPhase, rows.get(0).getPhaseValue());
        }
        TestUtil.dumpTable(conn,
            TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
    }

    private void deleteOneRowFromResultTable(Connection conn,  Long scn, String indexTable)
            throws SQLException, IOException {
        Table hIndexToolTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(RESULT_TABLE_NAME_BYTES);
        Scan s = new Scan();
        s.setRowPrefixFilter(Bytes.toBytes(String.format("%s%s%s", scn, ROW_KEY_SEPARATOR, indexTable)));
        ResultScanner rs = hIndexToolTable.getScanner(s);
        hIndexToolTable.delete(new Delete(rs.next().getRow()));
    }

    private List<String> verifyRunStatusFromResultTable(Connection conn, Long scn, String indexTable, int totalRows, List<String> expectedStatus) throws SQLException, IOException {
        Table hIndexToolTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(RESULT_TABLE_NAME_BYTES);
        Assert.assertEquals(totalRows, TestUtil.getRowCount(hIndexToolTable, false));
        List<String> output = new ArrayList<>();
        Scan s = new Scan();
        s.setRowPrefixFilter(Bytes.toBytes(String.format("%s%s%s", scn, ROW_KEY_SEPARATOR, indexTable)));
        ResultScanner rs = hIndexToolTable.getScanner(s);
        int count =0;
        for(Result r : rs) {
            Assert.assertTrue(r != null);
            List<Cell> cells = r.getColumnCells(RESULT_TABLE_COLUMN_FAMILY, INDEX_TOOL_RUN_STATUS_BYTES);
            Assert.assertEquals(cells.size(), 1);
            Assert.assertTrue(Bytes.toString(cells.get(0).getRow()).startsWith(String.valueOf(scn)));
            output.add(Bytes.toString(cells.get(0).getValue()));
            count++;
        }
        //for each region
        Assert.assertEquals(3, count);
        for(int i=0; i< count; i++) {
            Assert.assertEquals(expectedStatus.get(i), output.get(i));
        }
        return output;
    }

}
