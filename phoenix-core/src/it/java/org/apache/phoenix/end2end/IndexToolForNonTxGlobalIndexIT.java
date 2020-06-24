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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfoUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
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
import org.apache.phoenix.util.ManualEnvironmentEdge;
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
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.INDEX_TOOL_RUN_STATUS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RESULT_TABLE_COLUMN_FAMILY;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RESULT_TABLE_NAME;
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
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_OLD_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT;
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
        serverProps.put(ScanInfoUtil.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Long.toString(3600));
        serverProps.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
            Long.toString(Long.MAX_VALUE));
        serverProps.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
            Long.toString(Long.MAX_VALUE));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        clientProps.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        destroyDriver();
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
        //IndexToolIT.runIndexTool pulls from the minicluster's config directly
        getUtility().getConfiguration().set(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER, "1");
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
        EnvironmentEdgeManager.reset();
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
        if (!mutable) {
            return; //nothing in this test is mutable specific, so no need to run twice
        }
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
            conn.commit();

            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            // Insert two rows
            IndexToolIT.upsertRow(stmt1, 1);
            IndexToolIT.upsertRow(stmt1, 2);
            conn.commit();

            String stmtString2 =
                    String.format(
                            "CREATE INDEX %s ON %s  (LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz') ", indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);
            conn.commit();
            String qIndexTableName = SchemaUtil.getQualifiedTableName(schemaName, indexTableName);

            // Verify that the index table is in the ACTIVE state
            assertEquals(PIndexState.ACTIVE, TestUtil.getIndexState(conn, qIndexTableName));

            ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();
            Admin admin = queryServices.getAdmin();
            TableName tn = TableName.valueOf(Bytes.toBytes(dataTableFullName));
            HTableDescriptor td =
                admin.getTableDescriptor(tn);
            //add the fast fail coproc and make sure it goes first
            td.addCoprocessor(FastFailRegionObserver.class.getName(), null, 1, null);
            admin.modifyTable(tn, td);
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
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_OLD_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT).getValue());
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
            IndexRebuildRegionScanner.setIgnoreIndexRebuildForTesting(true);
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
            IndexRebuildRegionScanner.setIgnoreIndexRebuildForTesting(false);
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
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
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

            deleteAllRows(conn, TableName.valueOf(indexTableFullName));

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
    public void testIndexToolForIncrementalVerify() throws Exception {
        ManualEnvironmentEdge customEdge = new ManualEnvironmentEdge();
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String viewName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        String indexTableName = generateUniqueName();
        String viewIndexName = generateUniqueName();
        long waitForUpsert = 2;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("CREATE TABLE "+dataTableFullName+" "
                    + "(key1 BIGINT NOT NULL, key2 BIGINT NOT NULL, val1 VARCHAR, val2 BIGINT, "
                    + "val3 BIGINT, val4 DOUBLE, val5 BIGINT, val6 VARCHAR "
                    + "CONSTRAINT my_pk PRIMARY KEY(key1, key2)) "+tableDDLOptions);
            conn.createStatement().execute("CREATE VIEW "+viewFullName+" AS SELECT * FROM "+dataTableFullName);

            conn.createStatement().execute(String.format(
                    "CREATE INDEX "+viewIndexName+" ON "+viewFullName+" (val3) INCLUDE(val5)"));

            conn.createStatement().execute(String.format(
                    "CREATE INDEX "+indexTableName+" ON "+dataTableFullName+" (val3) INCLUDE(val5)"));

            customEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(customEdge);
            long t0 = customEdge.currentTime();
            customEdge.incrementValue(waitForUpsert);
            conn.createStatement().execute("UPSERT INTO "+viewFullName+"(key1, key2, val1, val2) VALUES (4,5,'abc',3)");
            customEdge.incrementValue(waitForUpsert);
            long t1 = customEdge.currentTime();
            customEdge.incrementValue(waitForUpsert);
            conn.createStatement().execute("UPSERT INTO "+viewFullName+"(key1, key2, val1, val2) VALUES (1,2,'abc',3)");
            customEdge.incrementValue(waitForUpsert);
            long t2 = customEdge.currentTime();
            customEdge.incrementValue(waitForUpsert);
            conn.createStatement().execute("UPSERT INTO "+viewFullName+"(key1, key2, val3, val4) VALUES (1,2,4,1.2)");
            customEdge.incrementValue(waitForUpsert);
            long t3 = customEdge.currentTime();
            customEdge.incrementValue(waitForUpsert);
            conn.createStatement().execute("UPSERT INTO "+viewFullName+"(key1, key2, val5, val6) VALUES (1,2,5,'def')");
            customEdge.incrementValue(waitForUpsert);
            long t4 = customEdge.currentTime();
            customEdge.incrementValue(waitForUpsert);
            conn.createStatement().execute("DELETE FROM "+viewFullName+" WHERE key1=4");
            customEdge.incrementValue(waitForUpsert);
            long t5 = customEdge.currentTime();
            customEdge.incrementValue(10);
            long t6 = customEdge.currentTime();
            IndexTool it;
            if(!mutable) {
                // job with 2 rows
                it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                        null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t0),"-et", String.valueOf(t2));
                verifyCounters(it, 2, 2);
                //increment time between rebuilds so that PHOENIX_INDEX_TOOL and
                // PHOENIX_INDEX_TOOL_RESULT tables get unique keys for each run
                customEdge.incrementValue(waitForUpsert);

                // only one row
                it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                        null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t1),"-et", String.valueOf(t2));
                verifyCounters(it, 1, 1);
                customEdge.incrementValue(waitForUpsert);
                // no rows
                it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                        null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t5),"-et", String.valueOf(t6));
                verifyCounters(it, 0, 0);
                customEdge.incrementValue(waitForUpsert);
                //view index
                // job with 2 rows
                it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName, viewIndexName,
                        null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t0),"-et", String.valueOf(t2));
                verifyCounters(it, 2, 2);
                customEdge.incrementValue(waitForUpsert);
                // only one row
                it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName, viewIndexName,
                        null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t1),"-et", String.valueOf(t2));
                verifyCounters(it, 1, 1);
                customEdge.incrementValue(waitForUpsert);
                // no rows
                it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName, viewIndexName,
                        null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t5),"-et", String.valueOf(t6));
                verifyCounters(it, 0, 0);
                customEdge.incrementValue(waitForUpsert);
                return;
            }
            // regular job without delete row
            it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t0),"-et", String.valueOf(t4));
            verifyCounters(it, 2, 3);
            customEdge.incrementValue(waitForUpsert);

            // job with 2 rows
            it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t0),"-et", String.valueOf(t2));
            verifyCounters(it, 2, 2);
            customEdge.incrementValue(waitForUpsert);

            // job with update on only one row
            it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t1),"-et", String.valueOf(t3));
            verifyCounters(it, 1, 2);
            customEdge.incrementValue(waitForUpsert);

            // job with update on only one row
            it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t2),"-et", String.valueOf(t4));
            verifyCounters(it, 1, 2);
            customEdge.incrementValue(waitForUpsert);

            // job with update on only one row
            it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t4),"-et", String.valueOf(t5));
            verifyCounters(it, 1, 1);
            customEdge.incrementValue(waitForUpsert);

            // job with no new updates on any row
            it = IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t5),"-et", String.valueOf(t6));
            verifyCounters(it, 0, 0);
            customEdge.incrementValue(waitForUpsert);
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testIndexToolForIncrementalVerify_viewIndex() throws Exception {
        ManualEnvironmentEdge customeEdge = new ManualEnvironmentEdge();
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String viewName = generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String viewIndexName = generateUniqueName();
        long waitForUpsert = 2;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName + " "
                    + "(key1 BIGINT NOT NULL, key2 BIGINT NOT NULL, val1 VARCHAR, val2 BIGINT, "
                    + "val3 BIGINT, val4 DOUBLE, val5 BIGINT, val6 VARCHAR "
                    + "CONSTRAINT my_pk PRIMARY KEY(key1, key2)) COLUMN_ENCODED_BYTES=0");
            conn.createStatement().execute(
                    "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName + " WHERE val6 = 'def'");
            conn.createStatement().execute(String.format(
                    "CREATE INDEX " + viewIndexName + " ON " + viewFullName
                            + " (val3) INCLUDE(val5)"));
            customeEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(customeEdge);

            long t1 = customeEdge.currentTime();
            customeEdge.incrementValue(waitForUpsert);
            conn.createStatement()
                    .execute("UPSERT INTO " + viewFullName + " VALUES (5,6,'abc',8,4,1.3,6,'def')");
            customeEdge.incrementValue(waitForUpsert);
            long t2 = customeEdge.currentTime();
            customeEdge.incrementValue(waitForUpsert);
            conn.createStatement()
                    .execute("UPSERT INTO " + viewFullName + " VALUES (1,2,'abc',3,4,1.2,5,'def')");
            customeEdge.incrementValue(waitForUpsert);
            long t3 = customeEdge.currentTime();
            customeEdge.incrementValue(waitForUpsert);
            conn.createStatement().execute("DELETE FROM " + viewFullName + " WHERE key1=5");
            customeEdge.incrementValue(waitForUpsert);
            long t4 = customeEdge.currentTime();
            customeEdge.incrementValue(10);
            long t5 = customeEdge.currentTime();
            IndexTool it;
            // regular job with delete row
            it =
                    IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName,
                            viewIndexName, null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t1),
                            "-et", String.valueOf(t4));
            verifyCounters(it, 2, 2);

            // job with 1 row
            it =
                    IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName,
                            viewIndexName, null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t1),
                            "-et", String.valueOf(t2));
            verifyCounters(it, 1, 1);

            // job with update on only one row
            it =
                    IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName,
                            viewIndexName, null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t2),
                            "-et", String.valueOf(t3));
            verifyCounters(it, 1, 1);

            // job with update on 2 rows
            it =
                    IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName,
                            viewIndexName, null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t1),
                            "-et", String.valueOf(t3));
            verifyCounters(it, 2, 2);
/*
            // job with update on only one row
            it =
                    IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName,
                            viewIndexName, null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t3),
                            "-et", String.valueOf(t4));
            verifyCounters(it, 1, 1);
*/
            // job with no new updates on any row
            it =
                    IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, viewName,
                            viewIndexName, null, 0, IndexTool.IndexVerifyType.ONLY, "-st", String.valueOf(t4),
                            "-et", String.valueOf(t5));
            verifyCounters(it, 0, 0);
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    private void verifyCounters(IndexTool it, int scanned, int valid) throws IOException {
        assertEquals(scanned, it.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
        assertEquals(valid, it.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(BEFORE_REBUILD_OLD_INDEX_ROW_COUNT).getValue());
        assertEquals(0, it.getJob().getCounters().findCounter(BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT).getValue());
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
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
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

            // disabling logging AFTER on an AFTER run should leave no output rows
            assertDisableLogging(conn, 0, IndexTool.IndexVerifyType.AFTER,
                IndexTool.IndexDisableLoggingType.AFTER, null, schemaName, dataTableName,
                indexTableName,
                indexTableFullName, 0);

            //disabling logging BEFORE on a BEFORE run should leave no output rows
            assertDisableLogging(conn, 0, IndexTool.IndexVerifyType.BEFORE,
                IndexTool.IndexDisableLoggingType.BEFORE, null, schemaName, dataTableName, indexTableName,
                indexTableFullName, 0);
            //now clear out all the rebuilt index rows
            deleteAllRows(conn, TableName.valueOf(indexTableFullName));

            //now check that disabling logging AFTER leaves only the BEFORE logs on a BOTH run
            assertDisableLogging(conn, 2, IndexTool.IndexVerifyType.BOTH,
                IndexTool.IndexDisableLoggingType.AFTER,
                IndexVerificationOutputRepository.PHASE_BEFORE_VALUE, schemaName,
                dataTableName, indexTableName,
                indexTableFullName, -1);

            //clear out both the output table and the index
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
            deleteAllRows(conn, TableName.valueOf(indexTableFullName));

            //now check that disabling logging BEFORE creates only the AFTER logs on a BOTH run
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
                IndexVerificationOutputRepository.PHASE_BEFORE_VALUE, schemaName,
                dataTableName, indexTableName,
                indexTableFullName, -1);

        }
    }

    @Test
    public void testUpdatablePKFilterViewIndexRebuild() throws Exception {
        if (!mutable) {
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String view1Name = generateUniqueName();
        String view1FullName = SchemaUtil.getTableName(schemaName, view1Name);
        String view2Name = generateUniqueName();
        String view2FullName = SchemaUtil.getTableName(schemaName, view2Name);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // Create Table and Views. Note the view is on a non leading PK data table column
            String createTable =
                    "CREATE TABLE IF NOT EXISTS " + dataTableFullName + " (\n"
                            + "    ORGANIZATION_ID VARCHAR NOT NULL,\n"
                            + "    KEY_PREFIX CHAR(3) NOT NULL,\n" + "    CREATED_BY VARCHAR,\n"
                            + "    CONSTRAINT PK PRIMARY KEY (\n" + "        ORGANIZATION_ID,\n"
                            + "        KEY_PREFIX\n" + "    )\n"
                            + ") VERSIONS=1, COLUMN_ENCODED_BYTES=0";
            conn.createStatement().execute(createTable);
            String createView1 =
                    "CREATE VIEW IF NOT EXISTS " + view1FullName + " (\n"
                            + " VIEW_COLA VARCHAR NOT NULL,\n"
                            + " VIEW_COLB CHAR(1) CONSTRAINT PKVIEW PRIMARY KEY (\n"
                            + " VIEW_COLA\n" + " )) AS \n" + " SELECT * FROM " + dataTableFullName
                            + " WHERE KEY_PREFIX = 'aaa'";
            conn.createStatement().execute(createView1);
            String createView2 =
                    "CREATE VIEW IF NOT EXISTS " + view2FullName + " (\n"
                            + " VIEW_COL1 VARCHAR NOT NULL,\n"
                            + " VIEW_COL2 CHAR(1) CONSTRAINT PKVIEW PRIMARY KEY (\n"
                            + " VIEW_COL1\n" + " )) AS \n" + " SELECT * FROM " + dataTableFullName
                            + " WHERE KEY_PREFIX = 'ccc'";
            conn.createStatement().execute(createView2);

            // We want to verify if deletes and set null result in expected rebuild of view index
            conn.createStatement().execute("UPSERT INTO " + view1FullName
                    + "(ORGANIZATION_ID, VIEW_COLA, VIEW_COLB) VALUES('ORG1', 'A', 'G')");
            conn.createStatement().execute("UPSERT INTO " + view1FullName
                    + "(ORGANIZATION_ID, VIEW_COLA, VIEW_COLB) VALUES('ORG1', 'C', 'I')");
            conn.createStatement().execute("UPSERT INTO " + view1FullName
                    + "(ORGANIZATION_ID, VIEW_COLA, VIEW_COLB) VALUES('ORG1', 'D', 'J')");

            conn.createStatement().execute("UPSERT INTO " + view2FullName
                    + "(ORGANIZATION_ID, VIEW_COL1, VIEW_COL2) VALUES('ORG2', 'B', 'H')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + view1FullName
                    + " WHERE ORGANIZATION_ID = 'ORG1' AND VIEW_COLA = 'C'");
            conn.createStatement().execute("UPSERT INTO " + view1FullName
                    + "(ORGANIZATION_ID, VIEW_COLA, VIEW_COLB) VALUES('ORG1', 'D', NULL)");
            conn.commit();

            String createViewIndex =
                    "CREATE INDEX IF NOT EXISTS " + indexTableName + " ON " + view1FullName
                            + " (VIEW_COLB) ASYNC";
            conn.createStatement().execute(createViewIndex);
            conn.commit();
            // Rebuild using index tool
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, view1Name, indexTableName);
            ResultSet rs =
                    conn.createStatement()
                            .executeQuery("SELECT COUNT(*) FROM " + indexTableFullName);
            rs.next();
            assertEquals(2, rs.getInt(1));

            Pair<Integer, Integer> putsAndDeletes =
                    countPutsAndDeletes("_IDX_" + dataTableFullName);
            assertEquals(4, (int) putsAndDeletes.getFirst());
            assertEquals(2, (int) putsAndDeletes.getSecond());
        }
    }

    @Test
    public void testUpdatableNonPkFilterViewIndexRebuild() throws Exception {
        if (!mutable) {
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String view1Name = generateUniqueName();
        String view1FullName = SchemaUtil.getTableName(schemaName, view1Name);
        String view2Name = generateUniqueName();
        String view2FullName = SchemaUtil.getTableName(schemaName, view2Name);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // Create Table and Views. Note the view is on a non PK data table column
            String createTable =
                    "CREATE TABLE IF NOT EXISTS " + dataTableFullName + " (\n"
                            + "    ORGANIZATION_ID VARCHAR NOT NULL,\n"
                            + "    KEY_PREFIX CHAR(3) NOT NULL,\n" + "    CREATED_BY VARCHAR,\n"
                            + "    CONSTRAINT PK PRIMARY KEY (\n" + "        ORGANIZATION_ID,\n"
                            + "        KEY_PREFIX\n" + "    )\n"
                            + ") VERSIONS=1, COLUMN_ENCODED_BYTES=0";
            conn.createStatement().execute(createTable);
            String createView1 =
                    "CREATE VIEW IF NOT EXISTS " + view1FullName + " (\n"
                            + " VIEW_COLA VARCHAR NOT NULL,\n"
                            + " VIEW_COLB CHAR(1) CONSTRAINT PKVIEW PRIMARY KEY (\n"
                            + " VIEW_COLA\n" + " )) AS \n" + " SELECT * FROM " + dataTableFullName
                            + " WHERE CREATED_BY = 'foo'";
            conn.createStatement().execute(createView1);
            String createView2 =
                    "CREATE VIEW IF NOT EXISTS " + view2FullName + " (\n"
                            + " VIEW_COL1 VARCHAR NOT NULL,\n"
                            + " VIEW_COL2 CHAR(1) CONSTRAINT PKVIEW PRIMARY KEY (\n"
                            + " VIEW_COL1\n" + " )) AS \n" + " SELECT * FROM " + dataTableFullName
                            + " WHERE CREATED_BY = 'bar'";
            conn.createStatement().execute(createView2);

            // We want to verify if deletes and set null result in expected rebuild of view index
            conn.createStatement().execute("UPSERT INTO " + view1FullName
                    + "(ORGANIZATION_ID, KEY_PREFIX, VIEW_COLA, VIEW_COLB) VALUES('ORG1', 'aaa', 'A', 'G')");
            conn.createStatement().execute("UPSERT INTO " + view1FullName
                    + "(ORGANIZATION_ID, KEY_PREFIX, VIEW_COLA, VIEW_COLB) VALUES('ORG1', 'ccc', 'C', 'I')");
            conn.createStatement().execute("UPSERT INTO " + view1FullName
                    + "(ORGANIZATION_ID, KEY_PREFIX, VIEW_COLA, VIEW_COLB) VALUES('ORG1', 'ddd', 'D', 'J')");

            conn.createStatement().execute("UPSERT INTO " + view2FullName
                    + "(ORGANIZATION_ID, KEY_PREFIX, VIEW_COL1, VIEW_COL2) VALUES('ORG2', 'bbb', 'B', 'H')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + view1FullName
                    + " WHERE ORGANIZATION_ID = 'ORG1' AND VIEW_COLA = 'C'");
            conn.createStatement().execute("UPSERT INTO " + view1FullName
                    + "(ORGANIZATION_ID, KEY_PREFIX, VIEW_COLA, VIEW_COLB) VALUES('ORG1', 'ddd', 'D', NULL)");
            conn.commit();

            String createViewIndex =
                    "CREATE INDEX IF NOT EXISTS " + indexTableName + " ON " + view1FullName
                            + " (VIEW_COLB) ASYNC";
            conn.createStatement().execute(createViewIndex);
            conn.commit();
            // Rebuild using index tool
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, view1Name, indexTableName);
            ResultSet rs =
                    conn.createStatement()
                            .executeQuery("SELECT COUNT(*) FROM " + indexTableFullName);
            rs.next();
            assertEquals(2, rs.getInt(1));

            Pair<Integer, Integer> putsAndDeletes =
                    countPutsAndDeletes("_IDX_" + dataTableFullName);
            assertEquals(4, (int) putsAndDeletes.getFirst());
            assertEquals(2, (int) putsAndDeletes.getSecond());
        }
    }

    private Pair<Integer, Integer> countPutsAndDeletes(String tableName) throws Exception {
        int numPuts = 0;
        int numDeletes = 0;
        try (org.apache.hadoop.hbase.client.Connection hcon =
                ConnectionFactory.createConnection(config)) {
            Table htable = hcon.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setRaw(true);
            ResultScanner scanner = htable.getScanner(scan);

            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                for (Cell cell : result.rawCells()) {
                    if (KeyValue.Type.codeToType(cell.getTypeByte()) == KeyValue.Type.Put) {
                        numPuts++;
                    } else if (KeyValue.Type
                            .codeToType(cell.getTypeByte()) == KeyValue.Type.DeleteFamily) {
                                numDeletes++;
                            }
                }
            }
        }
        return new Pair<Integer, Integer>(numPuts, numDeletes);
    }

    public void deleteAllRows(Connection conn, TableName tableName) throws SQLException,
        IOException, InterruptedException {
        Scan scan = new Scan();
        HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().
            getAdmin();
        HConnection hbaseConn = admin.getConnection();
        HTableInterface table = hbaseConn.getTable(tableName);
        boolean deletedRows = false;
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result r : scanner) {
                Delete del = new Delete(r.getRow());
                table.delete(del);
                deletedRows = true;
            }
        } catch (Exception e) {
            //if the table doesn't exist, we have no rows to delete. Easier to catch
            //than to pre-check for existence
        }
        //don't flush/compact if we didn't write anything, because we'll hang forever
        if (deletedRows) {
            getUtility().getHBaseAdmin().flush(tableName);
            TestUtil.majorCompact(getUtility(), tableName);
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
            expectedStatus, verifyType, "-dl", disableLoggingType.toString());
        assertNotNull(tool);
        byte[] indexTableFullNameBytes = Bytes.toBytes(indexTableFullName);

        IndexVerificationOutputRepository outputRepository =
            new IndexVerificationOutputRepository(indexTableFullNameBytes, conn);
        List<IndexVerificationOutputRow> rows =
            outputRepository.getAllOutputRows();
        try {
            assertEquals(expectedRows, rows.size());
        } catch (AssertionError e) {
            TestUtil.dumpTable(conn, TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
            throw e;
        }
        if (expectedRows > 0) {
            assertArrayEquals(expectedPhase, rows.get(0).getPhaseValue());
        }
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

    public static class FastFailRegionObserver extends BaseRegionObserver {
        @Override
        public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
                                        final Scan scan,
                                               final RegionScanner s) throws IOException {
            throw new DoNotRetryIOException("I'm just a coproc that's designed to fail fast");
        }
    }
}
